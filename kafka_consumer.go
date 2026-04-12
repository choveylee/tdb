package tdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/choveylee/tlog"
)

// PermanentError wraps a handler failure that must not be retried; it participates in errors.Is matching.
type PermanentError struct {
	err error
}

// Error implements error and prefixes the message with a permanent-error marker.
func (p *PermanentError) Error() string {
	return "(permanent error)" + p.err.Error()
}

// Is supports errors.Is by detecting *PermanentError in the error chain.
func (p *PermanentError) Is(err error) bool {
	var permanentError *PermanentError

	ok := errors.As(err, &permanentError)

	return ok
}

// NewPermanentError wraps err as a *PermanentError.
func NewPermanentError(err error) *PermanentError {
	return &PermanentError{err: err}
}

// ConsumeFunc processes a single Kafka message; nil means success.
type ConsumeFunc func(context.Context, *sarama.ConsumerMessage) error

// KafkaReceiver runs a Sarama consumer group with exponential backoff on reconnect and per-message retry.
type KafkaReceiver struct {
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup

	topic string

	consumeFunc ConsumeFunc

	connectBackOff backoff.BackOff
	consumeBackOff backoff.BackOff

	ready chan struct{}

	wg sync.WaitGroup
}

// ReceiverHandler is a simplified handler that receives only the message value; [NewKafkaReceiver] adapts it to [ConsumeFunc].
type ReceiverHandler func(context.Context, []byte) error

// NewKafkaReceiver builds a consumer group client with separate backoff policies for Consume loops and message handling. Call [KafkaReceiver.Start] to begin consumption.
func NewKafkaReceiver(ctx context.Context, addresses []string, config *sarama.Config, groupId string, topic string, handler ReceiverHandler) (*KafkaReceiver, error) {
	client, err := sarama.NewClient(addresses, config)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupId, client)
	if err != nil {
		_ = client.Close()

		return nil, err
	}

	connectBackOff := backoff.NewExponentialBackOff()
	connectBackOff.RandomizationFactor = 0
	connectBackOff.MaxElapsedTime = 0
	connectBackOff.MaxInterval = 30 * time.Second

	consumeBackOff := backoff.NewExponentialBackOff()
	consumeBackOff.RandomizationFactor = 0
	consumeBackOff.MaxElapsedTime = 0
	consumeBackOff.MaxInterval = 30 * time.Second

	receiver := &KafkaReceiver{
		client:        client,
		consumerGroup: consumerGroup,

		topic: topic,

		consumeFunc: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return handler(ctx, msg.Value)
		},

		connectBackOff: connectBackOff,
		consumeBackOff: consumeBackOff,

		ready: make(chan struct{}),
	}

	return receiver, nil
}

// Start launches the Consume loop in a goroutine. It blocks until the first rebalance completes (Setup closes ready), then returns nil while consumption continues until ctx is done or the group closes.
func (p *KafkaReceiver) Start(ctx context.Context) error {
	handler := sarama.ConsumerGroupHandler(p)

	// handler = otelsarama.WrapConsumerGroupHandler(r)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		retry := func() error {
			err := p.consumerGroup.Consume(ctx, []string{p.topic}, handler)
			// Normal exit when the consumer group was closed explicitly.
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}

			if ctx.Err() != nil {
				return nil
			}

			// Transient broker errors: log and let backoff retry the Consume call.
			if err != nil {
				tlog.E(ctx).Err(err).Msgf("kafka consumer group stop (%s) err (%v).",
					p.topic, err)

				return err
			}

			// Rebalance: reset the ready channel and force a retry of Consume.
			p.ready = make(chan struct{})

			return fmt.Errorf("rebalance")
		}

		err := backoff.Retry(retry, p.connectBackOff)
		if err != nil {
			return
		}

		tlog.I(ctx).Msgf("kafka consumer group (%s) closed or stop.", p.topic)
	}()

	<-p.ready
	return nil
}

// Close closes the consumer group and waits for the background Consume goroutine to finish.
func (p *KafkaReceiver) Close(ctx context.Context) error {
	err := p.consumerGroup.Close()
	if err != nil {
		return err
	}

	p.wg.Wait()

	return nil
}

// Setup closes the ready channel so [KafkaReceiver.Start] can unblock after the first session assignment.
func (p *KafkaReceiver) Setup(sarama.ConsumerGroupSession) error {
	close(p.ready)

	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler; no extra teardown is required.
func (p *KafkaReceiver) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler. It retries transient handler errors with backoff, commits offsets after successful handling or when the error is a [PermanentError], and exits when the claim or session ends.
func (p *KafkaReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	p.consumeBackOff.Reset()

	ctx := context.Background()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				tlog.I(ctx).Msg("message channel closed.")

				return nil
			}

			retry := func() (err error) {
				startTime := time.Now()

				event := tlog.I(ctx)

				defer func() {
					if err != nil {
						event = tlog.E(ctx).Err(err)
					}

					latency := time.Since(startTime)

					event.Detailf("latency:%d", latency).
						Detailf("topic:%s", msg.Topic).
						Detailf("partition:%d", msg.Partition).
						Detailf("offset:%d", msg.Offset).
						Detailf("message:%s", string(msg.Value)).
						Msgf("consume log")
				}()

				defer func() {
					e := recover()
					if e != nil {
						err = fmt.Errorf("panic: %v", e)
					}
				}()

				// nil: success; *PermanentError: failure without retry; any other error: backoff and retry.
				err = p.consumeFunc(ctx, msg)
				if errors.Is(err, &PermanentError{}) {
					return nil
				}

				return err
			}

			// Backoff respects session.Context(); cancellation exits the retry loop.
			err := backoff.Retry(retry, backoff.WithContext(p.consumeBackOff, session.Context()))
			if err != nil {
				return err
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
