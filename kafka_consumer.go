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

var (
	newSaramaClient = sarama.NewClient

	newConsumerGroupFromClient = sarama.NewConsumerGroupFromClient
)

func newKafkaBackOff() backoff.BackOff {
	retryBackOff := backoff.NewExponentialBackOff()

	retryBackOff.RandomizationFactor = 0
	retryBackOff.MaxElapsedTime = 0
	retryBackOff.MaxInterval = 30 * time.Second

	return retryBackOff
}

// PermanentError wraps a handler failure that must not be retried; it participates in errors.Is matching.
type PermanentError struct {
	err error
}

// Error implements error and prefixes the message with a stable permanent-error marker.
func (p *PermanentError) Error() string {
	if p == nil || p.err == nil {
		return "permanent error"
	}

	return "permanent error: " + p.err.Error()
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

	newConnectBackOff func() backoff.BackOff
	newConsumeBackOff func() backoff.BackOff

	startMu sync.Mutex
	startCh chan error
	started bool

	wg sync.WaitGroup
}

// ReceiverHandler is a simplified handler that receives only the message value; [NewKafkaReceiver] adapts it to [ConsumeFunc].
type ReceiverHandler func(context.Context, []byte) error

// NewKafkaReceiver constructs a Kafka consumer group receiver with independent backoff policies for connection retries and message handling retries.
// Call [KafkaReceiver.Start] to begin consumption.
func NewKafkaReceiver(ctx context.Context, addresses []string, config *sarama.Config, groupId string, topic string, handler ReceiverHandler) (*KafkaReceiver, error) {
	client, err := newSaramaClient(addresses, config)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := newConsumerGroupFromClient(groupId, client)
	if err != nil {
		_ = client.Close()

		return nil, err
	}

	receiver := &KafkaReceiver{
		client:        client,
		consumerGroup: consumerGroup,

		topic: topic,

		consumeFunc: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			return handler(ctx, msg.Value)
		},

		newConnectBackOff: newKafkaBackOff,
		newConsumeBackOff: newKafkaBackOff,
	}

	return receiver, nil
}

func (p *KafkaReceiver) resetStartSignal() <-chan error {
	startCh := make(chan error, 1)

	p.startMu.Lock()

	p.startCh = startCh
	p.started = false

	p.startMu.Unlock()

	return startCh
}

func (p *KafkaReceiver) signalStart(err error) {
	p.startMu.Lock()

	startCh := p.startCh
	if startCh != nil {
		p.startCh = nil
	}

	p.startMu.Unlock()

	if startCh == nil {
		return
	}

	startCh <- err
	close(startCh)
}

func (p *KafkaReceiver) markStarted() {
	p.startMu.Lock()

	if p.started {
		p.startMu.Unlock()

		return
	}

	p.started = true
	startCh := p.startCh
	if startCh != nil {
		p.startCh = nil
	}

	p.startMu.Unlock()

	if startCh == nil {
		return
	}

	startCh <- nil
	close(startCh)
}

func (p *KafkaReceiver) hasStarted() bool {
	p.startMu.Lock()
	defer p.startMu.Unlock()

	return p.started
}

func (p *KafkaReceiver) connectBackOff() backoff.BackOff {
	if p.newConnectBackOff != nil {
		return p.newConnectBackOff()
	}

	return newKafkaBackOff()
}

func (p *KafkaReceiver) consumeBackOff() backoff.BackOff {
	if p.newConsumeBackOff != nil {
		return p.newConsumeBackOff()
	}

	return newKafkaBackOff()
}

// Start launches the Consume loop in a goroutine.
// It blocks until the first consumer-group session is established, or returns an error if startup fails or the context is canceled before [KafkaReceiver.Setup] runs.
func (p *KafkaReceiver) Start(ctx context.Context) error {
	handler := sarama.ConsumerGroupHandler(p)

	startCh := p.resetStartSignal()
	connectBackOff := p.connectBackOff()

	// handler = otelsarama.WrapConsumerGroupHandler(r)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		for {
			err := p.consumerGroup.Consume(ctx, []string{p.topic}, handler)

			switch {
			case errors.Is(err, sarama.ErrClosedConsumerGroup):
				p.signalStart(nil)

				tlog.I(ctx).Msgf("Kafka consumer group for topic %q has stopped.", p.topic)

				return
			case ctx.Err() != nil:
				p.signalStart(ctx.Err())

				return
			case err != nil:
				if !p.hasStarted() {
					p.signalStart(err)

					return
				}

				tlog.E(ctx).Err(err).Msgf("Kafka consumer group for topic %q encountered an error and will retry.",
					p.topic)

				waitDuration := connectBackOff.NextBackOff()
				if waitDuration == backoff.Stop {
					p.signalStart(err)

					return
				}

				timer := time.NewTimer(waitDuration)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}

					p.signalStart(ctx.Err())

					return
				case <-timer.C:
				}
			default:
				if !p.hasStarted() {
					p.signalStart(fmt.Errorf("consumer group for topic %q exited before initial setup completed", p.topic))

					return
				}

				connectBackOff.Reset()
			}
		}
	}()

	select {
	case err := <-startCh:
		return err
	case <-ctx.Done():
		p.signalStart(ctx.Err())

		return ctx.Err()
	}
}

// Close closes the consumer group, closes the underlying Kafka client, and waits for the background Consume goroutine to finish.
func (p *KafkaReceiver) Close(ctx context.Context) error {
	var err error

	if p.consumerGroup != nil {
		err = errors.Join(err, p.consumerGroup.Close())
	}

	if p.client != nil {
		err = errors.Join(err, p.client.Close())
	}

	p.wg.Wait()

	return err
}

// Setup marks the receiver as started so [KafkaReceiver.Start] can unblock after the first session assignment.
func (p *KafkaReceiver) Setup(sarama.ConsumerGroupSession) error {
	p.markStarted()

	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler; no extra teardown is required.
func (p *KafkaReceiver) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.
// It retries transient handler errors with backoff, commits offsets after successful handling or when the error is a [PermanentError], and exits when the claim or session ends.
func (p *KafkaReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				tlog.I(ctx).Msg("Kafka claim message channel has been closed.")

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
						Msg("Kafka message processing attempt completed.")
				}()

				defer func() {
					e := recover()
					if e != nil {
						err = fmt.Errorf("recovered from message handler panic: %v", e)
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
			err := backoff.Retry(retry, backoff.WithContext(p.consumeBackOff(), ctx))
			if err != nil {
				return err
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
