package tdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/choveylee/tlog"
)

var (
	newSaramaClient            = sarama.NewClient
	newConsumerGroupFromClient = sarama.NewConsumerGroupFromClient
)

// PermanentError wraps a handler failure that should be acknowledged without retry.
type PermanentError struct {
	err error
}

// Error describes the permanent failure.
func (permanent *PermanentError) Error() string {
	if permanent == nil || permanent.err == nil {
		return "permanent error"
	}

	return "permanent error: " + permanent.err.Error()
}

// Unwrap exposes the underlying handler error.
func (permanent *PermanentError) Unwrap() error {
	if permanent == nil {
		return nil
	}

	return permanent.err
}

// Is preserves matching against a PermanentError target.
func (permanent *PermanentError) Is(target error) bool {
	_, ok := target.(*PermanentError)

	return ok
}

// NewPermanentError wraps err so the receiver can acknowledge without retry.
func NewPermanentError(err error) *PermanentError {
	permanent := &PermanentError{err: err}

	return permanent
}

// ConsumeFunc processes a single Kafka message and returns nil on success.
type ConsumeFunc func(context.Context, *sarama.ConsumerMessage) error

// ReceiverHandler receives an independent copy of the message value on each
// attempt and the context of the consumer session.
type ReceiverHandler func(context.Context, []byte) error

// KafkaReceiver owns a Sarama consumer group and one background consume loop at a time.
type KafkaReceiver struct {
	client         sarama.Client
	consumerGroup  sarama.ConsumerGroup
	topic          string
	consumeFunc    ConsumeFunc
	groupErrorDone chan struct{}

	mu        sync.Mutex
	run       *kafkaReceiveRun
	closing   bool
	closeDone chan struct{}
	closeErr  error
}

type kafkaReceiveRun struct {
	cancel    context.CancelFunc
	ready     chan error
	readyOnce sync.Once
	started   atomic.Bool
	done      chan struct{}
}

// NewKafkaReceiver constructs a receiver. Start waits for its first session assignment.
func NewKafkaReceiver(ctx context.Context, addresses []string, config *sarama.Config, groupId string, topic string, handler ReceiverHandler) (*KafkaReceiver, error) {
	if err := checkKafkaContext(ctx); err != nil {
		return nil, err
	}

	if handler == nil {
		return nil, errors.New("tdb: Kafka receiver handler must not be nil")
	}

	if groupId == "" || topic == "" {
		return nil, errors.New("tdb: Kafka group ID and topic must not be empty")
	}

	consumerConfig := sarama.NewConfig()
	if config != nil {
		*consumerConfig = *config
	}

	consumerConfig.Consumer.Return.Errors = true

	client, err := newSaramaClient(addresses, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("tdb: create Kafka client: %w", err)
	}

	consumerGroup, err := newConsumerGroupFromClient(groupId, client)
	if err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("tdb: create Kafka consumer group: %w", err)
	}

	receiver := &KafkaReceiver{
		client:        client,
		consumerGroup: consumerGroup,
		topic:         topic,
		consumeFunc: func(ctx context.Context, message *sarama.ConsumerMessage) error {
			return handler(ctx, bytes.Clone(message.Value))
		},
		groupErrorDone: make(chan struct{}),
		closeDone:      make(chan struct{}),
	}

	go func() {
		defer close(receiver.groupErrorDone)

		for err := range consumerGroup.Errors() {
			event := tlog.E(ctx).Detailf("error type=%T", err)
			var protocolError sarama.KError
			if errors.As(err, &protocolError) {
				event.Detailf("Kafka error code=%d", protocolError)
			}

			event.Msgf("Observe Kafka consumer group error (topic=%q)", topic)
		}
	}()

	return receiver, nil
}

// Start runs a single consume loop and waits for its first session. The context
// also controls the running receiver. A receiver that has stopped can start again.
func (receiver *KafkaReceiver) Start(ctx context.Context) error {
	if err := checkKafkaContext(ctx); err != nil {
		return err
	}

	if receiver == nil || receiver.consumerGroup == nil {
		return errors.New("tdb: Kafka receiver must be initialized")
	}

	receiver.mu.Lock()
	if receiver.closing {
		receiver.mu.Unlock()

		return errors.New("tdb: Kafka receiver is closing")
	}

	if receiver.run != nil {
		receiver.mu.Unlock()

		return errors.New("tdb: Kafka receiver is already running")
	}

	runCtx, cancel := context.WithCancel(ctx)
	run := &kafkaReceiveRun{cancel: cancel, ready: make(chan error, 1), done: make(chan struct{})}

	receiver.run = run
	receiver.mu.Unlock()

	go receiver.consume(runCtx, run)

	select {
	case err := <-run.ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close cancels the active loop and closes the group and client once. The
// caller's context bounds the wait, including when a handler is slow to exit.
func (receiver *KafkaReceiver) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("tdb: context must not be nil")
	}

	if receiver == nil || receiver.closeDone == nil {
		return nil
	}

	receiver.mu.Lock()
	if !receiver.closing {
		receiver.closing = true
		run := receiver.run
		if run != nil {
			run.cancel()
		}

		go func() {
			if run != nil {
				<-run.done
			}

			receiver.closeErr = errors.Join(receiver.consumerGroup.Close(), receiver.client.Close())
			<-receiver.groupErrorDone
			close(receiver.closeDone)
		}()
	}

	receiver.mu.Unlock()

	if err := waitKafkaClose(ctx, receiver.closeDone); err != nil {
		return err
	}

	return receiver.closeErr
}

// Setup signals startup after Sarama assigns the first consumer group session.
func (receiver *KafkaReceiver) Setup(session sarama.ConsumerGroupSession) error {
	receiver.mu.Lock()
	defer receiver.mu.Unlock()

	if receiver.closing || session.Context().Err() != nil {
		return context.Canceled
	}

	if run := receiver.run; run != nil {
		run.started.Store(true)
		run.signalReady(nil)
	}

	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (receiver *KafkaReceiver) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim retries transient failures until the session ends. It marks a
// message after success or a PermanentError while the session is still active.
func (receiver *KafkaReceiver) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok || ctx.Err() != nil {
				return nil
			}

			handle := func() (err error) {
				if err := ctx.Err(); err != nil {
					return backoff.Permanent(err)
				}

				startedAt := time.Now()
				defer func() {
					if recover() != nil {
						err = errors.New("tdb: Kafka receiver handler panicked")
					}

					if err != nil {
						tlog.E(ctx).Msgf("Handle Kafka message error (topic=%q, partition=%d, offset=%d, elapsed=%s, error type=%T)", message.Topic, message.Partition, message.Offset, time.Since(startedAt), err)
					}
				}()

				err = receiver.consumeFunc(ctx, message)
				var permanent *PermanentError
				if errors.As(err, &permanent) {
					tlog.W(ctx).Msgf("Skip retry for permanent Kafka handler error (topic=%q, partition=%d, offset=%d)", message.Topic, message.Partition, message.Offset)

					return nil
				}

				return err
			}

			err := backoff.Retry(handle, backoff.WithContext(newKafkaBackoff(), ctx))
			if ctx.Err() != nil {
				return nil
			}

			if err != nil {
				return err
			}

			session.MarkMessage(message, "")
		}
	}
}

// consume owns one loop and its readiness signal. It clears the run before
// publishing terminal startup errors, so a caller can retry a failed start.
func (receiver *KafkaReceiver) consume(ctx context.Context, run *kafkaReceiveRun) {
	var runErr error
	defer func() {
		run.cancel()
		receiver.mu.Lock()
		receiver.run = nil
		close(run.done)
		receiver.mu.Unlock()

		run.signalReady(runErr)
	}()

	retry := newKafkaBackoff()
	topics := []string{receiver.topic}
	for ctx.Err() == nil {
		err := receiver.consumerGroup.Consume(ctx, topics, receiver)
		if ctx.Err() != nil {
			break
		}

		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			runErr = err

			return
		}

		if !run.started.Load() {
			if err == nil {
				runErr = errors.New("tdb: Kafka consumer group exited before its initial session")
			} else {
				runErr = fmt.Errorf("tdb: start Kafka consumer group: %w", err)
			}

			return
		}

		if err == nil {
			retry.Reset()

			continue
		}

		tlog.E(ctx).Err(err).Msgf("Retry Kafka consumer group (topic=%q)", receiver.topic)

		timer := time.NewTimer(retry.NextBackOff())
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	runErr = ctx.Err()
}

// signalReady publishes the first session or the terminal startup error once,
// including when the Start caller has already stopped waiting.
func (run *kafkaReceiveRun) signalReady(err error) {
	run.readyOnce.Do(func() {
		run.ready <- err
	})
}

// newKafkaBackoff provides a retry policy bounded by the caller's context.
func newKafkaBackoff() backoff.BackOff {
	retry := backoff.NewExponentialBackOff()
	retry.RandomizationFactor = 0
	retry.MaxElapsedTime = 0
	retry.MaxInterval = 30 * time.Second

	return retry
}
