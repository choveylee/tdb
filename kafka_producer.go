package tdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/choveylee/tlog"
)

var (
	newAsyncProducer = sarama.NewAsyncProducer
	newSyncProducer  = sarama.NewSyncProducer
)

type kafkaSenderLifecycle struct {
	mu        sync.Mutex
	sends     sync.WaitGroup
	closeOnce sync.Once
	closing   chan struct{}
	done      chan struct{}
	closeErr  error
}

// KafkaAsyncSender queues JSON messages for a fixed Kafka topic.
type KafkaAsyncSender struct {
	producer sarama.AsyncProducer
	topic    string

	lifecycle kafkaSenderLifecycle
}

// AsyncSenderSuccessCallback receives the encoded payload and its encoding error.
type AsyncSenderSuccessCallback func([]byte, error)

// AsyncSenderErrorCallback receives an asynchronous delivery error.
type AsyncSenderErrorCallback func(error)

// NewKafkaAsyncSender constructs an async producer without delivery callbacks.
func NewKafkaAsyncSender(ctx context.Context, addresses []string, config *sarama.Config, topic string) (*KafkaAsyncSender, error) {
	return newKafkaAsyncSender(ctx, addresses, config, topic, false, nil, nil)
}

// NewKafkaAsyncSenderWithCallback starts listeners for successful and failed deliveries.
// A callback must return before shutdown can finish. Call Close from outside the callbacks.
func NewKafkaAsyncSenderWithCallback(ctx context.Context, addresses []string, config *sarama.Config, topic string, successCallback AsyncSenderSuccessCallback, errorCallback AsyncSenderErrorCallback) (*KafkaAsyncSender, error) {
	return newKafkaAsyncSender(ctx, addresses, config, topic, true, successCallback, errorCallback)
}

// Send checks cancellation before encoding and while waiting for space in the producer.
// A successful return means the message was queued.
func (sender *KafkaAsyncSender) Send(ctx context.Context, key string, data any) error {
	if sender == nil || sender.producer == nil {
		return errors.New("tdb: Kafka sender must be initialized")
	}

	lifecycle := &sender.lifecycle
	message, err := lifecycle.newMessage(ctx, sender.topic, key, data)
	if err != nil {
		return err
	}

	if err := lifecycle.admit(ctx); err != nil {
		return err
	}

	defer lifecycle.sends.Done()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-lifecycle.closing:
		return errors.New("tdb: Kafka sender is closing")
	case sender.producer.Input() <- message:
		return nil
	}
}

// Close stops queue admission and starts draining the producer. ctx limits the wait;
// another Close can wait for work already in progress.
func (sender *KafkaAsyncSender) Close(ctx context.Context) error {
	if sender == nil || sender.producer == nil {
		return nil
	}

	return sender.lifecycle.close(ctx, sender.producer.AsyncClose)
}

// KafkaSyncSender publishes JSON messages synchronously to a fixed Kafka topic.
type KafkaSyncSender struct {
	producer sarama.SyncProducer
	topic    string

	lifecycle kafkaSenderLifecycle
}

// NewKafkaSyncSender constructs a sync producer for the supplied brokers and topic.
func NewKafkaSyncSender(ctx context.Context, addresses []string, config *sarama.Config, topic string) (*KafkaSyncSender, error) {
	if err := checkKafkaContext(ctx); err != nil {
		return nil, err
	}

	if topic == "" {
		return nil, errors.New("tdb: Kafka topic must not be empty")
	}

	producerConfig := sarama.NewConfig()
	if config != nil {
		*producerConfig = *config
	}

	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	producer, err := newSyncProducer(addresses, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("tdb: create Kafka sync producer: %w", err)
	}

	sender := &KafkaSyncSender{
		producer: producer,
		topic:    topic,
		lifecycle: kafkaSenderLifecycle{
			closing: make(chan struct{}),
			done:    make(chan struct{}),
		},
	}

	return sender, nil
}

// Send checks cancellation before dispatch. Once dispatched, Sarama's broker
// timeouts and retry settings govern the synchronous call.
func (sender *KafkaSyncSender) Send(ctx context.Context, key string, data any) (int32, int64, error) {
	if sender == nil || sender.producer == nil {
		return -1, -1, errors.New("tdb: Kafka sender must be initialized")
	}

	lifecycle := &sender.lifecycle
	message, err := lifecycle.newMessage(ctx, sender.topic, key, data)
	if err != nil {
		return -1, -1, err
	}

	if err := lifecycle.admit(ctx); err != nil {
		return -1, -1, err
	}

	defer lifecycle.sends.Done()

	partition, offset, err := sender.producer.SendMessage(message)
	if err != nil {
		return partition, offset, fmt.Errorf("tdb: send Kafka message: %w", err)
	}

	return partition, offset, nil
}

// Close starts shutdown after admitted sends return. ctx limits how long the caller waits.
func (sender *KafkaSyncSender) Close(ctx context.Context) error {
	if sender == nil || sender.producer == nil {
		return nil
	}

	return sender.lifecycle.close(ctx, func() {
		sender.lifecycle.closeErr = sender.producer.Close()
		close(sender.lifecycle.done)
	})
}

// newKafkaAsyncSender owns the result channels so Sarama Close cannot compete
// with callbacks. Both listeners drain until the producer closes the channels.
func newKafkaAsyncSender(ctx context.Context, addresses []string, config *sarama.Config, topic string, withCallbacks bool, successCallback AsyncSenderSuccessCallback, errorCallback AsyncSenderErrorCallback) (*KafkaAsyncSender, error) {
	if err := checkKafkaContext(ctx); err != nil {
		return nil, err
	}

	if topic == "" {
		return nil, errors.New("tdb: Kafka topic must not be empty")
	}

	producerConfig := sarama.NewConfig()
	if config != nil {
		*producerConfig = *config
	}

	producerConfig.Producer.Return.Successes = withCallbacks
	producerConfig.Producer.Return.Errors = withCallbacks

	producer, err := newAsyncProducer(addresses, producerConfig)
	if err != nil {
		return nil, fmt.Errorf("tdb: create Kafka async producer: %w", err)
	}

	sender := &KafkaAsyncSender{
		producer: producer,
		topic:    topic,
		lifecycle: kafkaSenderLifecycle{
			closing: make(chan struct{}),
			done:    make(chan struct{}),
		},
	}

	go func() {
		defer close(sender.lifecycle.done)

		var listeners sync.WaitGroup
		listeners.Go(func() {
			for message := range producer.Successes() {
				if successCallback != nil {
					runKafkaCallback(ctx, topic, func() {
						payload, err := message.Value.Encode()
						successCallback(payload, err)
					})
				}
			}
		})

		for deliveryErr := range producer.Errors() {
			if errorCallback != nil {
				runKafkaCallback(ctx, topic, func() {
					errorCallback(deliveryErr)
				})
			}
		}

		listeners.Wait()
	}()

	return sender, nil
}

// admit enrolls a send before shutdown can begin waiting for in-flight sends.
// The admission lock covers only the check and counter update.
func (lifecycle *kafkaSenderLifecycle) admit(ctx context.Context) error {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-lifecycle.closing:
		return errors.New("tdb: Kafka sender is closing")
	default:
		lifecycle.sends.Add(1)
	}

	return nil
}

// close stops admission, then waits for admitted sends before shutting down the
// producer. The supplied context bounds only the caller's wait.
func (lifecycle *kafkaSenderLifecycle) close(ctx context.Context, shutdown func()) error {
	if ctx == nil {
		return errors.New("tdb: context must not be nil")
	}

	lifecycle.closeOnce.Do(func() {
		lifecycle.mu.Lock()
		close(lifecycle.closing)
		lifecycle.mu.Unlock()

		go func() {
			lifecycle.sends.Wait()
			shutdown()
		}()
	})

	if err := waitKafkaClose(ctx, lifecycle.done); err != nil {
		return err
	}

	return lifecycle.closeErr
}

// newMessage avoids encoding when the caller has canceled or shutdown has begun.
// Admission is checked again after encoding.
func (lifecycle *kafkaSenderLifecycle) newMessage(ctx context.Context, topic, key string, data any) (*sarama.ProducerMessage, error) {
	if err := checkKafkaContext(ctx); err != nil {
		return nil, err
	}

	select {
	case <-lifecycle.closing:
		return nil, errors.New("tdb: Kafka sender is closing")
	default:
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("tdb: encode Kafka payload: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payload),
	}

	return message, nil
}

func checkKafkaContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("tdb: context must not be nil")
	}

	return ctx.Err()
}

// waitKafkaClose returns an already completed shutdown result even if ctx has
// expired. Otherwise it waits for shutdown or cancellation.
func waitKafkaClose(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// runKafkaCallback isolates a callback panic so the result channel keeps draining.
func runKafkaCallback(ctx context.Context, topic string, callback func()) {
	defer func() {
		if recover() != nil {
			tlog.E(ctx).Msgf("Recover Kafka callback panic (topic=%q)", topic)
		}
	}()

	callback()
}
