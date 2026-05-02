package tdb

import (
	"context"
	"encoding/json"
	"sync"

	// otelsarama "github.com/DataDog/dd-trace-go/contrib/IBM/sarama.v1"
	"github.com/IBM/sarama"
	"github.com/choveylee/tlog"
)

var (
	newAsyncProducer = sarama.NewAsyncProducer
	newSyncProducer  = sarama.NewSyncProducer
)

func cloneSaramaConfig(config *sarama.Config) *sarama.Config {
	if config == nil {
		return sarama.NewConfig()
	}

	cfg := *config

	return &cfg
}

// KafkaAsyncSender publishes JSON-encoded messages to a fixed Kafka topic through a Sarama async producer.
type KafkaAsyncSender struct {
	producer sarama.AsyncProducer

	topic string

	wg sync.WaitGroup
}

// NewKafkaAsyncSender constructs an async producer without exposing success or error channels.
// It disables those result channels on the internal Sarama config so callers are not required to drain them.
func NewKafkaAsyncSender(ctx context.Context, addrs []string, config *sarama.Config, topic string) (*KafkaAsyncSender, error) {
	cfg := cloneSaramaConfig(config)
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = false

	producer, err := newAsyncProducer(addrs, cfg)
	if err != nil {
		return nil, err
	}

	sender := &KafkaAsyncSender{
		producer: producer,

		topic: topic,
	}

	// producer = otelsarama.WrapSyncProducer(c.Config, producer)

	return sender, nil
}

// AsyncSenderSuccessCallback is invoked for each successful produce; the arguments are the encoded payload and any Encode error returned by the value encoder.
type AsyncSenderSuccessCallback func([]byte, error)

// AsyncSenderErrorCallback receives delivery errors emitted on the producer Errors channel.
type AsyncSenderErrorCallback func(error)

// NewKafkaAsyncSenderWithCallback constructs an async producer, enables Return.Successes and Return.Errors, and starts background listeners that invoke the supplied callbacks.
func NewKafkaAsyncSenderWithCallback(ctx context.Context, addrs []string, config *sarama.Config, topic string,
	successCallback AsyncSenderSuccessCallback, errorCallback AsyncSenderErrorCallback) (*KafkaAsyncSender, error) {
	cfg := cloneSaramaConfig(config)
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	producer, err := newAsyncProducer(addrs, cfg)
	if err != nil {
		return nil, err
	}

	sender := &KafkaAsyncSender{
		producer: producer,

		topic: topic,
	}

	// producer = otelsarama.WrapSyncProducer(c.Config, producer)

	sender.wg.Add(1)
	go func() {
		defer sender.wg.Done()
		for {
			select {
			case msg, ok := <-producer.Successes():
				if !ok {
					tlog.I(context.Background()).Msgf("Kafka producer success listener for topic %q stopped because the producer was closed.", topic)

					return
				}

				data, err := msg.Value.Encode()

				if successCallback != nil {
					successCallback(data, err)
				}
			}
		}
	}()

	sender.wg.Add(1)
	go func() {
		defer sender.wg.Done()
		for {
			select {
			case err, ok := <-producer.Errors():
				if !ok {
					tlog.I(context.Background()).Msgf("Kafka producer error listener for topic %q stopped because the producer was closed.", topic)

					return
				}

				if errorCallback != nil {
					errorCallback(err)
				}
			}
		}
	}()

	return sender, nil
}

// Send marshals data to JSON and enqueues a producer message with the supplied key on the async input channel.
func (p *KafkaAsyncSender) Send(ctx context.Context, key string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,

		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(bytes),
	}

	// injectToProducerMessage(ctx, msg)

	p.producer.Input() <- msg

	return nil
}

// Close flushes in-flight messages, shuts down the async producer, and waits for any callback goroutines started by [NewKafkaAsyncSenderWithCallback].
func (p *KafkaAsyncSender) Close(ctx context.Context) error {
	err := p.producer.Close()

	p.wg.Wait()

	return err
}

// KafkaSyncSender publishes JSON-encoded messages to a fixed Kafka topic through a Sarama sync producer.
type KafkaSyncSender struct {
	producer sarama.SyncProducer

	topic string
}

// NewKafkaSyncSender constructs a sync producer for the specified brokers and topic.
func NewKafkaSyncSender(ctx context.Context, addrs []string, config *sarama.Config, topic string) (*KafkaSyncSender, error) {
	producer, err := newSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	sender := &KafkaSyncSender{
		producer: producer,

		topic: topic,
	}

	// producer = otelsarama.WrapSyncProducer(c.Config, producer)

	return sender, nil
}

// Send marshals data to JSON and produces the message synchronously, returning the partition and offset reported by Kafka.
func (p *KafkaSyncSender) Send(ctx context.Context, key string, data interface{}) (int32, int64, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return -1, -1, err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,

		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(bytes),
	}

	// injectToProducerMessage(ctx, msg)

	partition, offset, err := p.producer.SendMessage(msg)

	return partition, offset, err
}

// Close closes the underlying sync producer.
func (p *KafkaSyncSender) Close(ctx context.Context) error {
	return p.producer.Close()
}
