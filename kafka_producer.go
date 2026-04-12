package tdb

import (
	"context"
	"encoding/json"
	"sync"

	// otelsarama "github.com/DataDog/dd-trace-go/contrib/IBM/sarama.v1"
	"github.com/IBM/sarama"
	"github.com/choveylee/tlog"
)

// KafkaAsyncSender publishes JSON-encoded values to a fixed topic using a Sarama async producer.
type KafkaAsyncSender struct {
	producer sarama.AsyncProducer

	topic string

	wg sync.WaitGroup
}

// NewKafkaAsyncSender constructs an async producer without subscribing to success or error channels.
func NewKafkaAsyncSender(ctx context.Context, addrs []string, config *sarama.Config, topic string) (*KafkaAsyncSender, error) {
	producer, err := sarama.NewAsyncProducer(addrs, config)
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

// AsyncSenderSuccessCallback is invoked on each successful produce; arguments are the encoded payload and any Encode error from the value.
type AsyncSenderSuccessCallback func([]byte, error)

// AsyncSenderErrorCallback is invoked when the producer reports an error on the Errors channel.
type AsyncSenderErrorCallback func(error)

// NewKafkaAsyncSenderWithCallback builds an async producer, enables Return.Successes and Return.Errors, and starts background listeners that invoke the callbacks.
func NewKafkaAsyncSenderWithCallback(ctx context.Context, addrs []string, config *sarama.Config, topic string,
	successCallback AsyncSenderSuccessCallback, errorCallback AsyncSenderErrorCallback) (*KafkaAsyncSender, error) {
	cfg := config

	if cfg == nil {
		cfg = sarama.NewConfig()
	}

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(addrs, cfg)
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
					tlog.I(context.Background()).Msgf("success events listener for topic %s stops: kafka producer is closed", topic)

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
					tlog.I(context.Background()).Msgf("error events listener for topic %s stops: kafka producer is closed", topic)

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

// Send marshals data to JSON and enqueues a producer message with the given key on the async input channel.
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

// Close shuts down the async producer and waits for callback goroutines started by WithCallback.
func (p *KafkaAsyncSender) Close(ctx context.Context) error {
	p.producer.AsyncClose()

	p.wg.Wait()

	return nil
}

// KafkaSyncSender publishes JSON-encoded values to a fixed topic using a Sarama sync producer.
type KafkaSyncSender struct {
	producer sarama.SyncProducer

	topic string
}

// NewKafkaSyncSender constructs a sync producer for the given brokers and topic.
func NewKafkaSyncSender(ctx context.Context, addrs []string, config *sarama.Config, topic string) (*KafkaSyncSender, error) {
	producer, err := sarama.NewSyncProducer(addrs, config)
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

// Send marshals data to JSON and produces synchronously, returning the partition and offset.
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
