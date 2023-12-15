/**
 * @Author: lidonglin
 * @Description:
 * @File:  kafka.go
 * @Version: 1.0.0
 * @Date: 2023/12/14 10:43
 */

package tdb

import (
	"context"
	"encoding/json"
	"sync"

	// otelsarama "github.com/DataDog/dd-trace-go/contrib/IBM/sarama.v1"
	"github.com/IBM/sarama"
)

type KafkaAsyncSender struct {
	producer sarama.AsyncProducer

	topic string

	wg sync.WaitGroup
}

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

func (p *KafkaAsyncSender) Close(ctx context.Context) error {
	p.producer.AsyncClose()

	p.wg.Wait()

	return nil
}

type KafkaSyncSender struct {
	producer sarama.SyncProducer

	topic string
}

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

func (p *KafkaSyncSender) Close(ctx context.Context) error {
	return p.producer.Close()
}
