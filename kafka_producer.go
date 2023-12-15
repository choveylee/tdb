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
	"github.com/choveylee/tlog"
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

// AsyncSenderSuccessCallback  生产者所需要，异步发消息时候接口回调消息的函数类型
type AsyncSenderSuccessCallback func([]byte, error)

// AsyncSenderErrorCallback 生产者所需要，异步发消息时候接口回调消息的函数类型
type AsyncSenderErrorCallback func(error)

func NewKafkaAsyncSenderWithCallback(ctx context.Context, addrs []string, config *sarama.Config, topic string,
	successCallback AsyncSenderSuccessCallback, errorCallback AsyncSenderErrorCallback) (*KafkaAsyncSender, error) {
	producer, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	sender := &KafkaAsyncSender{
		producer: producer,

		topic: topic,
	}

	// producer = otelsarama.WrapSyncProducer(c.Config, producer)

	if config == nil {
		config = sarama.NewConfig()
	}

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

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
