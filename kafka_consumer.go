/**
 * @Author: lidonglin
 * @Description:
 * @File:  kafka_receiver.go
 * @Version: 1.0.0
 * @Date: 2023/12/14 13:28
 */

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

type PermanentError struct {
	err error
}

func (p *PermanentError) Error() string {
	return "(permanent error)" + p.err.Error()
}

func (p *PermanentError) Is(err error) bool {
	var permanentError *PermanentError

	ok := errors.As(err, &permanentError)

	return ok
}

func NewPermanentError(err error) *PermanentError {
	return &PermanentError{err: err}
}

type ConsumeFunc func(context.Context, *sarama.ConsumerMessage) error

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

type ReceiverHandler func(context.Context, []byte) error

func NewKafkaReceiver(ctx context.Context, addrs []string, config *sarama.Config, groupId string, topicId string, handler ReceiverHandler) (*KafkaReceiver, error) {
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupId, client)
	if err != nil {
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

		topic: topicId,

		connectBackOff: connectBackOff,
		consumeBackOff: consumeBackOff,

		ready: make(chan struct{}),
	}

	return receiver, nil
}

func (p *KafkaReceiver) Start(ctx context.Context) error {
	handler := sarama.ConsumerGroupHandler(p)

	// handler = otelsarama.WrapConsumerGroupHandler(r)

	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		retry := func() error {
			err := p.consumerGroup.Consume(ctx, []string{p.topic}, handler)
			// 主动调用了Stop接口，消费组已关闭，退出消费逻辑
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return nil
			}

			// context cancel
			if ctx.Err() != nil {
				return nil
			}

			// 消费组运行其他异常的错误处理（例如与kafka broker断开连接），则尝试重新启动消费
			if err != nil {
				tlog.E(ctx).Err(err).Msgf("kafka consumer group stop (%s) err (%v).",
					p.topic, err)

				return err
			}

			// consumer rebalanced 处理逻辑，返回error后尝试重新启动消费
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

func (p *KafkaReceiver) Close(ctx context.Context) error {
	err := p.consumerGroup.Close()
	if err != nil {
		return err
	}

	p.wg.Wait()

	return nil
}

func (p *KafkaReceiver) Setup(sarama.ConsumerGroupSession) error {
	close(p.ready)

	return nil
}

func (p *KafkaReceiver) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

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
					if e := recover(); e != nil {
						err = fmt.Errorf("panic: %v", e)
					}
				}()

				// consumeFunc返回值的处理逻辑：
				// * 若返回nil，则代表处理成功，退出重试循环，标注消费完成。
				// * 若返回PermanentError，则代表消费失败，且无需重试，退出重试循环，标注消费完成。
				// * 若返回其他error，则代表消费失败，并进入重试逻辑。
				err = p.consumeFunc(ctx, msg)
				if errors.Is(err, &PermanentError{}) {
					return nil
				}

				return err
			}

			// 重试过程中，检测到当前session已结束，则立即退出
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
