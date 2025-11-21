package transaction

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"

	"github.com/bytedance/sonic"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type KafkaSmartTxWriter struct {
	mq *kafka.Writer
	tl *zap.Logger

	topic string
}

func NewKafkaSmartTxWriter(mq *kafka.Writer, tl *zap.Logger, topic string) writer.BatchWriter[model.WalletTransaction] {
	return &KafkaSmartTxWriter{mq: mq, tl: tl, topic: topic}
}

func (w *KafkaSmartTxWriter) BWrite(ctx context.Context, transactions []model.WalletTransaction) error {
	if len(transactions) == 0 {
		return nil
	}

	msgs := make([]kafka.Message, 0, len(transactions))
	for _, tx := range transactions {
		msgs = append(msgs, w.marshalToMsg(tx))
	}

	newCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// 重试机制
	var err error
	for attempt := 0; attempt < RETRY_COUNT; attempt++ {
		err = w.mq.WriteMessages(newCtx, msgs...)
		if err == nil {
			break // 成功则退出重试
		}
	}
	if err != nil {
		w.tl.Warn("❌ MQ write failed, exceeded the maximum number of retries", zap.Error(err))
		return err
	}
	return nil
}

func (w *KafkaSmartTxWriter) Close() error {
	return nil
}

func (w *KafkaSmartTxWriter) marshalToMsg(tx model.WalletTransaction) kafka.Message {
	event := model.NewSmartTxEvent(tx)
	jsonData, _ := sonic.Marshal(event)
	return kafka.Message{
		Topic: w.topic,
		Key:   []byte(tx.TokenAddress),
		Value: jsonData,
	}
}
