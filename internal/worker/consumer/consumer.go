package consumer

import (
	"context"
	"errors"
	"strings"
	"time"
	"web3-smart/internal/worker/config"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// KafkaConsumer 接口
type KafkaConsumer interface {
	Run(ctx context.Context)
	Stop() error
	ID() string
}

// MessageHandler 解耦消息处理逻辑
type MessageHandler interface {
	HandleMessage(msg kafka.Message)
}

// Consumer 结构体
type Consumer struct {
	logger      *zap.Logger
	kafkaReader *kafka.Reader
	limiter     *rate.Limiter
}

// NewConsumer 创建一个新的通用 Consumer 实例
func NewConsumer(conf config.KafkaConfig, logger *zap.Logger, topic string) *Consumer {
	reader := newKafkaReader(conf, topic)
	// 创建一个速率限制器，每秒补充3000个令牌，桶大小为3000
	limiter := rate.NewLimiter(rate.Limit(3000), 3000)
	return &Consumer{
		logger:      logger,
		kafkaReader: reader,
		limiter:     limiter,
	}
}

// Start 启动消费者主循环
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) {
	go c.run(ctx, handler)
}

// 主消费逻辑
func (c *Consumer) run(ctx context.Context, handler MessageHandler) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Warn("closing Kafka consumer...")
			_ = c.kafkaReader.Close()
			return
		default:
		}

		// 等待令牌可用，实现速率限制
		err := c.limiter.Wait(ctx)

		ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := c.kafkaReader.ReadMessage(ctxWithTimeout)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				c.logger.Warn("⌛ Kafka running...")
			} else {
				c.logger.Warn("❌ Kafka Read Error", zap.Error(err))
			}
			continue
		}

		handler.HandleMessage(msg)
	}
}

// Stop 停止消费者
func (c *Consumer) Stop() error {
	return c.kafkaReader.Close()
}

// 创建 Kafka Reader
func newKafkaReader(conf config.KafkaConfig, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:                strings.Split(conf.Brokers, ","),
		Topic:                  topic,
		GroupID:                conf.GroupID,
		StartOffset:            kafka.LastOffset,
		CommitInterval:         5 * time.Second,
		QueueCapacity:          2000,                   // 限制队列容量
		MinBytes:               1024,                   // 最小读取字节数
		MaxBytes:               10e6,                   // 最大读取字节数(1MB)
		ReadBatchTimeout:       500 * time.Millisecond, // 读取超时
		PartitionWatchInterval: 5 * time.Second,        // 分区监控间隔
	})
}
