package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strconv"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/handler"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/monitor"
	"web3-smart/internal/worker/repository"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type TradeConsumer struct {
	*Consumer                            // 组合通用 Consumer
	id           string                  // 消费者ID
	workerSize   int                     // 消费者组大小
	buffers      []chan model.TradeEvent // 消息队列
	tradeHandler *handler.TradeHandler   // trade处理器
	repo         repository.Repository
}

// NewTradeConsumer 创建 TradeConsumer 实例
func NewTradeConsumer(conf config.Config, logger *zap.Logger, repo repository.Repository) *TradeConsumer {
	// 初始化id
	newConsumer := NewConsumer(conf.Kafka, logger, conf.Kafka.TopicTrade)

	// 初始化buffers
	workerSize := conf.Worker.WorkerNum
	buffers := make([]chan model.TradeEvent, workerSize)
	for i := 0; i < workerSize; i++ {
		buffers[i] = make(chan model.TradeEvent, 2000)
	}

	return &TradeConsumer{
		id:           "trade_consumer",
		workerSize:   workerSize,
		Consumer:     newConsumer,
		buffers:      buffers,
		tradeHandler: handler.NewTradeHandler(conf, logger, repo),
		repo:         repo,
	}
}

// Run 启动trade消费者
func (tc *TradeConsumer) Run(ctx context.Context) {
	// 处理trade数据
	for i := 0; i < tc.workerSize; i++ {
		idx := i
		go func() {
			workerID := strconv.Itoa(idx)
			for {
				select {
				case trade, ok := <-tc.buffers[i]:
					if !ok {
						tc.logger.Warn("❌ buffer is closed", zap.String("consumerID", tc.id), zap.Any("idx", i))
						return
					}
					startTime := time.Now()
					tc.logger.Debug("✅ Process trade", zap.String("consumerID", tc.id), zap.Any("trade", trade))
					tc.tradeHandler.HandleTrade(trade)

					// 统计消息处理次数与耗时
					elapsed := time.Since(startTime).Seconds()
					monitor.KafkaWorkerMessagesProcessed.WithLabelValues(workerID).Inc()
					monitor.KafkaWorkerProcessDuration.WithLabelValues(workerID).Observe(elapsed)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// 启动消费者
	time.Sleep(time.Second * 5) // 等待前面的组件准备完成
	tc.Consumer.Start(ctx, tc)
}

// HandleMessage 实现 MessageHandler 接口
func (tc *TradeConsumer) HandleMessage(msg kafka.Message) {
	monitor.KafkaMessagesReceived.WithLabelValues("trade").Inc()

	var trade model.TradeEvent
	if err := json.Unmarshal(msg.Value, &trade); err != nil {
		tc.logger.Warn("❌ JSON Parse Error", zap.String("consumerID", tc.id), zap.Error(err), zap.String("raw", string(msg.Value)))
		return
	}

	// 过滤掉24h前的交易数据
	tradeTime := time.Unix(trade.Event.Time, 0)
	if time.Since(tradeTime) > 24*time.Hour {
		return
	}

	// 过滤掉交易量过小的trade
	if trade.Event.VolumeUsd < 0.01 {
		return
	}

	// 过滤掉非TradeEvent
	if trade.Type != model.TRADE_EVENT_TYPE {
		return
	}

	tc.dispatch(trade)
}

func (tc *TradeConsumer) ID() string {
	return tc.id
}

// Stop 停止trade消费者
func (tc *TradeConsumer) Stop() error {
	// 先停止 Kafka 消费
	if err := tc.Consumer.Stop(); err != nil {
		return err
	}

	// 关闭所有 buffer channels
	for i := 0; i < tc.workerSize; i++ {
		close(tc.buffers[i])
	}

	// 停止 trade 处理器
	tc.tradeHandler.Stop()

	return nil
}

// dispatch 按 chain:wallet:token 分组处理
func (tc *TradeConsumer) dispatch(trade model.TradeEvent) {
	idx := tc.hashBy(fmt.Sprintf("%s:%s:%s", trade.Event.Network, trade.Event.Address, trade.Event.TokenAddress))

	// 检测 buffer 是否接近满载，触发短暂休眠
	if len(tc.buffers[idx]) > cap(tc.buffers[idx])*8/10 {
		time.Sleep(100 * time.Millisecond)
	}

	// 发送到通道
	select {
	case tc.buffers[idx] <- trade:
		monitor.KafkaWorkerMessagesDispatched.WithLabelValues(strconv.Itoa(int(idx))).Inc()
	default:
		tc.logger.Warn("❌ buffers is full", zap.String("consumerID", tc.id), zap.Any("idx", idx))
	}
}

func (tc *TradeConsumer) hashBy(key string) uint32 {
	// 后续如果交易都集中在某几个池子里，导致worker负载不一致，修改hash算法
	return crc32.ChecksumIEEE([]byte(key)) % uint32(tc.workerSize)
}
