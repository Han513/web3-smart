package consumer

import (
	"context"
	"hash/crc32"
	"strconv"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/handler"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/monitor"
	"web3-smart/internal/worker/repository"

	"github.com/bytedance/sonic"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type BalanceConsumer struct {
	*Consumer
	id             string
	workerSize     int
	buffers        []chan model.BlockBalance
	balanceHandler *handler.BalanceHandler
	repo           repository.Repository
}

func NewBalanceConsumer(conf config.Config, logger *zap.Logger, repo repository.Repository) *BalanceConsumer {
	workerSize := conf.Worker.WorkerNum
	buffers := make([]chan model.BlockBalance, workerSize)
	for i := range workerSize {
		buffers[i] = make(chan model.BlockBalance, 200)
	}

	return &BalanceConsumer{
		id:             "balance_consumer",
		workerSize:     conf.Worker.WorkerNum,
		Consumer:       NewConsumer(conf.Kafka, logger, conf.Kafka.TopicBalance),
		balanceHandler: handler.NewBalanceHandler(conf, logger, repo),
		buffers:        buffers,
		repo:           repo,
	}
}

func (bc *BalanceConsumer) Run(ctx context.Context) {
	for i := 0; i < bc.workerSize; i++ {
		idx := i
		go func() {
			workerID := strconv.Itoa(idx)
			for {
				select {
				case balanceEvent := <-bc.buffers[idx]:
					startTime := time.Now()
					bc.logger.Debug("✅ Process balance", zap.String("consumerID", bc.id), zap.Any("balance", balanceEvent))
					bc.balanceHandler.HandleBalance(ctx, balanceEvent)
					elapsed := time.Since(startTime).Seconds()
					monitor.KafkaWorkerMessagesProcessed.WithLabelValues(workerID).Inc()
					monitor.KafkaWorkerProcessDuration.WithLabelValues(workerID).Observe(elapsed)
				case <-ctx.Done():
					return
				}
			}

		}()
	}
	time.Sleep(time.Second * 5) // 等待前面的组件准备完成
	bc.Consumer.Start(ctx, bc)
}

func (bc *BalanceConsumer) HandleMessage(msg kafka.Message) {
	monitor.KafkaMessagesReceived.WithLabelValues("balance").Inc()
	var balance model.BlockBalance
	if err := sonic.Unmarshal(msg.Value, &balance); err != nil {
		bc.logger.Warn("❌ JSON Parse Error", zap.String("consumerID", bc.id), zap.Error(err), zap.String("raw", string(msg.Value)))
		return
	}
	bc.dispatch(balance)
}

func (bc *BalanceConsumer) ID() string {
	return bc.id
}

func (bc *BalanceConsumer) Stop() error {
	// 先停止 Kafka 消费
	if err := bc.Consumer.Stop(); err != nil {
		return err
	}

	// 关闭所有 buffer channels
	for i := 0; i < bc.workerSize; i++ {
		close(bc.buffers[i])
	}

	// 停止 trade 处理器
	// bc.tradeHandler.Stop()

	return nil
}

func (bc *BalanceConsumer) dispatch(blockBalanceEvent model.BlockBalance) {
	idx := bc.hashBy(blockBalanceEvent.Hash)
	select {
	case bc.buffers[idx] <- blockBalanceEvent:
		monitor.KafkaWorkerMessagesDispatched.WithLabelValues(strconv.Itoa(int(idx))).Inc()
	default:
		bc.logger.Warn("❌ buffers is full", zap.String("consumerID", bc.id), zap.Any("idx", idx))
	}
}

func (bc *BalanceConsumer) hashBy(key string) uint32 {
	// 后续如果交易都集中在某几个池子里，导致worker负载不一致，修改hash算法
	return crc32.ChecksumIEEE([]byte(key)) % uint32(bc.workerSize)
}
