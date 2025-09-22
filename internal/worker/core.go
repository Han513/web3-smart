package worker

import (
	"context"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/consumer"
	"web3-smart/internal/worker/job"
	"web3-smart/internal/worker/monitor"
	"web3-smart/internal/worker/repository"

	"go.uber.org/zap"
)

type Core struct {
	cfg       config.Config
	tl        *zap.Logger
	repo      repository.Repository
	scheduler *job.Scheduler
	consumers []consumer.KafkaConsumer
	metrics   *monitor.MetricsServer // 新增
}

func New(cfg config.Config, logger *zap.Logger) *Core {
	// 初始化作业调度器
	scheduler := job.NewScheduler(logger)

	// 初始化repo
	repo := repository.New(cfg, logger)

	// 加载历史数据
	cacheLoad := job.NewCacheLoad(cfg, repo, logger)
	scheduler.RegisterOnceJob("cache_load", cacheLoad.Run)

	// 定時：聰明錢資料聚合更新（每 10 分鐘）
	analyzer := job.NewSmartMoneyAnalyzer(repo, logger)
	analyzer.Cfg = cfg
	scheduler.RegisterJob("smart_money_analyze", 10*time.Minute, analyzer.Run)

	// 定時：錢包聰明錢分類（每 30 分鐘）
	classifier := job.NewSmartWalletClassifier(repo, logger)
	scheduler.RegisterJob("smart_wallet_classifier", 30*time.Minute, classifier.Run)

	// 定時：清理 30 天前舊交易（每天）
	cleanup := job.NewCleanupJob(repo, logger)
	scheduler.RegisterJob("cleanup_old_transactions", 24*time.Hour, cleanup.Run)

	// 初始化消费者
	// consumers := []consumer.KafkaConsumer{
	// 	consumer.NewTradeConsumer(cfg, logger, repo),
	// }

	core := &Core{
		cfg:       cfg,
		repo:      repo,
		tl:        logger,
		scheduler: scheduler,
		// consumers: consumers,
		metrics: monitor.NewMetricsServer(cfg.Monitor),
	}
	return core
}

func (c *Core) Start(ctx context.Context) {
	c.tl.Info("Starting worker core...")
	// 启动监控服务
	if c.metrics != nil {
		c.metrics.Run()
	}

	// 启动消费者
	// for _, cons := range c.consumers {
	// 	go cons.Run(ctx)
	// }

	// 启动调度器
	c.scheduler.Start(ctx)
	c.tl.Info("Worker started successfully")

	// 等待外部关闭信号
	<-ctx.Done()
	c.tl.Info("Shutting down worker due to context cancellation...")

}

// Stop 优雅关闭 Core 的所有资源
func (c *Core) Stop(ctx context.Context) {
	c.tl.Info("Stopping worker core...")

	// 停止消费者
	for _, cons := range c.consumers {
		cons.Stop()
	}

	// 停止调度器
	if c.scheduler != nil {
		c.scheduler.Stop(ctx)
	}

	// 停止 Prometheus 监控服务
	if c.metrics != nil {
		_ = c.metrics.Stop(ctx)
	}

	c.repo.Close()

	c.tl.Info("Worker core stopped.")
}
