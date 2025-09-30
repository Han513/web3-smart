package main

import (
	"context"
	"os"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/job"
	"web3-smart/internal/worker/repository"
	"web3-smart/pkg/logger"

	"go.uber.org/zap"
)

// 一次性任务

func main() {
	startTime := time.Now()
	// 初始化配置文件
	cfg := config.InitConfig()

	// 初始化 trace provider
	logger.InitTrace("web3-kline", "prepare")
	// 启动主 span
	ctx, span := logger.StartSpan(context.Background(), "main", "main")
	defer span.End()

	// 创建 root logger 并注入 trace 上下文
	rootLogger := logger.NewLogger("prepare")
	logger.SetLogLevel(cfg.Log.Level)
	tl := logger.WithTrace(ctx, rootLogger)

	// 初始化 repository
	repo := repository.New(cfg, tl)
	defer repo.Close()

	tl.Info("Starting web3-smart to prepare for migration data to new table...")
	aggKlineMigration := job.NewMigrationTable(cfg, repo, tl)
	if err := aggKlineMigration.Run(ctx); err != nil {
		tl.Error("Failed to run migration", zap.Error(err))
		os.Exit(1)
	}
	tl.Info("Task completed successfully", zap.Duration("taken_time", time.Since(startTime)))
}
