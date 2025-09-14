package main

import (
	"context"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"web3-smart/internal/worker"
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/logger"
)

func main() {
	// 初始化配置文件
	cfg := config.InitConfig()

	// 初始化 trace provider
	logger.InitTrace("web3-smart", "worker")
	// 启动主 span
	ctx, span := logger.StartSpan(context.Background(), "main", "main")
	defer span.End()

	// 创建 root logger 并注入 trace 上下文
	rootLogger := logger.NewLogger("worker")
	logger.SetLogLevel(cfg.Log.Level)
	tl := logger.WithTrace(ctx, rootLogger)

	// 启动配置热加载监听
	go config.WatchConfig(&cfg)

	// 初始化worker
	core := worker.New(cfg, tl)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动 worker
	go func() {
		// 启动worker
		tl.Info("Starting web3-smart worker...")
		core.Start(ctx)
	}()

	// 监听操作系统信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	tl.Info("Received shutdown signal, starting graceful shutdown...")

	// 关闭资源
	core.Stop(ctx)

	tl.Info("Shutting down all cores...")
}
