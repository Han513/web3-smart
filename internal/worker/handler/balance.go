package handler

import (
	"context"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/service"
	"web3-smart/pkg/logger"

	"go.uber.org/zap"
)

type BalanceHandler struct {
	cfg            config.Config
	logger         *zap.Logger
	repo           repository.Repository
	balanceService *service.BalanceUpdate
}

func NewBalanceHandler(cfg config.Config, logger *zap.Logger, repo repository.Repository) *BalanceHandler {
	return &BalanceHandler{
		cfg:            cfg,
		repo:           repo,
		logger:         logger,
		balanceService: service.NewBalanceUpdate(cfg, logger, repo),
	}
}

func (h *BalanceHandler) HandleBalance(ctx context.Context, blockBalance model.BlockBalance) {
	if blockBalance.Hash == "" && len(blockBalance.Balances) == 0 {
		return
	}
	ctx, span := logger.StartSpan(ctx, "block_balance", blockBalance.Hash)
	defer span.End()
	tl := logger.NewLoggerWithTrace(ctx, h.logger)
	tl.Info("HandlerBalance start", zap.String("Hash", blockBalance.Hash), zap.Int("len balances", len(blockBalance.Balances)))
	start := time.Now()
	defer func() {
		tl.Info("HandlerBalance end", zap.String("Hash", blockBalance.Hash), zap.Int("len balances", len(blockBalance.Balances)), zap.Float64("cost", time.Since(start).Seconds()))
	}()
	h.balanceService.UpdateBalance(blockBalance)
}

func (h *BalanceHandler) Stop() {
	h.balanceService.Stop()
}
