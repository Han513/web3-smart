package handler

import (
	"go.uber.org/zap"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/service"
)

type TradeHandler struct {
	tl      *zap.Logger
	cfg     config.Config
	repo    repository.Repository
	wpaServ *service.WalletPositonAnalyze
}

func NewTradeHandler(cfg config.Config, logger *zap.Logger, repo repository.Repository) *TradeHandler {
	return &TradeHandler{
		tl:      logger,
		cfg:     cfg,
		repo:    repo,
		wpaServ: service.NewWalletPositonAnalyze(cfg, logger, repo),
	}
}

func (h *TradeHandler) HandleTrade(trade model.TradeEvent) {
	h.wpaServ.ProcessTrade(trade)
}

func (h *TradeHandler) Stop() {
	h.wpaServ.Close()
}
