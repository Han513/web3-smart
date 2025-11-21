package handler

import (
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/service"

	"go.uber.org/zap"
)

type TradeHandler struct {
	tl      *zap.Logger
	cfg     config.Config
	repo    repository.Repository
	wpaServ *service.WalletPositonAnalyze
	wisServ *service.WalletIndicatorStatistics
	tcsServ *service.TopCardsService
}

func NewTradeHandler(cfg config.Config, logger *zap.Logger, repo repository.Repository) *TradeHandler {
	return &TradeHandler{
		tl:      logger,
		cfg:     cfg,
		repo:    repo,
		wpaServ: service.NewWalletPositonAnalyze(cfg, logger, repo),
		wisServ: service.NewWalletIndicatorStatistics(cfg, logger, repo),
		tcsServ: service.NewTopCardsService(cfg, logger, repo),
	}
}

func (h *TradeHandler) HandleTrade(trade model.TradeEvent) {
	smartMoney, prevHolding, currentHolding, txType := h.wpaServ.ProcessTrade(trade)
	if smartMoney != nil {
		h.wisServ.Statistics(trade, smartMoney, prevHolding, currentHolding, txType)
		h.tcsServ.HandleSmartTrade(trade, smartMoney, txType)
	}
}

func (h *TradeHandler) Stop() {
	h.wpaServ.Close()
	h.wisServ.Close()
	h.tcsServ.Close()
}
