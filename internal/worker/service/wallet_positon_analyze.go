package service

import (
	"go.uber.org/zap"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
)

type WalletPositonAnalyze struct {
	cfg  config.Config
	tl   *zap.Logger
	repo repository.Repository
}

func NewWalletPositonAnalyze(cfg config.Config, logger *zap.Logger, repo repository.Repository) *WalletPositonAnalyze {
	return &WalletPositonAnalyze{
		cfg:  cfg,
		tl:   logger,
		repo: repo,
	}
}

func (s *WalletPositonAnalyze) ProcessTrade(trade model.TradeEvent) {

}

// Close 关闭服务时优雅关闭所有异步写入器
func (s *WalletPositonAnalyze) Close() {

}
