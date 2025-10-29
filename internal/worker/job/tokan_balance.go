package job

import (
	"context"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/service"

	"github.com/sourcegraph/conc/pool"
	"go.uber.org/zap"
)

type TokenBalance struct {
	cfg                 config.Config
	repo                repository.Repository
	tl                  *zap.Logger
	tokenBalanceService service.BalanceUpdate
}

func NewTokenBalance(cfg config.Config, repo repository.Repository, logger *zap.Logger) *TokenBalance {
	return &TokenBalance{
		repo:                repo,
		tl:                  logger,
		cfg:                 cfg,
		tokenBalanceService: *service.NewBalanceUpdate(cfg, logger, repo),
	}
}

func (t *TokenBalance) Run(ctx context.Context) error {
	networks := []string{"SOLANA", "BSC"}
	worker := pool.New().WithMaxGoroutines(10)
	for _, network := range networks {
		var tokens []model.HotToken
		if err := t.repo.GetDB().Model(&model.HotToken{}).Where("network = ?", network).Where("done = ?", false).Order("rank ASC").Limit(10).Find(&tokens).Error; err != nil {
			return err
		}

		for _, token := range tokens {
			tokenFake := token
			worker.Go(func() {
				t.tl.Debug("load balance", zap.String("network", tokenFake.Network), zap.String("address", tokenFake.Address))
				err := t.tokenBalanceService.LoadBalanceFromExternal(ctx, tokenFake)
				if err != nil {
					t.tl.Error("load balance error", zap.String("network", tokenFake.Network), zap.String("address", tokenFake.Address), zap.Error(err))
				} else {
					t.tl.Info("load balance success", zap.String("network", tokenFake.Network), zap.String("address", tokenFake.Address))
					t.repo.GetDB().Model(&model.HotToken{}).Where("network = ?", tokenFake.Network).Where("address = ?", tokenFake.Address).Update("done", true)
				}
			})
		}
	}
	worker.Wait()

	return nil
}
