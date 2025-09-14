package job

import (
	"context"
	"go.uber.org/zap"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/repository"
)

type CacheLoad struct {
	cfg  config.Config
	repo repository.Repository
	tl   *zap.Logger
}

func NewCacheLoad(cfg config.Config, repo repository.Repository, logger *zap.Logger) *CacheLoad {
	return &CacheLoad{
		repo: repo,
		tl:   logger,
		cfg:  cfg,
	}
}

func (j *CacheLoad) Run(ctx context.Context) error {

	return nil
}
