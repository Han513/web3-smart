package job

import (
	"context"
	"fmt"
	"time"

	"web3-smart/internal/worker/repository"

	"go.uber.org/zap"
)

// CleanupJob 清理過舊資料等週期任務
type CleanupJob struct {
	repo   repository.Repository
	logger *zap.Logger
}

func NewCleanupJob(repo repository.Repository, logger *zap.Logger) *CleanupJob {
	return &CleanupJob{repo: repo, logger: logger}
}

// Run 刪除 30 天前的交易資料
func (j *CleanupJob) Run(ctx context.Context) error {
	db := j.repo.GetDB()
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	cutoff := time.Now().Add(-30 * 24 * time.Hour).Unix()
	res := db.WithContext(ctx).
		Exec("DELETE FROM dex_query_v1.t_smart_transaction WHERE transaction_time < ?", cutoff)
	if res.Error != nil {
		return res.Error
	}
	j.logger.Info("cleaned old transactions", zap.Int64("cutoff", cutoff), zap.Int64("rows", res.RowsAffected))
	return nil
}
