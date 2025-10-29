package job

import (
	"context"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"go.uber.org/zap"
)

// TransactionCleanup 定时清理交易记录
type TransactionCleanup struct {
	cfg  config.Config
	repo repository.Repository
	tl   *zap.Logger
}

// NewTransactionCleanup 创建交易清理任务
func NewTransactionCleanup(cfg config.Config, repo repository.Repository, logger *zap.Logger) *TransactionCleanup {
	return &TransactionCleanup{
		repo: repo,
		tl:   logger,
		cfg:  cfg,
	}
}

// Run 执行清理任务
func (j *TransactionCleanup) Run(ctx context.Context) error {
	j.tl.Info("Starting transaction cleanup job")

	// 计算一个月前的时间戳（ms级）
	oneMonthAgo := time.Now().AddDate(0, -1, 0).UnixMilli()

	j.tl.Info("Deleting transactions older than one month",
		zap.Int64("cutoff_timestamp", oneMonthAgo),
		zap.String("cutoff_time", time.Unix(oneMonthAgo, 0).Format("2006-01-02 15:04:05")))

	// 执行删除操作
	db := j.repo.GetDB()
	result := db.WithContext(ctx).
		Where("transaction_time < ?", oneMonthAgo).
		Delete(&model.WalletTransaction{})

	if result.Error != nil {
		j.tl.Warn("Failed to cleanup old transactions",
			zap.Error(result.Error),
			zap.Int64("cutoff_timestamp", oneMonthAgo))
		return result.Error
	}

	j.tl.Info("Transaction cleanup completed successfully",
		zap.Int64("deleted_rows", result.RowsAffected),
		zap.Int64("cutoff_timestamp", oneMonthAgo))

	return nil
}
