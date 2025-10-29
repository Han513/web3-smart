package balance

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type DbBalanceWriter struct {
	db *gorm.DB
	tl *zap.Logger
	// wm walletManager
}

func NewDbBalanceWriter(db *gorm.DB, tl *zap.Logger) writer.BatchWriter[model.Balance] {
	return &DbBalanceWriter{db: db, tl: tl}
}

func (bw *DbBalanceWriter) BWrite(ctx context.Context, balances []model.Balance) error {

	if len(balances) == 0 {
		return nil
	}

	// 1️⃣ 去重（按 network+token_address+token_account）
	balances = deduplicateBalances(balances)

	newCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	const batchSize = 1000
	const retryCount = 3

	var err error
	for range retryCount {
		for i := 0; i < len(balances); i += batchSize {
			end := min(i+batchSize, len(balances))
			batch := balances[i:end]

			// 使用 GORM 批量 upsert
			err = bw.db.WithContext(newCtx).Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "network"},
					{Name: "token_address"},
					{Name: "token_account"},
				},
				DoUpdates: clause.Assignments(map[string]any{
					"wallet":       gorm.Expr("EXCLUDED.wallet"),
					"amount":       gorm.Expr("EXCLUDED.amount"),
					"decimal":      gorm.Expr("EXCLUDED.decimal"),
					"block_number": gorm.Expr("EXCLUDED.block_number"),
					"version":      gorm.Expr("EXCLUDED.version"),
					"updated_at":   gorm.Expr("EXCLUDED.updated_at"),
				}),
			}).CreateInBatches(batch, batchSize).Error

			if err != nil {
				bw.tl.Warn("❌ PG insert failed", zap.Error(err))
				break
			}
		}

		if err == nil {
			break
		}
		// 可选短暂延迟
		// time.Sleep(100 * time.Millisecond)
	}

	if err != nil {
		bw.tl.Warn("❌ PG write failed after retries", zap.Error(err), zap.Any("balances", balances))
		return err
	}
	return nil
}

func (bw *DbBalanceWriter) Close() error {
	return nil
}

func deduplicateBalances(bals []model.Balance) []model.Balance {
	m := make(map[string]model.Balance, len(bals))
	for _, b := range bals {
		key := b.Network + "|" + b.TokenAddress + "|" + b.TokenAccount
		if existing, ok := m[key]; ok {
			// 保留 version 最大的
			if b.Version > existing.Version {
				m[key] = b
			}
		} else {
			m[key] = b
		}
	}

	res := make([]model.Balance, 0, len(m))
	for _, b := range m {
		res = append(res, b)
	}
	return res
}
