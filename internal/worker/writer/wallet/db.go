package wallet

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	walletUtils "web3-smart/pkg/utils/wallet_utils"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	RETRY_COUNT = 3
)

type DbWalletWriter struct {
	db *gorm.DB
	tl *zap.Logger
}

func NewDbWalletWriter(db *gorm.DB, tl *zap.Logger) writer.BatchWriter[model.WalletSummary] {
	return &DbWalletWriter{db: db, tl: tl}
}

func (w *DbWalletWriter) BWrite(ctx context.Context, wallets []model.WalletSummary) error {
	if len(wallets) == 0 {
		return nil
	}

	wallets = walletUtils.DeduplicateWallets(wallets)

	newCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 重试机制
	var err error
	for attempt := 0; attempt < RETRY_COUNT; attempt++ {
		// 使用ON CONFLICT更新策略，匹配唯一索引 unique_wallet_address
		// 设置事务隔离级别为READ COMMITTED，提高并发性能
		err = w.db.WithContext(newCtx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "wallet_address"},
			},
			// 使用DO UPDATE SET代替DoUpdates，减少SQL解析开销
			DoUpdates: clause.Assignments(map[string]interface{}{
				"avatar":                               gorm.Expr("EXCLUDED.avatar"),
				"balance":                              gorm.Expr("EXCLUDED.balance"),
				"balance_usd":                          gorm.Expr("EXCLUDED.balance_usd"),
				"chain_id":                             gorm.Expr("EXCLUDED.chain_id"),
				"tags":                                 gorm.Expr("EXCLUDED.tags"),
				"twitter_name":                         gorm.Expr("EXCLUDED.twitter_name"),
				"twitter_username":                     gorm.Expr("EXCLUDED.twitter_username"),
				"wallet_type":                          gorm.Expr("EXCLUDED.wallet_type"),
				"asset_multiple":                       gorm.Expr("EXCLUDED.asset_multiple"),
				"token_list":                           gorm.Expr("EXCLUDED.token_list"),
				"avg_cost_30d":                         gorm.Expr("EXCLUDED.avg_cost_30d"),
				"buy_num_30d":                          gorm.Expr("EXCLUDED.buy_num_30d"),
				"sell_num_30d":                         gorm.Expr("EXCLUDED.sell_num_30d"),
				"win_rate_30d":                         gorm.Expr("EXCLUDED.win_rate_30d"),
				"avg_cost_7d":                          gorm.Expr("EXCLUDED.avg_cost_7d"),
				"buy_num_7d":                           gorm.Expr("EXCLUDED.buy_num_7d"),
				"sell_num_7d":                          gorm.Expr("EXCLUDED.sell_num_7d"),
				"win_rate_7d":                          gorm.Expr("EXCLUDED.win_rate_7d"),
				"avg_cost_1d":                          gorm.Expr("EXCLUDED.avg_cost_1d"),
				"buy_num_1d":                           gorm.Expr("EXCLUDED.buy_num_1d"),
				"sell_num_1d":                          gorm.Expr("EXCLUDED.sell_num_1d"),
				"win_rate_1d":                          gorm.Expr("EXCLUDED.win_rate_1d"),
				"pnl_30d":                              gorm.Expr("EXCLUDED.pnl_30d"),
				"pnl_percentage_30d":                   gorm.Expr("EXCLUDED.pnl_percentage_30d"),
				"pnl_pic_30d":                          gorm.Expr("EXCLUDED.pnl_pic_30d"),
				"unrealized_profit_30d":                gorm.Expr("EXCLUDED.unrealized_profit_30d"),
				"total_cost_30d":                       gorm.Expr("EXCLUDED.total_cost_30d"),
				"avg_realized_profit_30d":              gorm.Expr("EXCLUDED.avg_realized_profit_30d"),
				"pnl_7d":                               gorm.Expr("EXCLUDED.pnl_7d"),
				"pnl_percentage_7d":                    gorm.Expr("EXCLUDED.pnl_percentage_7d"),
				"unrealized_profit_7d":                 gorm.Expr("EXCLUDED.unrealized_profit_7d"),
				"total_cost_7d":                        gorm.Expr("EXCLUDED.total_cost_7d"),
				"avg_realized_profit_7d":               gorm.Expr("EXCLUDED.avg_realized_profit_7d"),
				"pnl_1d":                               gorm.Expr("EXCLUDED.pnl_1d"),
				"pnl_percentage_1d":                    gorm.Expr("EXCLUDED.pnl_percentage_1d"),
				"unrealized_profit_1d":                 gorm.Expr("EXCLUDED.unrealized_profit_1d"),
				"total_cost_1d":                        gorm.Expr("EXCLUDED.total_cost_1d"),
				"avg_realized_profit_1d":               gorm.Expr("EXCLUDED.avg_realized_profit_1d"),
				"distribution_gt500_30d":               gorm.Expr("EXCLUDED.distribution_gt500_30d"),
				"distribution_200to500_30d":            gorm.Expr("EXCLUDED.distribution_200to500_30d"),
				"distribution_0to200_30d":              gorm.Expr("EXCLUDED.distribution_0to200_30d"),
				"distribution_n50to0_30d":              gorm.Expr("EXCLUDED.distribution_n50to0_30d"),
				"distribution_lt50_30d":                gorm.Expr("EXCLUDED.distribution_lt50_30d"),
				"distribution_gt500_percentage_30d":    gorm.Expr("EXCLUDED.distribution_gt500_percentage_30d"),
				"distribution_200to500_percentage_30d": gorm.Expr("EXCLUDED.distribution_200to500_percentage_30d"),
				"distribution_0to200_percentage_30d":   gorm.Expr("EXCLUDED.distribution_0to200_percentage_30d"),
				"distribution_n50to0_percentage_30d":   gorm.Expr("EXCLUDED.distribution_n50to0_percentage_30d"),
				"distribution_lt50_percentage_30d":     gorm.Expr("EXCLUDED.distribution_lt50_percentage_30d"),
				"distribution_gt500_7d":                gorm.Expr("EXCLUDED.distribution_gt500_7d"),
				"distribution_200to500_7d":             gorm.Expr("EXCLUDED.distribution_200to500_7d"),
				"distribution_0to200_7d":               gorm.Expr("EXCLUDED.distribution_0to200_7d"),
				"distribution_n50to0_7d":               gorm.Expr("EXCLUDED.distribution_n50to0_7d"),
				"distribution_lt50_7d":                 gorm.Expr("EXCLUDED.distribution_lt50_7d"),
				"distribution_gt500_percentage_7d":     gorm.Expr("EXCLUDED.distribution_gt500_percentage_7d"),
				"distribution_200to500_percentage_7d":  gorm.Expr("EXCLUDED.distribution_200to500_percentage_7d"),
				"distribution_0to200_percentage_7d":    gorm.Expr("EXCLUDED.distribution_0to200_percentage_7d"),
				"distribution_n50to0_percentage_7d":    gorm.Expr("EXCLUDED.distribution_n50to0_percentage_7d"),
				"distribution_lt50_percentage_7d":      gorm.Expr("EXCLUDED.distribution_lt50_percentage_7d"),
				"last_transaction_time":                gorm.Expr("EXCLUDED.last_transaction_time"),
				"is_active":                            gorm.Expr("EXCLUDED.is_active"),
				"updated_at":                           gorm.Expr("EXCLUDED.updated_at"),
				"created_at":                           gorm.Expr("EXCLUDED.created_at"),
			}),
		}).CreateInBatches(wallets, 1000).Error

		if err == nil {
			break // 成功则退出重试
		}
		//time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		//w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err))
		w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err), zap.Any("wallets", wallets))
		return err
	}
	return nil
}

func (w *DbWalletWriter) Close() error {
	return nil
}
