package wallet

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	walletUtils "web3-smart/pkg/utils/wallet_utils"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	RETRY_COUNT = 3
)

// limitDecimal 限制decimal.Decimal值的范围，防止PostgreSQL DECIMAL(50,20)溢出
func limitDecimal(value decimal.Decimal) decimal.Decimal {
	// DECIMAL(50,20)的最大值和最小值 (30位整数 + 20位小数)
	maxDecimal50_20, _ := decimal.NewFromString("999999999999999999999999999999.99999999999999999999")
	minDecimal50_20 := maxDecimal50_20.Neg()

	// 1. 先检查是否超过总体范围
	if value.GreaterThan(maxDecimal50_20) {
		return maxDecimal50_20
	}
	if value.LessThan(minDecimal50_20) {
		return minDecimal50_20
	}

	// 2. 精度处理：四舍五入到20位小数
	return value.Round(20)
}

// limitWalletPrecision 限制钱包数据中所有decimal.Decimal字段的精度
func limitWalletPrecision(wallet *model.WalletSummary) {
	wallet.Balance = limitDecimal(wallet.Balance)
	wallet.BalanceUSD = limitDecimal(wallet.BalanceUSD)
	wallet.AssetMultiple = limitDecimal(wallet.AssetMultiple)

	// 交易数据
	wallet.AvgCost30d = limitDecimal(wallet.AvgCost30d)
	wallet.WinRate30d = limitDecimal(wallet.WinRate30d)
	wallet.AvgCost7d = limitDecimal(wallet.AvgCost7d)
	wallet.WinRate7d = limitDecimal(wallet.WinRate7d)
	wallet.AvgCost1d = limitDecimal(wallet.AvgCost1d)
	wallet.WinRate1d = limitDecimal(wallet.WinRate1d)

	// 盈亏数据
	wallet.PNL30d = limitDecimal(wallet.PNL30d)
	wallet.PNLPercentage30d = limitDecimal(wallet.PNLPercentage30d)
	wallet.UnrealizedProfit30d = limitDecimal(wallet.UnrealizedProfit30d)
	wallet.TotalCost30d = limitDecimal(wallet.TotalCost30d)
	wallet.AvgRealizedProfit30d = limitDecimal(wallet.AvgRealizedProfit30d)

	wallet.PNL7d = limitDecimal(wallet.PNL7d)
	wallet.PNLPercentage7d = limitDecimal(wallet.PNLPercentage7d)
	wallet.UnrealizedProfit7d = limitDecimal(wallet.UnrealizedProfit7d)
	wallet.TotalCost7d = limitDecimal(wallet.TotalCost7d)
	wallet.AvgRealizedProfit7d = limitDecimal(wallet.AvgRealizedProfit7d)

	wallet.PNL1d = limitDecimal(wallet.PNL1d)
	wallet.PNLPercentage1d = limitDecimal(wallet.PNLPercentage1d)
	wallet.UnrealizedProfit1d = limitDecimal(wallet.UnrealizedProfit1d)
	wallet.TotalCost1d = limitDecimal(wallet.TotalCost1d)
	wallet.AvgRealizedProfit1d = limitDecimal(wallet.AvgRealizedProfit1d)

	// 收益分布百分比数据
	wallet.DistributionGt500Percentage30d = limitDecimal(wallet.DistributionGt500Percentage30d)
	wallet.Distribution200to500Percentage30d = limitDecimal(wallet.Distribution200to500Percentage30d)
	wallet.Distribution0to200Percentage30d = limitDecimal(wallet.Distribution0to200Percentage30d)
	wallet.DistributionN50to0Percentage30d = limitDecimal(wallet.DistributionN50to0Percentage30d)
	wallet.DistributionLt50Percentage30d = limitDecimal(wallet.DistributionLt50Percentage30d)

	wallet.DistributionGt500Percentage7d = limitDecimal(wallet.DistributionGt500Percentage7d)
	wallet.Distribution200to500Percentage7d = limitDecimal(wallet.Distribution200to500Percentage7d)
	wallet.Distribution0to200Percentage7d = limitDecimal(wallet.Distribution0to200Percentage7d)
	wallet.DistributionN50to0Percentage7d = limitDecimal(wallet.DistributionN50to0Percentage7d)
	wallet.DistributionLt50Percentage7d = limitDecimal(wallet.DistributionLt50Percentage7d)
}

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

	// 对所有wallet数据应用精度限制
	for i := range wallets {
		limitWalletPrecision(&wallets[i])
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
