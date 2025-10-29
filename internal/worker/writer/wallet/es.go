package wallet

import (
	"context"
	"crypto/md5"
	"fmt"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	"web3-smart/pkg/elasticsearch"

	"go.uber.org/zap"
)

type ESWalletWriter struct {
	esClient *elasticsearch.Client
	logger   *zap.Logger
	index    string
}

func NewESWalletWriter(esClient *elasticsearch.Client, logger *zap.Logger, index string) writer.BatchWriter[model.WalletSummary] {
	return &ESWalletWriter{
		esClient: esClient,
		logger:   logger,
		index:    index,
	}
}

func (w *ESWalletWriter) BWrite(ctx context.Context, wallets []model.WalletSummary) error {
	if len(wallets) == 0 {
		return nil
	}

	// 对所有wallet数据应用精度限制
	for i := range wallets {
		limitWalletPrecision(&wallets[i])
	}

	// 尝试3次写入
	var err error
	for i := 0; i < RETRY_COUNT; i++ {
		err = w.writeBulkOperations(ctx, wallets)
		if err == nil {
			break
		}
	}

	if err != nil {
		w.logger.Warn("❌ ES write failed, exceeded the maximum number of retries", zap.Error(err), zap.Any("wallets", wallets))
		return err
	}

	return nil
}

// writeBulkOperations 构建并执行批量操作
func (w *ESWalletWriter) writeBulkOperations(ctx context.Context, wallets []model.WalletSummary) error {
	operations := make([]elasticsearch.BulkOperation, 0, len(wallets))

	for _, wallet := range wallets {
		// 生成文档ID
		docID := w.generateDocID(&wallet)

		// 转换为ES文档
		doc := w.convertToESDoc(&wallet)

		// 构建批量操作
		operation := elasticsearch.BulkOperation{
			Action:   "index", // 使用index操作（存在则更新，不存在则创建）
			Index:    w.index,
			ID:       docID,
			Routing:  wallet.WalletAddress, // 使用wallet_address作为routing
			Document: doc,
		}

		operations = append(operations, operation)
	}

	// 调用ES客户端执行批量操作
	return w.esClient.BulkWrite(ctx, operations)
}

func (w *ESWalletWriter) Close() error {
	return nil
}

// generateDocID 生成文档ID
func (w *ESWalletWriter) generateDocID(wallet *model.WalletSummary) string {
	return fmt.Sprintf("%d_%s", wallet.ChainID, wallet.WalletAddress)
}

// convertToESDoc 将WalletSummary转换为ES文档
func (w *ESWalletWriter) convertToESDoc(wallet *model.WalletSummary) map[string]interface{} {
	// 生成组合标签字段，便于快速过滤
	combinedTags := w.generateCombinedTags(wallet)

	// 生成wallet hash用于routing
	walletHash := w.generateWalletHash(wallet.WalletAddress)

	doc := map[string]interface{}{
		"wallet_address":       wallet.WalletAddress,
		"wallet_hash":          walletHash,
		"avatar":               wallet.Avatar,
		"balance":              wallet.Balance,
		"balance_usd":          wallet.BalanceUSD,
		"chain_id":             wallet.ChainID,
		"tags":                 wallet.Tags,
		"wallet_tags_combined": combinedTags,
		"twitter_name":         wallet.TwitterName,
		"twitter_username":     wallet.TwitterUsername,
		"wallet_type":          wallet.WalletType,
		"asset_multiple":       wallet.AssetMultiple,
		"token_list":           wallet.TokenList,

		// 交易数据 - 30天
		"avg_cost_30d":  wallet.AvgCost30d,
		"total_num_30d": wallet.BuyNum30d + wallet.SellNum30d,
		"buy_num_30d":   wallet.BuyNum30d,
		"sell_num_30d":  wallet.SellNum30d,
		"win_rate_30d":  wallet.WinRate30d,

		// 交易数据 - 7天
		"avg_cost_7d":  wallet.AvgCost7d,
		"total_num_7d": wallet.BuyNum7d + wallet.SellNum7d,
		"buy_num_7d":   wallet.BuyNum7d,
		"sell_num_7d":  wallet.SellNum7d,
		"win_rate_7d":  wallet.WinRate7d,

		// 交易数据 - 1天
		"avg_cost_1d":  wallet.AvgCost1d,
		"total_num_1d": wallet.BuyNum1d + wallet.SellNum1d,
		"buy_num_1d":   wallet.BuyNum1d,
		"sell_num_1d":  wallet.SellNum1d,
		"win_rate_1d":  wallet.WinRate1d,

		// 盈亏数据 - 30天
		"pnl_30d":                 wallet.PNL30d,
		"pnl_percentage_30d":      wallet.PNLPercentage30d,
		"pnl_pic_30d":             wallet.PNLPic30d,
		"unrealized_profit_30d":   wallet.UnrealizedProfit30d,
		"total_cost_30d":          wallet.TotalCost30d,
		"avg_realized_profit_30d": wallet.AvgRealizedProfit30d,

		// 盈亏数据 - 7天
		"pnl_7d":                 wallet.PNL7d,
		"pnl_percentage_7d":      wallet.PNLPercentage7d,
		"unrealized_profit_7d":   wallet.UnrealizedProfit7d,
		"total_cost_7d":          wallet.TotalCost7d,
		"avg_realized_profit_7d": wallet.AvgRealizedProfit7d,

		// 盈亏数据 - 1天
		"pnl_1d":                 wallet.PNL1d,
		"pnl_percentage_1d":      wallet.PNLPercentage1d,
		"unrealized_profit_1d":   wallet.UnrealizedProfit1d,
		"total_cost_1d":          wallet.TotalCost1d,
		"avg_realized_profit_1d": wallet.AvgRealizedProfit1d,

		// 收益分布数据 - 30天
		"distribution_gt500_30d":               wallet.DistributionGt500_30d,
		"distribution_200to500_30d":            wallet.Distribution200to500_30d,
		"distribution_0to200_30d":              wallet.Distribution0to200_30d,
		"distribution_n50to0_30d":              wallet.DistributionN50to0_30d,
		"distribution_lt50_30d":                wallet.DistributionLt50_30d,
		"distribution_gt500_percentage_30d":    wallet.DistributionGt500Percentage30d,
		"distribution_200to500_percentage_30d": wallet.Distribution200to500Percentage30d,
		"distribution_0to200_percentage_30d":   wallet.Distribution0to200Percentage30d,
		"distribution_n50to0_percentage_30d":   wallet.DistributionN50to0Percentage30d,
		"distribution_lt50_percentage_30d":     wallet.DistributionLt50Percentage30d,

		// 收益分布数据 - 7天
		"distribution_gt500_7d":               wallet.DistributionGt500_7d,
		"distribution_200to500_7d":            wallet.Distribution200to500_7d,
		"distribution_0to200_7d":              wallet.Distribution0to200_7d,
		"distribution_n50to0_7d":              wallet.DistributionN50to0_7d,
		"distribution_lt50_7d":                wallet.DistributionLt50_7d,
		"distribution_gt500_percentage_7d":    wallet.DistributionGt500Percentage7d,
		"distribution_200to500_percentage_7d": wallet.Distribution200to500Percentage7d,
		"distribution_0to200_percentage_7d":   wallet.Distribution0to200Percentage7d,
		"distribution_n50to0_percentage_7d":   wallet.DistributionN50to0Percentage7d,
		"distribution_lt50_percentage_7d":     wallet.DistributionLt50Percentage7d,

		// 时间和状态
		"is_active":  wallet.IsActive,
		"updated_at": wallet.UpdatedAt,
		"created_at": wallet.CreatedAt,
	}

	// 时间字段已经是毫秒时间戳(int64)，可直接使用
	if wallet.LastTransactionTime > 0 {
		doc["last_transaction_time"] = wallet.LastTransactionTime
	}

	return doc
}

// generateCombinedTags 生成组合标签，便于快速查询
func (w *ESWalletWriter) generateCombinedTags(wallet *model.WalletSummary) []string {
	var combined []string

	// // 添加基础状态标签
	// if wallet.IsActive {
	// 	combined = append(combined, "active")
	// } else {
	// 	combined = append(combined, "inactive")
	// }

	// // 添加钱包类型标签
	// switch wallet.WalletType {
	// case 0:
	// 	combined = append(combined, "general_smart_money")
	// case 1:
	// 	combined = append(combined, "pump_smart_money")
	// case 2:
	// 	combined = append(combined, "moonshot_smart_money")
	// }

	// // 添加资产倍数标签
	// if wallet.AssetMultiple.GreaterThanOrEqual(decimal.NewFromInt(10)) {
	// 	combined = append(combined, "high_performer")
	// } else if wallet.AssetMultiple.GreaterThanOrEqual(decimal.NewFromInt(2)) {
	// 	combined = append(combined, "good_performer")
	// } else if wallet.AssetMultiple.GreaterThanOrEqual(decimal.Zero) {
	// 	combined = append(combined, "break_even")
	// } else {
	// 	combined = append(combined, "loss_maker")
	// }

	// // 添加余额区间标签
	// if wallet.BalanceUSD.GreaterThanOrEqual(decimal.NewFromInt(1000000)) {
	// 	combined = append(combined, "whale")
	// } else if wallet.BalanceUSD.GreaterThanOrEqual(decimal.NewFromInt(100000)) {
	// 	combined = append(combined, "big_holder")
	// } else if wallet.BalanceUSD.GreaterThanOrEqual(decimal.NewFromInt(10000)) {
	// 	combined = append(combined, "medium_holder")
	// } else {
	// 	combined = append(combined, "small_holder")
	// }

	// // 添加交易活跃度标签 (基于30天交易次数)
	// totalTrades30d := wallet.BuyNum30d + wallet.SellNum30d
	// if totalTrades30d >= 100 {
	// 	combined = append(combined, "very_active_trader")
	// } else if totalTrades30d >= 20 {
	// 	combined = append(combined, "active_trader")
	// } else if totalTrades30d >= 5 {
	// 	combined = append(combined, "regular_trader")
	// } else {
	// 	combined = append(combined, "low_activity_trader")
	// }

	// // 添加胜率标签 (基于30天胜率)
	// if wallet.WinRate30d.GreaterThanOrEqual(decimal.NewFromFloat(0.8)) {
	// 	combined = append(combined, "high_win_rate")
	// } else if wallet.WinRate30d.GreaterThanOrEqual(decimal.NewFromFloat(0.6)) {
	// 	combined = append(combined, "good_win_rate")
	// } else if wallet.WinRate30d.GreaterThanOrEqual(decimal.NewFromFloat(0.4)) {
	// 	combined = append(combined, "average_win_rate")
	// } else {
	// 	combined = append(combined, "low_win_rate")
	// }

	// 添加原始标签
	combined = append(combined, []string(wallet.Tags)...)

	return combined
}

func (w *ESWalletWriter) generateWalletHash(walletAddress string) string {
	hash := md5.Sum([]byte(walletAddress))
	return fmt.Sprintf("%x", hash)[:8]
}
