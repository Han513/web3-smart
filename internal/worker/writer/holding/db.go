package holding

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	holdingUtils "web3-smart/pkg/utils/holding_utils"

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

// limitHoldingPrecision 限制钱包持仓数据中所有decimal.Decimal字段的精度
func limitHoldingPrecision(holding *model.WalletHolding) {
	holding.Amount = limitDecimal(holding.Amount)
	holding.ValueUSD = limitDecimal(holding.ValueUSD)
	holding.UnrealizedProfits = limitDecimal(holding.UnrealizedProfits)
	holding.PNL = limitDecimal(holding.PNL)
	holding.PNLPercentage = limitDecimal(holding.PNLPercentage)
	holding.AvgPrice = limitDecimal(holding.AvgPrice)
	holding.CurrentTotalCost = limitDecimal(holding.CurrentTotalCost)
	holding.MarketCap = limitDecimal(holding.MarketCap)

	// 历史累计状态
	holding.HistoricalBuyAmount = limitDecimal(holding.HistoricalBuyAmount)
	holding.HistoricalSellAmount = limitDecimal(holding.HistoricalSellAmount)
	holding.HistoricalBuyCost = limitDecimal(holding.HistoricalBuyCost)
	holding.HistoricalSellValue = limitDecimal(holding.HistoricalSellValue)
}

type DbHoldingWriter struct {
	db *gorm.DB
	tl *zap.Logger
}

func NewDbHoldingWriter(db *gorm.DB, tl *zap.Logger) writer.BatchWriter[model.WalletHolding] {
	return &DbHoldingWriter{db: db, tl: tl}
}

func (w *DbHoldingWriter) BWrite(ctx context.Context, holdings []model.WalletHolding) error {
	if len(holdings) == 0 {
		return nil
	}

	// 对所有holding数据应用精度限制
	for i := range holdings {
		limitHoldingPrecision(&holdings[i])
	}

	holdings = holdingUtils.DeduplicateHoldings(holdings)

	newCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 重试机制
	var err error
	for attempt := 0; attempt < RETRY_COUNT; attempt++ {
		// 使用ON CONFLICT更新策略，匹配唯一索引 idx_unique_*
		// 设置事务隔离级别为READ COMMITTED，提高并发性能
		err = w.db.WithContext(newCtx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "wallet_address"},
				{Name: "token_address"},
				{Name: "chain_id"},
			},
			// 使用DO UPDATE SET代替DoUpdates，减少SQL解析开销
			DoUpdates: clause.Assignments(map[string]interface{}{
				"token_icon":             gorm.Expr("EXCLUDED.token_icon"),
				"token_name":             gorm.Expr("EXCLUDED.token_name"),
				"amount":                 gorm.Expr("EXCLUDED.amount"),
				"value_usd":              gorm.Expr("EXCLUDED.value_usd"),
				"unrealized_profits":     gorm.Expr("EXCLUDED.unrealized_profits"),
				"pnl":                    gorm.Expr("EXCLUDED.pnl"),
				"pnl_percentage":         gorm.Expr("EXCLUDED.pnl_percentage"),
				"avg_price":              gorm.Expr("EXCLUDED.avg_price"),
				"current_total_cost":     gorm.Expr("EXCLUDED.current_total_cost"),
				"marketcap":              gorm.Expr("EXCLUDED.marketcap"),
				"is_cleared":             gorm.Expr("EXCLUDED.is_cleared"),
				"is_dev":                 gorm.Expr("EXCLUDED.is_dev"),
				"tags":                   gorm.Expr("EXCLUDED.tags"),
				"historical_buy_amount":  gorm.Expr("EXCLUDED.historical_buy_amount"),
				"historical_sell_amount": gorm.Expr("EXCLUDED.historical_sell_amount"),
				"historical_buy_cost":    gorm.Expr("EXCLUDED.historical_buy_cost"),
				"historical_sell_value":  gorm.Expr("EXCLUDED.historical_sell_value"),
				"historical_buy_count":   gorm.Expr("EXCLUDED.historical_buy_count"),
				"historical_sell_count":  gorm.Expr("EXCLUDED.historical_sell_count"),
				"position_opened_at":     gorm.Expr("EXCLUDED.position_opened_at"),
				"last_transaction_time":  gorm.Expr("EXCLUDED.last_transaction_time"),
				"updated_at":             gorm.Expr("EXCLUDED.updated_at"),
				"created_at":             gorm.Expr("EXCLUDED.created_at"),
			}),
		}).CreateInBatches(holdings, 1000).Error

		if err == nil {
			break // 成功则退出重试
		}
		//time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		//w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err))
		w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err), zap.Any("holdings", holdings))
		return err
	}
	return nil
}

func (w *DbHoldingWriter) Close() error {
	return nil
}
