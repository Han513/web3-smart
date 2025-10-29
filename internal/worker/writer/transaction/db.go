package transaction

import (
	"context"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	transactionUtils "web3-smart/pkg/utils/transaction_utils"

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

// limitTransactionPrecision 限制钱包交易数据中所有decimal.Decimal字段的精度
func limitTransactionPrecision(transaction *model.WalletTransaction) {
	transaction.WalletBalance = limitDecimal(transaction.WalletBalance)
	transaction.Price = limitDecimal(transaction.Price)
	transaction.Amount = limitDecimal(transaction.Amount)
	transaction.MarketCap = limitDecimal(transaction.MarketCap)
	transaction.Value = limitDecimal(transaction.Value)
	transaction.HoldingPercentage = limitDecimal(transaction.HoldingPercentage)
	transaction.RealizedProfit = limitDecimal(transaction.RealizedProfit)
	transaction.RealizedProfitPercentage = limitDecimal(transaction.RealizedProfitPercentage)
	transaction.FromTokenAmount = limitDecimal(transaction.FromTokenAmount)
	transaction.DestTokenAmount = limitDecimal(transaction.DestTokenAmount)
}

type DbTransactionWriter struct {
	db *gorm.DB
	tl *zap.Logger
}

func NewDbTransactionWriter(db *gorm.DB, tl *zap.Logger) writer.BatchWriter[model.WalletTransaction] {
	return &DbTransactionWriter{db: db, tl: tl}
}

func (w *DbTransactionWriter) BWrite(ctx context.Context, transactions []model.WalletTransaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// 对所有transaction数据应用精度限制
	for i := range transactions {
		limitTransactionPrecision(&transactions[i])
	}

	transactions = transactionUtils.DeduplicateTransactions(transactions)

	newCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 重试机制
	var err error
	for attempt := 0; attempt < RETRY_COUNT; attempt++ {
		// 使用ON CONFLICT更新策略，匹配唯一索引 unique_transaction
		// (wallet_address, token_address, signature, transaction_time, chain_id)
		// 设置事务隔离级别为READ COMMITTED，提高并发性能
		err = w.db.WithContext(newCtx).Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "wallet_address"},
				{Name: "token_address"},
				{Name: "signature"},
				{Name: "log_index"},
				{Name: "transaction_time"},
				{Name: "chain_id"},
			},
			// 使用DO UPDATE SET代替DoUpdates，减少SQL解析开销
			DoUpdates: clause.Assignments(map[string]interface{}{
				"wallet_balance":             gorm.Expr("EXCLUDED.wallet_balance"),
				"token_icon":                 gorm.Expr("EXCLUDED.token_icon"),
				"token_name":                 gorm.Expr("EXCLUDED.token_name"),
				"price":                      gorm.Expr("EXCLUDED.price"),
				"amount":                     gorm.Expr("EXCLUDED.amount"),
				"marketcap":                  gorm.Expr("EXCLUDED.marketcap"),
				"value":                      gorm.Expr("EXCLUDED.value"),
				"holding_percentage":         gorm.Expr("EXCLUDED.holding_percentage"),
				"realized_profit":            gorm.Expr("EXCLUDED.realized_profit"),
				"realized_profit_percentage": gorm.Expr("EXCLUDED.realized_profit_percentage"),
				"transaction_type":           gorm.Expr("EXCLUDED.transaction_type"),
				"from_token_address":         gorm.Expr("EXCLUDED.from_token_address"),
				"from_token_symbol":          gorm.Expr("EXCLUDED.from_token_symbol"),
				"from_token_amount":          gorm.Expr("EXCLUDED.from_token_amount"),
				"dest_token_address":         gorm.Expr("EXCLUDED.dest_token_address"),
				"dest_token_symbol":          gorm.Expr("EXCLUDED.dest_token_symbol"),
				"dest_token_amount":          gorm.Expr("EXCLUDED.dest_token_amount"),
				"created_at":                 gorm.Expr("EXCLUDED.created_at"),
			}),
		}).CreateInBatches(transactions, 1000).Error

		if err == nil {
			break // 成功则退出重试
		}
		//time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		//w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err))
		w.tl.Warn("❌ DB write failed, exceeded the maximum number of retries", zap.Error(err), zap.Any("transactions", transactions))
		return err
	}
	return nil
}

func (w *DbTransactionWriter) Close() error {
	return nil
}
