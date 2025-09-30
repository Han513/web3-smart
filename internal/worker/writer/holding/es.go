package holding

import (
	"context"
	"crypto/md5"
	"fmt"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	"web3-smart/pkg/elasticsearch"

	"go.uber.org/zap"
)

type ESHoldingWriter struct {
	esClient *elasticsearch.Client
	logger   *zap.Logger
	index    string
}

func NewESHoldingWriter(esClient *elasticsearch.Client, logger *zap.Logger, index string) writer.BatchWriter[model.WalletHolding] {
	return &ESHoldingWriter{
		esClient: esClient,
		logger:   logger,
		index:    index,
	}
}

func (w *ESHoldingWriter) BWrite(ctx context.Context, holdings []model.WalletHolding) error {
	if len(holdings) == 0 {
		return nil
	}

	// 按token_address分组 - 使用具体的业务类型，更清晰
	tokenGroups := make(map[string][]model.WalletHolding)
	for _, holding := range holdings {
		tokenAddr := holding.TokenAddress
		tokenGroups[tokenAddr] = append(tokenGroups[tokenAddr], holding)
	}

	// 并发处理不同token的数据
	errChan := make(chan error, len(tokenGroups))
	for tokenAddr, group := range tokenGroups {
		go func(token string, holdingGroup []model.WalletHolding) {
			if err := w.writeBulkOperations(ctx, token, holdingGroup); err != nil {
				errChan <- err
				return
			}
			errChan <- nil
		}(tokenAddr, group)
	}

	// 等待所有写入完成
	var lastErr error
	for i := 0; i < len(tokenGroups); i++ {
		if err := <-errChan; err != nil {
			lastErr = err
		}
	}

	close(errChan)

	return lastErr
}

// writeBulkOperations 构建并执行批量操作
func (w *ESHoldingWriter) writeBulkOperations(ctx context.Context, tokenAddress string, holdings []model.WalletHolding) error {
	operations := make([]elasticsearch.BulkOperation, 0, len(holdings))

	for _, holding := range holdings {
		// 生成文档ID
		docID := w.generateDocID(&holding)

		// 转换为ES文档
		doc := w.convertToESDoc(&holding)

		// 构建批量操作
		operation := elasticsearch.BulkOperation{
			Action:   "index", // 使用index操作（存在则更新，不存在则创建）
			Index:    w.index,
			ID:       docID,
			Routing:  tokenAddress, // 使用token_address作为routing
			Document: doc,
		}

		operations = append(operations, operation)
	}

	// 调用ES客户端执行批量操作
	return w.esClient.BulkWrite(ctx, operations)
}

func (w *ESHoldingWriter) Close() error {
	return nil
}

// generateDocID 生成文档ID
func (w *ESHoldingWriter) generateDocID(holding *model.WalletHolding) string {
	return fmt.Sprintf("%d_%s_%s", holding.ChainID, holding.WalletAddress, holding.TokenAddress)
}

// convertToESDoc 将WalletHolding转换为ES文档
func (w *ESHoldingWriter) convertToESDoc(holding *model.WalletHolding) map[string]interface{} {
	// 生成组合标签字段，便于快速过滤
	combinedTags := w.generateCombinedTags(holding)

	// 生成token hash用于routing
	tokenHash := w.generateTokenHash(holding.TokenAddress)

	doc := map[string]interface{}{
		"chain_id":               holding.ChainID,
		"wallet_address":         holding.WalletAddress,
		"token_address":          holding.TokenAddress,
		"token_hash":             tokenHash,
		"token_icon":             holding.TokenIcon,
		"token_name":             holding.TokenName,
		"amount":                 holding.Amount,
		"value_usd":              holding.ValueUSD,
		"unrealized_profits":     holding.UnrealizedProfits,
		"pnl":                    holding.PNL,
		"pnl_percentage":         holding.PNLPercentage,
		"avg_price":              holding.AvgPrice,
		"current_total_cost":     holding.CurrentTotalCost,
		"marketcap":              holding.MarketCap,
		"is_cleared":             holding.IsCleared,
		"is_dev":                 holding.IsDev,
		"tags":                   holding.Tags,
		"wallet_tags_combined":   combinedTags,
		"historical_buy_amount":  holding.HistoricalBuyAmount,
		"historical_sell_amount": holding.HistoricalSellAmount,
		"historical_buy_cost":    holding.HistoricalBuyCost,
		"historical_sell_value":  holding.HistoricalSellValue,
		"historical_buy_count":   holding.HistoricalBuyCount,
		"historical_sell_count":  holding.HistoricalSellCount,
		"updated_at":             holding.UpdatedAt,
		"created_at":             holding.CreatedAt,
	}

	if holding.PositionOpenedAt != nil {
		doc["position_opened_at"] = time.UnixMilli(*holding.PositionOpenedAt)
	}

	if holding.LastTransactionTime > 0 {
		doc["last_transaction_time"] = time.UnixMilli(holding.LastTransactionTime)
	}

	return doc
}

// generateCombinedTags 生成组合标签，便于快速查询
func (w *ESHoldingWriter) generateCombinedTags(holding *model.WalletHolding) []string {
	var combined []string

	// 添加基础标签
	if !holding.IsCleared {
		combined = append(combined, "active")
	} else {
		combined = append(combined, "cleared")
	}

	// 添加价值区间标签
	if holding.ValueUSD >= 100000 {
		combined = append(combined, "whale")
	} else if holding.ValueUSD >= 10000 {
		combined = append(combined, "big_holder")
	} else if holding.ValueUSD >= 1000 {
		combined = append(combined, "medium_holder")
	} else {
		combined = append(combined, "small_holder")
	}

	// 添加原始标签
	combined = append(combined, holding.Tags...)

	return combined
}

func (w *ESHoldingWriter) generateTokenHash(tokenAddress string) string {
	hash := md5.Sum([]byte(tokenAddress))
	return fmt.Sprintf("%x", hash)[:8]
}
