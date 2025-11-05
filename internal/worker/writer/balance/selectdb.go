package balance

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	selectdbclient "web3-smart/pkg/selectdb_client"

	"github.com/bytedance/sonic"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SelectDBBalanceWriter struct {
	selectDBClient     *gorm.DB
	selectDBHttpClient *selectdbclient.Client
	tl                 *zap.Logger
}

func NewSelectDBBalanceWriter(client *selectdbclient.Client, logger *zap.Logger) writer.BatchWriter[model.Balance] {
	return &SelectDBBalanceWriter{
		selectDBHttpClient: client,
		tl:                 logger,
	}
}

func (sw *SelectDBBalanceWriter) BWrite(ctx context.Context, balances []model.Balance) error {
	if len(balances) == 0 {
		return nil
	}
	start := time.Now()

	// 1️⃣ 去重（按 network+token_address+token_account）
	balances = deduplicateBalances(balances)

	newCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const batchSize = 3000
	const retryCount = 3

	successBatch := make(map[int]bool)
	for range retryCount {
		// 分批插入
		for i := 0; i < len(balances); i += batchSize {
			if b, ok := successBatch[i]; ok && b {
				continue
			}
			end := min(i+batchSize, len(balances))
			batch := balances[i:end]

			data, err := sonic.Marshal(batch)
			if err == nil {
				err = sw.selectDBHttpClient.StreamLoadBalance(newCtx, bytes.NewBuffer(data), selectdbclient.StreamLoadOptions{Table: "balance", Format: "json"})
			}
			// sql := buildInsertSQL(batch)
			// err = sw.selectDBClient.WithContext(newCtx).Exec(sql).Error
			if err != nil {
				sw.tl.Warn("❌ SelectDB insert failed", zap.Error(err))
				successBatch[i] = false
				continue
			} else {
				if time.Now().UnixMilli()%100 == 0 {
					sw.tl.Info("✅ SelectDB insert success", zap.Any("balances", batch))
				}
			}
			successBatch[i] = true
		}
		if checkAllDown(successBatch) {
			break
		}

		// 可选短暂延迟
		time.Sleep(100 * time.Millisecond)
	}

	sw.tl.Info("✅ DB write success", zap.Int("len", len(balances)), zap.Duration("cost", time.Since(start)))
	return nil
}

func buildInsertSQL(batch []model.Balance) string {
	sql := `INSERT INTO balance
(network, token_address, wallet, token_account, amount, decimal, block_number, version, updated_at)
VALUES `

	valuesList := []string{}
	for _, b := range batch {
		values := fmt.Sprintf("(%s, %s, %s, %s, %s, %d, %d, %d, '%s')",
			quote(b.Network),
			quote(b.TokenAddress),
			quote(b.Wallet),
			quote(b.TokenAccount),
			b.Amount,
			b.Decimal,
			b.BlockNumber,
			b.Version,
			b.UpdatedAt,
		)
		valuesList = append(valuesList, values)
	}

	sql += strings.Join(valuesList, ",")
	return sql
}

func quote(s string) string {
	s = strings.ReplaceAll(s, `'`, `''`) // 单引号转义
	return fmt.Sprintf("'%s'", s)
}

func checkAllDown(m map[int]bool) bool {
	for _, v := range m {
		if !v {
			return false
		}
	}
	return true
}

func (sw *SelectDBBalanceWriter) Close() error {
	return nil
}
