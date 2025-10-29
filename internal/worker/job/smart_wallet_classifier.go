package job

import (
	"context"
	"strings"
	"time"

	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SmartWalletClassifier struct {
	repo   repository.Repository
	logger *zap.Logger
}

func NewSmartWalletClassifier(repo repository.Repository, logger *zap.Logger) *SmartWalletClassifier {
	return &SmartWalletClassifier{repo: repo, logger: logger}
}

func (j *SmartWalletClassifier) Run(ctx context.Context) error {
	db := j.repo.GetDB()
	if db == nil {
		return nil
	}

	const pageSize = 500
	var offset int
	processed := 0
	start := time.Now()
	j.logger.Info("smart_wallet_classifier start")
	for {
		if ctx.Err() != nil {
			j.logger.Warn("smart_wallet_classifier cancelled", zap.Error(ctx.Err()))
			return ctx.Err()
		}

		var wallets []model.WalletSummary
		tx := db.WithContext(ctx).
			Order("id ASC").
			Limit(pageSize).
			Offset(offset).
			Find(&wallets)
		if tx.Error != nil {
			return tx.Error
		}
		if len(wallets) == 0 {
			break
		}

		for i := range wallets {
			w := &wallets[i]
			if hasSmartMoneyTag(w.Tags) {
				j.logger.Info("smart_wallet_classifier tag check",
					zap.String("wallet", w.WalletAddress),
					zap.Any("tags", w.Tags),
					zap.Bool("is_smart_by_tag", true),
				)
				processed++
				continue
			}

			winRatePct := w.WinRate30d.Mul(decimal.NewFromInt(100)).InexactFloat64()
			condWinRate := winRatePct > 60
			condTx7d := (w.BuyNum7d + w.SellNum7d) > 100
			condPNL30d := w.PNL30d.GreaterThan(decimal.NewFromFloat(1000))
			condPNLPct := w.PNLPercentage30d.GreaterThan(decimal.NewFromFloat(100))
			condDist := w.DistributionLt50Percentage30d.LessThan(decimal.NewFromFloat(30))
			passed := condWinRate && condTx7d && condPNL30d && condPNLPct && condDist

			j.logger.Info("smart_wallet_classifier rule check",
				zap.String("wallet", w.WalletAddress),
				zap.Float64("win_rate_30d_pct", winRatePct),
				zap.Int("total_tx_7d", w.BuyNum7d+w.SellNum7d),
				zap.Float64("pnl_30d", w.PNL30d.InexactFloat64()),
				zap.Float64("pnl_pct_30d", w.PNLPercentage30d.InexactFloat64()),
				zap.Float64("dist_lt50_pct_30d", w.DistributionLt50Percentage30d.InexactFloat64()),
				zap.Bool("cond_win_rate_gt_60", condWinRate),
				zap.Bool("cond_tx7d_gt_100", condTx7d),
				zap.Bool("cond_pnl30d_gt_1000", condPNL30d),
				zap.Bool("cond_pnlpct30d_gt_100", condPNLPct),
				zap.Bool("cond_dist_lt50_lt_30", condDist),
				zap.Bool("passed", passed),
			)

			if passed {
				newTags := append([]string{}, w.Tags...)
				newTags = append(newTags, "smart money")
				if err := db.WithContext(ctx).Model(&model.WalletSummary{}).
					Where("wallet_address = ?", w.WalletAddress).
					Update("tags", newTags).Error; err != nil {
					j.logger.Error("append smart money tag failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
				} else {
					j.logger.Info("appended smart money tag", zap.String("wallet", w.WalletAddress))
				}
			}

			processed++
		}

		offset += len(wallets)
	}
	j.logger.Info("smart_wallet_classifier done", zap.Int("processed", processed), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func hasSmartMoneyTag(tags []string) bool {
	for _, t := range tags {
		if strings.EqualFold(strings.TrimSpace(t), "smart money") {
			return true
		}
	}
	return false
}

func (j *SmartWalletClassifier) shouldMarkSmart(_ context.Context, _ *gorm.DB, w *model.WalletSummary) (bool, error) {
	// 已改為基於 tags 決策，保留此函式以便未來擴充（目前不使用）
	return hasSmartMoneyTag(w.Tags), nil
}
