package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"io"

	"github.com/gagliardetto/solana-go"
	rpcsol "github.com/gagliardetto/solana-go/rpc"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SmartMoneyAnalyzer 依據 t_smart_transaction 聚合並回寫 t_smart_wallet 指標
type SmartMoneyAnalyzer struct {
	repo   repository.Repository
	logger *zap.Logger
	Cfg    config.Config
}

func NewSmartMoneyAnalyzer(repo repository.Repository, logger *zap.Logger) *SmartMoneyAnalyzer {
	return &SmartMoneyAnalyzer{
		repo:   repo,
		logger: logger,
	}
}

// Run 掃描全部錢包（按鏈分批），為每個錢包統計近 30/7/1 天指標後更新
func (j *SmartMoneyAnalyzer) Run(ctx context.Context) error {
	db := j.repo.GetDB()
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	// 取出所有錢包（限制每批處理數量，避免一次載入過大）
	const pageSize = 500
	var page int64 = 0
	var processed int64 = 0
	start := time.Now()
	j.logger.Info("smart_money_analyze start")

	for {
		if ctx.Err() != nil {
			j.logger.Warn("smart_money_analyze cancelled", zap.Error(ctx.Err()))
			return ctx.Err()
		}
		var wallets []model.WalletSummary
		tx := db.WithContext(ctx).Limit(pageSize).Offset(int(page * pageSize)).Order("id ASC").Find(&wallets)
		if tx.Error != nil {
			return tx.Error
		}
		if len(wallets) == 0 {
			break
		}

		j.logger.Info("smart_money_analyze page fetched", zap.Int64("page", page), zap.Int("count", len(wallets)))

		for _, w := range wallets {
			// 可選：只處理近 30 天有交易的錢包，減少計算
			if err := j.updateOneWallet(ctx, db, &w); err != nil {
				j.logger.Error("update wallet failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
			}
			processed++
			if processed%200 == 0 {
				j.logger.Info("smart_money_analyze progress", zap.Int64("processed", processed), zap.Duration("elapsed", time.Since(start)))
			}
			if ctx.Err() != nil {
				j.logger.Warn("smart_money_analyze cancelled mid-run", zap.Error(ctx.Err()))
				return ctx.Err()
			}
		}

		page++
	}
	j.logger.Info("smart_money_analyze done", zap.Int64("processed", processed), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (j *SmartMoneyAnalyzer) updateOneWallet(ctx context.Context, db *gorm.DB, w *model.WalletSummary) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// 時間邊界
	now := time.Now()
	tsNow := now.Unix()
	ts1d := now.Add(-24 * time.Hour).Unix()
	ts7d := now.Add(-7 * 24 * time.Hour).Unix()
	ts30d := now.Add(-30 * 24 * time.Hour).Unix()

	// 取近 30 天的全部交易（一次取回，內存聚合）
	var txs []model.WalletTransaction
	if err := db.WithContext(ctx).
		Where("wallet_address = ? AND chain_id = ? AND transaction_time >= ?", w.WalletAddress, w.ChainID, ts30d).
		Order("transaction_time ASC").
		Find(&txs).Error; err != nil {
		return err
	}

	// 構建更新欄位，先放置「一定要更新」的欄位，例如餘額
	updates := map[string]any{}

	// 取得 Solana 錢包餘額（僅針對 chain_id=501）
	if w.ChainID == 501 {
		if client := j.repo.GetSolanaClient(); client != nil {
			if pub, err := solana.PublicKeyFromBase58(w.WalletAddress); err == nil {
				if balRes, err := client.GetBalance(ctx, pub, rpcsol.CommitmentFinalized); err == nil {
					// lamports -> SOL
					updates["balance"] = float64(balRes.Value) / 1e9
				}
			}
		}
	}

	// 按窗口聚合
	type stats struct {
		avgCost   float64
		totalTx   int
		buyNum    int
		sellNum   int
		winRate   float64
		pnl       float64
		pnlPct    float64
		totalCost float64
	}
	s30 := stats{}
	s7 := stats{}
	s1 := stats{}

	// PNL 日線圖（只針對 30 天，賣出/清倉視為實現損益）
	dailyPNL := map[string]float64{}

	// 最近交易 token（最多三個，按最近時間）
	// 先蒐集後再去重
	var recentTokens []string

	lastTxTime := w.LastTransactionTime

	for _, t := range txs {
		tt := t.TransactionTime
		if tt > lastTxTime {
			lastTxTime = tt
		}

		txType := strings.ToLower(t.TransactionType)
		isBuy := txType == "buy" || txType == "build"
		isSell := txType == "sell" || txType == "clean"

		// 最近 token 收集（按照時間順序，最後再從尾端取三個不重複）
		recentTokens = append(recentTokens, t.TokenAddress)

		// 30d 聚合
		if tt >= ts30d && tt <= tsNow {
			s30.totalTx++
			if isBuy {
				s30.buyNum++
				s30.totalCost += t.Value
			} else if isSell {
				s30.sellNum++
				s30.pnl += t.RealizedProfit
			}

			// 日線 PNL（僅賣出/清倉記錄）
			if isSell {
				day := time.Unix(tt, 0).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02")
				dailyPNL[day] += t.RealizedProfit
			}
		}

		// 7d 聚合
		if tt >= ts7d && tt <= tsNow {
			s7.totalTx++
			if isBuy {
				s7.buyNum++
				s7.totalCost += t.Value
			} else if isSell {
				s7.sellNum++
				s7.pnl += t.RealizedProfit
			}
		}

		// 1d 聚合
		if tt >= ts1d && tt <= tsNow {
			s1.totalTx++
			if isBuy {
				s1.buyNum++
				s1.totalCost += t.Value
			} else if isSell {
				s1.sellNum++
				s1.pnl += t.RealizedProfit
			}
		}
	}

	// 勝率（僅賣出/清倉）
	if s30.sellNum > 0 {
		// 近似：把 RealizedProfit > 0 視為獲勝次數
		var wins int
		for _, t := range txs {
			if t.TransactionTime >= ts30d && t.TransactionTime <= tsNow {
				tp := strings.ToLower(t.TransactionType)
				if (tp == "sell" || tp == "clean") && t.RealizedProfit > 0 {
					wins++
				}
			}
		}
		s30.winRate = float64(wins) / float64(s30.sellNum) * 100
	}
	if s7.sellNum > 0 {
		var wins int
		for _, t := range txs {
			if t.TransactionTime >= ts7d && t.TransactionTime <= tsNow {
				tp := strings.ToLower(t.TransactionType)
				if (tp == "sell" || tp == "clean") && t.RealizedProfit > 0 {
					wins++
				}
			}
		}
		s7.winRate = float64(wins) / float64(s7.sellNum) * 100
	}
	if s1.sellNum > 0 {
		var wins int
		for _, t := range txs {
			if t.TransactionTime >= ts1d && t.TransactionTime <= tsNow {
				tp := strings.ToLower(t.TransactionType)
				if (tp == "sell" || tp == "clean") && t.RealizedProfit > 0 {
					wins++
				}
			}
		}
		s1.winRate = float64(wins) / float64(s1.sellNum) * 100
	}

	// 平均成本
	if s30.buyNum > 0 {
		s30.avgCost = s30.totalCost / float64(s30.buyNum)
	}
	if s7.buyNum > 0 {
		s7.avgCost = s7.totalCost / float64(s7.buyNum)
	}
	if s1.buyNum > 0 {
		s1.avgCost = s1.totalCost / float64(s1.buyNum)
	}

	// PNL 百分比（以期間買入總成本為分母）
	if s30.totalCost > 0 {
		s30.pnlPct = s30.pnl / s30.totalCost * 100
	}
	if s7.totalCost > 0 {
		s7.pnlPct = s7.pnl / s7.totalCost * 100
	}
	if s1.totalCost > 0 {
		s1.pnlPct = s1.pnl / s1.totalCost * 100
	}

	// 以當前幣價計算未實現盈虧（仍依時間窗切分）。價格透過 ES 查詢，並在單錢包內做簡單快取
	tokenPriceCache := map[string]float64{}
	getPrice := func(token string) float64 {
		if v, ok := tokenPriceCache[token]; ok {
			return v
		}
		p, err := getTokenPriceUSDWithCfg(ctx, j.Cfg, token)
		if err != nil {
			j.logger.Warn("es price fetch failed", zap.String("token", token), zap.Error(err))
			return 0
		}
		if p == 0 {
			j.logger.Debug("es price zero", zap.String("token", token))
		}
		tokenPriceCache[token] = p
		return p
	}
	calcUnrealized := func(startTs, endTs int64) float64 {
		hold := map[string]float64{}
		for _, t := range txs {
			if t.TransactionTime < startTs || t.TransactionTime > endTs {
				continue
			}
			txType := strings.ToLower(t.TransactionType)
			if txType == "buy" || txType == "build" {
				hold[t.TokenAddress] += t.Amount
			} else if txType == "sell" || txType == "clean" {
				hold[t.TokenAddress] -= t.Amount
				if hold[t.TokenAddress] < 0 {
					hold[t.TokenAddress] = 0
				}
			}
		}
		var total float64
		for token, remain := range hold {
			if remain <= 0 {
				continue
			}
			price := getPrice(token)
			total += remain * price
		}
		return total
	}
	unreal1d := calcUnrealized(ts1d, tsNow)
	unreal7d := calcUnrealized(ts7d, tsNow)
	unreal30d := calcUnrealized(ts30d, tsNow)

	// 產出 30 天日線 PNL 字串（從昨天往回 30 天）
	pnlPic30d := buildPNLPic30(dailyPNL, now)

	// 最近三個不重複 token（從最新開始）
	recent := lastNDistinct(recentTokens, 3)
	tokenList := strings.Join(recent, ",")

	// 是否視為活躍/聰明錢（與 python 規則一致）
	isSmart := s30.pnl > 0 && s30.winRate > 30 && s30.winRate != 100 && s30.totalTx < 2000 && lastTxTime >= ts30d

	// 僅在「近30天有交易」時才寫入統計類欄位；避免把原本有值的欄位覆蓋為 0
	if len(txs) > 0 {
		updates["token_list"] = tokenList
		updates["asset_multiple"] = pctToMultiple(s30.pnlPct)
		updates["last_transaction_time"] = lastTxTime

		// 30d
		updates["avg_cost_30d"] = s30.avgCost
		updates["total_transaction_num_30d"] = s30.totalTx
		updates["buy_num_30d"] = s30.buyNum
		updates["sell_num_30d"] = s30.sellNum
		updates["win_rate_30d"] = s30.winRate
		updates["pnl_30d"] = s30.pnl
		updates["pnl_percentage_30d"] = s30.pnlPct
		updates["pnl_pic_30d"] = pnlPic30d
		updates["unrealized_profit_30d"] = unreal30d
		updates["total_cost_30d"] = s30.totalCost
		updates["avg_realized_profit_30d"] = avgRealized(s30.pnl, s30.sellNum)

		// 7d
		updates["avg_cost_7d"] = s7.avgCost
		updates["total_transaction_num_7d"] = s7.totalTx
		updates["buy_num_7d"] = s7.buyNum
		updates["sell_num_7d"] = s7.sellNum
		updates["win_rate_7d"] = s7.winRate
		updates["pnl_7d"] = s7.pnl
		updates["pnl_percentage_7d"] = s7.pnlPct
		updates["unrealized_profit_7d"] = unreal7d
		updates["total_cost_7d"] = s7.totalCost
		updates["avg_realized_profit_7d"] = avgRealized(s7.pnl, s7.sellNum)

		// 1d
		updates["avg_cost_1d"] = s1.avgCost
		updates["total_transaction_num_1d"] = s1.totalTx
		updates["buy_num_1d"] = s1.buyNum
		updates["sell_num_1d"] = s1.sellNum
		updates["win_rate_1d"] = s1.winRate
		updates["pnl_1d"] = s1.pnl
		updates["pnl_percentage_1d"] = s1.pnlPct
		updates["unrealized_profit_1d"] = unreal1d
		updates["total_cost_1d"] = s1.totalCost
		updates["avg_realized_profit_1d"] = avgRealized(s1.pnl, s1.sellNum)

		// 狀態
		updates["is_active"] = isSmart
	}

	// 計算 balance_usd：balance 為 0 → 直接 0；否則使用 ES 的 SOL 價格換算
	{
		var bal float64
		if v, ok := updates["balance"]; ok {
			if f, ok2 := v.(float64); ok2 {
				bal = f
			}
		} else {
			bal = w.Balance
		}
		if bal == 0 {
			updates["balance_usd"] = 0.0
		} else if w.ChainID == 501 {
			if price, err := getTokenPriceUSDWithCfg(ctx, j.Cfg, "So11111111111111111111111111111111111111112"); err == nil {
				updates["balance_usd"] = bal * price
			} else {
				j.logger.Warn("es price for SOL failed", zap.Error(err))
			}
		}
	}

	// 最後更新時間無論如何刷新，代表本次任務已跑過；如果你希望「僅變化才更新」，可以再細化為 dirty 檢查
	updates["updated_at"] = time.Now()

	if err := db.WithContext(ctx).Model(&model.WalletSummary{}).
		Where("wallet_address = ?", w.WalletAddress).
		Updates(updates).Error; err != nil {
		return err
	}

	return nil
}

func avgRealized(pnl float64, sellNum int) float64 {
	if sellNum <= 0 {
		return 0
	}
	return pnl / float64(sellNum)
}

func pctToMultiple(pct float64) float64 {
	// (pnl_percentage_30d / 100) + 1
	return pct/100 + 1
}

func buildPNLPic30(daily map[string]float64, now time.Time) string {
	// 產出 30 個數（從昨天往回），缺漏補 0
	tz := time.FixedZone("CST", 8*3600)
	end := time.Date(now.In(tz).Year(), now.In(tz).Month(), now.In(tz).Day(), 0, 0, 0, 0, tz)
	res := make([]string, 0, 30)
	cur := end.Add(-24 * time.Hour)
	for i := 0; i < 30; i++ {
		day := cur.Format("2006-01-02")
		val := daily[day]
		res = append(res, fmt.Sprintf("%0.2f", val))
		cur = cur.Add(-24 * time.Hour)
	}
	return strings.Join(res, ",")
}

func lastNDistinct(tokens []string, n int) []string {
	// 從尾端（最新）向前去重
	if len(tokens) == 0 || n <= 0 {
		return nil
	}
	// 先以時間順序加入，取最後的順序再去重
	// 這裡 tokens 已按交易時間 asc 蒐集，所以我們從尾端往回
	seen := map[string]struct{}{}
	var res []string
	for i := len(tokens) - 1; i >= 0 && len(res) < n; i-- {
		t := tokens[i]
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		res = append(res, t)
	}
	// 目前是從最新往舊的加入，保留原順序（最新在前）
	// 若需要由新到舊即可直接返回；若需要由舊到新可反轉。
	// 這裡維持由新到舊。
	return res
}

// getTokenPriceUSDWithCfg: 查 ES _source.price_usd（容錯加強，擴充多字段匹配）
func getTokenPriceUSDWithCfg(ctx context.Context, cfg config.Config, tokenAddress string) (float64, error) {
	baseURL := strings.TrimRight(cfg.ES.BaseURL, "/")
	if baseURL == "" {
		return 0, fmt.Errorf("ES base_url not set")
	}
	index := cfg.ES.Index
	if index == "" {
		index = "web3_tokens"
	}
	url := baseURL + "/" + index + "/_search"

	payload := map[string]any{
		"size": 1,
		"query": map[string]any{
			"bool": map[string]any{
				"should": []any{
					map[string]any{"term": map[string]any{"address.keyword": tokenAddress}},
					map[string]any{"term": map[string]any{"address": tokenAddress}},
					map[string]any{"term": map[string]any{"address_normalized": tokenAddress}},
				},
				"minimum_should_match": 1,
			},
		},
	}
	buf, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.ES.Username != "" || cfg.ES.Password != "" {
		req.SetBasicAuth(cfg.ES.Username, cfg.ES.Password)
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		msg := string(b)
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return 0, fmt.Errorf("es status %d body=%s", resp.StatusCode, msg)
	}
	var data struct {
		Hits struct {
			Hits []struct {
				Source struct {
					PriceUSD float64 `json:"price_usd"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0, err
	}
	if len(data.Hits.Hits) == 0 {
		return 0, fmt.Errorf("price not found")
	}
	return data.Hits.Hits[0].Source.PriceUSD, nil
}
