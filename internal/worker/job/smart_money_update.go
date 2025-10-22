package job

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	walletwriter "web3-smart/internal/worker/writer/wallet"

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

	// 初始化 ES 異步批量寫入器（若 ES 可用）
	var esAsync *writer.AsyncBatchWriter[model.WalletSummary]
	if esClient := j.repo.GetElasticsearchClient(); esClient != nil && j.Cfg.Elasticsearch.WalletsIndexName != "" {
		esWriter := walletwriter.NewESWalletWriter(esClient, j.logger, j.Cfg.Elasticsearch.WalletsIndexName)
		esAsync = writer.NewAsyncBatchWriter[model.WalletSummary](j.logger, esWriter, 1000, 300*time.Millisecond, "wallet_es_writer", 3)
		esAsync.Start(ctx)
		defer esAsync.Close()
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
			if esDoc, err := j.updateOneWallet(ctx, db, &w); err != nil {
				j.logger.Error("update wallet failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
			} else if esAsync != nil && esDoc != nil {
				esAsync.Submit(*esDoc)
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

// getTokenPriceUSD 透過已初始化的 ES Client 查詢 token 價格（走 SDK，避免手寫 HTTP 與額外 config 差異）
func (j *SmartMoneyAnalyzer) getTokenPriceUSD(ctx context.Context, tokenAddress string) (float64, error) {
	es := j.repo.GetElasticsearchClient()
	if es == nil {
		return 0, fmt.Errorf("es client not initialized")
	}
	// 盡量使用 routing=tokenAddress，並兼容多欄位匹配
	query := map[string]any{
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
		"_source": []string{"price_usd"},
	}
	index := "web3_tokens"
	res, err := es.SearchWithRouting(ctx, index, tokenAddress, query)
	if err != nil {
		return 0, err
	}
	if len(res.Hits.Hits) == 0 {
		return 0, fmt.Errorf("price not found")
	}
	src := res.Hits.Hits[0].Source
	if v, ok := src["price_usd"].(float64); ok {
		return v, nil
	}
	return 0, fmt.Errorf("price_usd not found")
}

// getTokenMeta 從 ES 取得 token 名稱與圖示（若不存在則回傳空字串）
func (j *SmartMoneyAnalyzer) getTokenMeta(ctx context.Context, tokenAddress string) (name string, icon string) {
	es := j.repo.GetElasticsearchClient()
	if es == nil {
		return "", ""
	}
	query := map[string]any{
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
		"_source": []string{"name", "logo"},
	}
	index := "web3_tokens"
	res, err := es.SearchWithRouting(ctx, index, tokenAddress, query)
	if err != nil {
		return "", ""
	}
	if len(res.Hits.Hits) == 0 {
		return "", ""
	}
	src := res.Hits.Hits[0].Source
	if v, ok := src["name"].(string); ok {
		name = v
	}
	if v, ok := src["logo"].(string); ok {
		icon = v
	}
	return name, icon
}

func (j *SmartMoneyAnalyzer) updateOneWallet(ctx context.Context, db *gorm.DB, w *model.WalletSummary) (*model.WalletSummary, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
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
		return nil, err
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
		// 改為比例 0-1，避免寫入 DECIMAL(5,4) 溢出
		s30.winRate = float64(wins) / float64(s30.sellNum)
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
		s7.winRate = float64(wins) / float64(s7.sellNum)
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
		s1.winRate = float64(wins) / float64(s1.sellNum)
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
		s30.pnlPct = clampPercentage((s30.pnl / s30.totalCost) * 100)
	}
	if s7.totalCost > 0 {
		s7.pnlPct = clampPercentage((s7.pnl / s7.totalCost) * 100)
	}
	if s1.totalCost > 0 {
		s1.pnlPct = clampPercentage((s1.pnl / s1.totalCost) * 100)
	}

	// 以當前幣價計算未實現盈虧（仍依時間窗切分）。價格透過 ES 查詢，並在單錢包內做簡單快取
	tokenPriceCache := map[string]float64{}
	getPrice := func(token string) float64 {
		if v, ok := tokenPriceCache[token]; ok {
			return v
		}
		p, err := j.getTokenPriceUSD(ctx, token)
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

	// 分布統計：按 token 聚合成本與實現損益，計算每個 token 的 pnl% 並分桶（百分比單位）
	type distRes struct {
		gt500           int
		between200to500 int
		between0to200   int
		n50to0          int
		lt50            int
		pGt500          float64
		p200to500       float64
		p0to200         float64
		pN50to0         float64
		pLt50           float64
	}
	computeDist := func(startTs, endTs int64) distRes {
		type agg struct{ cost, pnl float64 }
		tokens := map[string]*agg{}
		for _, t := range txs {
			if t.TransactionTime < startTs || t.TransactionTime > endTs {
				continue
			}
			txType := strings.ToLower(t.TransactionType)
			if txType == "buy" || txType == "build" {
				if tokens[t.TokenAddress] == nil {
					tokens[t.TokenAddress] = &agg{}
				}
				tokens[t.TokenAddress].cost += t.Value
			} else if txType == "sell" || txType == "clean" {
				if tokens[t.TokenAddress] == nil {
					tokens[t.TokenAddress] = &agg{}
				}
				tokens[t.TokenAddress].pnl += t.RealizedProfit
			}
		}
		var res distRes
		total := 0
		for _, a := range tokens {
			if a.cost <= 0 {
				continue
			}
			total++
			pnlPct := (a.pnl / a.cost) * 100
			if pnlPct > 500 {
				res.gt500++
			} else if pnlPct >= 200 && pnlPct <= 500 {
				res.between200to500++
			} else if pnlPct >= 0 && pnlPct < 200 {
				res.between0to200++
			} else if pnlPct >= -50 && pnlPct < 0 {
				res.n50to0++
			} else if pnlPct < -50 {
				res.lt50++
			}
		}
		if total > 0 {
			fTotal := float64(total)
			res.pGt500 = float64(res.gt500) / fTotal
			res.p200to500 = float64(res.between200to500) / fTotal
			res.p0to200 = float64(res.between0to200) / fTotal
			res.pN50to0 = float64(res.n50to0) / fTotal
			res.pLt50 = float64(res.lt50) / fTotal
		}
		return res
	}

	d30 := computeDist(ts30d, tsNow)
	d7 := computeDist(ts7d, tsNow)

	// 產出 30 天日線 PNL 字串（從昨天往回 30 天）
	pnlPic30d := buildPNLPic30(dailyPNL, now)

	// 最近三個不重複 token（從最新開始）
	recent := lastNDistinct(recentTokens, 3)
	type tokenItem struct {
		TokenAddress string `json:"token_address"`
		TokenIcon    string `json:"token_icon"`
		TokenName    string `json:"token_name"`
	}
	items := make([]tokenItem, 0, len(recent))
	for _, addr := range recent {
		name, icon := j.getTokenMeta(ctx, addr)
		items = append(items, tokenItem{
			TokenAddress: addr,
			TokenIcon:    icon,
			TokenName:    name,
		})
	}
	tokenListBytes, _ := json.Marshal(items)
	tokenList := string(tokenListBytes)

	// 是否視為活躍/聰明錢（與 python 規則一致）
	isSmart := s30.pnl > 0 && s30.winRate > 0.3 && s30.winRate != 1 && s30.totalTx < 2000 && lastTxTime >= ts30d

	// j.logger.Info("is_active decision",
	// 	zap.String("wallet", w.WalletAddress),
	// 	zap.Float64("pnl_30d", s30.pnl),
	// 	zap.Float64("pnl_pct_30d", s30.pnlPct),
	// 	zap.Int("total_tx_30d", s30.totalTx),
	// 	zap.Int("buy_num_30d", s30.buyNum),
	// 	zap.Int("sell_num_30d", s30.sellNum),
	// 	zap.Float64("win_rate_30d", s30.winRate),
	// 	zap.Int64("last_tx_time", lastTxTime),
	// 	zap.Int64("ts30d", ts30d),
	// 	zap.Bool("cond_pnl_gt0", s30.pnl > 0),
	// 	zap.Bool("cond_winrate_gt_0_3", s30.winRate > 0.3),
	// 	zap.Bool("cond_winrate_not_1", s30.winRate != 1),
	// 	zap.Bool("cond_total_tx_lt_2000", s30.totalTx < 2000),
	// 	zap.Bool("cond_last_tx_in_30d", lastTxTime >= ts30d),
	// 	zap.Bool("is_active", isSmart),
	// )

	// 僅在「近30天有交易」時才寫入統計類欄位；避免把原本有值的欄位覆蓋為 0
	if len(txs) > 0 {
		// 計算五條件判斷結果（基於本次統計數據）
		winRatePct := s30.winRate * 100
		condWinRate := winRatePct > 60
		condTx7d := s7.totalTx > 100
		condPNL30d := s30.pnl > 1000
		condPNLPct := s30.pnlPct > 100
		condDist := (d30.pLt50 * 100) < 30
		passedClassifier := condWinRate && condTx7d && condPNL30d && condPNLPct && condDist

		// 若當前 tags 不包含 smart money，且通過五條件，追加標籤
		if !hasSmartMoneyTag(w.Tags) && passedClassifier {
			newTags := append([]string{}, w.Tags...)
			newTags = append(newTags, "smart money")
			updates["tags"] = newTags
		}
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

		// 分布統計 - 30d（比例以 0-100 儲存）
		updates["distribution_gt500_30d"] = d30.gt500
		updates["distribution_200to500_30d"] = d30.between200to500
		updates["distribution_0to200_30d"] = d30.between0to200
		updates["distribution_n50to0_30d"] = d30.n50to0
		updates["distribution_lt50_30d"] = d30.lt50
		updates["distribution_gt500_percentage_30d"] = d30.pGt500 * 100
		updates["distribution_200to500_percentage_30d"] = d30.p200to500 * 100
		updates["distribution_0to200_percentage_30d"] = d30.p0to200 * 100
		updates["distribution_n50to0_percentage_30d"] = d30.pN50to0 * 100
		updates["distribution_lt50_percentage_30d"] = d30.pLt50 * 100

		// 分布統計 - 7d（比例以 0-100 儲存）
		updates["distribution_gt500_7d"] = d7.gt500
		updates["distribution_200to500_7d"] = d7.between200to500
		updates["distribution_0to200_7d"] = d7.between0to200
		updates["distribution_n50to0_7d"] = d7.n50to0
		updates["distribution_lt50_7d"] = d7.lt50
		updates["distribution_gt500_percentage_7d"] = d7.pGt500 * 100
		updates["distribution_200to500_percentage_7d"] = d7.p200to500 * 100
		updates["distribution_0to200_percentage_7d"] = d7.p0to200 * 100
		updates["distribution_n50to0_percentage_7d"] = d7.pN50to0 * 100
		updates["distribution_lt50_percentage_7d"] = d7.pLt50 * 100

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

		// 狀態：若已有 smart money 標籤但本次未通過五條件，強制 false
		isActive := isSmart
		if hasSmartMoneyTag(w.Tags) && !passedClassifier {
			isActive = false
		}
		updates["is_active"] = isActive
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
			if price, err := j.getTokenPriceUSD(ctx, "So11111111111111111111111111111111111111112"); err == nil {
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
		return nil, err
	}

	// 構建 ES 文檔對應的 WalletSummary（儘量以本次計算值為準）
	es := &model.WalletSummary{
		WalletAddress:   w.WalletAddress,
		Avatar:          w.Avatar,
		ChainID:         w.ChainID,
		TwitterName:     w.TwitterName,
		TwitterUsername: w.TwitterUsername,
		WalletType:      w.WalletType,
		CreatedAt:       w.CreatedAt,
	}

	// 平衡：若本次拿到餘額則用新值，否則沿用舊值
	if v, ok := updates["balance"]; ok {
		if f, ok2 := v.(float64); ok2 {
			es.Balance = f
		}
	} else {
		es.Balance = w.Balance
	}
	if v, ok := updates["balance_usd"]; ok {
		if f, ok2 := v.(float64); ok2 {
			es.BalanceUSD = f
		}
	} else {
		es.BalanceUSD = w.BalanceUSD
	}

	// 標籤
	if v, ok := updates["tags"]; ok {
		if arr, ok2 := v.([]string); ok2 {
			es.Tags = arr
		} else {
			es.Tags = w.Tags
		}
	} else {
		es.Tags = w.Tags
	}

	// 指標 - 僅在本次有聚合時更新，否則保留原值
	if len(txs) > 0 {
		es.TokenList = tokenList
		es.AssetMultiple = pctToMultiple(s30.pnlPct)
		es.LastTransactionTime = lastTxTime

		es.AvgCost30d = s30.avgCost
		es.BuyNum30d = s30.buyNum
		es.SellNum30d = s30.sellNum
		es.WinRate30d = s30.winRate
		es.PNL30d = s30.pnl
		es.PNLPercentage30d = s30.pnlPct
		es.PNLPic30d = pnlPic30d
		es.UnrealizedProfit30d = unreal30d
		es.TotalCost30d = s30.totalCost
		es.AvgRealizedProfit30d = avgRealized(s30.pnl, s30.sellNum)

		es.AvgCost7d = s7.avgCost
		es.BuyNum7d = s7.buyNum
		es.SellNum7d = s7.sellNum
		es.WinRate7d = s7.winRate
		es.PNL7d = s7.pnl
		es.PNLPercentage7d = s7.pnlPct
		es.UnrealizedProfit7d = unreal7d
		es.TotalCost7d = s7.totalCost
		es.AvgRealizedProfit7d = avgRealized(s7.pnl, s7.sellNum)

		es.AvgCost1d = s1.avgCost
		es.BuyNum1d = s1.buyNum
		es.SellNum1d = s1.sellNum
		es.WinRate1d = s1.winRate
		es.PNL1d = s1.pnl
		es.PNLPercentage1d = s1.pnlPct
		es.UnrealizedProfit1d = unreal1d
		es.TotalCost1d = s1.totalCost
		es.AvgRealizedProfit1d = avgRealized(s1.pnl, s1.sellNum)

		es.DistributionGt500_30d = d30.gt500
		es.Distribution200to500_30d = d30.between200to500
		es.Distribution0to200_30d = d30.between0to200
		es.DistributionN50to0_30d = d30.n50to0
		es.DistributionLt50_30d = d30.lt50
		es.DistributionGt500Percentage30d = d30.pGt500 * 100
		es.Distribution200to500Percentage30d = d30.p200to500 * 100
		es.Distribution0to200Percentage30d = d30.p0to200 * 100
		es.DistributionN50to0Percentage30d = d30.pN50to0 * 100
		es.DistributionLt50Percentage30d = d30.pLt50 * 100

		es.DistributionGt500_7d = d7.gt500
		es.Distribution200to500_7d = d7.between200to500
		es.Distribution0to200_7d = d7.between0to200
		es.DistributionN50to0_7d = d7.n50to0
		es.DistributionLt50_7d = d7.lt50
		es.DistributionGt500Percentage7d = d7.pGt500 * 100
		es.Distribution200to500Percentage7d = d7.p200to500 * 100
		es.Distribution0to200Percentage7d = d7.p0to200 * 100
		es.DistributionN50to0Percentage7d = d7.pN50to0 * 100
		es.DistributionLt50Percentage7d = d7.pLt50 * 100

		if v, ok := updates["is_active"]; ok {
			if b, ok2 := v.(bool); ok2 {
				es.IsActive = b
			} else {
				es.IsActive = w.IsActive
			}
		} else {
			es.IsActive = w.IsActive
		}
	} else {
		// 無聚合時保留原值
		*es = *w
	}

	es.UpdatedAt = time.Now()

	return es, nil
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

// 將百分比值裁剪在資料庫欄位 DECIMAL(8,4) 可容納的範圍內
func clampPercentage(value float64) float64 {
	const max = 9999.9999
	const min = -9999.9999
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value > max {
		return max
	}
	if value < min {
		return min
	}
	return value
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

// 移除舊的 HTTP 直連 ES 查價函式，統一走 SDK 的 SearchWithRouting
