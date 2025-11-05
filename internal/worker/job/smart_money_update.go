package job

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	walletwriter "web3-smart/internal/worker/writer/wallet"

	"github.com/gagliardetto/solana-go"
	rpcsol "github.com/gagliardetto/solana-go/rpc"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

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

func (j *SmartMoneyAnalyzer) Run(ctx context.Context) error {
	db := j.repo.GetDB()
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	var esAsync *writer.AsyncBatchWriter[model.WalletSummary]
	if esClient := j.repo.GetElasticsearchClient(); esClient != nil && j.Cfg.Elasticsearch.WalletsIndexName != "" {
		esWriter := walletwriter.NewESWalletWriter(esClient, j.logger, j.Cfg.Elasticsearch.WalletsIndexName)
		// 提升 ES 批次、併發與 flush 頻率
		esAsync = writer.NewAsyncBatchWriter[model.WalletSummary](j.logger, esWriter, 2000, 150*time.Millisecond, "wallet_es_writer", 8)
		esAsync.Start(ctx)
		defer esAsync.Close()
	}

	const pageSize = 800
	const workersPerPage = 16
	var processed int64 = 0
	var lastID int64 = 0
	start := time.Now()
	j.logger.Info("smart_money_analyze start")

	for {
		if ctx.Err() != nil {
			j.logger.Warn("smart_money_analyze cancelled", zap.Error(ctx.Err()))
			return ctx.Err()
		}
		var wallets []model.WalletSummary
		// Keyset 分頁：避免大 Offset 退化
		tx := db.WithContext(ctx).Where("id > ?", lastID).Limit(pageSize).Order("id ASC").Find(&wallets)
		if tx.Error != nil {
			return tx.Error
		}
		if len(wallets) == 0 {
			break
		}

		j.logger.Info("smart_money_analyze page fetched", zap.Int("count", len(wallets)), zap.Int64("since_processed", atomic.LoadInt64(&processed)))

		// 併發處理當前頁的錢包
		jobs := make(chan *model.WalletSummary, len(wallets))
		var wg sync.WaitGroup
		worker := func() {
			defer wg.Done()
			for w := range jobs {
				if esDoc, err := j.updateOneWallet(ctx, db, w); err != nil {
					j.logger.Error("update wallet failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
				} else if esAsync != nil && esDoc != nil {
					esAsync.Submit(*esDoc, w.WalletAddress)
				}
				n := atomic.AddInt64(&processed, 1)
				if n%1000 == 0 {
					j.logger.Info("smart_money_analyze progress", zap.Int64("processed", n), zap.Duration("elapsed", time.Since(start)))
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		for i := 0; i < workersPerPage; i++ {
			wg.Add(1)
			go worker()
		}
		for i := range wallets {
			jobs <- &wallets[i]
		}
		close(jobs)
		wg.Wait()

		// 設定下一頁的起點
		lastID = wallets[len(wallets)-1].ID
	}
	elapsed := time.Since(start)
	var throughput float64
	if elapsed > 0 {
		throughput = float64(processed) / elapsed.Seconds()
	}
	var avgPerWalletMs float64
	if processed > 0 {
		avgPerWalletMs = float64(elapsed.Milliseconds()) / float64(processed)
	}
	// 觀察與 2 小時排程的關係
	twoHours := 2 * time.Hour
	percentOfTwoHours := (float64(elapsed) / float64(twoHours)) * 100
	j.logger.Info(
		"smart_money_analyze done",
		zap.Int64("processed", processed),
		zap.Duration("elapsed", elapsed),
		zap.Float64("throughput_wallets_per_sec", throughput),
		zap.Float64("avg_ms_per_wallet", avgPerWalletMs),
		zap.Float64("elapsed_vs_2h_percent", percentOfTwoHours),
	)
	return nil
}

func (j *SmartMoneyAnalyzer) getTokenPriceUSD(ctx context.Context, tokenAddress string) (float64, error) {
	es := j.repo.GetElasticsearchClient()
	if es == nil {
		return 0, fmt.Errorf("es client not initialized")
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
		"_source": []string{"price_usd"},
	}
	index := "web3_tokens"
	res, err := es.SearchWithRouting(ctx, index, tokenAddress, query)
	if err != nil {
		j.logger.Debug("token price search failed", zap.String("token", tokenAddress), zap.Error(err))
	}
	if err != nil || len(res.Hits.Hits) == 0 {
		if err == nil {
			j.logger.Debug("token price search miss with routing", zap.String("token", tokenAddress))
		}
		res, err = es.Search(ctx, index, query)
		if err != nil {
			j.logger.Error("token price fallback search failed", zap.String("token", tokenAddress), zap.Error(err))
			return 0, err
		}
		if len(res.Hits.Hits) == 0 {
			j.logger.Debug("token price not found", zap.String("token", tokenAddress))
			return 0, fmt.Errorf("price not found")
		}
		j.logger.Debug("token price fetched without routing", zap.String("token", tokenAddress))
	}
	src := res.Hits.Hits[0].Source
	if v, ok := src["price_usd"].(float64); ok {
		return v, nil
	}
	j.logger.Debug("token price field missing", zap.String("token", tokenAddress), zap.Any("source", src))
	return 0, fmt.Errorf("price_usd not found")
}

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
		"_source": []string{"symbol", "logo"},
	}
	index := "web3_tokens"
	res, err := es.SearchWithRouting(ctx, index, tokenAddress, query)
	if err != nil {
		j.logger.Debug("token meta search failed", zap.String("token", tokenAddress), zap.Error(err))
	}
	if err != nil || len(res.Hits.Hits) == 0 {
		if err == nil {
			j.logger.Debug("token meta search miss with routing", zap.String("token", tokenAddress))
		}
		res, err = es.Search(ctx, index, query)
		if err != nil {
			j.logger.Debug("token meta fallback search failed", zap.String("token", tokenAddress), zap.Error(err))
			return "", ""
		}
		if len(res.Hits.Hits) == 0 {
			j.logger.Debug("token meta not found", zap.String("token", tokenAddress))
			return "", ""
		}
		j.logger.Debug("token meta fetched without routing", zap.String("token", tokenAddress))
	}
	src := res.Hits.Hits[0].Source
	if v, ok := src["symbol"].(string); ok {
		name = v
	}
	if v, ok := src["logo"].(string); ok {
		icon = v
	}
	if name == "" && icon == "" {
		j.logger.Debug("token meta empty", zap.String("token", tokenAddress))
	}
	return name, icon
}

// 帶重試的 token meta 查詢：避免 ES 瞬時 miss 導致空值
func (j *SmartMoneyAnalyzer) getTokenMetaWithRetry(ctx context.Context, tokenAddress string) (string, string) {
	// 最多嘗試 3 次，簡單退避 50/100ms
	for attempt := 0; attempt < 3; attempt++ {
		name, icon := j.getTokenMeta(ctx, tokenAddress)
		if name != "" || icon != "" {
			return name, icon
		}
		// 若 ctx 已取消則提前返回
		if ctx.Err() != nil {
			return "", ""
		}
		time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
	}
	return "", ""
}

func (j *SmartMoneyAnalyzer) updateOneWallet(ctx context.Context, db *gorm.DB, w *model.WalletSummary) (*model.WalletSummary, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	now := time.Now()
	// 統一使用毫秒時間戳，與資料表中的 transaction_time/last_transaction_time 保持一致
	tsNow := now.UnixMilli()
	ts1d := now.Add(-24 * time.Hour).UnixMilli()
	ts7d := now.Add(-7 * 24 * time.Hour).UnixMilli()
	ts30d := now.Add(-30 * 24 * time.Hour).UnixMilli()

	var txs []model.WalletTransaction
	if err := db.WithContext(ctx).
		Where("wallet_address = ? AND chain_id = ? AND transaction_time >= ?", w.WalletAddress, w.ChainID, ts30d).
		Order("transaction_time ASC").
		Find(&txs).Error; err != nil {
		return nil, err
	}

	updates := map[string]any{}

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
		avgCost   decimal.Decimal
		totalTx   int
		buyNum    int
		sellNum   int
		winRate   decimal.Decimal
		pnl       decimal.Decimal
		pnlPct    decimal.Decimal
		totalCost decimal.Decimal
	}
	s30 := stats{}
	s7 := stats{}
	s1 := stats{}

	// PNL 日線圖（只針對 30 天，賣出/清倉視為實現損益）
	dailyPNL := map[string]decimal.Decimal{}

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

		// 成本回退：若某些來源未填 value，改用 amount * price 作為 USD 成本
		buyCost := t.Value
		if isBuy && (buyCost.LessThanOrEqual(decimal.Zero)) {
			buyCost = t.Amount.Mul(t.Price)
		}

		// 最近 token 收集（按照時間順序，最後再從尾端取三個不重複）
		recentTokens = append(recentTokens, t.TokenAddress)

		// 30d 聚合
		if tt >= ts30d && tt <= tsNow {
			s30.totalTx++
			if isBuy {
				s30.buyNum++
				s30.totalCost = s30.totalCost.Add(buyCost)
			} else if isSell {
				s30.sellNum++
				s30.pnl = s30.pnl.Add(t.RealizedProfit)
			}

			// 日線 PNL（僅賣出/清倉記錄）
			if isSell {
				day := time.UnixMilli(tt).In(time.FixedZone("CST", 8*3600)).Format("2006-01-02")
				dailyPNL[day] = dailyPNL[day].Add(t.RealizedProfit)
			}
		}

		// 7d 聚合
		if tt >= ts7d && tt <= tsNow {
			s7.totalTx++
			if isBuy {
				s7.buyNum++
				s7.totalCost = s7.totalCost.Add(buyCost)
			} else if isSell {
				s7.sellNum++
				s7.pnl = s7.pnl.Add(t.RealizedProfit)
			}
		}

		// 1d 聚合
		if tt >= ts1d && tt <= tsNow {
			s1.totalTx++
			if isBuy {
				s1.buyNum++
				s1.totalCost = s1.totalCost.Add(buyCost)
			} else if isSell {
				s1.sellNum++
				s1.pnl = s1.pnl.Add(t.RealizedProfit)
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
				if (tp == "sell" || tp == "clean") && t.RealizedProfit.GreaterThan(decimal.Zero) {
					wins++
				}
			}
		}
		// 改為比例 0-1，避免寫入 DECIMAL(5,4) 溢出
		s30.winRate = decimal.NewFromInt(int64(wins)).Div(decimal.NewFromInt(int64(s30.sellNum)))
	}
	if s7.sellNum > 0 {
		var wins int
		for _, t := range txs {
			if t.TransactionTime >= ts7d && t.TransactionTime <= tsNow {
				tp := strings.ToLower(t.TransactionType)
				if (tp == "sell" || tp == "clean") && t.RealizedProfit.GreaterThan(decimal.Zero) {
					wins++
				}
			}
		}
		s7.winRate = decimal.NewFromInt(int64(wins)).Div(decimal.NewFromInt(int64(s7.sellNum)))
	}
	if s1.sellNum > 0 {
		var wins int
		for _, t := range txs {
			if t.TransactionTime >= ts1d && t.TransactionTime <= tsNow {
				tp := strings.ToLower(t.TransactionType)
				if (tp == "sell" || tp == "clean") && t.RealizedProfit.GreaterThan(decimal.Zero) {
					wins++
				}
			}
		}
		s1.winRate = decimal.NewFromInt(int64(wins)).Div(decimal.NewFromInt(int64(s1.sellNum)))
	}

	// 平均成本
	if s30.buyNum > 0 {
		s30.avgCost = s30.totalCost.Div(decimal.NewFromInt(int64(s30.buyNum)))
	}
	if s7.buyNum > 0 {
		s7.avgCost = s7.totalCost.Div(decimal.NewFromInt(int64(s7.buyNum)))
	}
	if s1.buyNum > 0 {
		s1.avgCost = s1.totalCost.Div(decimal.NewFromInt(int64(s1.buyNum)))
	}

	// PNL 百分比（以期間買入總成本為分母）
	if s30.totalCost.GreaterThan(decimal.Zero) {
		s30.pnlPct = clampDecimal(s30.pnl.Div(s30.totalCost).Mul(decimal.NewFromInt(100)))
	}
	if s7.totalCost.GreaterThan(decimal.Zero) {
		s7.pnlPct = clampDecimal(s7.pnl.Div(s7.totalCost).Mul(decimal.NewFromInt(100)))
	}
	if s1.totalCost.GreaterThan(decimal.Zero) {
		s1.pnlPct = clampDecimal(s1.pnl.Div(s1.totalCost).Mul(decimal.NewFromInt(100)))
	}

	// 以當前幣價計算未實現盈虧（仍依時間窗切分）。價格透過 ES 查詢，並在單錢包內做簡單快取
	tokenPriceCache := map[string]float64{}
	getPrice := func(token string) float64 {
		if v, ok := tokenPriceCache[token]; ok {
			return v
		}
		p, err := j.getTokenPriceUSD(ctx, token)
		if err != nil {
			j.logger.Debug("es price fetch failed", zap.String("token", token), zap.Error(err))
			return 0
		}
		if p == 0 {
			j.logger.Debug("es price zero", zap.String("token", token))
		}
		tokenPriceCache[token] = p
		return p
	}
	calcUnrealized := func(startTs, endTs int64) float64 {
		hold := map[string]decimal.Decimal{}
		for _, t := range txs {
			if t.TransactionTime < startTs || t.TransactionTime > endTs {
				continue
			}
			txType := strings.ToLower(t.TransactionType)
			if txType == "buy" || txType == "build" {
				hold[t.TokenAddress] = hold[t.TokenAddress].Add(t.Amount)
			} else if txType == "sell" || txType == "clean" {
				hold[t.TokenAddress] = hold[t.TokenAddress].Sub(t.Amount)
				if hold[t.TokenAddress].LessThan(decimal.Zero) {
					hold[t.TokenAddress] = decimal.Zero
				}
			}
		}
		var total float64
		for token, remain := range hold {
			if remain.LessThanOrEqual(decimal.Zero) {
				continue
			}
			price := getPrice(token)
			total += remain.InexactFloat64() * price
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
		type agg struct{ cost, pnl decimal.Decimal }
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
				// 成本回退：優先用 value，為 0 則用 amount*price
				cost := t.Value
				if cost.LessThanOrEqual(decimal.Zero) {
					cost = t.Amount.Mul(t.Price)
				}
				tokens[t.TokenAddress].cost = tokens[t.TokenAddress].cost.Add(cost)
			} else if txType == "sell" || txType == "clean" {
				if tokens[t.TokenAddress] == nil {
					tokens[t.TokenAddress] = &agg{}
				}
				tokens[t.TokenAddress].pnl = tokens[t.TokenAddress].pnl.Add(t.RealizedProfit)
			}
		}
		var res distRes
		total := 0
		for _, a := range tokens {
			if a.cost.LessThanOrEqual(decimal.Zero) {
				continue
			}
			total++
			pnlPct := a.pnl.Div(a.cost).Mul(decimal.NewFromInt(100)).InexactFloat64()
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
	existingTokenMap := make(map[string]tokenItem)
	for _, info := range w.TokenList {
		existingTokenMap[info.TokenAddress] = tokenItem{
			TokenAddress: info.TokenAddress,
			TokenIcon:    info.TokenIcon,
			TokenName:    info.TokenName,
		}
	}
	// 每個錢包處理內對 token meta 做簡單快取，減少 ES QPS
	metaCache := make(map[string]tokenItem)
	items := make([]tokenItem, 0, len(recent))
	for _, addr := range recent {
		if cached, ok := metaCache[addr]; ok {
			// 命中快取
			if cached.TokenName != "" || cached.TokenIcon != "" {
				items = append(items, cached)
				continue
			}
		}

		name, icon := j.getTokenMetaWithRetry(ctx, addr)
		if prev, ok := existingTokenMap[addr]; ok {
			if name == "" && prev.TokenName != "" {
				j.logger.Debug("reuse existing token name", zap.String("wallet", w.WalletAddress), zap.String("token", addr), zap.String("name", prev.TokenName))
				name = prev.TokenName
			}
			if icon == "" && prev.TokenIcon != "" {
				j.logger.Debug("reuse existing token icon", zap.String("wallet", w.WalletAddress), zap.String("token", addr))
				icon = prev.TokenIcon
			}
		}
		// 仍然缺失則暫不寫入，避免以空值覆蓋；等待下輪再補
		if name == "" && icon == "" {
			j.logger.Debug("token meta still missing, skip this round", zap.String("wallet", w.WalletAddress), zap.String("token", addr))
			metaCache[addr] = tokenItem{TokenAddress: addr}
			continue
		}
		ti := tokenItem{TokenAddress: addr, TokenIcon: icon, TokenName: name}
		items = append(items, ti)
		metaCache[addr] = ti
	}
	j.logger.Debug("wallet token_list prepared", zap.String("wallet", w.WalletAddress), zap.Int("token_count", len(items)))
	tokenListBytes, _ := json.Marshal(items)
	tokenList := string(tokenListBytes)

	// 是否視為活躍/聰明錢（與 python 規則一致）
	isSmart := s30.pnl.GreaterThan(decimal.Zero) && s30.winRate.GreaterThan(decimal.NewFromFloat(0.3)) && !s30.winRate.Equal(decimal.NewFromInt(1)) && s30.totalTx < 2000 && lastTxTime >= ts30d

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
		winRatePct := s30.winRate.Mul(decimal.NewFromInt(100)).InexactFloat64()
		condWinRate := winRatePct > 60
		condTx7d := s7.totalTx > 100
		condPNL30d := s30.pnl.GreaterThan(decimal.NewFromFloat(1000))
		condPNLPct := s30.pnlPct.GreaterThan(decimal.NewFromFloat(100))
		condDist := (d30.pLt50 * 100) < 30
		passedClassifier := condWinRate && condTx7d && condPNL30d && condPNLPct && condDist

		// 若當前 tags 不包含 smart money，且通過五條件，追加標籤
		if !hasSmartMoneyTag(w.Tags) && passedClassifier {
			newTags := append([]string{}, w.Tags...)
			newTags = append(newTags, "smart money")
			// 注意：PG varchar[] 需要 pq.StringArray
			updates["tags"] = pq.StringArray(newTags)
		}
		updates["token_list"] = tokenList
		updates["asset_multiple"] = s30.pnlPct.Div(decimal.NewFromInt(100)).Add(decimal.NewFromInt(1))
		updates["last_transaction_time"] = lastTxTime

		// 30d
		updates["avg_cost_30d"] = s30.avgCost
		updates["buy_num_30d"] = s30.buyNum
		updates["sell_num_30d"] = s30.sellNum
		updates["win_rate_30d"] = s30.winRate.Mul(decimal.NewFromInt(100))
		updates["pnl_30d"] = s30.pnl
		updates["pnl_percentage_30d"] = s30.pnlPct
		updates["pnl_pic_30d"] = pnlPic30d
		updates["unrealized_profit_30d"] = unreal30d
		updates["total_cost_30d"] = s30.totalCost
		if s30.sellNum > 0 {
			updates["avg_realized_profit_30d"] = avgRealized(s30.pnl, s30.sellNum)
		}

		// 7d
		updates["avg_cost_7d"] = s7.avgCost
		updates["buy_num_7d"] = s7.buyNum
		updates["sell_num_7d"] = s7.sellNum
		updates["win_rate_7d"] = s7.winRate.Mul(decimal.NewFromInt(100))
		updates["pnl_7d"] = s7.pnl
		updates["pnl_percentage_7d"] = s7.pnlPct
		updates["unrealized_profit_7d"] = unreal7d
		updates["total_cost_7d"] = s7.totalCost
		if s7.sellNum > 0 {
			updates["avg_realized_profit_7d"] = avgRealized(s7.pnl, s7.sellNum)
		}

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
		updates["buy_num_1d"] = s1.buyNum
		updates["sell_num_1d"] = s1.sellNum
		updates["win_rate_1d"] = s1.winRate.Mul(decimal.NewFromInt(100))
		updates["pnl_1d"] = s1.pnl
		updates["pnl_percentage_1d"] = s1.pnlPct
		updates["unrealized_profit_1d"] = unreal1d
		updates["total_cost_1d"] = s1.totalCost
		if s1.sellNum > 0 {
			updates["avg_realized_profit_1d"] = s1.pnl.Div(decimal.NewFromInt(int64(s1.sellNum)))
		}

		// 狀態：若已有 smart money 標籤但本次未通過五條件，強制 false
		isActive := isSmart
		if hasSmartMoneyTag(w.Tags) && !passedClassifier {
			isActive = false
		}
		updates["is_active"] = isActive
	}

	// 計算 balance_usd：balance 為 0 → 直接 0；否則使用 ES 的 SOL 價格換算
	{
		var bal decimal.Decimal
		if v, ok := updates["balance"]; ok {
			// 兼容前面寫入 float64 的情況，統一轉成 decimal 計算
			switch t := v.(type) {
			case decimal.Decimal:
				bal = t
			case float64:
				bal = decimal.NewFromFloat(t)
				updates["balance"] = bal
			case float32:
				bal = decimal.NewFromFloat(float64(t))
				updates["balance"] = bal
			}
		} else {
			bal = w.Balance
		}
		if bal.Equal(decimal.Zero) {
			updates["balance_usd"] = decimal.Zero
		} else if w.ChainID == 501 {
			if price, err := j.getTokenPriceUSD(ctx, "So11111111111111111111111111111111111111112"); err == nil {
				updates["balance_usd"] = bal.Mul(decimal.NewFromFloat(price))
			} else {
				j.logger.Warn("es price for SOL failed", zap.Error(err))
			}
		}
	}

	// 最後更新時間無論如何刷新，代表本次任務已跑過；如果你希望「僅變化才更新」，可以再細化為 dirty 檢查
	updates["updated_at"] = time.Now().UnixMilli()

	if err := db.WithContext(ctx).Model(&model.WalletSummary{}).
		Where("wallet_address = ?", w.WalletAddress).
		Updates(updates).Error; err != nil {
		return nil, err
	}

	es := &model.WalletSummary{
		WalletAddress:   w.WalletAddress,
		Avatar:          w.Avatar,
		ChainID:         w.ChainID,
		TwitterName:     w.TwitterName,
		TwitterUsername: w.TwitterUsername,
		WalletType:      w.WalletType,
		CreatedAt:       w.CreatedAt,
	}

	if v, ok := updates["balance"]; ok {
		if d, ok2 := v.(decimal.Decimal); ok2 {
			es.Balance = d
		}
	} else {
		es.Balance = w.Balance
	}
	if v, ok := updates["balance_usd"]; ok {
		if d, ok2 := v.(decimal.Decimal); ok2 {
			es.BalanceUSD = d
		}
	} else {
		es.BalanceUSD = w.BalanceUSD
	}

	if v, ok := updates["tags"]; ok {
		if arr, ok2 := v.([]string); ok2 {
			es.Tags = arr
		} else if arr2, ok3 := v.(pq.StringArray); ok3 {
			es.Tags = []string(arr2)
		} else {
			es.Tags = w.Tags
		}
	} else {
		es.Tags = w.Tags
	}

	if len(txs) > 0 {
		result := make(model.TokenList, 0, len(items))
		for _, item := range items {
			result = append(result, model.TokenInfo{
				TokenAddress: item.TokenAddress,
				TokenIcon:    item.TokenIcon,
				TokenName:    item.TokenName,
			})
		}
		es.TokenList = result
		es.AssetMultiple = s30.pnlPct.Div(decimal.NewFromInt(100)).Add(decimal.NewFromInt(1))
		es.LastTransactionTime = lastTxTime

		es.AvgCost30d = s30.avgCost
		es.BuyNum30d = s30.buyNum
		es.SellNum30d = s30.sellNum
		es.WinRate30d = s30.winRate.Mul(decimal.NewFromInt(100))
		es.PNL30d = s30.pnl
		es.PNLPercentage30d = s30.pnlPct
		es.PNLPic30d = pnlPic30d
		es.UnrealizedProfit30d = decimal.NewFromFloat(unreal30d)
		es.TotalCost30d = s30.totalCost
		es.AvgRealizedProfit30d = avgRealized(s30.pnl, s30.sellNum)

		es.AvgCost7d = s7.avgCost
		es.BuyNum7d = s7.buyNum
		es.SellNum7d = s7.sellNum
		es.WinRate7d = s7.winRate.Mul(decimal.NewFromInt(100))
		es.PNL7d = s7.pnl
		es.PNLPercentage7d = s7.pnlPct
		es.UnrealizedProfit7d = decimal.NewFromFloat(unreal7d)
		es.TotalCost7d = s7.totalCost
		es.AvgRealizedProfit7d = avgRealized(s7.pnl, s7.sellNum)

		es.AvgCost1d = s1.avgCost
		es.BuyNum1d = s1.buyNum
		es.SellNum1d = s1.sellNum
		es.WinRate1d = s1.winRate.Mul(decimal.NewFromInt(100))
		es.PNL1d = s1.pnl
		es.PNLPercentage1d = s1.pnlPct
		es.UnrealizedProfit1d = decimal.NewFromFloat(unreal1d)
		es.TotalCost1d = s1.totalCost
		es.AvgRealizedProfit1d = avgRealized(s1.pnl, s1.sellNum)

		es.DistributionGt500_30d = d30.gt500
		es.Distribution200to500_30d = d30.between200to500
		es.Distribution0to200_30d = d30.between0to200
		es.DistributionN50to0_30d = d30.n50to0
		es.DistributionLt50_30d = d30.lt50
		es.DistributionGt500Percentage30d = decimal.NewFromFloat(d30.pGt500 * 100)
		es.Distribution200to500Percentage30d = decimal.NewFromFloat(d30.p200to500 * 100)
		es.Distribution0to200Percentage30d = decimal.NewFromFloat(d30.p0to200 * 100)
		es.DistributionN50to0Percentage30d = decimal.NewFromFloat(d30.pN50to0 * 100)
		es.DistributionLt50Percentage30d = decimal.NewFromFloat(d30.pLt50 * 100)

		es.DistributionGt500_7d = d7.gt500
		es.Distribution200to500_7d = d7.between200to500
		es.Distribution0to200_7d = d7.between0to200
		es.DistributionN50to0_7d = d7.n50to0
		es.DistributionLt50_7d = d7.lt50
		es.DistributionGt500Percentage7d = decimal.NewFromFloat(d7.pGt500 * 100)
		es.Distribution200to500Percentage7d = decimal.NewFromFloat(d7.p200to500 * 100)
		es.Distribution0to200Percentage7d = decimal.NewFromFloat(d7.p0to200 * 100)
		es.DistributionN50to0Percentage7d = decimal.NewFromFloat(d7.pN50to0 * 100)
		es.DistributionLt50Percentage7d = decimal.NewFromFloat(d7.pLt50 * 100)

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
		*es = *w
	}

	es.UpdatedAt = time.Now().UnixMilli()

	return es, nil
}

func avgRealized(pnl decimal.Decimal, sellNum int) decimal.Decimal {
	if sellNum <= 0 {
		return decimal.Zero
	}
	return pnl.Div(decimal.NewFromInt(int64(sellNum)))
}

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

func buildPNLPic30(daily map[string]decimal.Decimal, now time.Time) string {
	// 產出 30 個數（從昨天往回），缺漏補 0
	tz := time.FixedZone("CST", 8*3600)
	end := time.Date(now.In(tz).Year(), now.In(tz).Month(), now.In(tz).Day(), 0, 0, 0, 0, tz)
	res := make([]string, 0, 30)
	cur := end.Add(-24 * time.Hour)
	for i := 0; i < 30; i++ {
		day := cur.Format("2006-01-02")
		val := daily[day]
		res = append(res, val.StringFixed(2))
		cur = cur.Add(-24 * time.Hour)
	}
	return strings.Join(res, ",")
}

func clampDecimal(d decimal.Decimal) decimal.Decimal {
	const max = 9999.9999
	const min = -9999.9999
	if d.GreaterThan(decimal.NewFromFloat(max)) {
		return decimal.NewFromFloat(max)
	}
	if d.LessThan(decimal.NewFromFloat(min)) {
		return decimal.NewFromFloat(min)
	}
	return d
}

func lastNDistinct(tokens []string, n int) []string {
	if len(tokens) == 0 || n <= 0 {
		return nil
	}
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
	return res
}
