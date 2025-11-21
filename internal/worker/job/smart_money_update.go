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
	getOnchainInfo "web3-smart/pkg/utils/get_onchain_info"

	"github.com/gagliardetto/solana-go"
	rpcsol "github.com/gagliardetto/solana-go/rpc"
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/quotecoin"
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
	var errors int64 = 0
	var lastID int64 = 0
	start := time.Now()
	j.logger.Info("smart_money_analyze start", zap.Time("start_time", start))

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

		pageStart := time.Now()
		j.logger.Info("smart_money_analyze page fetched", zap.Int("count", len(wallets)), zap.Int64("since_processed", atomic.LoadInt64(&processed)))

		// 併發處理當前頁的錢包
		jobs := make(chan *model.WalletSummary, len(wallets))
		var wg sync.WaitGroup
		worker := func() {
			defer wg.Done()
			for w := range jobs {
				if esDoc, err := j.updateOneWallet(ctx, db, w); err != nil {
					atomic.AddInt64(&errors, 1)
					j.logger.Error("update wallet failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
				} else if esAsync != nil && esDoc != nil {
					esAsync.Submit(*esDoc, w.WalletAddress)
				}
				n := atomic.AddInt64(&processed, 1)
				if n%1000 == 0 {
					elapsed := time.Since(start)
					j.logger.Info("smart_money_analyze progress",
						zap.Int64("processed", n),
						zap.Int64("errors", atomic.LoadInt64(&errors)),
						zap.Duration("elapsed", elapsed),
						zap.Float64("throughput_wallets_per_sec", float64(n)/elapsed.Seconds()))
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
		pageElapsed := time.Since(pageStart)
		j.logger.Debug("smart_money_analyze page processed",
			zap.Int("count", len(wallets)),
			zap.Duration("page_elapsed", pageElapsed),
			zap.Float64("avg_ms_per_wallet_in_page", float64(pageElapsed.Milliseconds())/float64(len(wallets))))

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
	errorCount := atomic.LoadInt64(&errors)
	successCount := processed - errorCount
	var successRate float64
	if processed > 0 {
		successRate = float64(successCount) / float64(processed) * 100
	}
	j.logger.Info(
		"smart_money_analyze done",
		zap.Time("start_time", start),
		zap.Time("end_time", time.Now()),
		zap.Int64("processed", processed),
		zap.Int64("success", successCount),
		zap.Int64("errors", errorCount),
		zap.Float64("success_rate_percent", successRate),
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

func (j *SmartMoneyAnalyzer) getTokenMeta(ctx context.Context, tokenAddress string, chainID uint64) (name string, icon string) {
	es := j.repo.GetElasticsearchClient()
	if es == nil {
		return "", ""
	}

	// 根據 chain_id 映射到 network 名稱
	var network string
	switch chainID {
	case 501:
		network = "SOLANA"
	case 1:
		network = "ETHEREUM"
	case 9006:
		network = "BSC"
	case 137:
		network = "POLYGON"
	case 43114:
		network = "AVALANCHE"
	default:
		network = ""
	}

	// 構建查詢條件：必須匹配 address 和 chain_id/network
	mustClauses := []any{
		map[string]any{
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

	// 如果有 network，添加 network 過濾條件
	if network != "" {
		mustClauses = append(mustClauses, map[string]any{"term": map[string]any{"network": network}})
	} else if chainID > 0 {
		// 如果沒有 network 映射，嘗試使用 chain_id
		mustClauses = append(mustClauses, map[string]any{"term": map[string]any{"chain_id": chainID}})
	}

	query := map[string]any{
		"size": 1,
		"query": map[string]any{
			"bool": map[string]any{
				"must": mustClauses,
			},
		},
		"_source": []string{"symbol", "logo"},
	}
	index := "web3_tokens"
	res, err := es.SearchWithRouting(ctx, index, tokenAddress, query)
	if err != nil {
		j.logger.Debug("token meta search failed", zap.String("token", tokenAddress), zap.Uint64("chain_id", chainID), zap.Error(err))
	}
	if err != nil || len(res.Hits.Hits) == 0 {
		if err == nil {
			j.logger.Debug("token meta search miss with routing", zap.String("token", tokenAddress), zap.Uint64("chain_id", chainID))
		}
		res, err = es.Search(ctx, index, query)
		if err != nil {
			j.logger.Debug("token meta fallback search failed", zap.String("token", tokenAddress), zap.Uint64("chain_id", chainID), zap.Error(err))
			return "", ""
		}
		if len(res.Hits.Hits) == 0 {
			j.logger.Debug("token meta not found", zap.String("token", tokenAddress), zap.Uint64("chain_id", chainID))
			return "", ""
		}
		j.logger.Debug("token meta fetched without routing", zap.String("token", tokenAddress), zap.Uint64("chain_id", chainID))
	}
	src := res.Hits.Hits[0].Source

	// 獲取 symbol，如果沒有則為空
	if v, ok := src["symbol"].(string); ok && v != "" {
		name = v
	}

	// 處理 logo 字段：可能是 string、null 或其他類型
	if logoVal, exists := src["logo"]; exists && logoVal != nil {
		switch v := logoVal.(type) {
		case string:
			if v != "" {
				icon = v
			}
		case []byte:
			icon = string(v)
		default:
			// 嘗試轉換為字符串
			str := fmt.Sprintf("%v", v)
			if str != "" && str != "<nil>" {
				icon = str
			}
		}
	}

	// 增強日誌：記錄獲取結果
	if icon == "" && name != "" {
		j.logger.Debug("token meta logo missing",
			zap.String("token", tokenAddress),
			zap.Uint64("chain_id", chainID),
			zap.String("symbol", name),
			zap.Any("logo_field", src["logo"]))
	} else if name == "" && icon == "" {
		j.logger.Debug("token meta empty",
			zap.String("token", tokenAddress),
			zap.Uint64("chain_id", chainID),
			zap.Any("source", src))
	}

	return name, icon
}

// 帶重試的 token meta 查詢：避免 ES 瞬時 miss 導致空值
func (j *SmartMoneyAnalyzer) getTokenMetaWithRetry(ctx context.Context, tokenAddress string, chainID uint64) (string, string) {
	// 最多嘗試 3 次，簡單退避 50/100ms
	for attempt := 0; attempt < 3; attempt++ {
		name, icon := j.getTokenMeta(ctx, tokenAddress, chainID)
		if name != "" || icon != "" {
			return name, icon
		}
		// 若 ctx 已取消則提前返回
		if ctx.Err() != nil {
			return "", ""
		}
		// 最後一次嘗試前增加等待時間
		if attempt < 2 {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
	j.logger.Warn("token meta fetch failed after retries",
		zap.String("token", tokenAddress),
		zap.Uint64("chain_id", chainID),
		zap.Int("attempts", 3))
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

	// 從鏈上獲取原生代幣餘額
	if w.ChainID == bip0044.SOLANA {
		if client := j.repo.GetSolanaClient(); client != nil {
			if pub, err := solana.PublicKeyFromBase58(w.WalletAddress); err == nil {
				if balRes, err := client.GetBalance(ctx, pub, rpcsol.CommitmentFinalized); err == nil {
					// lamports -> SOL: 使用 float64 轉換避免 uint64 到 int64 的溢出問題
					updates["balance"] = decimal.NewFromFloat(float64(balRes.Value) / 1e9)
				} else {
					j.logger.Debug("failed to get SOL balance from chain", zap.String("wallet", w.WalletAddress), zap.Error(err))
				}
			}
		}
	} else if w.ChainID == bip0044.BSC {
		// BSC 鏈：獲取 BNB 餘額
		if client := j.repo.GetBscClient(); client != nil {
			nativeBal, quoteTokenMap, err := getOnchainInfo.GetBscBalance(ctx, client, w.WalletAddress)
			if err != nil {
				j.logger.Debug("failed to get BSC balance from chain", zap.String("wallet", w.WalletAddress), zap.Error(err))
			} else {
				// 原生 BNB 餘額 + WBNB 餘額
				totalBNB := nativeBal
				if wbnbBal, ok := quoteTokenMap[quotecoin.ID9006_WBNB_ADDRESS]; ok {
					totalBNB = totalBNB.Add(wbnbBal)
				}
				updates["balance"] = totalBNB
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
			// 計算原始百分比（0-1範圍）
			res.pGt500 = float64(res.gt500) / fTotal
			res.p200to500 = float64(res.between200to500) / fTotal
			res.p0to200 = float64(res.between0to200) / fTotal
			res.pN50to0 = float64(res.n50to0) / fTotal
			res.pLt50 = float64(res.lt50) / fTotal

			// 確保總和嚴格等於 1.0（避免浮點數精度問題）
			sum := res.pGt500 + res.p200to500 + res.p0to200 + res.pN50to0 + res.pLt50
			if sum > 0 && math.Abs(sum-1.0) > 0.0001 {
				// 規範化：按比例調整使總和為 1
				res.pGt500 /= sum
				res.p200to500 /= sum
				res.p0to200 /= sum
				res.pN50to0 /= sum
				res.pLt50 /= sum
			}
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

		name, icon := j.getTokenMetaWithRetry(ctx, addr, w.ChainID)
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
		// 調整為：即使缺失也保留資料格式（字段置空），避免最終為空陣列 []
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

	// 根據最後交易時間判斷哪些時間窗口需要更新統計
	// 如果最後交易時間在某個時間窗口內，則更新該窗口的統計；否則設置為 0
	hasTxIn1d := lastTxTime >= ts1d && lastTxTime <= tsNow
	hasTxIn7d := lastTxTime >= ts7d && lastTxTime <= tsNow
	hasTxIn30d := lastTxTime >= ts30d && lastTxTime <= tsNow

	// 計算五條件判斷結果（基於本次統計數據，僅在有30天交易時計算）
	var passedClassifier bool
	if hasTxIn30d {
		winRatePct := s30.winRate.Mul(decimal.NewFromInt(100)).InexactFloat64()
		condWinRate := winRatePct > 60
		condTx7d := s7.totalTx > 100
		condPNL30d := s30.pnl.GreaterThan(decimal.NewFromFloat(1000))
		condPNLPct := s30.pnlPct.GreaterThan(decimal.NewFromFloat(100))
		condDist := (d30.pLt50 * 100) < 30
		passedClassifier = condWinRate && condTx7d && condPNL30d && condPNLPct && condDist
	}

	// 若當前 tags 不包含 smart_wallet，且通過五條件，追加標籤
	if hasTxIn30d && !hasSmartMoneyTag(w.Tags) && passedClassifier {
		newTags := append([]string{}, w.Tags...)
		newTags = append(newTags, model.TAG_SMART_MONEY)
		// 注意：PG varchar[] 需要 pq.StringArray
		updates["tags"] = pq.StringArray(newTags)
	}

	// 更新 token_list 和 asset_multiple（僅在有30天交易時）
	if hasTxIn30d {
		updates["token_list"] = tokenList
		updates["asset_multiple"] = s30.pnlPct.Div(decimal.NewFromInt(100)).Add(decimal.NewFromInt(1))
	}
	// 更新 last_transaction_time（如果有任何交易記錄）
	if len(txs) > 0 {
		updates["last_transaction_time"] = lastTxTime
	}

	// 根據時間窗口更新統計字段
	// 30d 統計
	if hasTxIn30d {
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
		} else {
			updates["avg_realized_profit_30d"] = decimal.Zero
		}
		// 分布統計 - 30d（數量和比例都儲存）
		updates["distribution_gt500_30d"] = d30.gt500
		updates["distribution_200to500_30d"] = d30.between200to500
		updates["distribution_0to200_30d"] = d30.between0to200
		updates["distribution_n50to0_30d"] = d30.n50to0
		updates["distribution_lt50_30d"] = d30.lt50
		// 百分比以 0-100 範圍儲存，確保總和為 100
		updates["distribution_gt500_percentage_30d"] = d30.pGt500 * 100.0
		updates["distribution_200to500_percentage_30d"] = d30.p200to500 * 100.0
		updates["distribution_0to200_percentage_30d"] = d30.p0to200 * 100.0
		updates["distribution_n50to0_percentage_30d"] = d30.pN50to0 * 100.0
		updates["distribution_lt50_percentage_30d"] = d30.pLt50 * 100.0
	} else {
		// 如果最後交易時間不在30天內，將30d統計設為0（確保每個字段都被更新）
		updates["avg_cost_30d"] = decimal.Zero
		updates["buy_num_30d"] = 0
		updates["sell_num_30d"] = 0
		updates["win_rate_30d"] = decimal.Zero
		updates["pnl_30d"] = decimal.Zero
		updates["pnl_percentage_30d"] = decimal.Zero
		updates["pnl_pic_30d"] = ""
		updates["unrealized_profit_30d"] = 0.0
		updates["total_cost_30d"] = decimal.Zero
		updates["avg_realized_profit_30d"] = decimal.Zero
		// 分布統計也全部清零
		updates["distribution_gt500_30d"] = 0
		updates["distribution_200to500_30d"] = 0
		updates["distribution_0to200_30d"] = 0
		updates["distribution_n50to0_30d"] = 0
		updates["distribution_lt50_30d"] = 0
		updates["distribution_gt500_percentage_30d"] = 0.0
		updates["distribution_200to500_percentage_30d"] = 0.0
		updates["distribution_0to200_percentage_30d"] = 0.0
		updates["distribution_n50to0_percentage_30d"] = 0.0
		updates["distribution_lt50_percentage_30d"] = 0.0
	}

	// 7d 統計
	if hasTxIn7d {
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
		} else {
			updates["avg_realized_profit_7d"] = decimal.Zero
		}
		// 分布統計 - 7d（數量和比例都儲存）
		updates["distribution_gt500_7d"] = d7.gt500
		updates["distribution_200to500_7d"] = d7.between200to500
		updates["distribution_0to200_7d"] = d7.between0to200
		updates["distribution_n50to0_7d"] = d7.n50to0
		updates["distribution_lt50_7d"] = d7.lt50
		// 百分比以 0-100 範圍儲存，確保總和為 100
		updates["distribution_gt500_percentage_7d"] = d7.pGt500 * 100.0
		updates["distribution_200to500_percentage_7d"] = d7.p200to500 * 100.0
		updates["distribution_0to200_percentage_7d"] = d7.p0to200 * 100.0
		updates["distribution_n50to0_percentage_7d"] = d7.pN50to0 * 100.0
		updates["distribution_lt50_percentage_7d"] = d7.pLt50 * 100.0
	} else {
		// 如果最後交易時間不在7天內，將7d統計設為0（確保每個字段都被更新）
		updates["avg_cost_7d"] = decimal.Zero
		updates["buy_num_7d"] = 0
		updates["sell_num_7d"] = 0
		updates["win_rate_7d"] = decimal.Zero
		updates["pnl_7d"] = decimal.Zero
		updates["pnl_percentage_7d"] = decimal.Zero
		updates["unrealized_profit_7d"] = 0.0
		updates["total_cost_7d"] = decimal.Zero
		updates["avg_realized_profit_7d"] = decimal.Zero
		// 分布統計也全部清零
		updates["distribution_gt500_7d"] = 0
		updates["distribution_200to500_7d"] = 0
		updates["distribution_0to200_7d"] = 0
		updates["distribution_n50to0_7d"] = 0
		updates["distribution_lt50_7d"] = 0
		updates["distribution_gt500_percentage_7d"] = 0.0
		updates["distribution_200to500_percentage_7d"] = 0.0
		updates["distribution_0to200_percentage_7d"] = 0.0
		updates["distribution_n50to0_percentage_7d"] = 0.0
		updates["distribution_lt50_percentage_7d"] = 0.0
	}

	// 1d 統計
	if hasTxIn1d {
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
		} else {
			updates["avg_realized_profit_1d"] = decimal.Zero
		}
	} else {
		// 如果最後交易時間不在1天內，將1d統計設為0
		updates["avg_cost_1d"] = decimal.Zero
		updates["buy_num_1d"] = 0
		updates["sell_num_1d"] = 0
		updates["win_rate_1d"] = decimal.Zero
		updates["pnl_1d"] = decimal.Zero
		updates["pnl_percentage_1d"] = decimal.Zero
		updates["unrealized_profit_1d"] = 0.0
		updates["total_cost_1d"] = decimal.Zero
		updates["avg_realized_profit_1d"] = decimal.Zero
	}

	// 狀態：若已有 smart_wallet 標籤但本次未通過五條件，強制 false
	// 如果最後交易時間不在30天內，is_active 設為 false
	if hasTxIn30d {
		isActive := isSmart
		if hasSmartMoneyTag(w.Tags) && !passedClassifier {
			isActive = false
		}
		updates["is_active"] = isActive
	} else {
		// 如果最後交易時間不在30天內，is_active 設為 false
		updates["is_active"] = false
	}

	// 計算 balance_usd：balance 為 0 → 直接 0；否則使用 ES 的價格換算
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
		} else if w.ChainID == bip0044.SOLANA {
			// SOL 鏈：使用 wSOL 地址查詢價格
			if price, err := j.getTokenPriceUSD(ctx, "So11111111111111111111111111111111111111112"); err == nil {
				updates["balance_usd"] = bal.Mul(decimal.NewFromFloat(price))
			} else {
				j.logger.Debug("es price for SOL failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
			}
		} else if w.ChainID == bip0044.BSC {
			// BSC 鏈：使用 WBNB 地址查詢價格
			if price, err := j.getTokenPriceUSD(ctx, quotecoin.ID9006_WBNB_ADDRESS); err == nil {
				updates["balance_usd"] = bal.Mul(decimal.NewFromFloat(price))
			} else {
				j.logger.Debug("es price for BNB failed", zap.String("wallet", w.WalletAddress), zap.Error(err))
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

	// 更新 token_list 和 asset_multiple（僅在有30天交易時）
	if hasTxIn30d {
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
	} else {
		es.TokenList = w.TokenList
		es.AssetMultiple = w.AssetMultiple
	}

	// 更新 last_transaction_time（如果有任何交易記錄）
	if len(txs) > 0 {
		es.LastTransactionTime = lastTxTime
	} else {
		es.LastTransactionTime = w.LastTransactionTime
	}

	// 根據時間窗口更新 ES 統計字段（與數據庫更新邏輯一致）
	// 30d 統計
	if hasTxIn30d {
		es.AvgCost30d = s30.avgCost
		es.BuyNum30d = s30.buyNum
		es.SellNum30d = s30.sellNum
		es.WinRate30d = s30.winRate.Mul(decimal.NewFromInt(100))
		es.PNL30d = s30.pnl
		es.PNLPercentage30d = s30.pnlPct
		es.PNLPic30d = pnlPic30d
		es.UnrealizedProfit30d = decimal.NewFromFloat(unreal30d)
		es.TotalCost30d = s30.totalCost
		if s30.sellNum > 0 {
			es.AvgRealizedProfit30d = avgRealized(s30.pnl, s30.sellNum)
		} else {
			es.AvgRealizedProfit30d = decimal.Zero
		}
		// 分布統計 - 30d（數量和百分比）
		es.DistributionGt500_30d = d30.gt500
		es.Distribution200to500_30d = d30.between200to500
		es.Distribution0to200_30d = d30.between0to200
		es.DistributionN50to0_30d = d30.n50to0
		es.DistributionLt50_30d = d30.lt50
		// 百分比以 0-100 範圍儲存，確保總和為 100
		es.DistributionGt500Percentage30d = decimal.NewFromFloat(d30.pGt500 * 100.0)
		es.Distribution200to500Percentage30d = decimal.NewFromFloat(d30.p200to500 * 100.0)
		es.Distribution0to200Percentage30d = decimal.NewFromFloat(d30.p0to200 * 100.0)
		es.DistributionN50to0Percentage30d = decimal.NewFromFloat(d30.pN50to0 * 100.0)
		es.DistributionLt50Percentage30d = decimal.NewFromFloat(d30.pLt50 * 100.0)
	} else {
		// 如果最後交易時間不在30天內，所有30d統計設為0（確保每個字段都被更新）
		es.AvgCost30d = decimal.Zero
		es.BuyNum30d = 0
		es.SellNum30d = 0
		es.WinRate30d = decimal.Zero
		es.PNL30d = decimal.Zero
		es.PNLPercentage30d = decimal.Zero
		es.PNLPic30d = ""
		es.UnrealizedProfit30d = decimal.Zero
		es.TotalCost30d = decimal.Zero
		es.AvgRealizedProfit30d = decimal.Zero
		// 分布統計也全部清零
		es.DistributionGt500_30d = 0
		es.Distribution200to500_30d = 0
		es.Distribution0to200_30d = 0
		es.DistributionN50to0_30d = 0
		es.DistributionLt50_30d = 0
		es.DistributionGt500Percentage30d = decimal.Zero
		es.Distribution200to500Percentage30d = decimal.Zero
		es.Distribution0to200Percentage30d = decimal.Zero
		es.DistributionN50to0Percentage30d = decimal.Zero
		es.DistributionLt50Percentage30d = decimal.Zero
	}

	// 7d 統計
	if hasTxIn7d {
		es.AvgCost7d = s7.avgCost
		es.BuyNum7d = s7.buyNum
		es.SellNum7d = s7.sellNum
		es.WinRate7d = s7.winRate.Mul(decimal.NewFromInt(100))
		es.PNL7d = s7.pnl
		es.PNLPercentage7d = s7.pnlPct
		es.UnrealizedProfit7d = decimal.NewFromFloat(unreal7d)
		es.TotalCost7d = s7.totalCost
		if s7.sellNum > 0 {
			es.AvgRealizedProfit7d = avgRealized(s7.pnl, s7.sellNum)
		} else {
			es.AvgRealizedProfit7d = decimal.Zero
		}
		// 分布統計 - 7d（數量和百分比）
		es.DistributionGt500_7d = d7.gt500
		es.Distribution200to500_7d = d7.between200to500
		es.Distribution0to200_7d = d7.between0to200
		es.DistributionN50to0_7d = d7.n50to0
		es.DistributionLt50_7d = d7.lt50
		// 百分比以 0-100 範圍儲存，確保總和為 100
		es.DistributionGt500Percentage7d = decimal.NewFromFloat(d7.pGt500 * 100.0)
		es.Distribution200to500Percentage7d = decimal.NewFromFloat(d7.p200to500 * 100.0)
		es.Distribution0to200Percentage7d = decimal.NewFromFloat(d7.p0to200 * 100.0)
		es.DistributionN50to0Percentage7d = decimal.NewFromFloat(d7.pN50to0 * 100.0)
		es.DistributionLt50Percentage7d = decimal.NewFromFloat(d7.pLt50 * 100.0)
	} else {
		// 如果最後交易時間不在7天內，所有7d統計設為0（確保每個字段都被更新）
		es.AvgCost7d = decimal.Zero
		es.BuyNum7d = 0
		es.SellNum7d = 0
		es.WinRate7d = decimal.Zero
		es.PNL7d = decimal.Zero
		es.PNLPercentage7d = decimal.Zero
		es.UnrealizedProfit7d = decimal.Zero
		es.TotalCost7d = decimal.Zero
		es.AvgRealizedProfit7d = decimal.Zero
		// 分布統計也全部清零
		es.DistributionGt500_7d = 0
		es.Distribution200to500_7d = 0
		es.Distribution0to200_7d = 0
		es.DistributionN50to0_7d = 0
		es.DistributionLt50_7d = 0
		es.DistributionGt500Percentage7d = decimal.Zero
		es.Distribution200to500Percentage7d = decimal.Zero
		es.Distribution0to200Percentage7d = decimal.Zero
		es.DistributionN50to0Percentage7d = decimal.Zero
		es.DistributionLt50Percentage7d = decimal.Zero
	}

	// 1d 統計
	if hasTxIn1d {
		es.AvgCost1d = s1.avgCost
		es.BuyNum1d = s1.buyNum
		es.SellNum1d = s1.sellNum
		es.WinRate1d = s1.winRate.Mul(decimal.NewFromInt(100))
		es.PNL1d = s1.pnl
		es.PNLPercentage1d = s1.pnlPct
		es.UnrealizedProfit1d = decimal.NewFromFloat(unreal1d)
		es.TotalCost1d = s1.totalCost
		if s1.sellNum > 0 {
			es.AvgRealizedProfit1d = avgRealized(s1.pnl, s1.sellNum)
		} else {
			es.AvgRealizedProfit1d = decimal.Zero
		}
	} else {
		es.AvgCost1d = decimal.Zero
		es.BuyNum1d = 0
		es.SellNum1d = 0
		es.WinRate1d = decimal.Zero
		es.PNL1d = decimal.Zero
		es.PNLPercentage1d = decimal.Zero
		es.UnrealizedProfit1d = decimal.Zero
		es.TotalCost1d = decimal.Zero
		es.AvgRealizedProfit1d = decimal.Zero
	}

	// is_active 狀態
	if v, ok := updates["is_active"]; ok {
		if b, ok2 := v.(bool); ok2 {
			es.IsActive = b
		} else {
			es.IsActive = w.IsActive
		}
	} else {
		es.IsActive = w.IsActive
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
