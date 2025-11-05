package job

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	"web3-smart/internal/worker/writer/holding"
	"web3-smart/internal/worker/writer/transaction"
	"web3-smart/internal/worker/writer/wallet"
	"web3-smart/pkg/utils"

	"github.com/lib/pq"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// 每秒最大写入速度
	MaxWritesPerSecond = 5000
	// 批量处理大小
	BatchSize = 500
	// 限流器时间窗口
	RateLimitWindow = time.Second
)

// limitDecimal 限制decimal.Decimal值的范围，防止PostgreSQL DECIMAL(50,20)溢出
// DECIMAL(50,20)的整数部分最多30位，小数部分20位
// 最大值: 999999999999999999999999999999.99999999999999999999
// 最小值: -999999999999999999999999999999.99999999999999999999
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

// limitFloat64ToDecimal 将float64值转为decimal.Decimal并限制精度
func limitFloat64ToDecimal(value float64) decimal.Decimal {
	// 处理NaN和Inf
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return decimal.Zero
	}
	return limitDecimal(decimal.NewFromFloat(value))
}

type MigrationTable struct {
	cfg  config.Config
	repo repository.Repository
	tl   *zap.Logger

	// 限流器
	rateLimiter chan struct{}
	lastReset   time.Time
	mutex       sync.Mutex

	// Writer 组件
	walletDBWriter      writer.BatchWriter[model.WalletSummary]
	walletESWriter      writer.BatchWriter[model.WalletSummary]
	holdingDBWriter     writer.BatchWriter[model.WalletHolding]
	holdingESWriter     writer.BatchWriter[model.WalletHolding]
	transactionDBWriter writer.BatchWriter[model.WalletTransaction]

	// 异步批量写入器
	walletAsyncWriter      *writer.AsyncBatchWriter[model.WalletSummary]
	holdingAsyncWriter     *writer.AsyncBatchWriter[model.WalletHolding]
	transactionAsyncWriter *writer.AsyncBatchWriter[model.WalletTransaction]
}

func NewMigrationTable(cfg config.Config, repo repository.Repository, logger *zap.Logger) *MigrationTable {
	m := &MigrationTable{
		repo:        repo,
		tl:          logger,
		cfg:         cfg,
		rateLimiter: make(chan struct{}, MaxWritesPerSecond),
		lastReset:   time.Now(),
	}

	// 初始化所有的限流tokens
	for i := 0; i < MaxWritesPerSecond; i++ {
		m.rateLimiter <- struct{}{}
	}

	m.initWriters()
	return m
}

func (m *MigrationTable) initWriters() {
	db := m.repo.GetDB()
	esClient := m.repo.GetElasticsearchClient()

	// 初始化基础写入器
	m.walletDBWriter = wallet.NewDbWalletWriter(db, m.tl)
	m.walletESWriter = wallet.NewESWalletWriter(esClient, m.tl, m.cfg.Elasticsearch.WalletsIndexName)
	m.holdingDBWriter = holding.NewDbHoldingWriter(db, m.tl)
	// m.holdingESWriter = holding.NewESHoldingWriter(esClient, m.tl, m.cfg.Elasticsearch.HoldingsIndexName)
	m.transactionDBWriter = transaction.NewDbTransactionWriter(db, m.tl)

	// 初始化异步批量写入器
	m.walletAsyncWriter = writer.NewAsyncBatchWriter[model.WalletSummary](
		m.tl, &multiWriter[model.WalletSummary]{
			writers: []writer.BatchWriter[model.WalletSummary]{m.walletDBWriter, m.walletESWriter},
			// writers: []writer.BatchWriter[model.WalletSummary]{m.walletDBWriter},
		}, BatchSize, time.Millisecond*300, "wallet_migration", 2)

	m.holdingAsyncWriter = writer.NewAsyncBatchWriter[model.WalletHolding](
		m.tl, &multiWriter[model.WalletHolding]{
			// writers: []writer.BatchWriter[model.WalletHolding]{m.holdingDBWriter, m.holdingESWriter},
			writers: []writer.BatchWriter[model.WalletHolding]{m.holdingDBWriter},
		}, BatchSize, time.Millisecond*300, "holding_migration", 2)

	m.transactionAsyncWriter = writer.NewAsyncBatchWriter[model.WalletTransaction](
		m.tl, m.transactionDBWriter, BatchSize, time.Millisecond*300, "transaction_migration", 2)
}

// multiWriter 用于同时写入DB和ES
type multiWriter[T any] struct {
	writers []writer.BatchWriter[T]
}

func (mw *multiWriter[T]) BWrite(ctx context.Context, items []T) error {
	var firstErr error
	for _, w := range mw.writers {
		if err := w.BWrite(ctx, items); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (mw *multiWriter[T]) Close() error {
	var firstErr error
	for _, w := range mw.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *MigrationTable) Run(ctx context.Context) error {
	m.tl.Info("开始数据表迁移")

	// 启动异步写入器
	m.startAsyncWriters(ctx)
	defer m.closeAsyncWriters()

	// 启动限流器重置协程
	go m.resetRateLimiter(ctx)

	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	// 1. 首先迁移钱包摘要数据
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.migrateWalletSummary(ctx); err != nil {
			errChan <- fmt.Errorf("迁移钱包摘要数据失败: %w", err)
		}
	}()

	// 2. 然后迁移钱包持仓数据
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.migrateWalletHolding(ctx); err != nil {
			errChan <- fmt.Errorf("迁移钱包持仓数据失败: %w", err)
		}
	}()

	// 3. 最后迁移交易数据（需要验证钱包是否存在）
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.migrateWalletTransaction(ctx); err != nil {
			errChan <- fmt.Errorf("迁移钱包交易数据失败: %w", err)
		}
	}()

	// 等待所有迁移完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查错误
	for err := range errChan {
		if err != nil {
			m.tl.Error("数据迁移过程中发生错误", zap.Error(err))
			return err
		}
	}

	m.tl.Info("数据表迁移完成")
	return nil
}

func (m *MigrationTable) startAsyncWriters(ctx context.Context) {
	m.walletAsyncWriter.Start(ctx)
	m.holdingAsyncWriter.Start(ctx)
	m.transactionAsyncWriter.Start(ctx)
}

func (m *MigrationTable) closeAsyncWriters() {
	m.walletAsyncWriter.Close()
	m.holdingAsyncWriter.Close()
	m.transactionAsyncWriter.Close()
}

// 限流器重置协程
func (m *MigrationTable) resetRateLimiter(ctx context.Context) {
	ticker := time.NewTicker(RateLimitWindow)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mutex.Lock()
			// 清空并重新填充令牌桶
			select {
			case <-m.rateLimiter:
				// 清空现有token
			default:
			}

			// 重新填充令牌桶
			for i := 0; i < MaxWritesPerSecond; i++ {
				select {
				case m.rateLimiter <- struct{}{}:
				default:
					// 如果令牌桶已满，停止填充
					goto resetDone
				}
			}
		resetDone:
			m.lastReset = time.Now()
			m.mutex.Unlock()
		}
	}
}

// 获取限流令牌
func (m *MigrationTable) acquireToken(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.rateLimiter:
		return nil
	}
}

// migrateWalletSummary 迁移钱包摘要数据
func (m *MigrationTable) migrateWalletSummary(ctx context.Context) error {
	m.tl.Info("开始迁移钱包摘要数据")

	db := m.repo.GetDB()
	var totalCount int64

	// 获取总数量
	if err := db.Model(&model.OldWalletSummary{}).Count(&totalCount).Error; err != nil {
		return fmt.Errorf("获取钱包摘要数据总数失败: %w", err)
	}

	m.tl.Info("钱包摘要数据统计", zap.Int64("total", totalCount))

	// 预加载KOL钱包头像映射
	m.tl.Info("开始加载KOL钱包头像数据...")
	kolAvatarMap, err := m.loadKOLWalletAvatars()
	if err != nil {
		return fmt.Errorf("加载KOL钱包头像数据失败: %w", err)
	}
	m.tl.Info("KOL钱包头像数据加载完成", zap.Int("count", len(kolAvatarMap)))

	var processed int64
	offset := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var oldWallets []model.OldWalletSummary
		if err := db.Limit(BatchSize).Offset(offset).Find(&oldWallets).Error; err != nil {
			return fmt.Errorf("查询钱包摘要数据失败: %w", err)
		}

		if len(oldWallets) == 0 {
			break
		}

		// 转换数据格式，使用KOL头像映射
		newWallets := make([]model.WalletSummary, 0, len(oldWallets))
		for _, old := range oldWallets {
			new := m.convertOldWalletSummaryToNew(old, kolAvatarMap)
			newWallets = append(newWallets, new)
		}

		// 批量写入，应用限流
		for _, wallet := range newWallets {
			if err := m.acquireToken(ctx); err != nil {
				return err
			}
			m.walletAsyncWriter.MustSubmit(wallet, utils.MergeHashKey(wallet.WalletAddress, wallet.UpdatedAt))
		}

		processed += int64(len(oldWallets))
		offset += BatchSize

		// 打印进度
		if processed%10000 == 0 || processed == totalCount {
			progress := float64(processed) / float64(totalCount) * 100
			m.tl.Info("钱包摘要数据迁移进度",
				zap.Int64("processed", processed),
				zap.Int64("total", totalCount),
				zap.Float64("progress", progress))
		}
	}

	m.tl.Info("钱包摘要数据迁移完成", zap.Int64("total", processed))
	return nil
}

// loadKOLWalletAvatars 加载KOL钱包头像映射
func (m *MigrationTable) loadKOLWalletAvatars() (map[string]string, error) {
	db := m.repo.GetDB()

	// 查询KOL钱包数据
	type KOLWallet struct {
		WalletAddress string `db:"wallet_address"`
		Avatar        string `db:"avatar"`
	}

	var kolWallets []KOLWallet
	err := db.Raw("SELECT address as wallet_address, logo as avatar FROM moonx.t_web3_kol_wallet WHERE logo IS NOT NULL AND logo != ''").Scan(&kolWallets).Error
	if err != nil {
		return nil, err
	}

	// 创建映射，支持地址规范化查找
	avatarMap := make(map[string]string, len(kolWallets))
	for _, kol := range kolWallets {
		if kol.Avatar != "" {
			// 同时存储原地址和规范化地址
			avatarMap[kol.WalletAddress] = kol.Avatar
			// 尝试规范化地址（如果有chain信息的话）
			if normalizedAddr := m.normalizeAddress(kol.WalletAddress); normalizedAddr != kol.WalletAddress {
				avatarMap[normalizedAddr] = kol.Avatar
			}
		}
	}

	return avatarMap, nil
}

// normalizeAddress 规范化钱包地址（用于映射查找）
func (m *MigrationTable) normalizeAddress(address string) string {
	// 可以添加地址规范化逻辑，这里先简单返回原地址
	// 如果需要可以调用utils.ChecksumAddress等方法
	return address
}

// convertTokenListToArray 将逗号分隔的字符串转换为TokenList对象数组
func convertTokenListToArray(tokenListStr string) model.TokenList {
	if tokenListStr == "" {
		return model.TokenList{}
	}
	tokens := strings.Split(tokenListStr, ",")
	// 过滤空字符串并构建TokenInfo对象
	var result model.TokenList
	for _, tokenAddr := range tokens {
		tokenAddr = strings.TrimSpace(tokenAddr)
		if tokenAddr != "" {
			result = append(result, model.TokenInfo{
				TokenAddress: tokenAddr,
				TokenIcon:    "", // 旧数据没有icon信息，设置为空
				TokenName:    "", // 旧数据没有name信息，设置为空
			})
		}
	}
	return result
}

// convertOldWalletSummaryToNew 转换旧钱包摘要数据到新格式
func (m *MigrationTable) convertOldWalletSummaryToNew(old model.OldWalletSummary, kolAvatarMap map[string]string) model.WalletSummary {
	// 基础信息映射
	checksumAddress := utils.ChecksumAddress(old.WalletAddress, old.Chain)
	new := model.WalletSummary{
		WalletAddress:   checksumAddress,
		Avatar:          "", // 初始为空，后面会从KOL数据中设置
		Balance:         limitFloat64ToDecimal(old.Balance),
		BalanceUSD:      limitFloat64ToDecimal(old.BalanceUSD),
		ChainID:         old.ChainID,
		TwitterName:     old.TwitterName,
		TwitterUsername: old.TwitterUsername,
		WalletType:      old.WalletType,
		AssetMultiple:   limitFloat64ToDecimal(old.AssetMultiple),
		TokenList:       convertTokenListToArray(old.TokenList),

		// 交易数据 - 30天
		AvgCost30d: limitFloat64ToDecimal(old.AvgCost30d),
		BuyNum30d:  old.BuyNum30d,
		SellNum30d: old.SellNum30d,
		WinRate30d: limitFloat64ToDecimal(old.WinRate30d),

		// 交易数据 - 7天
		AvgCost7d: limitFloat64ToDecimal(old.AvgCost7d),
		BuyNum7d:  old.BuyNum7d,
		SellNum7d: old.SellNum7d,
		WinRate7d: limitFloat64ToDecimal(old.WinRate7d),

		// 交易数据 - 1天
		AvgCost1d: limitFloat64ToDecimal(old.AvgCost1d),
		BuyNum1d:  old.BuyNum1d,
		SellNum1d: old.SellNum1d,
		WinRate1d: limitFloat64ToDecimal(old.WinRate1d),

		// 盈亏数据 - 30天
		PNL30d:               limitFloat64ToDecimal(old.PNL30d),
		PNLPercentage30d:     limitFloat64ToDecimal(old.PNLPercentage30d),
		PNLPic30d:            old.PNLPic30d,
		UnrealizedProfit30d:  limitFloat64ToDecimal(old.UnrealizedProfit30d),
		TotalCost30d:         limitFloat64ToDecimal(old.TotalCost30d),
		AvgRealizedProfit30d: limitFloat64ToDecimal(old.AvgRealizedProfit30d),

		// 盈亏数据 - 7天
		PNL7d:               limitFloat64ToDecimal(old.PNL7d),
		PNLPercentage7d:     limitFloat64ToDecimal(old.PNLPercentage7d),
		UnrealizedProfit7d:  limitFloat64ToDecimal(old.UnrealizedProfit7d),
		TotalCost7d:         limitFloat64ToDecimal(old.TotalCost7d),
		AvgRealizedProfit7d: limitFloat64ToDecimal(old.AvgRealizedProfit7d),

		// 盈亏数据 - 1天
		PNL1d:               limitFloat64ToDecimal(old.PNL1d),
		PNLPercentage1d:     limitFloat64ToDecimal(old.PNLPercentage1d),
		UnrealizedProfit1d:  limitFloat64ToDecimal(old.UnrealizedProfit1d),
		TotalCost1d:         limitFloat64ToDecimal(old.TotalCost1d),
		AvgRealizedProfit1d: limitFloat64ToDecimal(old.AvgRealizedProfit1d),

		// 收益分布数据 - 30天
		DistributionGt500_30d:             old.DistributionGt500_30d,
		Distribution200to500_30d:          old.Distribution200to500_30d,
		Distribution0to200_30d:            old.Distribution0to200_30d,
		DistributionN50to0_30d:            old.Distribution0to50_30d, // 映射字段名不同
		DistributionLt50_30d:              old.DistributionLt50_30d,
		DistributionGt500Percentage30d:    limitFloat64ToDecimal(old.DistributionGt500Percentage30d),
		Distribution200to500Percentage30d: limitFloat64ToDecimal(old.Distribution200to500Percentage30d),
		Distribution0to200Percentage30d:   limitFloat64ToDecimal(old.Distribution0to200Percentage30d),
		DistributionN50to0Percentage30d:   limitFloat64ToDecimal(old.Distribution0to50Percentage30d), // 映射字段名不同
		DistributionLt50Percentage30d:     limitFloat64ToDecimal(old.DistributionLt50Percentage30d),

		// 收益分布数据 - 7天
		DistributionGt500_7d:             old.DistributionGt500_7d,
		Distribution200to500_7d:          old.Distribution200to500_7d,
		Distribution0to200_7d:            old.Distribution0to200_7d,
		DistributionN50to0_7d:            old.Distribution0to50_7d, // 映射字段名不同
		DistributionLt50_7d:              old.DistributionLt50_7d,
		DistributionGt500Percentage7d:    limitFloat64ToDecimal(old.DistributionGt500Percentage7d),
		Distribution200to500Percentage7d: limitFloat64ToDecimal(old.Distribution200to500Percentage7d),
		Distribution0to200Percentage7d:   limitFloat64ToDecimal(old.Distribution0to200Percentage7d),
		DistributionN50to0Percentage7d:   limitFloat64ToDecimal(old.Distribution0to50Percentage7d), // 映射字段名不同
		DistributionLt50Percentage7d:     limitFloat64ToDecimal(old.DistributionLt50Percentage7d),

		// 时间和状态
		LastTransactionTime: old.LastTransactionTime * 1000,
		IsActive:            old.IsActive,
		UpdatedAt:           old.UpdateTime.UnixMilli(),
		CreatedAt:           time.Now().UnixMilli(), // 新建记录的创建时间
	}

	// 设置默认标签
	var tags []string
	if old.IsSmartWallet {
		tags = []string{model.TAG_SMART_MONEY}
	}

	if old.Tag != "" {
		tags = append(tags, old.Tag)
	}

	// 直接使用字符串数组
	new.Tags = pq.StringArray(tags)

	// 从KOL钱包映射中设置头像
	if avatar, exists := kolAvatarMap[old.WalletAddress]; exists {
		new.Avatar = avatar
	} else if avatar, exists := kolAvatarMap[checksumAddress]; exists {
		// 尝试使用规范化地址查找
		new.Avatar = avatar
	} else {
		// 如果没有找到KOL头像，生成默认头像
		new.Avatar = fmt.Sprintf("https://uploads.bydfi.in/moonx/avatar/%d.svg", utils.GetHashBucket(checksumAddress, 1000))
	}

	return new
}

// migrateWalletHolding 迁移钱包持仓数据
func (m *MigrationTable) migrateWalletHolding(ctx context.Context) error {
	m.tl.Info("开始迁移钱包持仓数据")

	db := m.repo.GetDB()
	var totalCount int64

	// 获取总数量
	if err := db.Model(&model.OldWalletHolding{}).Count(&totalCount).Error; err != nil {
		return fmt.Errorf("获取钱包持仓数据总数失败: %w", err)
	}

	m.tl.Info("钱包持仓数据统计", zap.Int64("total", totalCount))

	var processed int64
	offset := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 使用JOIN查询，在数据库层面关联holding和token_state表
		holdingWithStates, err := m.queryHoldingWithTokenState(db, BatchSize, offset)
		if err != nil {
			return fmt.Errorf("查询钱包持仓数据失败: %w", err)
		}

		if len(holdingWithStates) == 0 {
			break
		}

		// 转换数据格式
		newHoldings := make([]model.WalletHolding, 0, len(holdingWithStates))
		for _, data := range holdingWithStates {
			new := m.convertHoldingWithStateToNew(data)
			newHoldings = append(newHoldings, new)
		}

		// 批量写入，应用限流
		for _, holding := range newHoldings {
			if err := m.acquireToken(ctx); err != nil {
				return err
			}
			m.holdingAsyncWriter.MustSubmit(holding, utils.MergeHashKey(holding.WalletAddress, holding.UpdatedAt))
		}

		processed += int64(len(holdingWithStates))
		offset += BatchSize

		// 打印进度
		if processed%10000 == 0 || processed == totalCount {
			progress := float64(processed) / float64(totalCount) * 100
			m.tl.Info("钱包持仓数据迁移进度",
				zap.Int64("processed", processed),
				zap.Int64("total", totalCount),
				zap.Float64("progress", progress))
		}
	}

	m.tl.Info("钱包持仓数据迁移完成", zap.Int64("total", processed))
	return nil
}

// HoldingWithTokenState 存储holding和token_state的关联查询结果
type HoldingWithTokenState struct {
	// Holding数据
	HID                  int       `db:"h_id"`
	HWalletAddress       string    `db:"h_wallet_address"`
	HTokenAddress        string    `db:"h_token_address"`
	HTokenIcon           string    `db:"h_token_icon"`
	HTokenName           string    `db:"h_token_name"`
	HChain               string    `db:"h_chain"`
	HAmount              float64   `db:"h_amount"`
	HValue               float64   `db:"h_value"`
	HValueUSDT           float64   `db:"h_value_usdt"`
	HUnrealizedProfits   float64   `db:"h_unrealized_profits"`
	HPNL                 float64   `db:"h_pnl"`
	HPNLPercentage       float64   `db:"h_pnl_percentage"`
	HAvgPrice            float64   `db:"h_avg_price"`
	HMarketCap           float64   `db:"h_marketcap"`
	HIsCleared           bool      `db:"h_is_cleared"`
	HCumulativeCost      float64   `db:"h_cumulative_cost"`
	HCumulativeProfit    float64   `db:"h_cumulative_profit"`
	HLastTransactionTime int64     `db:"h_last_transaction_time"`
	HTime                time.Time `db:"h_time"`

	// TokenState数据（可能为NULL）
	TSCurrentAmount         *float64 `db:"ts_current_amount"`
	TSCurrentTotalCost      *float64 `db:"ts_current_total_cost"`
	TSCurrentAvgBuyPrice    *float64 `db:"ts_current_avg_buy_price"`
	TSPositionOpenedAt      *int64   `db:"ts_position_opened_at"`
	TSHistoricalBuyAmount   *float64 `db:"ts_historical_buy_amount"`
	TSHistoricalSellAmount  *float64 `db:"ts_historical_sell_amount"`
	TSHistoricalBuyCost     *float64 `db:"ts_historical_buy_cost"`
	TSHistoricalSellValue   *float64 `db:"ts_historical_sell_value"`
	TSHistoricalRealizedPNL *float64 `db:"ts_historical_realized_pnl"`
	TSHistoricalBuyCount    *int     `db:"ts_historical_buy_count"`
	TSHistoricalSellCount   *int     `db:"ts_historical_sell_count"`
	TSLastTransactionTime   *int64   `db:"ts_last_transaction_time"`
}

// queryHoldingWithTokenState 使用JOIN查询holding和token_state数据
func (m *MigrationTable) queryHoldingWithTokenState(db *gorm.DB, limit, offset int) ([]HoldingWithTokenState, error) {
	var results []HoldingWithTokenState

	// 使用原生SQL查询，LEFT JOIN确保即使没有token_state也能查出holding数据
	query := `
		SELECT 
			h.id as h_id, h.wallet_address as h_wallet_address, h.token_address as h_token_address, 
			h.token_icon as h_token_icon, h.token_name as h_token_name, h.chain as h_chain,
			h.amount as h_amount, h.value as h_value, h.value_usdt as h_value_usdt,
			h.unrealized_profits as h_unrealized_profits, h.pnl as h_pnl, 
			h.pnl_percentage as h_pnl_percentage, h.avg_price as h_avg_price,
			h.marketcap as h_marketcap, h.is_cleared as h_is_cleared,
			h.cumulative_cost as h_cumulative_cost, h.cumulative_profit as h_cumulative_profit,
			h.last_transaction_time as h_last_transaction_time, h.time as h_time,
			
			ts.current_amount as ts_current_amount, ts.current_total_cost as ts_current_total_cost,
			ts.current_avg_buy_price as ts_current_avg_buy_price, ts.position_opened_at as ts_position_opened_at,
			ts.historical_buy_amount as ts_historical_buy_amount, ts.historical_sell_amount as ts_historical_sell_amount,
			ts.historical_buy_cost as ts_historical_buy_cost, ts.historical_sell_value as ts_historical_sell_value,
			ts.historical_realized_pnl as ts_historical_realized_pnl, ts.historical_buy_count as ts_historical_buy_count,
			ts.historical_sell_count as ts_historical_sell_count, ts.last_transaction_time as ts_last_transaction_time
		FROM dex_query_v1.wallet_holding h
		LEFT JOIN dex_query_v1.wallet_token_state ts ON h.wallet_address = ts.wallet_address AND h.token_address = ts.token_address
		ORDER BY h.id
		LIMIT ? OFFSET ?
	`

	err := db.Raw(query, limit, offset).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	return results, nil
}

// convertHoldingWithStateToNew 转换关联查询结果到新格式
func (m *MigrationTable) convertHoldingWithStateToNew(data HoldingWithTokenState) model.WalletHolding {
	new := model.WalletHolding{
		// 基础信息（从holding表）
		WalletAddress: utils.ChecksumAddress(data.HWalletAddress, data.HChain),
		TokenAddress:  utils.ChecksumAddress(data.HTokenAddress, data.HChain),
		TokenIcon:     data.HTokenIcon,
		TokenName:     data.HTokenName,
		Amount:        limitFloat64ToDecimal(data.HAmount),
		ValueUSD:      limitFloat64ToDecimal(data.HValueUSDT), // 映射字段名

		// 盈亏数据（从holding表）
		UnrealizedProfits: limitFloat64ToDecimal(data.HUnrealizedProfits),
		PNL:               limitFloat64ToDecimal(data.HPNL),
		PNLPercentage:     limitFloat64ToDecimal(data.HPNLPercentage),
		AvgPrice:          limitFloat64ToDecimal(data.HAvgPrice),
		CurrentTotalCost:  limitFloat64ToDecimal(data.HCumulativeCost), // 映射字段名
		MarketCap:         limitFloat64ToDecimal(data.HMarketCap),
    // is_cleared 已移除，不再映射
		IsDev:             false, // 旧表没有这个字段，默认false

		// 历史累计状态 - 默认值，会被TokenState数据覆盖
		HistoricalBuyAmount:  limitFloat64ToDecimal(data.HAmount),           // 默认：假设当前持仓就是历史买入
		HistoricalSellAmount: decimal.Zero,                                  // 默认为0
		HistoricalBuyCost:    limitFloat64ToDecimal(data.HCumulativeCost),   // 默认：累计成本即为买入成本
		HistoricalSellValue:  limitFloat64ToDecimal(data.HCumulativeProfit), // 默认：累计盈利
		HistoricalBuyCount:   1,                                             // 默认为1
		HistoricalSellCount:  0,                                             // 默认为0

		// 时间信息（从holding表）
		LastTransactionTime: data.HLastTransactionTime * 1000,
		UpdatedAt:           data.HTime.UnixMilli(),
		CreatedAt:           time.Now().UnixMilli(),
	}

	// 设置ChainID，从chain字段推导
	chainID := bip0044.NetworkNameToChainId(data.HChain)
	new.ChainID = chainID

	// 如果有TokenState数据，使用其覆盖默认值
	if data.TSHistoricalBuyAmount != nil {
		new.HistoricalBuyAmount = limitFloat64ToDecimal(*data.TSHistoricalBuyAmount)
	}
	if data.TSHistoricalSellAmount != nil {
		new.HistoricalSellAmount = limitFloat64ToDecimal(*data.TSHistoricalSellAmount)
	}
	if data.TSHistoricalBuyCost != nil {
		new.HistoricalBuyCost = limitFloat64ToDecimal(*data.TSHistoricalBuyCost)
	}
	if data.TSHistoricalSellValue != nil {
		new.HistoricalSellValue = limitFloat64ToDecimal(*data.TSHistoricalSellValue)
	}
	if data.TSHistoricalBuyCount != nil {
		new.HistoricalBuyCount = *data.TSHistoricalBuyCount
	}
	if data.TSHistoricalSellCount != nil {
		new.HistoricalSellCount = *data.TSHistoricalSellCount
	}

	// 使用TokenState的建仓时间（如果有的话）
	if data.TSPositionOpenedAt != nil {
		tmp := *data.TSPositionOpenedAt * 1000
		new.PositionOpenedAt = &tmp
	}

	// 使用TokenState的最后交易时间（如果更准确的话）
	if data.TSLastTransactionTime != nil && *data.TSLastTransactionTime > new.LastTransactionTime {
		new.LastTransactionTime = *data.TSLastTransactionTime * 1000
		if *data.TSLastTransactionTime > 1800000000 { // 如果是毫秒级时间戳，则不操作
			new.LastTransactionTime = *data.TSLastTransactionTime
		}
	}

	// 使用TokenState的当前持仓数据（可能更准确）
	if data.TSCurrentAmount != nil && *data.TSCurrentAmount > 0 {
		new.Amount = limitFloat64ToDecimal(*data.TSCurrentAmount)
	}
	if data.TSCurrentTotalCost != nil && *data.TSCurrentTotalCost > 0 {
		new.CurrentTotalCost = limitFloat64ToDecimal(*data.TSCurrentTotalCost)
	}
	if data.TSCurrentAvgBuyPrice != nil && *data.TSCurrentAvgBuyPrice > 0 {
		new.AvgPrice = limitFloat64ToDecimal(*data.TSCurrentAvgBuyPrice)
	}

	// 设置首次建仓时间（如果还没有设置的话）
	if new.PositionOpenedAt == nil && new.LastTransactionTime > 0 {
		new.PositionOpenedAt = &new.LastTransactionTime
	}

	// 设置tags - 可以根据业务逻辑添加
	new.Tags = pq.StringArray([]string{})

	return new
}

// migrateWalletTransaction 迁移钱包交易数据
func (m *MigrationTable) migrateWalletTransaction(ctx context.Context) error {
	m.tl.Info("开始迁移钱包交易数据")

	db := m.repo.GetDB()
	var totalCount int64

	// 获取总数量
	if err := db.Model(&model.OldWalletTransaction{}).Count(&totalCount).Error; err != nil {
		return fmt.Errorf("获取钱包交易数据总数失败: %w", err)
	}

	m.tl.Info("钱包交易数据统计", zap.Int64("total", totalCount))

	// 预加载有效的钱包地址集合
	validWallets, err := m.loadValidWalletAddresses()
	if err != nil {
		return fmt.Errorf("加载有效钱包地址失败: %w", err)
	}

	m.tl.Info("有效钱包地址数量", zap.Int("count", len(validWallets)))

	var processed int64
	var skipped int64
	offset := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var oldTransactions []model.OldWalletTransaction
		if err := db.Limit(BatchSize).Offset(offset).Find(&oldTransactions).Error; err != nil {
			return fmt.Errorf("查询钱包交易数据失败: %w", err)
		}

		if len(oldTransactions) == 0 {
			break
		}

		// 过滤并转换数据格式
		newTransactions := make([]model.WalletTransaction, 0, len(oldTransactions))
		for _, old := range oldTransactions {
			// 检查钱包地址是否在有效钱包列表中
			if _, exists := validWallets[old.WalletAddress]; !exists {
				skipped++
				continue
			}

			new := m.convertOldWalletTransactionToNew(old)
			newTransactions = append(newTransactions, new)
		}

		// 批量写入，应用限流
		for _, transaction := range newTransactions {
			if err := m.acquireToken(ctx); err != nil {
				return err
			}
			m.transactionAsyncWriter.MustSubmit(transaction, utils.MergeHashKey(transaction.WalletAddress, transaction.TransactionTime))
		}

		processed += int64(len(oldTransactions))
		offset += BatchSize

		// 打印进度
		if processed%10000 == 0 || processed == totalCount {
			progress := float64(processed) / float64(totalCount) * 100
			m.tl.Info("钱包交易数据迁移进度",
				zap.Int64("processed", processed),
				zap.Int64("total", totalCount),
				zap.Int64("skipped", skipped),
				zap.Int64("migrated", processed-skipped),
				zap.Float64("progress", progress))
		}
	}

	m.tl.Info("钱包交易数据迁移完成",
		zap.Int64("total", processed),
		zap.Int64("skipped", skipped),
		zap.Int64("migrated", processed-skipped))
	return nil
}

// loadValidWalletAddresses 加载有效的钱包地址（存在于旧钱包表中的）
func (m *MigrationTable) loadValidWalletAddresses() (map[string]bool, error) {
	db := m.repo.GetDB()

	var walletAddresses []string
	if err := db.Model(&model.OldWalletSummary{}).Pluck("wallet_address", &walletAddresses).Error; err != nil {
		return nil, err
	}

	validWallets := make(map[string]bool, len(walletAddresses))
	for _, addr := range walletAddresses {
		validWallets[addr] = true
	}

	return validWallets, nil
}

// convertOldWalletTransactionToNew 转换旧钱包交易数据到新格式
func (m *MigrationTable) convertOldWalletTransactionToNew(old model.OldWalletTransaction) model.WalletTransaction {
	new := model.WalletTransaction{
		WalletAddress:            utils.ChecksumAddress(old.WalletAddress, old.Chain),
		WalletBalance:            limitFloat64ToDecimal(old.WalletBalance),
		TokenAddress:             utils.ChecksumAddress(old.TokenAddress, old.Chain),
		TokenIcon:                old.TokenIcon,
		TokenName:                old.TokenName,
		Price:                    limitFloat64ToDecimal(old.Price),
		Amount:                   limitFloat64ToDecimal(old.Amount),
		MarketCap:                limitFloat64ToDecimal(old.MarketCap),
		Value:                    limitFloat64ToDecimal(old.Value),
		HoldingPercentage:        limitFloat64ToDecimal(old.HoldingPercentage),
		ChainID:                  old.ChainID,
		RealizedProfit:           limitFloat64ToDecimal(old.RealizedProfit),
		RealizedProfitPercentage: limitFloat64ToDecimal(old.RealizedProfitPercentage),
		TransactionType:          old.TransactionType,
		TransactionTime:          old.TransactionTime * 1000,
		Signature:                old.Signature,
		FromTokenAddress:         utils.ChecksumAddress(old.FromTokenAddress, old.Chain),
		FromTokenSymbol:          old.FromTokenSymbol,
		FromTokenAmount:          limitFloat64ToDecimal(old.FromTokenAmount),
		DestTokenAddress:         utils.ChecksumAddress(old.DestTokenAddress, old.Chain),
		DestTokenSymbol:          old.DestTokenSymbol,
		DestTokenAmount:          limitFloat64ToDecimal(old.DestTokenAmount),
		CreatedAt:                old.Time.UnixMilli(),
	}

	return new
}
