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
func (m *MigrationTable) convertTokenListToArray(chainID uint64, tokenListStr string) model.TokenList {
	if tokenListStr == "" {
		return model.TokenList{}
	}
	tokens := strings.Split(tokenListStr, ",")
	// 过滤空字符串并构建TokenInfo对象
	var result model.TokenList
	for _, tokenAddr := range tokens {
		tokenAddr = strings.TrimSpace(tokenAddr)
		if tokenAddr != "" {
			var tokenInfo model.SmTokenRet
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := m.repo.GetDB().WithContext(ctx).
				Table("dex_query_v1.web3_tokens").
				Select("address, name, symbol, total_supply as supply, contract_info->>'creator' AS creater, logo").
				Where("chain_id = ? AND address = ?", chainID, tokenAddr).
				First(&tokenInfo).Error
			cancel()

			t := model.TokenInfo{
				TokenAddress: tokenAddr,
				TokenIcon:    "", // 旧数据没有icon信息，设置为空
				TokenName:    "", // 旧数据没有name信息，设置为空
			}

			if err == nil { // 查到数据则更新token信息
				t.TokenIcon = tokenInfo.Logo
				t.TokenName = tokenInfo.Symbol
			}

			result = append(result, t)
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
		TokenList:       m.convertTokenListToArray(old.ChainID, old.TokenList),

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

// migrateWalletHolding 迁移钱包持仓数据（分两阶段）
func (m *MigrationTable) migrateWalletHolding(ctx context.Context) error {
	m.tl.Info("开始迁移钱包持仓数据")

	// 阶段1：迁移旧wallet_holding表到新表
	if err := m.migrateHoldingBasic(ctx); err != nil {
		return fmt.Errorf("迁移基础持仓数据失败: %w", err)
	}

	// 阶段2：使用wallet_token_state表数据更新新表
	if err := m.updateHoldingFromTokenState(ctx); err != nil {
		return fmt.Errorf("使用token_state数据更新持仓失败: %w", err)
	}

	m.tl.Info("钱包持仓数据迁移完成")
	return nil
}

// migrateHoldingBasic 阶段1：迁移旧wallet_holding表的基础数据
func (m *MigrationTable) migrateHoldingBasic(ctx context.Context) error {
	m.tl.Info("阶段1：开始迁移基础持仓数据")

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

		var oldHoldings []model.OldWalletHolding
		if err := db.Limit(BatchSize).Offset(offset).Find(&oldHoldings).Error; err != nil {
			return fmt.Errorf("查询钱包持仓数据失败: %w", err)
		}

		if len(oldHoldings) == 0 {
			break
		}

		// 转换数据格式
		newHoldings := make([]model.WalletHolding, 0, len(oldHoldings))
		for _, old := range oldHoldings {
			new := m.convertOldHoldingToNew(old)
			newHoldings = append(newHoldings, new)
		}

		// 批量写入，应用限流
		for _, holding := range newHoldings {
			if err := m.acquireToken(ctx); err != nil {
				return err
			}
			m.holdingAsyncWriter.MustSubmit(holding, utils.MergeHashKey(holding.WalletAddress, holding.UpdatedAt))
		}

		processed += int64(len(oldHoldings))
		offset += BatchSize

		// 打印进度
		if processed%10000 == 0 || processed == totalCount {
			progress := float64(processed) / float64(totalCount) * 100
			m.tl.Info("基础持仓数据迁移进度",
				zap.Int64("processed", processed),
				zap.Int64("total", totalCount),
				zap.Float64("progress", progress))
		}
	}

	m.tl.Info("基础持仓数据迁移完成", zap.Int64("total", processed))
	return nil
}

// updateHoldingFromTokenState 阶段2：使用wallet_token_state表数据更新新表
func (m *MigrationTable) updateHoldingFromTokenState(ctx context.Context) error {
	m.tl.Info("阶段2：开始使用token_state数据更新持仓")

	db := m.repo.GetDB()
	var totalCount int64

	// 获取token_state总数量
	if err := db.Table("dex_query_v1.wallet_token_state").Count(&totalCount).Error; err != nil {
		return fmt.Errorf("获取token_state数据总数失败: %w", err)
	}

	m.tl.Info("token_state数据统计", zap.Int64("total", totalCount))

	var processed int64
	offset := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var tokenStates []TokenStateData
		err := db.Table("dex_query_v1.wallet_token_state").
			Limit(BatchSize).
			Offset(offset).
			Find(&tokenStates).Error
		if err != nil {
			return fmt.Errorf("查询token_state数据失败: %w", err)
		}

		if len(tokenStates) == 0 {
			break
		}

		// 批量更新
		for _, ts := range tokenStates {
			if err := m.acquireToken(ctx); err != nil {
				return err
			}

			// 更新对应的holding记录
			if err := m.updateHoldingRecord(db, ts); err != nil {
				m.tl.Warn("更新holding记录失败",
					zap.String("wallet", ts.WalletAddress),
					zap.String("token", ts.TokenAddress),
					zap.Error(err))
				// 继续处理下一条，不中断
			}
		}

		processed += int64(len(tokenStates))
		offset += BatchSize

		// 打印进度
		if processed%10000 == 0 || processed == totalCount {
			progress := float64(processed) / float64(totalCount) * 100
			m.tl.Info("token_state数据更新进度",
				zap.Int64("processed", processed),
				zap.Int64("total", totalCount),
				zap.Float64("progress", progress))
		}
	}

	m.tl.Info("token_state数据更新完成", zap.Int64("total", processed))
	return nil
}

// TokenStateData 存储wallet_token_state表的数据
type TokenStateData struct {
	WalletAddress         string  `gorm:"column:wallet_address"`
	TokenAddress          string  `gorm:"column:token_address"`
	Chain                 string  `gorm:"column:chain"`
	CurrentAmount         float64 `gorm:"column:current_amount"`
	CurrentTotalCost      float64 `gorm:"column:current_total_cost"`
	CurrentAvgBuyPrice    float64 `gorm:"column:current_avg_buy_price"`
	PositionOpenedAt      int64   `gorm:"column:position_opened_at"`
	HistoricalBuyAmount   float64 `gorm:"column:historical_buy_amount"`
	HistoricalSellAmount  float64 `gorm:"column:historical_sell_amount"`
	HistoricalBuyCost     float64 `gorm:"column:historical_buy_cost"`
	HistoricalSellValue   float64 `gorm:"column:historical_sell_value"`
	HistoricalRealizedPNL float64 `gorm:"column:historical_realized_pnl"`
	HistoricalBuyCount    int     `gorm:"column:historical_buy_count"`
	HistoricalSellCount   int     `gorm:"column:historical_sell_count"`
	LastTransactionTime   int64   `gorm:"column:last_transaction_time"`
}

// convertOldHoldingToNew 转换旧holding数据到新格式（仅基础数据）
func (m *MigrationTable) convertOldHoldingToNew(old model.OldWalletHolding) model.WalletHolding {
	new := model.WalletHolding{
		// 基础信息
		WalletAddress: utils.ChecksumAddress(old.WalletAddress, old.Chain),
		TokenAddress:  utils.ChecksumAddress(old.TokenAddress, old.Chain),
		TokenIcon:     old.TokenIcon,
		TokenName:     old.TokenName,
		Amount:        limitFloat64ToDecimal(old.Amount),
		ValueUSD:      limitFloat64ToDecimal(old.ValueUSDT),

		// 盈亏数据
		UnrealizedProfits: limitFloat64ToDecimal(old.UnrealizedProfits),
		PNL:               limitFloat64ToDecimal(old.PNL),
		PNLPercentage:     limitFloat64ToDecimal(old.PNLPercentage),
		AvgPrice:          limitFloat64ToDecimal(old.AvgPrice),
		CurrentTotalCost:  limitFloat64ToDecimal(old.CumulativeCost),
		MarketCap:         limitFloat64ToDecimal(old.MarketCap),
		IsDev:             false, // 旧表没有这个字段，默认false

		// 历史累计状态 - 使用默认值，后续会被token_state更新
		HistoricalBuyAmount:  limitFloat64ToDecimal(old.Amount),           // 默认：当前持仓
		HistoricalSellAmount: decimal.Zero,                                // 默认为0
		HistoricalBuyCost:    limitFloat64ToDecimal(old.CumulativeCost),   // 默认：累计成本
		HistoricalSellValue:  limitFloat64ToDecimal(old.CumulativeProfit), // 默认：累计盈利
		HistoricalBuyCount:   1,                                           // 默认为1
		HistoricalSellCount:  0,                                           // 默认为0

		// 时间信息
		LastTransactionTime: old.LastTransactionTime * 1000,
		UpdatedAt:           old.Time.UnixMilli(),
		CreatedAt:           time.Now().UnixMilli(),
	}

	// 设置ChainID
	chainID := bip0044.NetworkNameToChainId(old.Chain)
	new.ChainID = chainID

	// 设置首次建仓时间
	if new.LastTransactionTime > 0 {
		new.PositionOpenedAt = &new.LastTransactionTime
	}

	// 设置tags
	new.Tags = pq.StringArray([]string{})

	return new
}

// updateHoldingRecord 使用token_state数据更新holding记录
func (m *MigrationTable) updateHoldingRecord(db *gorm.DB, ts TokenStateData) error {
	// 规范化地址
	walletAddress := utils.ChecksumAddress(ts.WalletAddress, ts.Chain)
	tokenAddress := utils.ChecksumAddress(ts.TokenAddress, ts.Chain)

	// 构建更新map
	updates := make(map[string]interface{})

	// 更新历史累计数据
	if ts.HistoricalBuyAmount >= 0 {
		updates["historical_buy_amount"] = limitFloat64ToDecimal(ts.HistoricalBuyAmount)
	}
	if ts.HistoricalSellAmount >= 0 {
		updates["historical_sell_amount"] = limitFloat64ToDecimal(ts.HistoricalSellAmount)
	}
	if ts.HistoricalBuyCost >= 0 {
		updates["historical_buy_cost"] = limitFloat64ToDecimal(ts.HistoricalBuyCost)
	}
	if ts.HistoricalSellValue >= 0 {
		updates["historical_sell_value"] = limitFloat64ToDecimal(ts.HistoricalSellValue)
	}
	if ts.HistoricalBuyCount > 0 {
		updates["historical_buy_count"] = ts.HistoricalBuyCount
	}
	if ts.HistoricalSellCount >= 0 {
		updates["historical_sell_count"] = ts.HistoricalSellCount
	}

	// 更新当前持仓数据（如果token_state的数据更准确）
	if ts.CurrentAmount > 0 {
		updates["amount"] = limitFloat64ToDecimal(ts.CurrentAmount)
	}
	if ts.CurrentTotalCost > 0 {
		updates["current_total_cost"] = limitFloat64ToDecimal(ts.CurrentTotalCost)
	}
	if ts.CurrentAvgBuyPrice > 0 {
		updates["avg_price"] = limitFloat64ToDecimal(ts.CurrentAvgBuyPrice)
	}

	// 更新建仓时间
	if ts.PositionOpenedAt > 0 {
		positionOpenedAt := ts.PositionOpenedAt * 1000
		updates["position_opened_at"] = positionOpenedAt
	}

	// 更新最后交易时间（如果更新）
	if ts.LastTransactionTime > 0 {
		lastTransactionTime := ts.LastTransactionTime * 1000
		// 判断是否已经是毫秒级
		if ts.LastTransactionTime > 1800000000 {
			lastTransactionTime = ts.LastTransactionTime
		}
		updates["last_transaction_time"] = lastTransactionTime
	}

	// 如果没有需要更新的字段，直接返回
	if len(updates) == 0 {
		return nil
	}

	// 执行更新
	result := db.Table("dex_query_v1.t_smart_holding").
		Where("wallet_address = ? AND token_address = ?", walletAddress, tokenAddress).
		Updates(updates)

	if result.Error != nil {
		return result.Error
	}

	// 如果没有匹配到记录，记录警告（不返回错误，因为可能holding表中没有这条记录）
	if result.RowsAffected == 0 {
		m.tl.Debug("未找到对应的holding记录",
			zap.String("wallet", walletAddress),
			zap.String("token", tokenAddress))
	}

	return nil
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
