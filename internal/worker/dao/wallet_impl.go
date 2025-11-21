package dao

import (
	"context"
	"errors"
	"fmt"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/pkg/utils"

	"github.com/bytedance/sonic"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// walletDAO 实现WalletDAO接口
type walletDAO struct {
	cfg        *config.Config
	db         *gorm.DB
	rds        *redis.Client
	localCache *cache.Cache
}

// NewWalletDAO 创建WalletDAO实例
func NewWalletDAO(cfg *config.Config, db *gorm.DB, rds *redis.Client) WalletDAO {
	localCache := cache.New(10*time.Minute, time.Minute)
	return &walletDAO{
		cfg:        cfg,
		db:         db,
		rds:        rds,
		localCache: localCache,
	}
}

// GetByWalletAddress 通过钱包地址查询钱包摘要信息（核心需求）
func (w *walletDAO) GetByWalletAddress(ctx context.Context, chainId uint64, walletAddress string) (*model.WalletSummary, error) {
	cacheKey := utils.WalletSummaryKey(chainId, walletAddress)

	// 先查本地缓存
	if cached, found := w.localCache.Get(cacheKey); found {
		if wallet, ok := cached.(*model.WalletSummary); ok {
			return wallet, nil
		}
	}

	// 再查Redis缓存
	cached, err := w.rds.Get(ctx, cacheKey).Result()
	if err == nil {
		if cached == "null" {
			return nil, nil
		}

		var wallet model.WalletSummary
		if sonic.Unmarshal([]byte(cached), &wallet) == nil {
			// 更新本地缓存
			w.localCache.Set(cacheKey, &wallet, cache.DefaultExpiration)
			return &wallet, nil
		}
	}

	// 查数据库
	var wallet model.WalletSummary
	err = w.db.WithContext(ctx).
		Where("chain_id = ? AND wallet_address = ?", chainId, walletAddress).
		First(&wallet).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 缓存空结果，避免缓存穿透
			w.localCache.Set(cacheKey, (*model.WalletSummary)(nil), 1*time.Minute)
			w.rds.Set(ctx, cacheKey, "null", 1*time.Minute).Result()
			return nil, nil
		}
		return nil, err
	}

	// 更新缓存
	w.UpdateWalletCache(ctx, cacheKey, &wallet)
	return &wallet, nil
}

// updateWalletCache 更新钱包缓存
func (w *walletDAO) UpdateWalletCache(ctx context.Context, cacheKey string, wallet *model.WalletSummary) error {
	// 更新本地缓存
	w.localCache.Set(cacheKey, wallet, cache.DefaultExpiration)

	// 更新Redis缓存
	if data, err := sonic.Marshal(wallet); err == nil {
		_, err = w.rds.Set(ctx, cacheKey, string(data), 15*time.Minute).Result() // 钱包数据更新频繁，缓存时间适中
		return err
	}

	return nil
}

// clearWalletCache 清除钱包缓存
func (w *walletDAO) clearWalletCache(ctx context.Context, chainId uint64, walletAddress string) {
	cacheKey := utils.WalletSummaryKey(chainId, walletAddress)
	w.localCache.Delete(cacheKey)
	w.rds.Del(ctx, cacheKey).Result()
}

// GetByID 通过ID查询钱包信息
func (w *walletDAO) GetByID(ctx context.Context, id int) (*model.WalletSummary, error) {
	var wallet model.WalletSummary
	err := w.db.WithContext(ctx).
		Where("id = ?", id).
		First(&wallet).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &wallet, nil
}

// GetByChain 获取指定链上的钱包列表
func (w *walletDAO) GetByChain(ctx context.Context, chainID uint64, limit, offset int) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	err := w.db.WithContext(ctx).
		Where("chain_id = ?", chainID).
		Order("updated_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// GetByTags 通过标签查询钱包
func (w *walletDAO) GetByTags(ctx context.Context, tags []string, limit, offset int) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	query := w.db.WithContext(ctx)

	// 使用PostgreSQL数组操作符查询包含任意一个标签的钱包
	if len(tags) > 0 {
		query = query.Where("tags && ?", tags)
	}

	err := query.Order("updated_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// GetByWalletType 通过钱包类型查询
func (w *walletDAO) GetByWalletType(ctx context.Context, walletType int, limit, offset int) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	err := w.db.WithContext(ctx).
		Where("wallet_type = ?", walletType).
		Order("updated_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// GetActiveWallets 获取活跃钱包
func (w *walletDAO) GetActiveWallets(ctx context.Context, limit, offset int) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	err := w.db.WithContext(ctx).
		Where("is_active = ?", true).
		Order("last_transaction_time DESC").
		Limit(limit).
		Offset(offset).
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// GetTopPerformers 获取表现最好的钱包（按PNL排序）
func (w *walletDAO) GetTopPerformers(ctx context.Context, period string, limit, offset int) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	var orderBy string

	switch period {
	case "1d":
		orderBy = "pnl_1d DESC"
	case "7d":
		orderBy = "pnl_7d DESC"
	case "30d":
		orderBy = "pnl_30d DESC"
	default:
		orderBy = "pnl_30d DESC"
	}

	err := w.db.WithContext(ctx).
		Where("is_active = ?", true).
		Order(orderBy).
		Limit(limit).
		Offset(offset).
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// GetByTwitterUsername 通过Twitter用户名查询钱包
func (w *walletDAO) GetByTwitterUsername(ctx context.Context, twitterUsername string) ([]*model.WalletSummary, error) {
	var wallets []*model.WalletSummary
	err := w.db.WithContext(ctx).
		Where("twitter_username = ?", twitterUsername).
		Order("updated_at DESC").
		Find(&wallets).Error

	if err != nil {
		return nil, err
	}

	return wallets, nil
}

// Create 创建新的钱包记录
func (w *walletDAO) Create(ctx context.Context, wallet *model.WalletSummary) error {
	return w.db.WithContext(ctx).Create(wallet).Error
}

// Update 更新钱包记录
func (w *walletDAO) Update(ctx context.Context, wallet *model.WalletSummary) error {
	return w.db.WithContext(ctx).Save(wallet).Error
}

// Delete 删除钱包记录
func (w *walletDAO) Delete(ctx context.Context, id int) error {
	return w.db.WithContext(ctx).Delete(&model.WalletSummary{}, id).Error
}

// BatchUpdate 批量更新钱包信息
func (w *walletDAO) BatchUpdate(ctx context.Context, wallets []*model.WalletSummary) error {
	if len(wallets) == 0 {
		return nil
	}

	// 使用事务进行批量更新
	return w.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, wallet := range wallets {
			if err := tx.Save(wallet).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// GetWalletStats 获取钱包统计信息
func (w *walletDAO) GetWalletStats(ctx context.Context, walletAddress string) (*model.WalletStats, error) {
	var stats model.WalletStats

	// 获取总钱包数
	if err := w.db.WithContext(ctx).Model(&model.WalletSummary{}).Count(&stats.TotalWallets).Error; err != nil {
		return nil, fmt.Errorf("获取总钱包数失败: %w", err)
	}

	// 获取活跃钱包数
	if err := w.db.WithContext(ctx).Model(&model.WalletSummary{}).
		Where("is_active = ?", true).
		Count(&stats.ActiveWallets).Error; err != nil {
		return nil, fmt.Errorf("获取活跃钱包数失败: %w", err)
	}

	// 获取平均PNL和胜率
	var avgStats struct {
		AvgPNL30d     float64 `gorm:"column:avg_pnl_30d"`
		AvgWinRate30d float64 `gorm:"column:avg_win_rate_30d"`
	}

	if err := w.db.WithContext(ctx).Model(&model.WalletSummary{}).
		Select("AVG(pnl_30d) as avg_pnl_30d, AVG(win_rate_30d) as avg_win_rate_30d").
		Where("is_active = ?", true).
		Scan(&avgStats).Error; err != nil {
		return nil, fmt.Errorf("获取平均统计数据失败: %w", err)
	}

	stats.AvgPNL30d = avgStats.AvgPNL30d
	stats.AvgWinRate30d = avgStats.AvgWinRate30d

	// 获取表现优秀的钱包数（PNL > 0且胜率 > 0.6）
	if err := w.db.WithContext(ctx).Model(&model.WalletSummary{}).
		Where("is_active = ? AND pnl_30d > 0 AND win_rate_30d > 0.6", true).
		Count(&stats.TopPerformers).Error; err != nil {
		return nil, fmt.Errorf("获取优秀钱包数失败: %w", err)
	}

	// 获取聪明钱数量（包含smart_money标签）
	if err := w.db.WithContext(ctx).Model(&model.WalletSummary{}).
		Where("is_active = ? AND 'smart_money' = ANY(tags)", true).
		Count(&stats.SmartMoneyCount).Error; err != nil {
		return nil, fmt.Errorf("获取聪明钱数量失败: %w", err)
	}

	return &stats, nil
}
