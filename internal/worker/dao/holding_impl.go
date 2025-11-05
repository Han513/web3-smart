package dao

import (
	"context"
	"errors"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/pkg/utils"

	"github.com/bytedance/sonic"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// holdingDAO 实现HoldingDAO接口
type holdingDAO struct {
	db         *gorm.DB
	rds        *redis.Client
	localCache *cache.Cache
}

// NewHoldingDAO 创建HoldingDAO实例
func NewHoldingDAO(db *gorm.DB, rds *redis.Client) HoldingDAO {
	localCache := cache.New(10*time.Minute, time.Minute)
	return &holdingDAO{
		db:         db,
		rds:        rds,
		localCache: localCache,
	}
}

// GetByWalletAndToken 通过钱包地址和代币地址查询持仓信息
func (h *holdingDAO) GetByWalletAndToken(ctx context.Context, chainId uint64, walletAddress, tokenAddress string) (*model.WalletHolding, error) {
	cacheKey := utils.HoldingKey(chainId, walletAddress, tokenAddress)

	// 先查本地缓存
	if cached, found := h.localCache.Get(cacheKey); found {
		if holding, ok := cached.(*model.WalletHolding); ok {
			return holding, nil
		}
	}

	// 再查Redis缓存
	cached, err := h.rds.Get(ctx, cacheKey).Result()
	if err == nil {
		if cached == "null" {
			return nil, nil
		}

		var holding model.WalletHolding
		if sonic.Unmarshal([]byte(cached), &holding) == nil {
			// 更新本地缓存
			h.localCache.Set(cacheKey, &holding, cache.DefaultExpiration)
			return &holding, nil
		}
	}

	// 查数据库
	var holding model.WalletHolding
	err = h.db.WithContext(ctx).
		Where("chain_id = ? AND wallet_address = ? AND token_address = ?", chainId, walletAddress, tokenAddress).
		First(&holding).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 缓存空结果，避免缓存穿透
			h.localCache.Set(cacheKey, (*model.WalletHolding)(nil), 1*time.Minute)
			h.rds.Set(ctx, cacheKey, "null", 1*time.Minute).Result()
			return nil, nil
		}
		return nil, err
	}

	// 更新缓存
	h.UpdateHoldingCache(ctx, cacheKey, &holding)
	return &holding, nil
}

// updateHoldingCache 更新持仓缓存
func (h *holdingDAO) UpdateHoldingCache(ctx context.Context, cacheKey string, holding *model.WalletHolding) error {
	// 更新本地缓存
	h.localCache.Set(cacheKey, holding, cache.DefaultExpiration)

	// 更新Redis缓存
	if data, err := sonic.Marshal(holding); err == nil {
		_, err = h.rds.Set(ctx, cacheKey, string(data), 10*time.Minute).Result()
		return err
	}

	return nil
}

// clearHoldingCache 清除持仓缓存
func (h *holdingDAO) clearHoldingCache(ctx context.Context, chainId uint64, walletAddress, tokenAddress string) {
	cacheKey := utils.HoldingKey(chainId, walletAddress, tokenAddress)
	h.localCache.Delete(cacheKey)
	h.rds.Del(ctx, cacheKey).Result()
}

// GetByWallet 通过钱包地址查询所有持仓信息
func (h *holdingDAO) GetByWallet(ctx context.Context, chainId uint64, walletAddress string) ([]*model.WalletHolding, error) {
	var holdings []*model.WalletHolding
	err := h.db.WithContext(ctx).
		Where("chain_id = ? AND wallet_address = ?", chainId, walletAddress).
		Order("updated_at DESC").
		Find(&holdings).Error

	if err != nil {
		return nil, err
	}

	return holdings, nil
}

// Create 创建新的持仓记录
func (h *holdingDAO) Create(ctx context.Context, holding *model.WalletHolding) error {
	err := h.db.WithContext(ctx).Create(holding).Error
	if err == nil {
		// 清除相关缓存
		h.clearHoldingCache(ctx, holding.ChainID, holding.WalletAddress, holding.TokenAddress)
	}
	return err
}

// Update 更新持仓记录
func (h *holdingDAO) Update(ctx context.Context, holding *model.WalletHolding) error {
	err := h.db.WithContext(ctx).Save(holding).Error
	if err == nil {
		// 主动更新缓存，包括本地缓存和Redis
		h.UpdateHoldingCache(ctx, utils.HoldingKey(holding.ChainID, holding.WalletAddress, holding.TokenAddress), holding)
	}
	return err
}

// Delete 删除持仓记录
func (h *holdingDAO) Delete(ctx context.Context, id int) error {
	return h.db.WithContext(ctx).Delete(&model.WalletHolding{}, id).Error
}

// GetByChainAndWallet 通过链ID和钱包地址查询持仓信息
func (h *holdingDAO) GetByChainAndWallet(ctx context.Context, chainID uint64, walletAddress string) ([]*model.WalletHolding, error) {
	var holdings []*model.WalletHolding
	err := h.db.WithContext(ctx).
		Where("chain_id = ? AND wallet_address = ?", chainID, walletAddress).
		Order("updated_at DESC").
		Find(&holdings).Error

	if err != nil {
		return nil, err
	}

	return holdings, nil
}

// GetActiveHoldings 获取未清仓的持仓信息
func (h *holdingDAO) GetActiveHoldings(ctx context.Context, walletAddress string) ([]*model.WalletHolding, error) {
	var holdings []*model.WalletHolding
    err := h.db.WithContext(ctx).
        Where("wallet_address = ? AND amount > 0", walletAddress).
		Order("updated_at DESC").
		Find(&holdings).Error

	if err != nil {
		return nil, err
	}

	return holdings, nil
}
