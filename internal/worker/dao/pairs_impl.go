package dao

import (
	"context"
	"errors"
	"strconv"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/pkg/utils"

	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// pairsDAO 实现PairsDAO接口
type pairsDAO struct {
	cfg        *config.Config
	db         *gorm.DB
	rds        *redis.Client
	localCache *cache.Cache
}

// NewPairsDAO 创建PairsDAO实例
func NewPairsDAO(cfg *config.Config, db *gorm.DB, rds *redis.Client) PairsDAO {
	localCache := cache.New(10*time.Minute, time.Minute)
	return &pairsDAO{
		cfg:        cfg,
		db:         db,
		rds:        rds,
		localCache: localCache,
	}
}

// GetEarliestBlockTimestamp 获取指定base或quote代币的最早区块时间戳
// 对应SQL: SELECT block_timestamp FROM dex_query_v1.pairs WHERE base = ? OR "quote" = ? ORDER BY block_timestamp ASC limit 1;
func (p *pairsDAO) GetEarliestBlockTimestamp(ctx context.Context, chainId uint64, tokenAddress string) (*int64, error) {
	// 写死常见quote token最早池子创建时间，加速查询
	switch chainId {
	case 501:
		if tokenAddress == "So11111111111111111111111111111111111111111" || tokenAddress == "So11111111111111111111111111111111111111112" {
			timestamp := int64(1651167968000)
			return &timestamp, nil
		} else if tokenAddress == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" { // USDC
			timestamp := int64(1651167968000)
			return &timestamp, nil
		} else if tokenAddress == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" { // USDT
			timestamp := int64(1651167972000)
			return &timestamp, nil
		} else if tokenAddress == "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB" { // USD1
			timestamp := int64(1755887498000)
			return &timestamp, nil
		}
	case 9006:
		if tokenAddress == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c" { // WBNB
			timestamp := int64(1626362416000)
			return &timestamp, nil
		} else if tokenAddress == "0x55d398326f99059fF775485246999027B3197955" { // USDT
			timestamp := int64(1626362416000)
			return &timestamp, nil
		} else if tokenAddress == "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d" { // USD1
			timestamp := int64(1744411964000)
			return &timestamp, nil
		} else if tokenAddress == "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d" { // USDC
			timestamp := int64(1626362438000)
			return &timestamp, nil
		} else if tokenAddress == "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56" { // BUSD
			timestamp := int64(1626362416000)
			return &timestamp, nil
		} else if tokenAddress == "0x0000000000000000000000000000000000000000" { // Fourmeme BNB
			timestamp := int64(1751950227000)
			return &timestamp, nil
		}
	}

	cacheKey := utils.PairsEarliestBlockTimestampKey(chainId, tokenAddress)

	// 先查本地缓存
	if cached, found := p.localCache.Get(cacheKey); found {
		if timestamp, ok := cached.(*int64); ok {
			return timestamp, nil
		}
	}

	// 再查Redis缓存
	cached, err := p.rds.Get(ctx, cacheKey).Result()
	if err == nil {
		if cached == "null" {
			return nil, nil
		}

		if timestamp, err := strconv.ParseInt(cached, 10, 64); err == nil {
			// 更新本地缓存
			p.localCache.Set(cacheKey, &timestamp, cache.DefaultExpiration)
			return &timestamp, nil
		}
	}

	// 查数据库
	var blockTimestamp int64
	err = p.db.WithContext(ctx).
		Table("dex_query_v1.pairs").
		Select("block_timestamp").
		Where("chain_id = ? AND (base = ? OR quote = ?) AND block_timestamp IS NOT NULL", chainId, tokenAddress, tokenAddress).
		Order("block_timestamp ASC").
		Limit(1).
		Scan(&blockTimestamp).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 缓存空结果，避免缓存穿透
			p.localCache.Set(cacheKey, (*int64)(nil), 5*time.Minute)
			p.rds.Set(ctx, cacheKey, "null", 5*time.Minute)
			return nil, nil
		}
		return nil, err
	}

	// 更新缓存
	p.updatePairsEarliestTimestampCache(ctx, cacheKey, &blockTimestamp)
	return &blockTimestamp, nil
}

// updatePairsEarliestTimestampCache 更新交易对最早时间戳缓存
func (p *pairsDAO) updatePairsEarliestTimestampCache(ctx context.Context, cacheKey string, timestamp *int64) {
	// 更新本地缓存
	p.localCache.Set(cacheKey, timestamp, cache.DefaultExpiration)

	// 更新Redis缓存
	p.rds.Set(ctx, cacheKey, strconv.FormatInt(*timestamp, 10), 60*time.Minute) // 交易对时间戳相对稳定，缓存时间较长
}

// clearPairsEarliestTimestampCache 清除交易对最早时间戳缓存
func (p *pairsDAO) clearPairsEarliestTimestampCache(ctx context.Context, chainId uint64, tokenAddress string) {
	cacheKey := utils.PairsEarliestBlockTimestampKey(chainId, tokenAddress)
	p.localCache.Delete(cacheKey)
	p.rds.Del(ctx, cacheKey).Result()
}

// GetByAddress 通过pair地址获取交易对信息
func (p *pairsDAO) GetByAddress(ctx context.Context, chainID int32, address string) (*model.Pair, error) {
	var pair model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ? AND address = ?", chainID, address).
		First(&pair).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &pair, nil
}

// GetByBaseAndQuote 通过base和quote代币地址获取交易对
func (p *pairsDAO) GetByBaseAndQuote(ctx context.Context, chainID int32, base, quote string) (*model.Pair, error) {
	var pair model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ? AND ((base = ? AND quote = ?) OR (base = ? AND quote = ?))",
			chainID, base, quote, quote, base).
		First(&pair).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &pair, nil
}

// GetByToken 获取包含指定代币的所有交易对
func (p *pairsDAO) GetByToken(ctx context.Context, chainID int32, tokenAddress string) ([]*model.Pair, error) {
	var pairs []*model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ? AND (base = ? OR quote = ?)", chainID, tokenAddress, tokenAddress).
		Order("created_at DESC").
		Find(&pairs).Error

	if err != nil {
		return nil, err
	}

	return pairs, nil
}

// GetByChain 获取指定链上的所有交易对
func (p *pairsDAO) GetByChain(ctx context.Context, chainID int32, limit, offset int) ([]*model.Pair, error) {
	var pairs []*model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ?", chainID).
		Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&pairs).Error

	if err != nil {
		return nil, err
	}

	return pairs, nil
}

// Create 创建新的交易对记录
func (p *pairsDAO) Create(ctx context.Context, pair *model.Pair) error {
	return p.db.WithContext(ctx).Create(pair).Error
}

// Update 更新交易对记录
func (p *pairsDAO) Update(ctx context.Context, pair *model.Pair) error {
	return p.db.WithContext(ctx).Save(pair).Error
}

// Delete 删除交易对记录
func (p *pairsDAO) Delete(ctx context.Context, id int64) error {
	return p.db.WithContext(ctx).Delete(&model.Pair{}, id).Error
}

// GetLatestPairs 获取最新创建的交易对
func (p *pairsDAO) GetLatestPairs(ctx context.Context, chainID int32, limit int) ([]*model.Pair, error) {
	var pairs []*model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ?", chainID).
		Order("created_at DESC").
		Limit(limit).
		Find(&pairs).Error

	if err != nil {
		return nil, err
	}

	return pairs, nil
}

// GetPairsByCreator 获取指定创建者的交易对
func (p *pairsDAO) GetPairsByCreator(ctx context.Context, chainID int32, creator string) ([]*model.Pair, error) {
	var pairs []*model.Pair
	err := p.db.WithContext(ctx).
		Where("chain_id = ? AND creater = ?", chainID, creator).
		Order("created_at DESC").
		Find(&pairs).Error

	if err != nil {
		return nil, err
	}

	return pairs, nil
}
