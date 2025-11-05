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
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// tokenDAO 实现TokenDAO接口
type tokenDAO struct {
	db         *gorm.DB
	rds        *redis.Client
	localCache *cache.Cache
}

// NewTokenDAO 创建TokenDAO实例
func NewTokenDAO(db *gorm.DB, rds *redis.Client) TokenDAO {
	localCache := cache.New(10*time.Minute, time.Minute)
	return &tokenDAO{
		db:         db,
		rds:        rds,
		localCache: localCache,
	}
}

// GetTokenInfo 获取代币信息（简化版本）
func (t *tokenDAO) GetTokenInfo(ctx context.Context, chainID uint64, tokenAddress string) (*model.SmTokenRet, error) {
	// 如果是BNB和SOL，直接返回固定信息
	switch chainID {
	case 501:
		// 僅對已知的 SOL/wSOL 特殊地址做快捷返回；其他地址走資料庫/快取
		if tokenAddress == "So11111111111111111111111111111111111111111" { // SOL 合約地址
			tokenInfo := &model.SmTokenRet{
				Address: tokenAddress,
				Name:    "SOLANA",
				Symbol:  "SOL",
				Logo:    "https://uploads.bydfi.in/logo/20250921082428_615.png",
			}
			totalSupply := decimal.NewFromFloat(549820000)
			tokenInfo.Supply = &totalSupply
			return tokenInfo, nil
		}
		if tokenAddress == "So11111111111111111111111111111111111111112" { // wSOL 合約地址
			tokenInfo := &model.SmTokenRet{
				Address: tokenAddress,
				Name:    "Wrapped SOL",
				Symbol:  "WSOL",
				Logo:    "https://uploads.bydfi.in/logo/20250921082428_615.png",
			}
			totalSupply := decimal.NewFromFloat(12991751.535377756)
			tokenInfo.Supply = &totalSupply
			return tokenInfo, nil
		}
	case 9006:
		tokenInfo := &model.SmTokenRet{
			Address: tokenAddress,
			Name:    "Wrapped BNB",
			Symbol:  "WBNB",
			Logo:    "https://uploads.bydfi.in/logo/20250921092521_136.png",
		}

		if tokenAddress == "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c" {
			totalSupply := decimal.NewFromFloat(1294219.5681700353)
			tokenInfo.Supply = &totalSupply
			return tokenInfo, nil
		}
	}

	cacheKey := utils.TokenInfoKey(chainID, tokenAddress)

	// 先查本地缓存
	if cached, found := t.localCache.Get(cacheKey); found {
		if tokenInfo, ok := cached.(*model.SmTokenRet); ok {
			return tokenInfo, nil
		}
	}

	// 再查Redis缓存
	cached, err := t.rds.Get(ctx, cacheKey).Result()
	if err == nil {
		if cached == "null" {
			return nil, nil
		}

		var tokenInfo model.SmTokenRet
		if sonic.Unmarshal([]byte(cached), &tokenInfo) == nil {
			// 更新本地缓存
			t.localCache.Set(cacheKey, &tokenInfo, cache.DefaultExpiration)
			return &tokenInfo, nil
		}
	}

	// 查数据库
	var tokenInfo model.SmTokenRet
	err = t.db.WithContext(ctx).
		Table("dex_query_v1.web3_tokens").
		Select("address, name, symbol, total_supply as supply, contract_info->>'creator' AS creater, logo").
		Where("chain_id = ? AND address = ?", chainID, tokenAddress).
		First(&tokenInfo).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 缓存空结果，避免缓存穿透
			t.localCache.Set(cacheKey, (*model.SmTokenRet)(nil), 1*time.Minute)
			t.rds.Set(ctx, cacheKey, "null", 1*time.Minute).Result()
			return nil, nil
		}
		return nil, err
	}

	// 更新缓存
	t.updateTokenInfoCache(ctx, cacheKey, &tokenInfo)
	return &tokenInfo, nil
}

// updateTokenInfoCache 更新代币信息缓存
func (t *tokenDAO) updateTokenInfoCache(ctx context.Context, cacheKey string, tokenInfo *model.SmTokenRet) {
	// 更新本地缓存
	t.localCache.Set(cacheKey, tokenInfo, cache.DefaultExpiration)

	// 更新Redis缓存
	if data, err := sonic.Marshal(tokenInfo); err == nil {
		t.rds.Set(ctx, cacheKey, string(data), 30*time.Minute) // 代币信息相对稳定，缓存时间长一些
	}
}

// clearTokenInfoCache 清除代币信息缓存
func (t *tokenDAO) clearTokenInfoCache(ctx context.Context, chainID uint64, tokenAddress string) {
	cacheKey := utils.TokenInfoKey(chainID, tokenAddress)
	t.localCache.Delete(cacheKey)
	t.rds.Del(ctx, cacheKey)
}

// GetByAddress 通过地址获取完整token信息
func (t *tokenDAO) GetByAddress(ctx context.Context, chainID uint64, tokenAddress string) (*model.Token, error) {
	var token model.Token
	err := t.db.WithContext(ctx).
		Where("chain_id = ? AND address = ?", chainID, tokenAddress).
		First(&token).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return &token, nil
}

// GetByChain 获取指定链上的所有token
func (t *tokenDAO) GetByChain(ctx context.Context, chainID uint64, limit, offset int) ([]*model.Token, error) {
	var tokens []*model.Token
	err := t.db.WithContext(ctx).
		Where("chain_id = ?", chainID).
		Order("created_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&tokens).Error

	if err != nil {
		return nil, err
	}

	return tokens, nil
}

// Create 创建新的token记录
func (t *tokenDAO) Create(ctx context.Context, token *model.Token) error {
	return t.db.WithContext(ctx).Create(token).Error
}

// Update 更新token记录
func (t *tokenDAO) Update(ctx context.Context, token *model.Token) error {
	return t.db.WithContext(ctx).Save(token).Error
}

// GetBySymbol 通过symbol查询token
func (t *tokenDAO) GetBySymbol(ctx context.Context, chainID uint64, symbol string) ([]*model.Token, error) {
	var tokens []*model.Token
	err := t.db.WithContext(ctx).
		Where("chain_id = ? AND symbol = ?", chainID, symbol).
		Order("created_at DESC").
		Find(&tokens).Error

	if err != nil {
		return nil, err
	}

	return tokens, nil
}
