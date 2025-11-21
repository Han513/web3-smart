package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/pkg/elasticsearch"
	"web3-smart/pkg/utils"

	"github.com/bytedance/sonic"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"gorm.io/gorm"
)

// tokenDAO 实现TokenDAO接口
type tokenDAO struct {
	cfg        *config.Config
	db         *gorm.DB
	es         *elasticsearch.Client
	rds        *redis.Client
	localCache *cache.Cache
}

// NewTokenDAO 创建TokenDAO实例
func NewTokenDAO(cfg *config.Config, db *gorm.DB, es *elasticsearch.Client, rds *redis.Client) TokenDAO {
	localCache := cache.New(10*time.Minute, time.Minute)
	return &tokenDAO{
		cfg:        cfg,
		db:         db,
		es:         es,
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
		if tokenAddress == "So11111111111111111111111111111111111111111" { // native SOL 佔位標識
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
		if tokenAddress == "0x0000000000000000000000000000000000000000" {
			totalSupply := decimal.NewFromFloat(137740000)
			tokenInfo.Supply = &totalSupply
			tokenInfo.Symbol = "BNB"
			tokenInfo.Name = "Binance Chain"
			tokenInfo.Logo = "https://uploads.bydfi.in/events/chain-BNB.png"
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
	// var tokenInfo model.SmTokenRet
	// err = t.db.WithContext(ctx).
	// 	Table("dex_query_v1.web3_tokens").
	// 	Select("address, name, symbol, total_supply as supply, contract_info->>'creator' AS creater, logo").
	// 	Where("chain_id = ? AND address = ?", chainID, tokenAddress).
	// 	First(&tokenInfo).Error

	// if err != nil {
	// 	if errors.Is(err, gorm.ErrRecordNotFound) {
	// 		// 缓存空结果，避免缓存穿透
	// 		t.localCache.Set(cacheKey, (*model.SmTokenRet)(nil), 1*time.Minute)
	// 		t.rds.Set(ctx, cacheKey, "null", 1*time.Minute).Result()
	// 		return nil, nil
	// 	}
	// 	return nil, err
	// }

	// 查ES
	var tokenInfo *model.SmTokenRet
	var lastErr error

	// 重试3次
	for attempt := 1; attempt <= 3; attempt++ {
		tokenInfo, lastErr = t.getTokenFromES(ctx, chainID, tokenAddress)
		if lastErr == nil && tokenInfo != nil && strings.TrimSpace(tokenInfo.Symbol) != "" {
			break
		}

		// 查询出错，继续重试
		time.Sleep(time.Duration(attempt*800) * time.Millisecond) // 递增延迟
	}

	// 重试都失败，缓存空结果避免缓存穿透
	if lastErr != nil {
		t.localCache.Set(cacheKey, (*model.SmTokenRet)(nil), 1*time.Minute)
		t.rds.Set(ctx, cacheKey, "null", 1*time.Minute)
		return nil, lastErr
	}

	// 更新缓存
	t.UpdateTokenInfoCache(ctx, cacheKey, tokenInfo)
	return tokenInfo, nil
}

// updateTokenInfoCache 更新代币信息缓存
func (t *tokenDAO) UpdateTokenInfoCache(ctx context.Context, cacheKey string, tokenInfo *model.SmTokenRet) {
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

// getTokenFromES 从ES查询token信息
func (t *tokenDAO) getTokenFromES(ctx context.Context, chainID uint64, tokenAddress string) (*model.SmTokenRet, error) {
	// 获取network名称
	network := bip0044.ChainIdToString(chainID)

	// 构造文档ID: {network}_{address}
	docID := fmt.Sprintf("%s_%s", network, tokenAddress)
	indexName := t.cfg.Elasticsearch.Web3TokensIndexName

	// 从ES获取文档
	source, err := t.es.Get(ctx, indexName, docID)
	if err != nil {
		return nil, err
	}

	// 文档不存在
	if source == nil {
		return nil, nil
	}

	// 解析文档数据
	tokenInfo := &model.SmTokenRet{
		Address: tokenAddress,
	}

	// 提取name
	if name, ok := source["name"].(string); ok {
		tokenInfo.Name = name
	}

	// 提取symbol
	if symbol, ok := source["symbol"].(string); ok {
		tokenInfo.Symbol = symbol
	}

	// 提取logo
	if logo, ok := source["logo"].(string); ok {
		tokenInfo.Logo = logo
	}

	// 提取total_supply
	if totalSupply, ok := source["total_supply"].(float64); ok {
		supply := decimal.NewFromFloat(totalSupply)
		tokenInfo.Supply = &supply
	}

	// 提取creator (从contract_info.creator)
	if contractInfo, ok := source["contract_info"].(map[string]interface{}); ok {
		if creator, ok := contractInfo["creator"].(string); ok && creator != "" {
			tokenInfo.Creater = &creator
		}
	}

	return tokenInfo, nil
}
