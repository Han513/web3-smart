package cache

import (
	"context"
	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"time"
)

const (
	HOLDING_CACHE_TTL       = 10 * time.Minute // 本地缓存过期时间
	HOLDING_CACHE_REDIS_TTL = time.Hour * 25   // Redis中最新K线TTL
)

type HoldingCache struct {
	id         string
	ctx        context.Context
	tl         *zap.Logger
	localCache *cache.Cache
	redis      *redis.Client
	shutdown   chan struct{}
}

// NewHoldingCache 创建新的持仓缓存实例
func NewHoldingCache(ctx context.Context, tl *zap.Logger, rdb *redis.Client) *HoldingCache {
	localCache := cache.New(HOLDING_CACHE_TTL, time.Minute)

	c := &HoldingCache{
		id:         "redis_holding_last_writer",
		ctx:        ctx,
		tl:         tl,
		localCache: localCache,
		redis:      rdb,
		shutdown:   make(chan struct{}),
	}

	return c
}

// Shutdown 优雅关闭缓存
func (c *HoldingCache) Shutdown() {
	close(c.shutdown)
	c.tl.Info("HoldingCache shutdown")
}
