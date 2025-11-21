package missingtokeninfo

import (
	"context"
	"fmt"
	"time"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/writer"
	"web3-smart/pkg/utils"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	REDIS_MISSING_TOKENINFO_TTL = 26 * time.Hour
	RETRY_COUNT                 = 3
)

type RedisMissingTokenInfoWriter struct {
	redis *redis.Client
	tl    *zap.Logger
}

func NewRedisMissingTokenInfoWriter(rdb *redis.Client, tl *zap.Logger) writer.BatchWriter[model.TradeEvent] {
	return &RedisMissingTokenInfoWriter{redis: rdb, tl: tl}
}

func (w *RedisMissingTokenInfoWriter) BWrite(ctx context.Context, trades []model.TradeEvent) error {
	if len(trades) == 0 {
		return nil
	}

	pipe := w.redis.Pipeline()

	key := utils.MissingTokenInfoKey()
	for _, trade := range trades {
		pipe.ZAdd(ctx, key, w.marshalToZ(trade))
	}

	pipe.Expire(ctx, key, REDIS_MISSING_TOKENINFO_TTL)

	// 执行 Pipeline 并添加重试机制
	var attempt int
	var err error
	for attempt = 0; attempt < RETRY_COUNT; attempt++ {
		_, err = pipe.Exec(ctx)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		w.tl.Warn("❌ Redis pipeline exec failed, exceeded the maximum number of retries", zap.Error(err))
		return err
	}
	return nil
}

func (w *RedisMissingTokenInfoWriter) marshalToZ(trade model.TradeEvent) redis.Z {
	// member: network_walletAddress_tokenAddress
	return redis.Z{
		Score:  float64(trade.Event.Time),
		Member: fmt.Sprintf("%s_%s_%s", trade.Event.Network, trade.Event.Address, trade.Event.TokenAddress),
	}
}

func (w *RedisMissingTokenInfoWriter) Close() error {
	return nil
}
