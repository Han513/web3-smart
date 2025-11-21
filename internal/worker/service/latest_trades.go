package service

import (
	"context"
	"fmt"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// LatestTradesService 负责维护「系统聪明钱最新成交」列表
// 数据结构：
//
//	key:   smart_money:monitor:latest_trades:<bip0044_chain_id>:24h
//	type:  zset
//	score: t_smart_transaction.transaction_time（毫秒时间戳）
//	member: 一条 SmartTxEvent 的 JSON 字符串（对应 push.system.smart.money 的消息体）
//
// 规则：
//   - 每条新成交写入一次 ZADD
//   - 每次写入后只保留最新 50 条（按时间排序）
//   - key 过期时间：7 天
type LatestTradesService struct {
	cfg  config.Config
	tl   *zap.Logger
	repo repository.Repository
}

func NewLatestTradesService(cfg config.Config, logger *zap.Logger, repo repository.Repository) *LatestTradesService {
	return &LatestTradesService{
		cfg:  cfg,
		tl:   logger,
		repo: repo,
	}
}

// Record 接收一条已经计算完成的 WalletTransaction，并写入「最新成交」Redis 列表
func (s *LatestTradesService) Record(tx *model.WalletTransaction) {
	if tx == nil {
		return
	}
	rdb := s.repo.GetMetricsRDB()
	if rdb == nil {
		// 若未配置 metrics redis，则直接跳过
		return
	}

	// 使用较短超时，避免阻塞消费主流程
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// 与 Kafka 推送保持一致：用 SmartTxEvent 作为成员内容
	event := model.NewSmartTxEvent(*tx)
	jsonData, err := sonic.Marshal(event)
	if err != nil {
		s.tl.Warn("marshal SmartTxEvent failed for latest trades",
			zap.Error(err),
			zap.String("wallet_address", tx.WalletAddress),
			zap.String("token_address", tx.TokenAddress),
		)
		return
	}

	key := s.latestTradesKey(tx.ChainID)
	score := float64(tx.TransactionTime) // ms 时间戳

	if err := s.zaddLatestTrade(ctx, rdb, key, score, string(jsonData)); err != nil {
		s.tl.Warn("update latest trades zset failed",
			zap.Error(err),
			zap.String("key", key),
		)
	}
}

func (s *LatestTradesService) latestTradesKey(chainID uint64) string {
	// 名称中带 24h，实际读取时由 API 控制时间窗口
	return fmt.Sprintf("smart_money:monitor:latest_trades:%d:24h", chainID)
}

// zaddLatestTrade 写入一条最新成交，并维护 zset 大小 & TTL
func (s *LatestTradesService) zaddLatestTrade(
	ctx context.Context,
	rdb *redis.Client,
	key string,
	score float64,
	member string,
) error {
	// 写入一条记录
	if _, err := rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: member,
	}).Result(); err != nil {
		return err
	}

	// 只保留最新 50 条：
	// zset 默认按 score 升序，索引 0 是最旧的那条
	// 移除 0..-51 区间，可确保最多剩 50 条（不足 50 条时不生效）
	if _, err := rdb.ZRemRangeByRank(ctx, key, 0, -51).Result(); err != nil {
		return err
	}

	// 设置过期时间 7 天
	_ = rdb.Expire(ctx, key, 7*24*time.Hour).Err()
	return nil
}

// Close 目前无状态资源需要关闭，预留接口
func (s *LatestTradesService) Close() {
	// no-op
}
