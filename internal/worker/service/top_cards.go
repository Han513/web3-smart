package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/dao"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"github.com/redis/go-redis/v9"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"go.uber.org/zap"
)

// TopCardsService 负责基于聪明钱交易实时聚合小卡片相关数据到 Redis
//
// 目标：
//  1. token 统计 key：记录周期内哪些聪明钱交易过该 token（zset：member=wallet，score=timestamp）
//  2. 榜单 key：按周期内聪明钱交易过的数量排序（zset：member=token，score=smart money wallet count，最多保留 200）
//  3. 榜单前 10 token detail key：存储小卡片展示用的聚合信息（string，json）
//
// 说明：
//  - 这里只处理 “已确认是系统聪明钱的钱包” 的交易，调用入口在 TradeHandler 中。
//  - 价格、涨跌等信息可以先写入基础值，后续由 ticker 流程覆盖/补充。
type TopCardsService struct {
	cfg        config.Config
	tl         *zap.Logger
	repo       repository.Repository
	daoManager *dao.DAOManager
	periods    []periodConfig
}

type periodConfig struct {
	Name     string        // "1h" / "6h" / "24h"
	Duration time.Duration // 周期长度
}

// topCardDetail 小卡片详情结构，存为 JSON string
type topCardDetail struct {
	Symbol        string  `json:"symbol"`
	Logo          string  `json:"logo"`
	Price         float64 `json:"price"`          // 当前价格（USDT），先占位，后续可由 ticker 覆盖
	ChangePercent float64 `json:"change_percent"` // 周期内涨跌幅，先占位，后续可由 ticker 覆盖
	BuyTxns       int64   `json:"buy_txns"`       // 周期内聪明钱买入笔数
	SellTxns      int64   `json:"sell_txns"`      // 周期内聪明钱卖出笔数
}

// 需要从小卡片中过滤掉的 token 列表（原生币 & 稳定币等）
// 说明：Solana 地址区分大小写，这里直接使用原始字符串；BSC 地址使用 EqualFold 做大小写无关比较。
var topCardsBlacklist = []string{
	// Solana 稳定币 & 原生币等
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
	"2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",
	"A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6",
	"FR87nWEUxVgerFGhZM8Y4AggKGLnaXswr1Pd8wZ4kZcp",
	"Ea5SjE2Y6yvCeW5dYTn7PYMuW5ikXkvbGdcmSnXeaLjS",
	"So11111111111111111111111111111111111111112",

	// BSC 稳定币 & 原生币等
	"0x55d398326f99059ff775485246999027b3197955",
	"0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
	"0x1af3f329e8be154074d8769d1ffA4ee058b1dbC3",
	"0xe9e7cea3dedca5984780bafc599bd69add087d56",
	"0x14016e85a25aeb13065688cafb43044c2ef86784",
	"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
	"0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
}

func NewTopCardsService(cfg config.Config, logger *zap.Logger, repo repository.Repository) *TopCardsService {
	return &TopCardsService{
		cfg:        cfg,
		tl:         logger,
		repo:       repo,
		daoManager: repo.GetDAOManager(),
		periods: []periodConfig{
			{Name: "1h", Duration: time.Hour},
			{Name: "6h", Duration: 6 * time.Hour},
			{Name: "24h", Duration: 24 * time.Hour},
		},
	}
}

// HandleSmartTrade 处理一笔来自系统聪明钱的钱包交易
// 目前只统计建仓/买入类型的交易到小卡片中。
func (s *TopCardsService) HandleSmartTrade(trade model.TradeEvent, smartMoney *model.WalletSummary, txType string) {
	if smartMoney == nil {
		return
	}

	// 只统计买入/建仓方向
	if txType != model.TX_TYPE_BUILD && txType != model.TX_TYPE_BUY {
		return
	}

	chainID := bip0044.NetworkNameToChainId(trade.Event.Network)
	if chainID == 0 {
		return
	}

	tokenAddr := strings.TrimSpace(trade.Event.TokenAddress)
	if tokenAddr == "" {
		return
	}

	// 过滤掉原生币 / 稳定币等不需要在小卡片中展示的 token
	if s.isBlacklistedToken(tokenAddr) {
		return
	}

	// 使用较短超时，避免阻塞主消费逻辑
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	for _, p := range s.periods {
		if err := s.updateForPeriod(ctx, p, chainID, trade, smartMoney, txType); err != nil {
			s.tl.Warn("更新小卡片聚合数据失败",
				zap.Error(err),
				zap.Uint64("chain_id", chainID),
				zap.String("token", tokenAddr),
				zap.String("period", p.Name),
			)
		}
	}
}

func (s *TopCardsService) isBlacklistedToken(tokenAddr string) bool {
	if tokenAddr == "" {
		return false
	}
	for _, addr := range topCardsBlacklist {
		if strings.EqualFold(tokenAddr, addr) {
			return true
		}
	}
	return false
}

// updateForPeriod 针对某个统计周期更新 token 相关的 redis 数据
//
// 步骤：
//  1. 更新 token 统计 key（wallets zset）
//  2. 更新该 token 在榜单 key 中的 score（按聪明钱数量）
//  3. 更新周期内买卖次数统计（hash）
//  4. 重算榜单前 10 个 token 的 detail
func (s *TopCardsService) updateForPeriod(
	ctx context.Context,
	p periodConfig,
	chainID uint64,
	trade model.TradeEvent,
	smartMoney *model.WalletSummary,
	txType string,
) error {
	rdb := s.repo.GetMetricsRDB()
	if rdb == nil {
		return fmt.Errorf("metrics redis not configured")
	}

	tokenAddr := strings.TrimSpace(trade.Event.TokenAddress)
	walletAddr := strings.TrimSpace(smartMoney.WalletAddress)
	if tokenAddr == "" || walletAddr == "" {
		return nil
	}

	ts := trade.Event.Time // 目前事件时间为秒级时间戳

	// 1. token 统计 key：记录周期内哪些聪明钱交易过该 token
	walletsKey := fmt.Sprintf("smart_money:monitor:top_cards:%d:%s:%s:wallets", chainID, tokenAddr, p.Name)
	if err := s.updateTokenWallets(ctx, rdb, walletsKey, walletAddr, ts, p.Duration); err != nil {
		return err
	}

	// 2. 周期内买卖次数统计（hash）
	statsKey := fmt.Sprintf("smart_money:monitor:top_cards:%d:%s:%s:stats", chainID, tokenAddr, p.Name)
	if err := s.updateTokenStats(ctx, rdb, statsKey, txType, p.Duration); err != nil {
		return err
	}

	// 3. 根据 wallets zset 的 ZCARD 更新榜单 key
	leaderboardKey := fmt.Sprintf("smart_money:monitor:top_cards:%d:%s:leaderboard", chainID, p.Name)
	if err := s.updateLeaderboard(ctx, rdb, walletsKey, leaderboardKey, tokenAddr); err != nil {
		return err
	}

	// 4. 重算榜单前 10 token 的 detail
	if err := s.refreshTop10Details(ctx, rdb, leaderboardKey, chainID, p, trade.Event.Network); err != nil {
		return err
	}

	return nil
}

// updateTokenWallets 维护 token 统计 zset
//  - member: wallet address
//  - score:  timestamp（秒）
//  - 每次写入后，剔除超出统计周期的旧数据，并更新 TTL
func (s *TopCardsService) updateTokenWallets(
	ctx context.Context,
	rdb *redis.Client,
	key, walletAddr string,
	timestamp int64,
	period time.Duration,
) error {
	score := float64(timestamp)

	if _, err := rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: walletAddr,
	}).Result(); err != nil {
		return err
	}

	// 剔除周期外的历史记录
	cutoff := score - period.Seconds()
	if cutoff > 0 {
		if _, err := rdb.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("(%f", cutoff)).Result(); err != nil {
			return err
		}
	}

	// 设置过期时间，避免冷门 token 的 key 永久存在
	_ = rdb.Expire(ctx, key, period).Err()
	return nil
}

// updateTokenStats 维护周期内买卖次数统计
//  - key: smart_money:monitor:top_cards:<chain_id>:<token addr>:<period>:stats
//  - field: buy_txns / sell_txns
func (s *TopCardsService) updateTokenStats(
	ctx context.Context,
	rdb *redis.Client,
	key string,
	txType string,
	period time.Duration,
) error {
	var field string
	switch txType {
	case model.TX_TYPE_BUILD, model.TX_TYPE_BUY:
		field = "buy_txns"
	case model.TX_TYPE_SELL, model.TX_TYPE_CLEAN:
		field = "sell_txns"
	default:
		return nil
	}

	if err := rdb.HIncrBy(ctx, key, field, 1).Err(); err != nil {
		return err
	}
	_ = rdb.Expire(ctx, key, period).Err()
	return nil
}

// updateLeaderboard 根据 token wallets zset 的 ZCARD 更新榜单
//  - key: smart_money:monitor:top_cards:<chain_id>:<period>:leaderboard
//  - member: token addr
//  - score:  周期内聪明钱钱包数量
func (s *TopCardsService) updateLeaderboard(
	ctx context.Context,
	rdb *redis.Client,
	walletsKey, leaderboardKey, tokenAddr string,
) error {
	count, err := rdb.ZCard(ctx, walletsKey).Result()
	if err != nil {
		return err
	}

	if _, err := rdb.ZAdd(ctx, leaderboardKey, redis.Z{
		Score:  float64(count),
		Member: tokenAddr,
	}).Result(); err != nil {
		return err
	}

	// 只保留前 200 名
	if _, err := rdb.ZRemRangeByRank(ctx, leaderboardKey, 0, -201).Result(); err != nil {
		return err
	}

	// 过期时间 7 天
	_ = rdb.Expire(ctx, leaderboardKey, 7*24*time.Hour).Err()
	return nil
}

// refreshTop10Details 重算当前榜单前 10 个 token 的小卡片详情
func (s *TopCardsService) refreshTop10Details(
	ctx context.Context,
	rdb *redis.Client,
	leaderboardKey string,
	chainID uint64,
	p periodConfig,
	network string,
) error {
	// 取前 10 名 token 地址（按 score 从大到小）
	tokenAddrs, err := rdb.ZRevRange(ctx, leaderboardKey, 0, 9).Result()
	if err != nil {
		return err
	}
	if len(tokenAddrs) == 0 {
		return nil
	}

	for _, tokenAddr := range tokenAddrs {
		if err := s.buildAndSetDetail(ctx, rdb, chainID, p, network, tokenAddr); err != nil {
			s.tl.Warn("构建小卡片详情失败",
				zap.Error(err),
				zap.Uint64("chain_id", chainID),
				zap.String("token", tokenAddr),
				zap.String("period", p.Name),
			)
		}
	}

	return nil
}

// buildAndSetDetail 构建单个 token+period 的小卡片详情并写入 Redis
func (s *TopCardsService) buildAndSetDetail(
	ctx context.Context,
	rdb *redis.Client,
	chainID uint64,
	p periodConfig,
	network, tokenAddr string,
) error {
	// 1. 获取 token 基本信息（symbol / logo）
	var symbol, logo string
	if s.daoManager != nil && s.daoManager.TokenDAO != nil {
		tokenInfo, err := s.daoManager.TokenDAO.GetTokenInfo(ctx, chainID, tokenAddr)
		if err != nil {
			s.tl.Warn("获取 token 信息失败（小卡片）",
				zap.Error(err),
				zap.Uint64("chain_id", chainID),
				zap.String("token", tokenAddr),
			)
		}
		if tokenInfo != nil {
			symbol = tokenInfo.Symbol
			logo = tokenInfo.Logo
		}
	}

	// 2. 获取周期内买卖次数
	statsKey := fmt.Sprintf("smart_money:monitor:top_cards:%d:%s:%s:stats", chainID, tokenAddr, p.Name)
	var buyTxns, sellTxns int64
	if res, err := rdb.HGet(ctx, statsKey, "buy_txns").Result(); err == nil {
		if v, err2 := strconv.ParseInt(res, 10, 64); err2 == nil {
			buyTxns = v
		}
	}
	if res, err := rdb.HGet(ctx, statsKey, "sell_txns").Result(); err == nil {
		if v, err2 := strconv.ParseInt(res, 10, 64); err2 == nil {
			sellTxns = v
		}
	}

	// 3. 价格与涨跌，这里先占位 0，后续由 ticker 或其他行情服务覆盖
	price := 0.0
	changePercent := 0.0

	detail := topCardDetail{
		Symbol:        symbol,
		Logo:          logo,
		Price:         price,
		ChangePercent: changePercent,
		BuyTxns:       buyTxns,
		SellTxns:      sellTxns,
	}

	data, err := json.Marshal(detail)
	if err != nil {
		return err
	}

	detailKey := fmt.Sprintf("smart_money:monitor:top_cards:%d:%s:%s:detail", chainID, tokenAddr, p.Name)
	if err := rdb.Set(ctx, detailKey, string(data), p.Duration).Err(); err != nil {
		return err
	}

	return nil
}

// Close 目前没有需要关闭的资源，预留接口便于未来扩展
func (s *TopCardsService) Close() {
	// no-op
}


