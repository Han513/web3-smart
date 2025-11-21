package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/dao"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TransactionPairsService 负责维护「最新交易对」相关的 Redis 聚合数据。
//
// Redis 结构设计：
//
//  1) 最新交易对榜单（按最后一笔成交时间倒排，最多 50 个 token）
//     key:   smart_money:monitor:transaction_pairs:<bip0044_chain_id>:24h
//     type:  zset
//     score: 最新一笔成交的 transaction_time（毫秒）
//     member: token_address
//
//  2) 单个 token 的明细（用于列表展示）
//     key:   smart_money:monitor:transaction_pairs:<bip0044_chain_id>:<token_addr>:24h
//     type:  string（JSON）
//
//     推荐字段（后续可扩展）：
//       symbol, logo, price, marketcap,
//       volume_24h, change_24h,
//       buy_txns_24h, sell_txns_24h,
//       buy_value_24h, sell_value_24h,
//       smart_wallet_count, smart_trade_count, last_transaction_time
//
//  3) 单个 token 的聪明钱弹窗数据
//     key:   smart_money:monitor:transaction_pairs:smart_window:<bip0044_chain_id>:<token_addr>:24h
//     type:  string（JSON 数组，每行对应一个聪明钱钱包的聚合信息）
//
// 说明：
//  - 为了控制开销，只有进入榜单 Top50 的 token 才会触发明细与弹窗的更新。
//  - 聚合 SQL 与业务同学提供的语句保持一致，并增加 chain_id / 时间窗口过滤。
type TransactionPairsService struct {
	cfg        config.Config
	tl         *zap.Logger
	repo       repository.Repository
	daoManager *dao.DAOManager
	db         *gorm.DB
}

type transactionPairDetail struct {
	Symbol             string  `json:"symbol"`
	Logo               string  `json:"logo"`
	Price              float64 `json:"price"`               // 当前价格（占位，由 ticker 覆盖）
	MarketCap          float64 `json:"marketcap"`           // 当前市值（占位，由 ticker 覆盖）
	Volume24h          float64 `json:"volume_24h"`          // 24h 成交额（占位，后续可从行情或 SQL 获取）
	Change24h          float64 `json:"change_24h"`          // 24h 涨跌幅（占位）
	BuyTxns24h         int64   `json:"buy_txns_24h"`        // 24h 内 buy/build 笔数
	SellTxns24h        int64   `json:"sell_txns_24h"`       // 24h 内 sell/clean 笔数
	BuyValue24h        float64 `json:"buy_value_24h"`       // 24h 内买入金额 USD 累计
	SellValue24h       float64 `json:"sell_value_24h"`      // 24h 内卖出金额 USD 累计
	SmartWalletCount   int64   `json:"smart_wallet_count"`  // 24h 内参与该 token 的聪明钱包数量
	SmartTradeCount    int64   `json:"smart_trade_count"`   // 24h 内聪明钱总交易笔数（buy+sell）
	LastTransactionTime int64  `json:"last_transaction_time"` // 最新成交时间（毫秒）
}

// smartWindowRow 对应聪明钱弹窗 SQL 的返回结构
type smartWindowRow struct {
	WalletAddress      string          `json:"wallet_address"`
	BuyAmount          decimal.Decimal `json:"buy_amount"`
	SellAmount         decimal.Decimal `json:"sell_amount"`
	BuyValue           decimal.Decimal `json:"buy_value"`
	SellValue          decimal.Decimal `json:"sell_value"`
	BuyCount           int64           `json:"buy_count"`
	SellCount          int64           `json:"sell_count"`
	AvgBuyMarketcap    decimal.Decimal `json:"avg_buy_marketcap"`
	AvgSellMarketcap   decimal.Decimal `json:"avg_sell_marketcap"`
	LastTransactionTime int64          `json:"last_transaction_time"`
}

func NewTransactionPairsService(cfg config.Config, logger *zap.Logger, repo repository.Repository) *TransactionPairsService {
	db := repo.GetDB()

	return &TransactionPairsService{
		cfg:        cfg,
		tl:         logger,
		repo:       repo,
		daoManager: repo.GetDAOManager(),
		db:         db,
	}
}

// Record 在处理完一条 WalletTransaction 后调用，用于更新最新交易对榜单及相关明细。
func (s *TransactionPairsService) Record(tx *model.WalletTransaction) {
	if tx == nil || tx.TokenAddress == "" {
		return
	}

	chainID := tx.ChainID
	if chainID == 0 {
		// 兜底：根据 network 转换成 bip0044 id（如果后续有需要）
		chainID = bip0044.BSC // 默认值占位，防止写入错误数据
	}

	rdb := s.repo.GetMetricsRDB()
	if rdb == nil {
		return
	}

	// 使用较短超时，避免阻塞主流程
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	key := s.leaderboardKey(chainID)
	score := float64(tx.TransactionTime) // ms

	// 1. 更新最新交易对榜单（zset：member=token_addr, score=最新成交时间）
	if err := s.updateLeaderboard(ctx, rdb, key, tx.TokenAddress, score); err != nil {
		s.tl.Warn("update transaction_pairs leaderboard failed",
			zap.Error(err),
			zap.Uint64("chain_id", chainID),
			zap.String("token", tx.TokenAddress),
		)
		return
	}

	// 2. 仅当该 token 仍然在 Top50 内时，才更新明细与弹窗，避免对热点 token 频繁全量计算
	rank, err := rdb.ZRevRank(ctx, key, tx.TokenAddress).Result()
	if err != nil {
		// token 不在榜单中或其它错误，直接跳过
		return
	}
	if rank >= 50 {
		// 只关心前 50 名
		return
	}

	// 3. 异步刷新该 token 的 detail 和 smart_window 数据
	go s.refreshTokenData(context.Background(), chainID, tx.TokenAddress)
}

func (s *TransactionPairsService) leaderboardKey(chainID uint64) string {
	return fmt.Sprintf("smart_money:monitor:transaction_pairs:%d:24h", chainID)
}

func (s *TransactionPairsService) detailKey(chainID uint64, tokenAddr string) string {
	return fmt.Sprintf("smart_money:monitor:transaction_pairs:%d:%s:24h", chainID, tokenAddr)
}

func (s *TransactionPairsService) smartWindowKey(chainID uint64, tokenAddr string) string {
	return fmt.Sprintf("smart_money:monitor:transaction_pairs:smart_window:%d:%s:24h", chainID, tokenAddr)
}

// updateLeaderboard 写入最新一笔成交时间，并限制榜单最多 50 个 token，TTL=7d
func (s *TransactionPairsService) updateLeaderboard(
	ctx context.Context,
	rdb *redis.Client,
	key string,
	tokenAddr string,
	score float64,
) error {
	if _, err := rdb.ZAdd(ctx, key, redis.Z{
		Score:  score,
		Member: tokenAddr,
	}).Result(); err != nil {
		return err
	}

	// 只保留最新 50 个 token（score 越大越新）
	if _, err := rdb.ZRemRangeByRank(ctx, key, 0, -51).Result(); err != nil {
		return err
	}

	_ = rdb.Expire(ctx, key, 7*24*time.Hour).Err()
	return nil
}

// refreshTokenData 基于 PG 聚合结果与 Token 信息，刷新 token detail 与 smart_window。
func (s *TransactionPairsService) refreshTokenData(ctx context.Context, chainID uint64, tokenAddr string) {
	rdb := s.repo.GetMetricsRDB()
	if rdb == nil || s.db == nil {
		return
	}

	// 1. 查询 24h 内聪明钱弹窗数据
	rows, err := s.querySmartWindow(ctx, chainID, tokenAddr)
	if err != nil {
		s.tl.Warn("query smart_window data failed",
			zap.Error(err),
			zap.Uint64("chain_id", chainID),
			zap.String("token", tokenAddr),
		)
		return
	}

	// 2. 计算 token 级别的汇总指标
	var (
		buyValueTotal  decimal.Decimal
		sellValueTotal decimal.Decimal
		buyTxnsTotal   int64
		sellTxnsTotal  int64
		lastTs         int64
	)

	for _, r := range rows {
		buyValueTotal = buyValueTotal.Add(r.BuyValue)
		sellValueTotal = sellValueTotal.Add(r.SellValue)
		buyTxnsTotal += r.BuyCount
		sellTxnsTotal += r.SellCount
		if r.LastTransactionTime > lastTs {
			lastTs = r.LastTransactionTime
		}
	}

	smartWalletCount := int64(len(rows))
	smartTradeCount := buyTxnsTotal + sellTxnsTotal

	// 3. 获取 token 基本信息（symbol / logo）
	var symbol, logo string
	if s.daoManager != nil && s.daoManager.TokenDAO != nil {
		tokenInfo, err := s.daoManager.TokenDAO.GetTokenInfo(ctx, chainID, tokenAddr)
		if err != nil {
			s.tl.Warn("get token info for transaction_pairs detail failed",
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

	// 4. 价格、24h 成交额、涨跌幅暂时由行情模块 / ticker 覆盖，这里先占位 0
	detail := transactionPairDetail{
		Symbol:             symbol,
		Logo:               logo,
		Price:              0,
		MarketCap:          0,
		Volume24h:          0,
		Change24h:          0,
		BuyTxns24h:         buyTxnsTotal,
		SellTxns24h:        sellTxnsTotal,
		BuyValue24h:        buyValueTotal.InexactFloat64(),
		SellValue24h:       sellValueTotal.InexactFloat64(),
		SmartWalletCount:   smartWalletCount,
		SmartTradeCount:    smartTradeCount,
		LastTransactionTime: lastTs,
	}

	// 5. 写入 token detail key
	detailBytes, err := json.Marshal(detail)
	if err == nil {
		if err := rdb.Set(ctx, s.detailKey(chainID, tokenAddr), string(detailBytes), 24*time.Hour).Err(); err != nil {
			s.tl.Warn("set transaction_pairs detail failed",
				zap.Error(err),
				zap.Uint64("chain_id", chainID),
				zap.String("token", tokenAddr),
			)
		}
	} else {
		s.tl.Warn("marshal transaction_pairs detail failed",
			zap.Error(err),
			zap.Uint64("chain_id", chainID),
			zap.String("token", tokenAddr),
		)
	}

	// 6. 写入聪明钱弹窗 smart_window key（直接把 SQL 结果数组序列化）
	windowBytes, err := json.Marshal(rows)
	if err == nil {
		if err := rdb.Set(ctx, s.smartWindowKey(chainID, tokenAddr), string(windowBytes), 24*time.Hour).Err(); err != nil {
			s.tl.Warn("set transaction_pairs smart_window failed",
				zap.Error(err),
				zap.Uint64("chain_id", chainID),
				zap.String("token", tokenAddr),
			)
		}
	} else {
		s.tl.Warn("marshal transaction_pairs smart_window failed",
			zap.Error(err),
			zap.Uint64("chain_id", chainID),
			zap.String("token", tokenAddr),
		)
	}
}

// querySmartWindow 使用提供的 SQL 模板聚合 24h 内聪明钱的买卖行为
func (s *TransactionPairsService) querySmartWindow(
	ctx context.Context,
	chainID uint64,
	tokenAddr string,
) ([]smartWindowRow, error) {
	if s.db == nil {
		return nil, fmt.Errorf("db is nil")
	}

	threshold := time.Now().Add(-24 * time.Hour).UnixMilli()

	// 在原 SQL 的基础上增加 chain_id 与时间过滤
	sql := `
SELECT
    wallet_address,
    SUM(CASE 
            WHEN transaction_type IN ('buy', 'build') 
            THEN amount 
            ELSE 0 
        END) AS buy_amount,
    SUM(CASE 
            WHEN transaction_type IN ('sell', 'clean') 
            THEN amount 
            ELSE 0 
        END) AS sell_amount,
    SUM(CASE 
            WHEN transaction_type IN ('buy', 'build') 
            THEN value 
            ELSE 0 
        END) AS buy_value,
    SUM(CASE 
            WHEN transaction_type IN ('sell', 'clean') 
            THEN value 
            ELSE 0 
        END) AS sell_value,
    COUNT(CASE 
              WHEN transaction_type IN ('buy', 'build') 
              THEN 1 
         END) AS buy_count,
    COUNT(CASE 
              WHEN transaction_type IN ('sell', 'clean') 
              THEN 1 
         END) AS sell_count,
    CASE 
        WHEN COUNT(CASE WHEN transaction_type IN ('buy', 'build') THEN 1 END) = 0 THEN 0
        ELSE SUM(CASE WHEN transaction_type IN ('buy', 'build') THEN marketcap ELSE 0 END) 
             / COUNT(CASE WHEN transaction_type IN ('buy', 'build') THEN 1 END)
    END AS avg_buy_marketcap,

    CASE 
        WHEN COUNT(CASE WHEN transaction_type IN ('sell', 'clean') THEN 1 END) = 0 THEN 0
        ELSE SUM(CASE WHEN transaction_type IN ('sell', 'clean') THEN marketcap ELSE 0 END) 
             / COUNT(CASE WHEN transaction_type IN ('sell', 'clean') THEN 1 END)
    END AS avg_sell_marketcap,

    MAX(transaction_time) AS last_transaction_time

FROM dex_query_v1.t_smart_transaction
WHERE token_address = ?
  AND chain_id = ?
  AND transaction_time > ?
GROUP BY wallet_address`

	var rows []smartWindowRow
	if err := s.db.WithContext(ctx).Raw(sql, tokenAddr, chainID, threshold).Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// Close 目前没有需要关闭的资源，预留接口
func (s *TransactionPairsService) Close() {
	// no-op
}


