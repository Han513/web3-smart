package service

import (
	"context"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/dao"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	"web3-smart/internal/worker/writer/holding"
	"web3-smart/pkg/utils"

	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/quotecoin"
	"go.uber.org/zap"
)

// 持仓分析包含holding表的数据

type WalletPositonAnalyze struct {
	cfg             config.Config
	tl              *zap.Logger
	repo            repository.Repository
	daoManager      *dao.DAOManager
	holdingDbWriter *writer.AsyncBatchWriter[model.WalletHolding]
	//holdingEsWriter *writer.AsyncBatchWriter[model.WalletHolding]
}

func NewWalletPositonAnalyze(cfg config.Config, logger *zap.Logger, repo repository.Repository) *WalletPositonAnalyze {
	holdingDbWriter := writer.NewAsyncBatchWriter(logger, holding.NewDbHoldingWriter(repo.GetDB(), logger), 1000, 300*time.Millisecond, "holding_db_writer", 3)
	holdingDbWriter.Start(context.Background())

	//holdingEsWriter := writer.NewAsyncBatchWriter(logger, holding.NewESHoldingWriter(repo.GetElasticsearchClient(), logger, cfg.Elasticsearch.HoldingsIndexName), 1000, 300*time.Millisecond, "holding_es_writer", 3)
	//holdingEsWriter.Start(context.Background())

	return &WalletPositonAnalyze{
		cfg:             cfg,
		tl:              logger,
		repo:            repo,
		daoManager:      repo.GetDAOManager(),
		holdingDbWriter: holdingDbWriter,
		//holdingEsWriter: holdingEsWriter,
	}
}

func (s *WalletPositonAnalyze) ProcessTrade(trade model.TradeEvent) (smartMoney *model.WalletSummary, holding *model.WalletHolding, txType string) {
	tradeTime := trade.Event.Time

	// 过滤无意义的交易
	chainId := bip0044.NetworkNameToChainId(trade.Event.Network)
	if chainId == 0 {
		return nil, nil, ""
	}
	_, inIsUSD := quotecoin.IsUsdQuoteTokens(chainId, trade.Event.BaseMint)
	_, outIsUSD := quotecoin.IsUsdQuoteTokens(chainId, trade.Event.QuoteMint)
	if inIsUSD && outIsUSD {
		return nil, nil, ""
	}

	// 获取代币信息
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tokenInfo, err := s.getTokenInfo(ctx, chainId, trade.Event.TokenAddress)
	if err != nil {
		return nil, nil, ""
	}

	// 检查tags
	isDev := false
	tags := make([]string, 0, 3)
	if tokenInfo.Creater != nil && *tokenInfo.Creater == trade.Event.Address {
		isDev = true
		tags = append(tags, model.TAG_DEV)
	}
	// 检查是否是系统聪明钱smart money
	smartMoney, err = s.getSmartMoney(ctx, chainId, trade.Event.Address)
	if err == nil && smartMoney != nil { // 将聪明钱的tags合并去重
		tags = append(tags, smartMoney.Tags...)
		tags = s.uniqueTags(tags)
	}
	// 获取最早的池子创建时间，校验是否在一分钟内发生的交易
	blockTime, err := s.getEarliestPoolCreatedAt(ctx, chainId, trade.Event.TokenAddress)
	if err == nil && blockTime > 0 && tradeTime*1000-blockTime < 60000 { // 是sniper
		tags = append(tags, model.TAG_SNIPER)
	}

	s.tl.Debug("处理交易事件",
		zap.String("wallet", trade.Event.Address),
		zap.String("token", tokenInfo.Symbol),
		zap.String("side", trade.Event.Side),
		zap.Float64("amountIn", trade.Event.FromTokenAmount),
		zap.Float64("amountOut", trade.Event.ToTokenAmount),
		zap.Float64("price", trade.Event.Price),
		zap.Float64("priceNav", trade.Event.PriceNav),
		zap.Float64("txnValue", trade.Event.TxnValue),
		zap.Float64("volumeUsd", trade.Event.VolumeUsd),
		zap.Int64("tradeTime", tradeTime),
		zap.Strings("tags", tags))

	// 获取持仓信息
	holding, err = s.getHoldingByWalletAndToken(ctx, chainId, trade.Event.Address, trade.Event.TokenAddress)
	if err != nil {
		return nil, nil, ""
	}
	if holding == nil {
		holding = model.NewWalletHolding(trade, *tokenInfo, chainId, isDev, tags)
		txType = model.TX_TYPE_BUILD
	} else {
		txType = holding.AggregateTrade(trade, *tokenInfo, isDev, tags)
	}

	// 异步写入数据库
	s.holdingDbWriter.Submit(*holding)
	//s.holdingEsWriter.Submit(*holding)

	// 更新持仓缓存
	s.daoManager.HoldingDAO.UpdateHoldingCache(ctx, utils.HoldingKey(chainId, trade.Event.Address, trade.Event.TokenAddress), holding)

	return smartMoney, holding, txType
}

// getHoldingByWalletAndToken 通过钱包地址和代币地址查询持仓信息
func (s *WalletPositonAnalyze) getHoldingByWalletAndToken(ctx context.Context, chainId uint64, walletAddress, tokenAddress string) (*model.WalletHolding, error) {
	holding, err := s.daoManager.HoldingDAO.GetByWalletAndToken(ctx, chainId, walletAddress, tokenAddress)
	if err != nil {
		s.tl.Error("查询持仓信息失败",
			zap.String("wallet_address", walletAddress),
			zap.String("token_address", tokenAddress),
			zap.Error(err))
		return nil, err
	}

	if holding == nil {
		s.tl.Info("未找到持仓信息，新建持仓记录",
			zap.String("wallet_address", walletAddress),
			zap.String("token_address", tokenAddress))
	} else {
		s.tl.Debug("找到持仓信息",
			zap.String("wallet_address", walletAddress),
			zap.String("token_address", tokenAddress),
			zap.Float64("amount", holding.Amount),
			zap.Float64("value_usd", holding.ValueUSD))
	}

	return holding, nil
}

// getTokenInfo 获取tokeninfo，如果tokeninfo不存在，则返回空对象
func (s *WalletPositonAnalyze) getTokenInfo(ctx context.Context, chainId uint64, tokenAddress string) (*model.SmTokenRet, error) {
	tokenInfo, err := s.daoManager.TokenDAO.GetTokenInfo(ctx, chainId, tokenAddress)
	if err != nil {
		s.tl.Warn("获取代币信息失败",
			zap.String("token_address", tokenAddress),
			zap.Uint64("chain_id", chainId),
			zap.Error(err))
		return nil, err
	}
	if tokenInfo == nil {
		s.tl.Debug("未找到代币信息，给空对象",
			zap.String("token_address", tokenAddress),
			zap.Uint64("chain_id", chainId))
		tokenInfo = &model.SmTokenRet{}
	}
	return tokenInfo, nil
}

// getSmartMoney 检查是否是系统聪明钱smart money
func (s *WalletPositonAnalyze) getSmartMoney(ctx context.Context, chainId uint64, walletAddress string) (*model.WalletSummary, error) {
	smartMoney, err := s.daoManager.WalletDAO.GetByWalletAddress(ctx, chainId, walletAddress)
	if err != nil {
		s.tl.Warn("获取系统聪明钱信息失败",
			zap.Uint64("chain_id", chainId),
			zap.String("wallet_address", walletAddress),
			zap.Error(err))
		return nil, err
	}

	if smartMoney == nil {
		s.tl.Debug("未找到系统聪明钱信息，不是系统聪明钱，给空对象",
			zap.Uint64("chain_id", chainId),
			zap.String("wallet_address", walletAddress))
		return nil, nil
	}

	return smartMoney, nil
}

// getEarliestPoolCreatedAt 获取最早的池子创建时间(毫秒)
func (s *WalletPositonAnalyze) getEarliestPoolCreatedAt(ctx context.Context, chainId uint64, tokenAddress string) (int64, error) {
	blockTimestamp, err := s.daoManager.PairsDAO.GetEarliestBlockTimestamp(ctx, chainId, tokenAddress)
	if err != nil {
		s.tl.Warn("获取最早的池子创建时间失败",
			zap.Uint64("chain_id", chainId),
			zap.String("token_address", tokenAddress),
			zap.Error(err))
		return 0, err
	}

	if blockTimestamp == nil {
		s.tl.Warn("未找到池子创建时间",
			zap.Uint64("chain_id", chainId),
			zap.String("token_address", tokenAddress))
		return 0, nil
	}
	return *blockTimestamp, nil
}

// uniqueTags 去重tags
func (s *WalletPositonAnalyze) uniqueTags(tags []string) []string {
	tagMap := make(map[string]struct{})
	newTags := make([]string, 0, len(tags))
	for _, tag := range tags {
		if _, ok := tagMap[tag]; !ok {
			tagMap[tag] = struct{}{}
			newTags = append(newTags, tag)
		}
	}
	return newTags
}

// Close 关闭服务时优雅关闭所有异步写入器
func (s *WalletPositonAnalyze) Close() {
	s.holdingDbWriter.Close()
	//s.holdingEsWriter.Close()
}
