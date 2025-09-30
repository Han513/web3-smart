package service

import (
	"context"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/dao"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	"web3-smart/internal/worker/writer/transaction"
	"web3-smart/internal/worker/writer/wallet"
	"web3-smart/pkg/utils"
	getOnchainInfo "web3-smart/pkg/utils/get_onchain_info"

	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/quotecoin"
	"go.uber.org/zap"
)

// 指标统计包含wallet，transaction表的数据

type WalletIndicatorStatistics struct {
	cfg            config.Config
	tl             *zap.Logger
	repo           repository.Repository
	daoManager     *dao.DAOManager
	walletDbWriter *writer.AsyncBatchWriter[model.WalletSummary]
	//walletEsWriter *writer.AsyncBatchWriter[model.WalletSummary]
	txDbWriter *writer.AsyncBatchWriter[model.WalletTransaction]
}

func NewWalletIndicatorStatistics(cfg config.Config, logger *zap.Logger, repo repository.Repository) *WalletIndicatorStatistics {
	walletDbWriter := writer.NewAsyncBatchWriter(logger, wallet.NewDbWalletWriter(repo.GetDB(), logger), 1000, 300*time.Millisecond, "wallet_db_writer", 3)
	//walletEsWriter := writer.NewAsyncBatchWriter(logger, wallet.NewESWalletWriter(repo.GetElasticsearchClient(), logger, cfg.Elasticsearch.WalletsIndexName), 1000, 300*time.Millisecond, "wallet_es_writer", 3)
	txDbWriter := writer.NewAsyncBatchWriter(logger, transaction.NewDbTransactionWriter(repo.GetDB(), logger), 1000, 300*time.Millisecond, "tx_db_writer", 3)
	// 初始化后立即启动所有的 AsyncBatchWriter
	walletDbWriter.Start(context.Background())
	//walletEsWriter.Start(context.Background())
	txDbWriter.Start(context.Background())

	return &WalletIndicatorStatistics{
		cfg:            cfg,
		tl:             logger,
		repo:           repo,
		daoManager:     repo.GetDAOManager(),
		walletDbWriter: walletDbWriter,
		//walletEsWriter: walletEsWriter,
		txDbWriter: txDbWriter,
	}
}

func (s *WalletIndicatorStatistics) Statistics(trade model.TradeEvent, smartMoney *model.WalletSummary, updatedHolding *model.WalletHolding, txType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fromTokenInfo, toTokenInfo := &model.SmTokenRet{}, &model.SmTokenRet{}

	// 通过链上获取钱包native token余额
	nativeBalance, walletBalanceUsd, err := s.getWalletBalance(ctx, smartMoney)
	if err == nil {
		smartMoney.Balance = nativeBalance
		smartMoney.BalanceUSD = walletBalanceUsd
	}

	switch txType {
	case model.TX_TYPE_BUILD, model.TX_TYPE_BUY:
		if updatedHolding.TokenAddress == trade.Event.BaseMint {
			fromTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.QuoteMint)
			toTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.BaseMint)
		} else {
			fromTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.BaseMint)
			toTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.QuoteMint)
		}
	case model.TX_TYPE_SELL, model.TX_TYPE_CLEAN:
		if updatedHolding.TokenAddress == trade.Event.BaseMint {
			fromTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.BaseMint)
			toTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.QuoteMint)
		} else {
			fromTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.QuoteMint)
			toTokenInfo, _ = s.getTokenInfo(ctx, smartMoney.ChainID, trade.Event.BaseMint)
		}
	}

	// 更新wallet表
	smartMoney.UpdateIndicatorStatistics(&trade, updatedHolding, txType)

	// 更新tx表
	tx := model.NewWalletTransaction(trade, smartMoney, updatedHolding, txType, fromTokenInfo, toTokenInfo)

	s.walletDbWriter.Submit(*smartMoney)
	//s.walletEsWriter.Submit(*smartMoney)
	s.txDbWriter.Submit(*tx)

	// 更新wallet缓存
	s.daoManager.WalletDAO.UpdateWalletCache(ctx, utils.WalletSummaryKey(smartMoney.ChainID, smartMoney.WalletAddress), smartMoney)
}

func (s *WalletIndicatorStatistics) getWalletBalance(ctx context.Context, smartMoney *model.WalletSummary) (float64, float64, error) {
	nativeBalance := decimal.Decimal{}
	walletBalanceUsd := decimal.Decimal{}

	switch smartMoney.ChainID {
	case bip0044.BSC:
		nativeBal, quoteTokenMap, err := getOnchainInfo.GetBscBalance(ctx, s.repo.GetBscClient(), smartMoney.WalletAddress)
		if err != nil {
			s.tl.Warn("get bsc balance err",
				zap.String("wallet_address", smartMoney.WalletAddress),
				zap.Uint64("chain_id", smartMoney.ChainID),
				zap.Error(err))
		}

		v, ok := quoteTokenMap[quotecoin.ID9006_WBNB_ADDRESS]
		if !ok {
			v = decimal.NewFromFloat(0)
		}
		quoteTokenMap[quotecoin.ID9006_WBNB_ADDRESS] = v.Add(nativeBal)

		nativeBalance = quoteTokenMap[quotecoin.ID9006_WBNB_ADDRESS]
		walletBalanceUsd = s.getWalletTotalValUSD(ctx, smartMoney.ChainID, quoteTokenMap)
	case bip0044.SOLANA:
		nativeBal, quoteTokenMap, err := getOnchainInfo.GetSolanaBalance(ctx, s.repo.GetSolanaClient(), smartMoney.WalletAddress)
		if err != nil {
			s.tl.Warn("get solana balance err",
				zap.String("wallet_address", smartMoney.WalletAddress),
				zap.Uint64("chain_id", smartMoney.ChainID),
				zap.Error(err))
		}

		v, ok := quoteTokenMap[quotecoin.ID501_WSOL_ADDRESS]
		if !ok {
			v = decimal.NewFromFloat(0)
		}
		quoteTokenMap[quotecoin.ID501_WSOL_ADDRESS] = v.Add(nativeBal)

		nativeBalance = quoteTokenMap[quotecoin.ID501_WSOL_ADDRESS]
		walletBalanceUsd = s.getWalletTotalValUSD(ctx, smartMoney.ChainID, quoteTokenMap)
	}

	return nativeBalance.InexactFloat64(), walletBalanceUsd.InexactFloat64(), nil
}

// 获取钱包所有持仓的USD总价值
func (s *WalletIndicatorStatistics) getWalletTotalValUSD(ctx context.Context, bip0044ChainId uint64, quoteTokenMap map[string]decimal.Decimal) decimal.Decimal {
	totalValUSD := decimal.NewFromFloat(0)

	for quoteAddr, bal := range quoteTokenMap {
		nativeTokenSym := quotecoin.GetNativeTokenSymbol(bip0044ChainId, quoteAddr) // case: ETH, SOL ...
		if nativeTokenSym == "" {
			continue
		}

		var err error
		quotePriceUsd := float64(1) // default is USD token price
		if _, isUsdQuote := quotecoin.IsUsdQuoteTokens(bip0044ChainId, quoteAddr); !isUsdQuote {
			quotePriceUsd, err = s.repo.GetBydRpc().GetPrice(ctx, nativeTokenSym, "USDT")
			if err != nil {
				s.tl.Warn("get quote token price usd err",
					zap.String("native_token_sym", nativeTokenSym),
					zap.Error(err))
				continue
			}
		}

		valUSD := decimal.NewFromFloat(quotePriceUsd).Mul(bal)
		totalValUSD = totalValUSD.Add(valUSD)
	}

	return totalValUSD
}

// getTokenInfo 获取tokeninfo，如果tokeninfo不存在，则返回空对象
func (s *WalletIndicatorStatistics) getTokenInfo(ctx context.Context, chainId uint64, tokenAddress string) (*model.SmTokenRet, error) {
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

func (s *WalletIndicatorStatistics) Close() {
	s.walletDbWriter.Close()
	//s.walletEsWriter.Close()
	s.txDbWriter.Close()
}
