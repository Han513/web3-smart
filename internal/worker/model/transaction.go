package model

import (
	"time"

	"github.com/shopspring/decimal"
)

const (
	TX_TYPE_BUILD = "build"
	TX_TYPE_BUY   = "buy"
	TX_TYPE_SELL  = "sell"
	TX_TYPE_CLEAN = "clean"
)

// WalletTransaction 钱包交易记录
type WalletTransaction struct {
	ID            int64           `gorm:"primaryKey" json:"id"`
	WalletAddress string          `gorm:"column:wallet_address;type:varchar(100);not null;index" json:"wallet_address"`
	WalletBalance decimal.Decimal `gorm:"column:wallet_balance;type:decimal(50,20);not null;default:0" json:"wallet_balance"` // native token数量 * price
	TokenAddress  string          `gorm:"column:token_address;type:varchar(100);not null;index" json:"token_address"`
	TokenIcon     string          `gorm:"column:token_icon;type:text" json:"token_icon"`
	TokenName     string          `gorm:"column:token_name;type:varchar(512)" json:"token_name"`
	Price         decimal.Decimal `gorm:"column:price;type:decimal(50,20);not null;default:0" json:"price"`         // USD
	Amount        decimal.Decimal `gorm:"column:amount;type:decimal(50,20);not null;default:0" json:"amount"`       // token amount
	MarketCap     decimal.Decimal `gorm:"column:marketcap;type:decimal(50,20);not null;default:0" json:"marketcap"` // 该笔交易时的市值USD
	Value         decimal.Decimal `gorm:"column:value;type:decimal(50,20);not null;default:0" json:"value"`         // token amount * price

	// buy: (token amount * price) / (wallet_balance + value)
	// sell: token amount / holding token amount
	HoldingPercentage decimal.Decimal `gorm:"column:holding_percentage;type:decimal(50,20);not null;default:0" json:"holding_percentage"`
	ChainID           uint64          `gorm:"column:chain_id;type:int;not null;default:501" json:"chain_id"`

	// 已实现盈亏USD: token amount *（ price - holding avg_price ）
	RealizedProfit decimal.Decimal `gorm:"column:realized_profit;type:decimal(50,20);not null;default:0" json:"realized_profit"`
	// 已实现盈亏百分比: (已实现盈亏USD / holding current_total_cost) * 100
	RealizedProfitPercentage decimal.Decimal `gorm:"column:realized_profit_percentage;type:decimal(50,20);not null;default:0" json:"realized_profit_percentage"`
	TransactionType          string          `gorm:"column:transaction_type;type:varchar(10);not null" json:"transaction_type"` // build, buy, sell, clean
	TransactionTime          int64           `gorm:"column:transaction_time;not null" json:"transaction_time"`                  // blocktime
	Signature                string          `gorm:"column:signature;type:varchar(100);not null" json:"signature"`              // tx hash
	LogIndex                 int             `gorm:"column:log_index;not null;default:0" json:"log_index"`                      // log index
	FromTokenAddress         string          `gorm:"column:from_token_address;type:varchar(100);not null" json:"from_token_address"`
	FromTokenSymbol          string          `gorm:"column:from_token_symbol;type:varchar(512)" json:"from_token_symbol"`
	FromTokenAmount          decimal.Decimal `gorm:"column:from_token_amount;type:decimal(50,20);not null;default:0" json:"from_token_amount"`
	DestTokenAddress         string          `gorm:"column:dest_token_address;type:varchar(100);not null" json:"dest_token_address"`
	DestTokenSymbol          string          `gorm:"column:dest_token_symbol;type:varchar(512)" json:"dest_token_symbol"`
	DestTokenAmount          decimal.Decimal `gorm:"column:dest_token_amount;type:decimal(50,20);not null;default:0" json:"dest_token_amount"`
	CreatedAt                int64           `gorm:"column:created_at;not null" json:"created_at"` // 毫秒时间戳
}

func (w *WalletTransaction) TableName() string {
	return "dex_query_v1.t_smart_transaction"
}

func NewWalletTransaction(
	trade TradeEvent, smartMoney *WalletSummary, updatedHolding *WalletHolding, txType string,
	fromTokenInfo *SmTokenRet, toTokenInfo *SmTokenRet) *WalletTransaction {

	logIndex := 0
	if trade.Event.LogIndex != nil {
		logIndex = int(*trade.Event.LogIndex)
	}
	tx := &WalletTransaction{
		WalletAddress:            updatedHolding.WalletAddress,
		WalletBalance:            smartMoney.BalanceUSD,
		TokenAddress:             updatedHolding.TokenAddress,
		TokenIcon:                updatedHolding.TokenIcon,
		TokenName:                updatedHolding.TokenName,
		Price:                    decimal.NewFromFloat(trade.Event.Price),
		MarketCap:                updatedHolding.MarketCap,
		Value:                    decimal.NewFromFloat(trade.Event.VolumeUsd),
		ChainID:                  updatedHolding.ChainID,
		RealizedProfit:           decimal.Zero,
		RealizedProfitPercentage: decimal.Zero,
		TransactionType:          txType,
		TransactionTime:          trade.Event.Time * 1000, // 转换为毫秒时间戳
		Signature:                trade.Event.Hash,
		LogIndex:                 logIndex,
		FromTokenAmount:          decimal.NewFromFloat(trade.Event.FromTokenAmount),
		DestTokenAmount:          decimal.NewFromFloat(trade.Event.ToTokenAmount),
		CreatedAt:                time.Now().UnixMilli(),
	}

	switch txType {
	case TX_TYPE_BUILD, TX_TYPE_BUY:
		tx.Amount = decimal.NewFromFloat(trade.Event.ToTokenAmount)
		denominator := tx.WalletBalance.Add(tx.Value)
		if denominator.GreaterThan(decimal.Zero) {
			tx.HoldingPercentage = tx.Value.Div(denominator)
		}

		if tx.TokenAddress == trade.Event.BaseMint {
			symbol := ""
			if fromTokenInfo != nil {
				symbol = fromTokenInfo.Symbol
			}
			tx.updateTxTokenFromTo(trade.Event.BaseMint, symbol, trade.Event.QuoteMint, tx.TokenName)
		} else {
			symbol := ""
			if toTokenInfo != nil {
				symbol = toTokenInfo.Symbol
			}
			tx.updateTxTokenFromTo(trade.Event.QuoteMint, symbol, trade.Event.BaseMint, tx.TokenName)
		}
	case TX_TYPE_SELL, TX_TYPE_CLEAN:
		tx.Amount = decimal.NewFromFloat(trade.Event.FromTokenAmount)
		denominator := updatedHolding.Amount.Add(tx.Amount)
		if denominator.GreaterThan(decimal.Zero) {
			tx.HoldingPercentage = tx.Amount.Div(denominator)
		}
		priceDiff := decimal.NewFromFloat(trade.Event.Price).Sub(updatedHolding.AvgPrice)
		tx.RealizedProfit = tx.Amount.Mul(priceDiff)
		// 百分比以「本次賣出成本」為分母：avg_price * sell_amount
		costBasis := updatedHolding.AvgPrice.Mul(tx.Amount)
		if costBasis.GreaterThan(decimal.Zero) {
			tx.RealizedProfitPercentage = tx.RealizedProfit.Div(costBasis).Mul(decimal.NewFromInt(100))
			tx.RealizedProfitPercentage = decimal.Max(decimal.NewFromFloat(-100), tx.RealizedProfitPercentage)
		}

		if tx.TokenAddress == trade.Event.BaseMint {
			symbol := ""
			if toTokenInfo != nil {
				symbol = toTokenInfo.Symbol
			}
			tx.updateTxTokenFromTo(trade.Event.BaseMint, tx.TokenName, trade.Event.QuoteMint, symbol)
		} else {
			symbol := ""
			if toTokenInfo != nil {
				symbol = toTokenInfo.Symbol
			}
			tx.updateTxTokenFromTo(trade.Event.QuoteMint, tx.TokenName, trade.Event.BaseMint, symbol)
		}
	}

	return tx
}

func (w *WalletTransaction) updateTxTokenFromTo(fromAddress, fromSymbol, toAddress, toSymbol string) {
	w.FromTokenAddress = fromAddress
	w.FromTokenSymbol = fromSymbol

	w.DestTokenAddress = toAddress
	w.DestTokenSymbol = toSymbol
}
