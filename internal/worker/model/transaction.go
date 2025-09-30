package model

import "time"

const (
	TX_TYPE_BUILD = "build"
	TX_TYPE_BUY   = "buy"
	TX_TYPE_SELL  = "sell"
	TX_TYPE_CLEAN = "clean"
)

// WalletTransaction 钱包交易记录
type WalletTransaction struct {
	ID            int     `gorm:"primaryKey" json:"id"`
	WalletAddress string  `gorm:"column:wallet_address;type:varchar(100);not null;index" json:"wallet_address"`
	WalletBalance float64 `gorm:"column:wallet_balance;type:decimal(36,18);not null" json:"wallet_balance"` // native token数量 * price
	TokenAddress  string  `gorm:"column:token_address;type:varchar(100);not null;index" json:"token_address"`
	TokenIcon     string  `gorm:"column:token_icon;type:text" json:"token_icon"`
	TokenName     string  `gorm:"column:token_name;type:varchar(100)" json:"token_name"`
	Price         float64 `gorm:"column:price;type:decimal(36,18)" json:"price"`            // USD
	Amount        float64 `gorm:"column:amount;type:decimal(36,18);not null" json:"amount"` // token amount
	MarketCap     float64 `gorm:"column:marketcap;type:decimal(36,18)" json:"marketcap"`    // 该笔交易时的市值USD
	Value         float64 `gorm:"column:value;type:decimal(36,18)" json:"value"`            // token amount * price

	// buy: (token amount * price) / (wallet_balance + value)
	// sell: token amount / holding token amount
	HoldingPercentage float64 `gorm:"column:holding_percentage;type:decimal(10,4)" json:"holding_percentage"`
	ChainID           uint64  `gorm:"column:chain_id;type:int;not null;default:501" json:"chain_id"`

	// 已实现盈亏USD: token amount *（ price - holding avg_price ）
	RealizedProfit float64 `gorm:"column:realized_profit;type:decimal(36,18)" json:"realized_profit"`
	// 已实现盈亏百分比: (已实现盈亏USD / holding current_total_cost) * 100
	RealizedProfitPercentage float64   `gorm:"column:realized_profit_percentage;type:decimal(10,4)" json:"realized_profit_percentage"`
	TransactionType          string    `gorm:"column:transaction_type;type:varchar(10);not null" json:"transaction_type"` // build, buy, sell, clean
	TransactionTime          int64     `gorm:"column:transaction_time;not null" json:"transaction_time"`                  // blocktime
	Signature                string    `gorm:"column:signature;type:varchar(100);not null;uniqueIndex" json:"signature"`  // tx hash
	FromTokenAddress         string    `gorm:"column:from_token_address;type:varchar(100);not null" json:"from_token_address"`
	FromTokenSymbol          string    `gorm:"column:from_token_symbol;type:varchar(100)" json:"from_token_symbol"`
	FromTokenAmount          float64   `gorm:"column:from_token_amount;type:decimal(36,18);not null" json:"from_token_amount"`
	DestTokenAddress         string    `gorm:"column:dest_token_address;type:varchar(100);not null" json:"dest_token_address"`
	DestTokenSymbol          string    `gorm:"column:dest_token_symbol;type:varchar(100)" json:"dest_token_symbol"`
	DestTokenAmount          float64   `gorm:"column:dest_token_amount;type:decimal(36,18);not null" json:"dest_token_amount"`
	Time                     time.Time `gorm:"column:time;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"time"` // created time
}

func (w *WalletTransaction) TableName() string {
	return "dex_query_v1.t_smart_transaction"
}

func NewWalletTransaction(
	trade TradeEvent, smartMoney *WalletSummary, updatedHolding *WalletHolding, txType string,
	fromTokenInfo *SmTokenRet, toTokenInfo *SmTokenRet) *WalletTransaction {
	tx := &WalletTransaction{
		WalletAddress:            updatedHolding.WalletAddress,
		WalletBalance:            smartMoney.BalanceUSD,
		TokenAddress:             updatedHolding.TokenAddress,
		TokenIcon:                updatedHolding.TokenIcon,
		TokenName:                updatedHolding.TokenName,
		Price:                    trade.Event.Price,
		MarketCap:                updatedHolding.MarketCap,
		Value:                    trade.Event.VolumeUsd,
		ChainID:                  updatedHolding.ChainID,
		RealizedProfit:           0,
		RealizedProfitPercentage: 0,
		TransactionType:          txType,
		TransactionTime:          trade.Event.Time,
		Signature:                trade.Event.Hash,
		FromTokenAmount:          trade.Event.FromTokenAmount,
		DestTokenAmount:          trade.Event.ToTokenAmount,
		Time:                     time.Now(),
	}

	switch txType {
	case TX_TYPE_BUILD, TX_TYPE_BUY:
		tx.Amount = trade.Event.ToTokenAmount
		tx.HoldingPercentage = tx.Value / (tx.WalletBalance + tx.Value)

		if tx.TokenAddress == trade.Event.BaseMint {
			tx.updateTxTokenFromTo(trade.Event.QuoteMint, fromTokenInfo.Symbol, trade.Event.BaseMint, tx.TokenName)
		} else {
			tx.updateTxTokenFromTo(trade.Event.BaseMint, fromTokenInfo.Symbol, trade.Event.QuoteMint, tx.TokenName)
		}
	case TX_TYPE_SELL, TX_TYPE_CLEAN:
		tx.Amount = trade.Event.FromTokenAmount
		tx.HoldingPercentage = tx.Amount / (updatedHolding.Amount + tx.Amount)
		tx.RealizedProfit = tx.Amount * (trade.Event.Price - updatedHolding.AvgPrice)
		tx.RealizedProfitPercentage = tx.RealizedProfit / updatedHolding.CurrentTotalCost * 100

		if tx.TokenAddress == trade.Event.BaseMint {
			tx.updateTxTokenFromTo(trade.Event.BaseMint, tx.TokenName, trade.Event.QuoteMint, toTokenInfo.Symbol)
		} else {
			tx.updateTxTokenFromTo(trade.Event.QuoteMint, tx.TokenName, trade.Event.BaseMint, toTokenInfo.Symbol)
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
