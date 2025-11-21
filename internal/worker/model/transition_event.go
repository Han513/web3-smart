package model

import (
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

const SMART_TX_EVENT_TYPE = "com.zeroex.web3.core.event.internal.SmartTransactionEvent"

type SmartTxEvent struct {
	Event SmartTxEventDetails `json:"event"`
	Type  string              `json:"type"`
	Raw   []byte              `json:"raw"` // 添加 Raw 字段存储原始消息
}

type SmartTxEventDetails struct {
	ID                       string          `json:"id"`
	Brand                    string          `json:"brand"`
	WalletAddress            string          `json:"walletAddress"`
	WalletBalance            decimal.Decimal `json:"walletBalance"`
	TokenAddress             string          `json:"tokenAddress"`
	TokenIcon                string          `json:"tokenIcon"`
	TokenName                string          `json:"tokenName"`
	Price                    decimal.Decimal `json:"price"`
	Amount                   decimal.Decimal `json:"amount"`
	MarketCap                decimal.Decimal `json:"marketcap"`
	Value                    decimal.Decimal `json:"value"`
	HoldingPercentage        decimal.Decimal `json:"holdingPercentage"`
	ChainID                  uint64          `json:"chainId"`
	RealizedProfit           decimal.Decimal `json:"realizedProfit"`
	RealizedProfitPercentage decimal.Decimal `json:"realizedProfitPercentage"`
	TransactionType          string          `json:"transactionType"`
	TransactionTime          int64           `json:"transactionTime"`
	Signature                string          `json:"signature"`
	LogIndex                 int             `json:"logIndex"`
	FromTokenAddress         string          `json:"fromTokenAddress"`
	FromTokenSymbol          string          `json:"fromTokenSymbol"`
	FromTokenAmount          decimal.Decimal `json:"fromTokenAmount"`
	DestTokenAddress         string          `json:"destTokenAddress"`
	DestTokenSymbol          string          `json:"destTokenSymbol"`
	DestTokenAmount          decimal.Decimal `json:"destTokenAmount"`
	CreatedAt                int64           `json:"createdAt"`
}

func NewSmartTxEvent(tx WalletTransaction) *SmartTxEvent {
	return &SmartTxEvent{
		Event: SmartTxEventDetails{
			ID:                       uuid.New().String(),
			Brand:                    "BYD",
			WalletAddress:            tx.WalletAddress,
			WalletBalance:            tx.WalletBalance,
			TokenAddress:             tx.TokenAddress,
			TokenIcon:                tx.TokenIcon,
			TokenName:                tx.TokenName,
			Price:                    tx.Price,
			Amount:                   tx.Amount,
			MarketCap:                tx.MarketCap,
			Value:                    tx.Value,
			HoldingPercentage:        tx.HoldingPercentage,
			ChainID:                  tx.ChainID,
			RealizedProfit:           tx.RealizedProfit,
			RealizedProfitPercentage: tx.RealizedProfitPercentage,
			TransactionType:          tx.TransactionType,
			TransactionTime:          tx.TransactionTime,
			Signature:                tx.Signature,
			LogIndex:                 tx.LogIndex,
			FromTokenAddress:         tx.FromTokenAddress,
			FromTokenSymbol:          tx.FromTokenSymbol,
			FromTokenAmount:          tx.FromTokenAmount,
			DestTokenAddress:         tx.DestTokenAddress,
			DestTokenSymbol:          tx.DestTokenSymbol,
			DestTokenAmount:          tx.DestTokenAmount,
			CreatedAt:                tx.CreatedAt,
		},
		Type: SMART_TX_EVENT_TYPE,
		Raw:  nil,
	}
}
