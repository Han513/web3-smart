package model

import (
	"math"
	"time"

	"github.com/shopspring/decimal"
)

const (
	TAG_DEV          = "dev"
	TAG_SMART_MONEY  = "smart_money"
	TAG_SNIPER       = "sniper"
	TAG_KOL          = "kol"
	TAG_FRESH_WALLET = "fresh_wallet"
)

// WalletHolding 钱包持仓信息
type WalletHolding struct {
	ID                int      `gorm:"primaryKey;autoIncrement" json:"id"`
	ChainID           uint64   `gorm:"column:chain_id;not null" json:"chain_id"`
	WalletAddress     string   `gorm:"column:wallet_address;type:varchar(255);not null;index" json:"wallet_address"`
	TokenAddress      string   `gorm:"column:token_address;type:varchar(255);not null;index" json:"token_address"`
	TokenIcon         string   `gorm:"column:token_icon;type:varchar(255)" json:"token_icon"`
	TokenName         string   `gorm:"column:token_name;type:varchar(255)" json:"token_name"`
	Amount            float64  `gorm:"column:amount;type:decimal(36,18);not null;default:0" json:"amount"`                         // 当前持仓数量
	ValueUSD          float64  `gorm:"column:value_usd;type:decimal(36,18);not null;default:0" json:"value_usd"`                   // 当前持仓价值USD
	UnrealizedProfits float64  `gorm:"column:unrealized_profits;type:decimal(36,18);not null;default:0" json:"unrealized_profits"` // 当前持仓未实现盈亏USD
	PNL               float64  `gorm:"column:pnl;type:decimal(36,18);not null;default:0" json:"pnl"`                               // 已实现盈亏USD（累加）
	PNLPercentage     float64  `gorm:"column:pnl_percentage;type:decimal(10,4);not null;default:0" json:"pnl_percentage"`          // 已实现盈亏百分比（累加）
	AvgPrice          float64  `gorm:"column:avg_price;type:decimal(36,18);not null;default:0" json:"avg_price"`                   // 平均买入价USD
	CurrentTotalCost  float64  `gorm:"column:current_total_cost;type:decimal(36,18);not null;default:0" json:"current_total_cost"` // 当前持仓总花费成本USD
	MarketCap         float64  `gorm:"column:marketcap;type:decimal(36,18);not null;default:0" json:"marketcap"`                   // 买入/卖出时的市值USD
	IsCleared         bool     `gorm:"column:is_cleared;type:boolean;not null;default:false" json:"is_cleared"`                    // 是否已清仓
	IsDev             bool     `gorm:"column:is_dev;type:boolean;not null;default:false" json:"is_dev"`                            // 是否是该token dev
	Tags              []string `gorm:"column:tags;type:varchar(50)[]" json:"tags"`                                                 // smart money, sniper...

	// 历史累计状态
	HistoricalBuyAmount  float64 `gorm:"column:historical_buy_amount;type:decimal(36,18);not null;default:0" json:"historical_buy_amount"`
	HistoricalSellAmount float64 `gorm:"column:historical_sell_amount;type:decimal(36,18);not null;default:0" json:"historical_sell_amount"`
	HistoricalBuyCost    float64 `gorm:"column:historical_buy_cost;type:decimal(36,18);not null;default:0" json:"historical_buy_cost"`
	HistoricalSellValue  float64 `gorm:"column:historical_sell_value;type:decimal(36,18);not null;default:0" json:"historical_sell_value"`
	HistoricalBuyCount   int     `gorm:"column:historical_buy_count;not null;default:0" json:"historical_buy_count"`
	HistoricalSellCount  int     `gorm:"column:historical_sell_count;not null;default:0" json:"historical_sell_count"`

	PositionOpenedAt    *int64 `gorm:"column:position_opened_at" json:"position_opened_at"`       // 首次建仓时间（第一次购买）
	LastTransactionTime int64  `gorm:"column:last_transaction_time" json:"last_transaction_time"` // 最近一次交易时间（blocktime）

	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"updated_at"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (w *WalletHolding) TableName() string {
	return "dex_query_v1.t_smart_holding"
}

func NewWalletHolding(trade TradeEvent, tokenInfo SmTokenRet, chainId uint64, isDev bool, tags []string) *WalletHolding {
	if trade.Event.Side == "sell" {
		return nil
	}

	totalSupply := tokenInfo.Supply
	tokenAmount := trade.Event.ToTokenAmount
	marketCap := 0.0
	if totalSupply != nil {
		marketCap = totalSupply.Mul(decimal.NewFromFloat(trade.Event.Price)).InexactFloat64()
	}
	blockTime := trade.Event.Time * 1000

	return &WalletHolding{
		ChainID:             chainId,
		WalletAddress:       trade.Event.Address,
		TokenAddress:        trade.Event.TokenAddress,
		TokenIcon:           tokenInfo.Logo,
		TokenName:           tokenInfo.Symbol,
		Amount:              tokenAmount,
		ValueUSD:            trade.Event.VolumeUsd,
		AvgPrice:            trade.Event.Price,
		CurrentTotalCost:    trade.Event.VolumeUsd,
		MarketCap:           marketCap,
		IsDev:               isDev,
		Tags:                tags,
		HistoricalBuyAmount: tokenAmount,
		HistoricalBuyCost:   trade.Event.VolumeUsd,
		HistoricalBuyCount:  1,
		PositionOpenedAt:    &blockTime,
		LastTransactionTime: blockTime,
		UpdatedAt:           time.Now(),
		CreatedAt:           time.Now(),
	}
}

func (w *WalletHolding) AggregateTrade(trade TradeEvent, tokenInfo SmTokenRet, isDev bool, tags []string) string {
	txType := TX_TYPE_BUY

	totalSupply := tokenInfo.Supply
	marketCap := w.MarketCap
	if totalSupply != nil {
		marketCap = totalSupply.Mul(decimal.NewFromFloat(trade.Event.Price)).InexactFloat64()
	}

	w.TokenIcon = tokenInfo.Logo
	w.TokenName = tokenInfo.Symbol
	w.IsDev = isDev
	w.Tags = tags
	w.LastTransactionTime = trade.Event.Time * 1000
	w.UpdatedAt = time.Now()

	switch trade.Event.Side {
	case "buy":
		if w.Amount <= 0 { // 如果持仓为0，则认为是建仓
			w.Amount = 0
			w.ValueUSD = 0
			w.CurrentTotalCost = 0
			w.AvgPrice = 0
			w.UnrealizedProfits = 0
			w.IsCleared = false
			txType = TX_TYPE_BUILD
		}
		w.Amount += trade.Event.ToTokenAmount
		w.ValueUSD = w.Amount * trade.Event.Price
		w.CurrentTotalCost += trade.Event.VolumeUsd
		w.AvgPrice = w.CurrentTotalCost / w.Amount
		w.MarketCap = marketCap
		w.UnrealizedProfits = w.ValueUSD - w.CurrentTotalCost
		w.IsCleared = false

		w.HistoricalBuyAmount += trade.Event.ToTokenAmount
		w.HistoricalBuyCost += trade.Event.VolumeUsd
		w.HistoricalBuyCount++
	case "sell":
		txType = TX_TYPE_SELL

		// pnl = 卖出价值USD - 卖出数量 * 平均买入价
		sellAmount := trade.Event.FromTokenAmount
		sellValueUSD := trade.Event.VolumeUsd
		w.PNL += sellValueUSD - sellAmount*w.AvgPrice
		w.PNLPercentage = math.Max(-100, (w.PNL/w.HistoricalBuyCost)*100)

		w.Amount -= trade.Event.FromTokenAmount
		w.ValueUSD = w.Amount * trade.Event.Price
		w.CurrentTotalCost -= trade.Event.VolumeUsd
		w.AvgPrice = w.CurrentTotalCost / w.Amount
		w.MarketCap = marketCap
		w.UnrealizedProfits = w.ValueUSD - w.CurrentTotalCost
		w.IsCleared = false
		if w.Amount <= 0 {
			w.IsCleared = true
			w.Amount = 0
			w.ValueUSD = 0
			w.CurrentTotalCost = 0
			w.AvgPrice = 0
			w.UnrealizedProfits = 0
			txType = TX_TYPE_CLEAN
		}

		w.HistoricalSellAmount += trade.Event.FromTokenAmount
		w.HistoricalSellValue += trade.Event.VolumeUsd
		w.HistoricalSellCount++
	}

	return txType
}

func (w *WalletHolding) ToESIndex() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"_routing": map[string]interface{}{
				"required": true,
			},
			"properties": map[string]interface{}{
				"chain_id":       map[string]interface{}{"type": "long"},
				"wallet_address": map[string]interface{}{"type": "keyword"},
				"token_address":  map[string]interface{}{"type": "keyword"},
				"token_hash":     map[string]interface{}{"type": "keyword"},
				"token_name": map[string]interface{}{
					"type":     "text",
					"analyzer": "standard",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"type":         "keyword",
							"ignore_above": 256,
						},
					},
				},
				"amount":                 map[string]interface{}{"type": "double"},
				"value_usd":              map[string]interface{}{"type": "double"},
				"unrealized_profits":     map[string]interface{}{"type": "double"},
				"pnl":                    map[string]interface{}{"type": "double"},
				"pnl_percentage":         map[string]interface{}{"type": "double"},
				"avg_price":              map[string]interface{}{"type": "double"},
				"current_total_cost":     map[string]interface{}{"type": "double"},
				"marketcap":              map[string]interface{}{"type": "double"},
				"is_cleared":             map[string]interface{}{"type": "boolean"},
				"is_dev":                 map[string]interface{}{"type": "boolean"},
				"tags":                   map[string]interface{}{"type": "keyword"},
				"wallet_tags_combined":   map[string]interface{}{"type": "keyword"},
				"historical_buy_amount":  map[string]interface{}{"type": "double"},
				"historical_sell_amount": map[string]interface{}{"type": "double"},
				"historical_buy_cost":    map[string]interface{}{"type": "double"},
				"historical_sell_value":  map[string]interface{}{"type": "double"},
				"historical_buy_count":   map[string]interface{}{"type": "integer"},
				"historical_sell_count":  map[string]interface{}{"type": "integer"},
				"position_opened_at":     map[string]interface{}{"type": "date"},
				"last_transaction_time":  map[string]interface{}{"type": "date"},
				"updated_at":             map[string]interface{}{"type": "date"},
				"created_at":             map[string]interface{}{"type": "date"},
			},
		},
		"settings": map[string]interface{}{
			"number_of_shards":   20,
			"number_of_replicas": 1,
			"index": map[string]interface{}{
				"routing": map[string]interface{}{
					"allocation": map[string]interface{}{
						"include": map[string]interface{}{
							"_tier_preference": "data_hot",
						},
					},
				},
				"refresh_interval":  "5s",
				"max_result_window": 50000,
			},
		},
	}
}
