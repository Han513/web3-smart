package model

import (
	"time"

	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

const (
	TAG_DEV          = "dev"
	TAG_SMART_MONEY  = "smart_wallet"
	TAG_SNIPER       = "sniper"
	TAG_KOL          = "kol"
	TAG_FRESH_WALLET = "fresh_wallet"
)

// WalletHolding 钱包持仓信息
type WalletHolding struct {
	ID                int64           `gorm:"primaryKey;autoIncrement" json:"id"`
	ChainID           uint64          `gorm:"column:chain_id;not null" json:"chain_id"`
	WalletAddress     string          `gorm:"column:wallet_address;type:varchar(255);not null;index" json:"wallet_address"`
	TokenAddress      string          `gorm:"column:token_address;type:varchar(255);not null;index" json:"token_address"`
	TokenIcon         string          `gorm:"column:token_icon;type:varchar(255)" json:"token_icon"`
	TokenName         string          `gorm:"column:token_name;type:varchar(512)" json:"token_name"`
	Amount            decimal.Decimal `gorm:"column:amount;type:decimal(50,20);not null;default:0" json:"amount"`                         // 当前持仓数量
	ValueUSD          decimal.Decimal `gorm:"column:value_usd;type:decimal(50,20);not null;default:0" json:"value_usd"`                   // 当前持仓价值USD
	UnrealizedProfits decimal.Decimal `gorm:"column:unrealized_profits;type:decimal(50,20);not null;default:0" json:"unrealized_profits"` // 当前持仓未实现盈亏USD
	PNL               decimal.Decimal `gorm:"column:pnl;type:decimal(50,20);not null;default:0" json:"pnl"`                               // 已实现盈亏USD（累加）
	PNLPercentage     decimal.Decimal `gorm:"column:pnl_percentage;type:decimal(50,20);not null;default:0" json:"pnl_percentage"`         // 已实现盈亏百分比（累加）
	AvgPrice          decimal.Decimal `gorm:"column:avg_price;type:decimal(50,20);not null;default:0" json:"avg_price"`                   // 平均买入价USD
	CurrentTotalCost  decimal.Decimal `gorm:"column:current_total_cost;type:decimal(50,20);not null;default:0" json:"current_total_cost"` // 当前持仓总花费成本USD
	MarketCap         decimal.Decimal `gorm:"column:marketcap;type:decimal(50,20);not null;default:0" json:"marketcap"`                   // 买入/卖出时的市值USD
	IsCleared         bool            `gorm:"column:is_cleared;type:boolean;not null;default:false" json:"is_cleared"`                    // 是否已清仓
	IsDev             bool            `gorm:"column:is_dev;type:boolean;not null;default:false" json:"is_dev"`                            // 是否是该token dev
	Tags              pq.StringArray  `gorm:"column:tags;type:varchar(50)[]" json:"tags"`                                                 // smart money, sniper...

	// 历史累计状态
	HistoricalBuyAmount  decimal.Decimal `gorm:"column:historical_buy_amount;type:decimal(50,20);not null;default:0" json:"historical_buy_amount"`
	HistoricalSellAmount decimal.Decimal `gorm:"column:historical_sell_amount;type:decimal(50,20);not null;default:0" json:"historical_sell_amount"`
	HistoricalBuyCost    decimal.Decimal `gorm:"column:historical_buy_cost;type:decimal(50,20);not null;default:0" json:"historical_buy_cost"`
	HistoricalSellValue  decimal.Decimal `gorm:"column:historical_sell_value;type:decimal(50,20);not null;default:0" json:"historical_sell_value"`
	HistoricalBuyCount   int             `gorm:"column:historical_buy_count;not null;default:0" json:"historical_buy_count"`
	HistoricalSellCount  int             `gorm:"column:historical_sell_count;not null;default:0" json:"historical_sell_count"`

	PositionOpenedAt    *int64 `gorm:"column:position_opened_at" json:"position_opened_at"`       // 首次建仓时间（第一次购买）
	LastTransactionTime int64  `gorm:"column:last_transaction_time" json:"last_transaction_time"` // 最近一次交易时间（blocktime）

	UpdatedAt int64 `gorm:"column:updated_at;not null" json:"updated_at"` // 毫秒时间戳
	CreatedAt int64 `gorm:"column:created_at;not null" json:"created_at"` // 毫秒时间戳
}

func (w *WalletHolding) TableName() string {
	return "dex_query_v1.t_smart_holding"
}

func NewWalletHolding(trade TradeEvent, tokenInfo SmTokenRet, chainId uint64, isDev bool, tags []string) *WalletHolding {
	if trade.Event.Side == "sell" {
		return nil
	}

	totalSupply := tokenInfo.Supply
	tokenAmount := decimal.NewFromFloat(trade.Event.ToTokenAmount)
	marketCap := decimal.Zero
	if totalSupply != nil {
		marketCap = totalSupply.Mul(decimal.NewFromFloat(trade.Event.Price))
	}
	blockTime := trade.Event.Time * 1000

	return &WalletHolding{
		ChainID:             chainId,
		WalletAddress:       trade.Event.Address,
		TokenAddress:        trade.Event.TokenAddress,
		TokenIcon:           tokenInfo.Logo,
		TokenName:           tokenInfo.Symbol,
		Amount:              tokenAmount,
		ValueUSD:            decimal.NewFromFloat(trade.Event.VolumeUsd),
		AvgPrice:            decimal.NewFromFloat(trade.Event.Price),
		CurrentTotalCost:    decimal.NewFromFloat(trade.Event.VolumeUsd),
		MarketCap:           marketCap,
		IsDev:               isDev,
		Tags:                pq.StringArray(tags),
		HistoricalBuyAmount: tokenAmount,
		HistoricalBuyCost:   decimal.NewFromFloat(trade.Event.VolumeUsd),
		HistoricalBuyCount:  1,
		PositionOpenedAt:    &blockTime,
		LastTransactionTime: blockTime,
		UpdatedAt:           time.Now().UnixMilli(),
		CreatedAt:           time.Now().UnixMilli(),
	}
}

func (w *WalletHolding) AggregateTrade(trade TradeEvent, tokenInfo SmTokenRet, isDev bool, tags []string) string {
	txType := TX_TYPE_BUY

	totalSupply := tokenInfo.Supply
	marketCap := w.MarketCap
	if totalSupply != nil {
		marketCap = totalSupply.Mul(decimal.NewFromFloat(trade.Event.Price))
	}

	w.TokenIcon = tokenInfo.Logo
	w.TokenName = tokenInfo.Symbol
	w.IsDev = isDev
	w.Tags = pq.StringArray(tags)
	w.LastTransactionTime = trade.Event.Time * 1000
	w.UpdatedAt = time.Now().UnixMilli()

	switch trade.Event.Side {
	case "buy":
		if w.Amount.LessThanOrEqual(decimal.Zero) { // 如果持仓为0，则认为是建仓
			w.Amount = decimal.Zero
			w.ValueUSD = decimal.Zero
			w.CurrentTotalCost = decimal.Zero
			w.AvgPrice = decimal.Zero
			w.UnrealizedProfits = decimal.Zero
			w.IsCleared = false
			txType = TX_TYPE_BUILD
		}
		tokenAmount := decimal.NewFromFloat(trade.Event.ToTokenAmount)
		price := decimal.NewFromFloat(trade.Event.Price)
		volumeUsd := decimal.NewFromFloat(trade.Event.VolumeUsd)

		w.Amount = w.Amount.Add(tokenAmount)
		w.ValueUSD = w.Amount.Mul(price)
		w.CurrentTotalCost = w.CurrentTotalCost.Add(volumeUsd)
		if w.Amount.GreaterThan(decimal.Zero) {
			w.AvgPrice = w.CurrentTotalCost.Div(w.Amount)
		}
		w.MarketCap = marketCap
		w.UnrealizedProfits = w.ValueUSD.Sub(w.CurrentTotalCost)
		w.IsCleared = false

		w.HistoricalBuyAmount = w.HistoricalBuyAmount.Add(tokenAmount)
		w.HistoricalBuyCost = w.HistoricalBuyCost.Add(volumeUsd)
		w.HistoricalBuyCount++
	case "sell":
		txType = TX_TYPE_SELL

		// pnl = 卖出价值USD - 卖出数量 * 平均买入价
		sellAmount := decimal.NewFromFloat(trade.Event.FromTokenAmount)
		sellValueUSD := decimal.NewFromFloat(trade.Event.VolumeUsd)
		price := decimal.NewFromFloat(trade.Event.Price)
		volumeUsd := decimal.NewFromFloat(trade.Event.VolumeUsd)

		pnlDelta := sellValueUSD.Sub(sellAmount.Mul(w.AvgPrice))
		w.PNL = w.PNL.Add(pnlDelta)
		if w.HistoricalBuyCost.GreaterThan(decimal.Zero) {
			pnlPercentage := w.PNL.Div(w.HistoricalBuyCost).Mul(decimal.NewFromInt(100))
			w.PNLPercentage = decimal.Max(decimal.NewFromInt(-100), pnlPercentage)
		}

		w.Amount = w.Amount.Sub(sellAmount)
		w.ValueUSD = w.Amount.Mul(price)
		w.CurrentTotalCost = w.CurrentTotalCost.Sub(volumeUsd)
		if w.Amount.GreaterThan(decimal.Zero) {
			w.AvgPrice = w.CurrentTotalCost.Div(w.Amount)
		}
		w.MarketCap = marketCap
		w.UnrealizedProfits = w.ValueUSD.Sub(w.CurrentTotalCost)
		w.IsCleared = false
		if w.Amount.LessThanOrEqual(decimal.Zero) {
			w.IsCleared = true
			w.Amount = decimal.Zero
			w.ValueUSD = decimal.Zero
			w.CurrentTotalCost = decimal.Zero
			w.AvgPrice = decimal.Zero
			w.UnrealizedProfits = decimal.Zero
			txType = TX_TYPE_CLEAN
		}

		w.HistoricalSellAmount = w.HistoricalSellAmount.Add(sellAmount)
		w.HistoricalSellValue = w.HistoricalSellValue.Add(sellValueUSD)
		w.HistoricalSellCount++
	}

	return txType
}

// ToESDocument converts WalletHolding to map with float64 values for ES indexing
func (w *WalletHolding) ToESDocument() map[string]interface{} {
	return map[string]interface{}{
		"id":                     w.ID,
		"chain_id":               w.ChainID,
		"wallet_address":         w.WalletAddress,
		"token_address":          w.TokenAddress,
		"token_icon":             w.TokenIcon,
		"token_name":             w.TokenName,
		"amount":                 w.Amount.InexactFloat64(),
		"value_usd":              w.ValueUSD.InexactFloat64(),
		"unrealized_profits":     w.UnrealizedProfits.InexactFloat64(),
		"pnl":                    w.PNL.InexactFloat64(),
		"pnl_percentage":         w.PNLPercentage.InexactFloat64(),
		"avg_price":              w.AvgPrice.InexactFloat64(),
		"current_total_cost":     w.CurrentTotalCost.InexactFloat64(),
		"marketcap":              w.MarketCap.InexactFloat64(),
		"is_cleared":             w.IsCleared,
		"is_dev":                 w.IsDev,
		"tags":                   w.Tags,
		"historical_buy_amount":  w.HistoricalBuyAmount.InexactFloat64(),
		"historical_sell_amount": w.HistoricalSellAmount.InexactFloat64(),
		"historical_buy_cost":    w.HistoricalBuyCost.InexactFloat64(),
		"historical_sell_value":  w.HistoricalSellValue.InexactFloat64(),
		"historical_buy_count":   w.HistoricalBuyCount,
		"historical_sell_count":  w.HistoricalSellCount,
		"position_opened_at":     w.PositionOpenedAt,
		"last_transaction_time":  w.LastTransactionTime,
		"updated_at":             w.UpdatedAt,
		"created_at":             w.CreatedAt,
	}
}

func (w *WalletHolding) ToESIndex() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			//"_routing": map[string]interface{}{
			//	"required": true,
			//},
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
				"position_opened_at":     map[string]interface{}{"type": "date", "format": "epoch_millis"},
				"last_transaction_time":  map[string]interface{}{"type": "date", "format": "epoch_millis"},
				"updated_at":             map[string]interface{}{"type": "date", "format": "epoch_millis"},
				"created_at":             map[string]interface{}{"type": "date", "format": "epoch_millis"},
			},
		},
		"settings": map[string]interface{}{
			"number_of_shards":   3,
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
