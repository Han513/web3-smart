package model

import "time"

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
	CurrentTotalCost  float64  `gorm:"column:current_total_cost;type:decimal(36,18);not null;default:0" json:"current_total_cost"` // 当前持仓总花费成本
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
