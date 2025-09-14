package model

import "time"

// WalletSummary 钱包摘要信息
type WalletSummary struct {
	ID              int      `gorm:"primaryKey" json:"id"`
	WalletAddress   string   `gorm:"column:wallet_address;type:varchar(512);not null;uniqueIndex" json:"wallet_address"`
	Avatar          string   `gorm:"column:avatar;type:varchar(512);not null" json:"avatar"` // 头像
	Balance         float64  `gorm:"column:balance;type:decimal(20,8)" json:"balance"`
	BalanceUSD      float64  `gorm:"column:balance_usd;type:decimal(20,8)" json:"balance_usd"`
	ChainID         uint64   `gorm:"column:chain_id;type:int" json:"chain_id"`
	Tags            []string `gorm:"column:tags;type:varchar(50)[]" json:"tags"` // smart money
	TwitterName     string   `gorm:"column:twitter_name;type:varchar(50)" json:"twitter_name"`
	TwitterUsername string   `gorm:"column:twitter_username;type:varchar(50)" json:"twitter_username"`
	WalletType      int      `gorm:"column:wallet_type;type:int;default:0" json:"wallet_type"`       // 0:一般聪明钱，1:pump聪明钱，2:moonshot聪明钱
	AssetMultiple   float64  `gorm:"column:asset_multiple;type:decimal(10,1)" json:"asset_multiple"` // 盈亏资产倍数
	TokenList       string   `gorm:"column:token_list;type:varchar(512)" json:"token_list"`          // 最近交易过的token(3个)

	// 交易数据 - 30天
	AvgCost30d             float64 `gorm:"column:avg_cost_30d;type:decimal(20,8)" json:"avg_cost_30d"`
	TotalTransactionNum30d int     `gorm:"column:total_transaction_num_30d" json:"total_transaction_num_30d"`
	BuyNum30d              int     `gorm:"column:buy_num_30d" json:"buy_num_30d"`
	SellNum30d             int     `gorm:"column:sell_num_30d" json:"sell_num_30d"`
	WinRate30d             float64 `gorm:"column:win_rate_30d;type:decimal(5,4)" json:"win_rate_30d"`

	// 交易数据 - 7天
	AvgCost7d             float64 `gorm:"column:avg_cost_7d;type:decimal(20,8)" json:"avg_cost_7d"`
	TotalTransactionNum7d int     `gorm:"column:total_transaction_num_7d" json:"total_transaction_num_7d"`
	BuyNum7d              int     `gorm:"column:buy_num_7d" json:"buy_num_7d"`
	SellNum7d             int     `gorm:"column:sell_num_7d" json:"sell_num_7d"`
	WinRate7d             float64 `gorm:"column:win_rate_7d;type:decimal(5,4)" json:"win_rate_7d"`

	// 交易数据 - 1天
	AvgCost1d             float64 `gorm:"column:avg_cost_1d;type:decimal(20,8)" json:"avg_cost_1d"`
	TotalTransactionNum1d int     `gorm:"column:total_transaction_num_1d" json:"total_transaction_num_1d"`
	BuyNum1d              int     `gorm:"column:buy_num_1d" json:"buy_num_1d"`
	SellNum1d             int     `gorm:"column:sell_num_1d" json:"sell_num_1d"`
	WinRate1d             float64 `gorm:"column:win_rate_1d;type:decimal(5,4)" json:"win_rate_1d"`

	// 盈亏数据 - 30天
	PNL30d               float64 `gorm:"column:pnl_30d;type:decimal(20,8)" json:"pnl_30d"`
	PNLPercentage30d     float64 `gorm:"column:pnl_percentage_30d;type:decimal(8,4)" json:"pnl_percentage_30d"`
	PNLPic30d            string  `gorm:"column:pnl_pic_30d;type:text" json:"pnl_pic_30d"`
	UnrealizedProfit30d  float64 `gorm:"column:unrealized_profit_30d;type:decimal(20,8)" json:"unrealized_profit_30d"`
	TotalCost30d         float64 `gorm:"column:total_cost_30d;type:decimal(20,8)" json:"total_cost_30d"`
	AvgRealizedProfit30d float64 `gorm:"column:avg_realized_profit_30d;type:decimal(20,8)" json:"avg_realized_profit_30d"`

	// 盈亏数据 - 7天
	PNL7d               float64 `gorm:"column:pnl_7d;type:decimal(20,8)" json:"pnl_7d"`
	PNLPercentage7d     float64 `gorm:"column:pnl_percentage_7d;type:decimal(8,4)" json:"pnl_percentage_7d"`
	UnrealizedProfit7d  float64 `gorm:"column:unrealized_profit_7d;type:decimal(20,8)" json:"unrealized_profit_7d"`
	TotalCost7d         float64 `gorm:"column:total_cost_7d;type:decimal(20,8)" json:"total_cost_7d"`
	AvgRealizedProfit7d float64 `gorm:"column:avg_realized_profit_7d;type:decimal(20,8)" json:"avg_realized_profit_7d"`

	// 盈亏数据 - 1天
	PNL1d               float64 `gorm:"column:pnl_1d;type:decimal(20,8)" json:"pnl_1d"`
	PNLPercentage1d     float64 `gorm:"column:pnl_percentage_1d;type:decimal(8,4)" json:"pnl_percentage_1d"`
	UnrealizedProfit1d  float64 `gorm:"column:unrealized_profit_1d;type:decimal(20,8)" json:"unrealized_profit_1d"`
	TotalCost1d         float64 `gorm:"column:total_cost_1d;type:decimal(20,8)" json:"total_cost_1d"`
	AvgRealizedProfit1d float64 `gorm:"column:avg_realized_profit_1d;type:decimal(20,8)" json:"avg_realized_profit_1d"`

	// 收益分布数据 - 30天
	DistributionGt500_30d             int     `gorm:"column:distribution_gt500_30d" json:"distribution_gt500_30d"`
	Distribution200to500_30d          int     `gorm:"column:distribution_200to500_30d" json:"distribution_200to500_30d"`
	Distribution0to200_30d            int     `gorm:"column:distribution_0to200_30d" json:"distribution_0to200_30d"`
	DistributionN50to0_30d            int     `gorm:"column:distribution_n50to0_30d" json:"distribution_n50to0_30d"`
	DistributionLt50_30d              int     `gorm:"column:distribution_lt50_30d" json:"distribution_lt50_30d"`
	DistributionGt500Percentage30d    float64 `gorm:"column:distribution_gt500_percentage_30d;type:decimal(5,4)" json:"distribution_gt500_percentage_30d"`
	Distribution200to500Percentage30d float64 `gorm:"column:distribution_200to500_percentage_30d;type:decimal(5,4)" json:"distribution_200to500_percentage_30d"`
	Distribution0to200Percentage30d   float64 `gorm:"column:distribution_0to200_percentage_30d;type:decimal(5,4)" json:"distribution_0to200_percentage_30d"`
	DistributionN50to0Percentage30d   float64 `gorm:"column:distribution_n50to0_percentage_30d;type:decimal(5,4)" json:"distribution_n50to0_percentage_30d"`
	DistributionLt50Percentage30d     float64 `gorm:"column:distribution_lt50_percentage_30d;type:decimal(5,4)" json:"distribution_lt50_percentage_30d"`

	// 收益分布数据 - 7天
	DistributionGt500_7d             int     `gorm:"column:distribution_gt500_7d" json:"distribution_gt500_7d"`
	Distribution200to500_7d          int     `gorm:"column:distribution_200to500_7d" json:"distribution_200to500_7d"`
	Distribution0to200_7d            int     `gorm:"column:distribution_0to200_7d" json:"distribution_0to200_7d"`
	DistributionN50to0_7d            int     `gorm:"column:distribution_n50to0_7d" json:"distribution_n50to0_7d"`
	DistributionLt50_7d              int     `gorm:"column:distribution_lt50_7d" json:"distribution_lt50_7d"`
	DistributionGt500Percentage7d    float64 `gorm:"column:distribution_gt500_percentage_7d;type:decimal(5,4)" json:"distribution_gt500_percentage_7d"`
	Distribution200to500Percentage7d float64 `gorm:"column:distribution_200to500_percentage_7d;type:decimal(5,4)" json:"distribution_200to500_percentage_7d"`
	Distribution0to200Percentage7d   float64 `gorm:"column:distribution_0to200_percentage_7d;type:decimal(5,4)" json:"distribution_0to200_percentage_7d"`
	DistributionN50to0Percentage7d   float64 `gorm:"column:distribution_n50to0_percentage_7d;type:decimal(5,4)" json:"distribution_n50to0_percentage_7d"`
	DistributionLt50Percentage7d     float64 `gorm:"column:distribution_lt50_percentage_7d;type:decimal(5,4)" json:"distribution_lt50_percentage_7d"`

	// 时间和状态
	LastTransactionTime int64 `gorm:"column:last_transaction_time" json:"last_transaction_time"` // blocktime
	IsActive            bool  `gorm:"column:is_active;type:boolean" json:"is_active"`

	UpdatedAt time.Time `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"updated_at"`
	CreatedAt time.Time `gorm:"column:created_at;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"created_at"`
}

func (w *WalletSummary) TableName() string {
	return "dex_query_v1.t_smart_wallet"
}
