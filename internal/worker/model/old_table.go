package model

import "time"

// OldWalletHolding 钱包持仓信息
type OldWalletHolding struct {
	ID                  int       `gorm:"primaryKey;autoIncrement" json:"id"`
	WalletAddress       string    `gorm:"column:wallet_address;type:varchar(255);not null;index" json:"wallet_address"`
	TokenAddress        string    `gorm:"column:token_address;type:varchar(255);not null;index" json:"token_address"`
	TokenIcon           string    `gorm:"column:token_icon;type:varchar(255)" json:"token_icon"`
	TokenName           string    `gorm:"column:token_name;type:varchar(255)" json:"token_name"`
	Chain               string    `gorm:"column:chain;type:varchar(50);not null;default:'Unknown'" json:"chain"`
	Amount              float64   `gorm:"column:amount;type:decimal(36,18);not null;default:0" json:"amount"`
	Value               float64   `gorm:"column:value;type:decimal(36,18);not null;default:0" json:"value"`
	ValueUSDT           float64   `gorm:"column:value_usdt;type:decimal(36,18);not null;default:0" json:"value_usdt"`
	UnrealizedProfits   float64   `gorm:"column:unrealized_profits;type:decimal(36,18);not null;default:0" json:"unrealized_profits"`
	PNL                 float64   `gorm:"column:pnl;type:decimal(36,18);not null;default:0" json:"pnl"`
	PNLPercentage       float64   `gorm:"column:pnl_percentage;type:decimal(10,4);not null;default:0" json:"pnl_percentage"`
	AvgPrice            float64   `gorm:"column:avg_price;type:decimal(36,18);not null;default:0" json:"avg_price"`
	MarketCap           float64   `gorm:"column:marketcap;type:decimal(36,18);not null;default:0" json:"marketcap"`
	IsCleared           bool      `gorm:"column:is_cleared;type:boolean;not null;default:false" json:"is_cleared"`
	CumulativeCost      float64   `gorm:"column:cumulative_cost;type:decimal(36,18);not null;default:0" json:"cumulative_cost"`
	CumulativeProfit    float64   `gorm:"column:cumulative_profit;type:decimal(36,18);not null;default:0" json:"cumulative_profit"`
	LastTransactionTime int64     `gorm:"column:last_transaction_time" json:"last_transaction_time"`
	Time                time.Time `gorm:"column:time;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"time"`
}

func (w *OldWalletHolding) TableName() string {
	return "dex_query_v1.wallet_holding"
}

// OldWalletTransaction 钱包交易记录
type OldWalletTransaction struct {
	ID                       int       `gorm:"primaryKey" json:"id"`
	WalletAddress            string    `gorm:"column:wallet_address;type:varchar(100);not null;index" json:"wallet_address"`
	WalletBalance            float64   `gorm:"column:wallet_balance;type:decimal(36,18);not null" json:"wallet_balance"`
	TokenAddress             string    `gorm:"column:token_address;type:varchar(100);not null;index" json:"token_address"`
	TokenIcon                string    `gorm:"column:token_icon;type:text" json:"token_icon"`
	TokenName                string    `gorm:"column:token_name;type:varchar(100)" json:"token_name"`
	Price                    float64   `gorm:"column:price;type:decimal(36,18)" json:"price"`
	Amount                   float64   `gorm:"column:amount;type:decimal(36,18);not null" json:"amount"`
	MarketCap                float64   `gorm:"column:marketcap;type:decimal(36,18)" json:"marketcap"`
	Value                    float64   `gorm:"column:value;type:decimal(36,18)" json:"value"`
	HoldingPercentage        float64   `gorm:"column:holding_percentage;type:decimal(10,4)" json:"holding_percentage"`
	Chain                    string    `gorm:"column:chain;type:varchar(50);not null" json:"chain"`
	ChainID                  uint64    `gorm:"column:chain_id;type:int;not null;default:501" json:"chain_id"`
	RealizedProfit           float64   `gorm:"column:realized_profit;type:decimal(36,18)" json:"realized_profit"`
	RealizedProfitPercentage float64   `gorm:"column:realized_profit_percentage;type:decimal(10,4)" json:"realized_profit_percentage"`
	TransactionType          string    `gorm:"column:transaction_type;type:varchar(10);not null" json:"transaction_type"` // build, buy, sell, clean
	TransactionTime          int64     `gorm:"column:transaction_time;not null" json:"transaction_time"`
	Time                     time.Time `gorm:"column:time;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"time"`
	Signature                string    `gorm:"column:signature;type:varchar(100);not null;uniqueIndex" json:"signature"`
	FromTokenAddress         string    `gorm:"column:from_token_address;type:varchar(100);not null" json:"from_token_address"`
	FromTokenSymbol          string    `gorm:"column:from_token_symbol;type:varchar(100)" json:"from_token_symbol"`
	FromTokenAmount          float64   `gorm:"column:from_token_amount;type:decimal(36,18);not null" json:"from_token_amount"`
	DestTokenAddress         string    `gorm:"column:dest_token_address;type:varchar(100);not null" json:"dest_token_address"`
	DestTokenSymbol          string    `gorm:"column:dest_token_symbol;type:varchar(100)" json:"dest_token_symbol"`
	DestTokenAmount          float64   `gorm:"column:dest_token_amount;type:decimal(36,18);not null" json:"dest_token_amount"`
}

func (w *OldWalletTransaction) TableName() string {
	return "dex_query_v1.wallet_transaction"
}

// OldWalletSummary 钱包摘要信息
type OldWalletSummary struct {
	ID              int     `gorm:"primaryKey" json:"id"`
	WalletAddress   string  `gorm:"column:wallet_address;type:varchar(512);not null;uniqueIndex" json:"wallet_address"`
	Balance         float64 `gorm:"column:balance;type:decimal(20,8)" json:"balance"`
	BalanceUSD      float64 `gorm:"column:balance_usd;type:decimal(20,8)" json:"balance_usd"`
	Chain           string  `gorm:"column:chain;type:varchar(50);not null" json:"chain"`
	ChainID         uint64  `gorm:"column:chain_id;type:int" json:"chain_id"`
	Tag             string  `gorm:"column:tag;type:varchar(50)" json:"tag"`
	TwitterName     string  `gorm:"column:twitter_name;type:varchar(50)" json:"twitter_name"`
	TwitterUsername string  `gorm:"column:twitter_username;type:varchar(50)" json:"twitter_username"`
	IsSmartWallet   bool    `gorm:"column:is_smart_wallet;type:boolean" json:"is_smart_wallet"`
	WalletType      int     `gorm:"column:wallet_type;type:int;default:0" json:"wallet_type"` // 0:一般聪明钱，1:pump聪明钱，2:moonshot聪明钱
	AssetMultiple   float64 `gorm:"column:asset_multiple;type:decimal(10,1)" json:"asset_multiple"`
	TokenList       string  `gorm:"column:token_list;type:varchar(512)" json:"token_list"`

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
	PNLPic7d            string  `gorm:"column:pnl_pic_7d;type:text" json:"pnl_pic_7d"`
	UnrealizedProfit7d  float64 `gorm:"column:unrealized_profit_7d;type:decimal(20,8)" json:"unrealized_profit_7d"`
	TotalCost7d         float64 `gorm:"column:total_cost_7d;type:decimal(20,8)" json:"total_cost_7d"`
	AvgRealizedProfit7d float64 `gorm:"column:avg_realized_profit_7d;type:decimal(20,8)" json:"avg_realized_profit_7d"`

	// 盈亏数据 - 1天
	PNL1d               float64 `gorm:"column:pnl_1d;type:decimal(20,8)" json:"pnl_1d"`
	PNLPercentage1d     float64 `gorm:"column:pnl_percentage_1d;type:decimal(8,4)" json:"pnl_percentage_1d"`
	PNLPic1d            string  `gorm:"column:pnl_pic_1d;type:text" json:"pnl_pic_1d"`
	UnrealizedProfit1d  float64 `gorm:"column:unrealized_profit_1d;type:decimal(20,8)" json:"unrealized_profit_1d"`
	TotalCost1d         float64 `gorm:"column:total_cost_1d;type:decimal(20,8)" json:"total_cost_1d"`
	AvgRealizedProfit1d float64 `gorm:"column:avg_realized_profit_1d;type:decimal(20,8)" json:"avg_realized_profit_1d"`

	// 收益分布数据 - 30天
	DistributionGt500_30d             int     `gorm:"column:distribution_gt500_30d" json:"distribution_gt500_30d"`
	Distribution200to500_30d          int     `gorm:"column:distribution_200to500_30d" json:"distribution_200to500_30d"`
	Distribution0to200_30d            int     `gorm:"column:distribution_0to200_30d" json:"distribution_0to200_30d"`
	Distribution0to50_30d             int     `gorm:"column:distribution_0to50_30d" json:"distribution_0to50_30d"`
	DistributionLt50_30d              int     `gorm:"column:distribution_lt50_30d" json:"distribution_lt50_30d"`
	DistributionGt500Percentage30d    float64 `gorm:"column:distribution_gt500_percentage_30d;type:decimal(5,4)" json:"distribution_gt500_percentage_30d"`
	Distribution200to500Percentage30d float64 `gorm:"column:distribution_200to500_percentage_30d;type:decimal(5,4)" json:"distribution_200to500_percentage_30d"`
	Distribution0to200Percentage30d   float64 `gorm:"column:distribution_0to200_percentage_30d;type:decimal(5,4)" json:"distribution_0to200_percentage_30d"`
	Distribution0to50Percentage30d    float64 `gorm:"column:distribution_0to50_percentage_30d;type:decimal(5,4)" json:"distribution_0to50_percentage_30d"`
	DistributionLt50Percentage30d     float64 `gorm:"column:distribution_lt50_percentage_30d;type:decimal(5,4)" json:"distribution_lt50_percentage_30d"`

	// 收益分布数据 - 7天
	DistributionGt500_7d             int     `gorm:"column:distribution_gt500_7d" json:"distribution_gt500_7d"`
	Distribution200to500_7d          int     `gorm:"column:distribution_200to500_7d" json:"distribution_200to500_7d"`
	Distribution0to200_7d            int     `gorm:"column:distribution_0to200_7d" json:"distribution_0to200_7d"`
	Distribution0to50_7d             int     `gorm:"column:distribution_0to50_7d" json:"distribution_0to50_7d"`
	DistributionLt50_7d              int     `gorm:"column:distribution_lt50_7d" json:"distribution_lt50_7d"`
	DistributionGt500Percentage7d    float64 `gorm:"column:distribution_gt500_percentage_7d;type:decimal(5,4)" json:"distribution_gt500_percentage_7d"`
	Distribution200to500Percentage7d float64 `gorm:"column:distribution_200to500_percentage_7d;type:decimal(5,4)" json:"distribution_200to500_percentage_7d"`
	Distribution0to200Percentage7d   float64 `gorm:"column:distribution_0to200_percentage_7d;type:decimal(5,4)" json:"distribution_0to200_percentage_7d"`
	Distribution0to50Percentage7d    float64 `gorm:"column:distribution_0to50_percentage_7d;type:decimal(5,4)" json:"distribution_0to50_percentage_7d"`
	DistributionLt50Percentage7d     float64 `gorm:"column:distribution_lt50_percentage_7d;type:decimal(5,4)" json:"distribution_lt50_percentage_7d"`

	// 时间和状态
	UpdateTime          time.Time `gorm:"column:update_time;type:timestamp;default:CURRENT_TIMESTAMP" json:"update_time"`
	LastTransactionTime int64     `gorm:"column:last_transaction_time" json:"last_transaction_time"`
	IsActive            bool      `gorm:"column:is_active;type:boolean" json:"is_active"`
}

func (w *OldWalletSummary) TableName() string {
	return "dex_query_v1.wallet"
}

func (w *WalletSummary) OldWalletSummary() string {
	return "dex_query_v1.wallet"
}

// OldWalletTokenState 钱包代币状态
type OldWalletTokenState struct {
	ID            int    `gorm:"primaryKey;autoIncrement" json:"id"`
	WalletAddress string `gorm:"column:wallet_address;type:varchar(100);not null;index:idx_wallet_token_chain,unique" json:"wallet_address"`
	TokenAddress  string `gorm:"column:token_address;type:varchar(100);not null;index:idx_wallet_token_chain,unique" json:"token_address"`
	Chain         string `gorm:"column:chain;type:varchar(50);not null;index:idx_wallet_token_chain,unique" json:"chain"`
	ChainID       uint64 `gorm:"column:chain_id;not null" json:"chain_id"`

	// 当前持仓状态
	CurrentAmount      float64 `gorm:"column:current_amount;type:decimal(36,18);not null;default:0" json:"current_amount"`
	CurrentTotalCost   float64 `gorm:"column:current_total_cost;type:decimal(36,18);not null;default:0" json:"current_total_cost"`
	CurrentAvgBuyPrice float64 `gorm:"column:current_avg_buy_price;type:decimal(36,18);not null;default:0" json:"current_avg_buy_price"`
	PositionOpenedAt   *int64  `gorm:"column:position_opened_at" json:"position_opened_at"`

	// 历史累计状态
	HistoricalBuyAmount   float64 `gorm:"column:historical_buy_amount;type:decimal(36,18);not null;default:0" json:"historical_buy_amount"`
	HistoricalSellAmount  float64 `gorm:"column:historical_sell_amount;type:decimal(36,18);not null;default:0" json:"historical_sell_amount"`
	HistoricalBuyCost     float64 `gorm:"column:historical_buy_cost;type:decimal(36,18);not null;default:0" json:"historical_buy_cost"`
	HistoricalSellValue   float64 `gorm:"column:historical_sell_value;type:decimal(36,18);not null;default:0" json:"historical_sell_value"`
	HistoricalRealizedPNL float64 `gorm:"column:historical_realized_pnl;type:decimal(36,18);not null;default:0" json:"historical_realized_pnl"`
	HistoricalBuyCount    int     `gorm:"column:historical_buy_count;not null;default:0" json:"historical_buy_count"`
	HistoricalSellCount   int     `gorm:"column:historical_sell_count;not null;default:0" json:"historical_sell_count"`

	// 元数据
	LastTransactionTime int64     `gorm:"column:last_transaction_time" json:"last_transaction_time"`
	UpdatedAt           time.Time `gorm:"column:updated_at;type:timestamp;not null;default:CURRENT_TIMESTAMP" json:"updated_at"`
}

func (w *OldWalletTokenState) TableName() string {
	return "dex_query_v1.wallet_token_state"
}
