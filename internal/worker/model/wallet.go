package model

import (
	"fmt"
	"strings"
	"time"
	"web3-smart/pkg/utils"
)

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
	AvgCost30d float64 `gorm:"column:avg_cost_30d;type:decimal(20,8)" json:"avg_cost_30d"`
	BuyNum30d  int     `gorm:"column:buy_num_30d" json:"buy_num_30d"`
	SellNum30d int     `gorm:"column:sell_num_30d" json:"sell_num_30d"`
	WinRate30d float64 `gorm:"column:win_rate_30d;type:decimal(5,4)" json:"win_rate_30d"`

	// 交易数据 - 7天
	AvgCost7d float64 `gorm:"column:avg_cost_7d;type:decimal(20,8)" json:"avg_cost_7d"`
	BuyNum7d  int     `gorm:"column:buy_num_7d" json:"buy_num_7d"`
	SellNum7d int     `gorm:"column:sell_num_7d" json:"sell_num_7d"`
	WinRate7d float64 `gorm:"column:win_rate_7d;type:decimal(5,4)" json:"win_rate_7d"`

	// 交易数据 - 1天
	AvgCost1d float64 `gorm:"column:avg_cost_1d;type:decimal(20,8)" json:"avg_cost_1d"`
	BuyNum1d  int     `gorm:"column:buy_num_1d" json:"buy_num_1d"`
	SellNum1d int     `gorm:"column:sell_num_1d" json:"sell_num_1d"`
	WinRate1d float64 `gorm:"column:win_rate_1d;type:decimal(5,4)" json:"win_rate_1d"`

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

// WalletStats 钱包统计信息
type WalletStats struct {
	TotalWallets    int64   `json:"total_wallets"`
	ActiveWallets   int64   `json:"active_wallets"`
	AvgPNL30d       float64 `json:"avg_pnl_30d"`
	AvgWinRate30d   float64 `json:"avg_win_rate_30d"`
	TopPerformers   int64   `json:"top_performers"`
	SmartMoneyCount int64   `json:"smart_money_count"`
}

func (w *WalletSummary) UpdateIndicatorStatistics(trade *TradeEvent, updatedHolding *WalletHolding, txType string) {
	// 检查头像是否为空，hash(wallet addr) % 1000 作为头像
	// 格式: https://uploads.bydfi.in/moonx/avatar/289.svg
	if strings.TrimSpace(w.Avatar) == "" {
		avatar := fmt.Sprintf("https://uploads.bydfi.in/moonx/avatar/%d.svg", utils.GetHashBucket(w.WalletAddress, 1000))
		w.Avatar = avatar
	}

	// 滚动替换TokenList，只保留最近3个
	w.TokenList = w.updateTokenList(updatedHolding.TokenAddress)

	// 更新交易数据
	switch txType {
	case TX_TYPE_BUILD, TX_TYPE_BUY:
		w.BuyNum30d++
		w.BuyNum7d++
		w.BuyNum1d++

		// 更新总花费
		w.TotalCost30d += trade.Event.VolumeUsd
		w.TotalCost7d += trade.Event.VolumeUsd
		w.TotalCost1d += trade.Event.VolumeUsd
	case TX_TYPE_SELL, TX_TYPE_CLEAN:
		w.SellNum30d++
		w.SellNum7d++
		w.SellNum1d++

		// 更新盈亏数据
		curTxPnl := (trade.Event.Price - updatedHolding.AvgPrice) * trade.Event.FromTokenAmount
		w.PNL30d += curTxPnl
		w.PNL7d += curTxPnl
		w.PNL1d += curTxPnl
		curTxPercentage := (trade.Event.Price/updatedHolding.AvgPrice - 1) * 100
		w.PNLPercentage30d += curTxPercentage
		w.PNLPercentage7d += curTxPercentage
		w.PNLPercentage1d += curTxPercentage

		// 更新盈亏分布数据
		if curTxPercentage > 500 {
			w.DistributionGt500_30d++
			w.DistributionGt500_7d++
		} else if curTxPercentage > 200 {
			w.Distribution200to500_30d++
			w.Distribution200to500_7d++
		} else if curTxPercentage > 0 {
			w.Distribution0to200_30d++
			w.Distribution0to200_7d++
		} else if curTxPercentage > -50 {
			w.DistributionN50to0_30d++
			w.DistributionN50to0_7d++
		} else {
			w.DistributionLt50_30d++
			w.DistributionLt50_7d++
		}

		// 更新盈亏分布数据百分比
		count := w.DistributionGt500_30d + w.Distribution200to500_30d + w.Distribution0to200_30d + w.DistributionN50to0_30d + w.DistributionLt50_30d
		if count > 0 {
			w.DistributionGt500Percentage30d = float64(w.DistributionGt500_30d) / float64(count)
			w.Distribution200to500Percentage30d = float64(w.Distribution200to500_30d) / float64(count)
			w.Distribution0to200Percentage30d = float64(w.Distribution0to200_30d) / float64(count)
			w.DistributionN50to0Percentage30d = float64(w.DistributionN50to0_30d) / float64(count)
			w.DistributionLt50Percentage30d = float64(w.DistributionLt50_30d) / float64(count)
		}
		count = w.DistributionGt500_7d + w.Distribution200to500_7d + w.Distribution0to200_7d + w.DistributionN50to0_7d + w.DistributionLt50_7d
		if count > 0 {
			w.DistributionGt500Percentage7d = float64(w.DistributionGt500_7d) / float64(count)
			w.Distribution200to500Percentage7d = float64(w.Distribution200to500_7d) / float64(count)
			w.Distribution0to200Percentage7d = float64(w.Distribution0to200_7d) / float64(count)
			w.DistributionN50to0Percentage7d = float64(w.DistributionN50to0_7d) / float64(count)
			w.DistributionLt50Percentage7d = float64(w.DistributionLt50_7d) / float64(count)
		}
	}

	w.AssetMultiple = w.PNLPercentage30d / 100 // 资产盈亏倍数

	w.LastTransactionTime = updatedHolding.LastTransactionTime / 1000
	w.IsActive = true
	w.UpdatedAt = time.Now()
}

func (w *WalletSummary) updateTokenList(tokenAddress string) string {
	tokenList := strings.Split(w.TokenList, ",")
	if len(tokenList) == 0 {
		return tokenAddress
	}

	// 如果tokenList中包含tokenAddress，则往后排
	for i, token := range tokenList {
		if token == tokenAddress && i < len(tokenList)-1 {
			tokenList = append(tokenList[:i], tokenList[i+1:]...)
			tokenList = append(tokenList, tokenAddress)
			return strings.Join(tokenList, ",")
		}
	}

	// 否则，直接添加到末尾，去掉第一个，最多保留3个
	if len(tokenList) < 3 {
		tokenList = append(tokenList, tokenAddress)
	} else {
		tokenList = append(tokenList[1:], tokenAddress)
	}

	return strings.Join(tokenList, ",")
}

func (w *WalletSummary) ToESIndex() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			"_routing": map[string]interface{}{
				"required": true,
			},
			"properties": map[string]interface{}{
				"wallet_address":       map[string]interface{}{"type": "keyword"},
				"wallet_hash":          map[string]interface{}{"type": "keyword"},
				"avatar":               map[string]interface{}{"type": "keyword", "index": false},
				"balance":              map[string]interface{}{"type": "double"},
				"balance_usd":          map[string]interface{}{"type": "double"},
				"chain_id":             map[string]interface{}{"type": "long"},
				"tags":                 map[string]interface{}{"type": "keyword"},
				"wallet_tags_combined": map[string]interface{}{"type": "keyword"},
				"twitter_name": map[string]interface{}{
					"type":     "text",
					"analyzer": "standard",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"type":         "keyword",
							"ignore_above": 256,
						},
					},
				},
				"twitter_username": map[string]interface{}{
					"type":     "text",
					"analyzer": "standard",
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"type":         "keyword",
							"ignore_above": 256,
						},
					},
				},
				"wallet_type":    map[string]interface{}{"type": "integer"},
				"asset_multiple": map[string]interface{}{"type": "double"},
				"token_list":     map[string]interface{}{"type": "keyword"},

				// 交易数据 - 30天
				"avg_cost_30d": map[string]interface{}{"type": "double"},
				"buy_num_30d":  map[string]interface{}{"type": "integer"},
				"sell_num_30d": map[string]interface{}{"type": "integer"},
				"win_rate_30d": map[string]interface{}{"type": "double"},

				// 交易数据 - 7天
				"avg_cost_7d": map[string]interface{}{"type": "double"},
				"buy_num_7d":  map[string]interface{}{"type": "integer"},
				"sell_num_7d": map[string]interface{}{"type": "integer"},
				"win_rate_7d": map[string]interface{}{"type": "double"},

				// 交易数据 - 1天
				"avg_cost_1d": map[string]interface{}{"type": "double"},
				"buy_num_1d":  map[string]interface{}{"type": "integer"},
				"sell_num_1d": map[string]interface{}{"type": "integer"},
				"win_rate_1d": map[string]interface{}{"type": "double"},

				// 盈亏数据 - 30天
				"pnl_30d":                 map[string]interface{}{"type": "double"},
				"pnl_percentage_30d":      map[string]interface{}{"type": "double"},
				"pnl_pic_30d":             map[string]interface{}{"type": "text", "index": false},
				"unrealized_profit_30d":   map[string]interface{}{"type": "double"},
				"total_cost_30d":          map[string]interface{}{"type": "double"},
				"avg_realized_profit_30d": map[string]interface{}{"type": "double"},

				// 盈亏数据 - 7天
				"pnl_7d":                 map[string]interface{}{"type": "double"},
				"pnl_percentage_7d":      map[string]interface{}{"type": "double"},
				"unrealized_profit_7d":   map[string]interface{}{"type": "double"},
				"total_cost_7d":          map[string]interface{}{"type": "double"},
				"avg_realized_profit_7d": map[string]interface{}{"type": "double"},

				// 盈亏数据 - 1天
				"pnl_1d":                 map[string]interface{}{"type": "double"},
				"pnl_percentage_1d":      map[string]interface{}{"type": "double"},
				"unrealized_profit_1d":   map[string]interface{}{"type": "double"},
				"total_cost_1d":          map[string]interface{}{"type": "double"},
				"avg_realized_profit_1d": map[string]interface{}{"type": "double"},

				// 收益分布数据 - 30天
				"distribution_gt500_30d":               map[string]interface{}{"type": "integer"},
				"distribution_200to500_30d":            map[string]interface{}{"type": "integer"},
				"distribution_0to200_30d":              map[string]interface{}{"type": "integer"},
				"distribution_n50to0_30d":              map[string]interface{}{"type": "integer"},
				"distribution_lt50_30d":                map[string]interface{}{"type": "integer"},
				"distribution_gt500_percentage_30d":    map[string]interface{}{"type": "double"},
				"distribution_200to500_percentage_30d": map[string]interface{}{"type": "double"},
				"distribution_0to200_percentage_30d":   map[string]interface{}{"type": "double"},
				"distribution_n50to0_percentage_30d":   map[string]interface{}{"type": "double"},
				"distribution_lt50_percentage_30d":     map[string]interface{}{"type": "double"},

				// 收益分布数据 - 7天
				"distribution_gt500_7d":               map[string]interface{}{"type": "integer"},
				"distribution_200to500_7d":            map[string]interface{}{"type": "integer"},
				"distribution_0to200_7d":              map[string]interface{}{"type": "integer"},
				"distribution_n50to0_7d":              map[string]interface{}{"type": "integer"},
				"distribution_lt50_7d":                map[string]interface{}{"type": "integer"},
				"distribution_gt500_percentage_7d":    map[string]interface{}{"type": "double"},
				"distribution_200to500_percentage_7d": map[string]interface{}{"type": "double"},
				"distribution_0to200_percentage_7d":   map[string]interface{}{"type": "double"},
				"distribution_n50to0_percentage_7d":   map[string]interface{}{"type": "double"},
				"distribution_lt50_percentage_7d":     map[string]interface{}{"type": "double"},

				// 时间和状态
				"last_transaction_time": map[string]interface{}{"type": "date"},
				"is_active":             map[string]interface{}{"type": "boolean"},
				"updated_at":            map[string]interface{}{"type": "date"},
				"created_at":            map[string]interface{}{"type": "date"},
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
