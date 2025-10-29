package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"web3-smart/pkg/utils"

	"github.com/lib/pq"
	"github.com/shopspring/decimal"
)

// TokenInfo token基本信息
type TokenInfo struct {
	TokenAddress string `json:"token_address"`
	TokenIcon    string `json:"token_icon"`
	TokenName    string `json:"token_name"`
}

// TokenList token列表，实现JSONB序列化
type TokenList []TokenInfo

// Value 实现 driver.Valuer 接口，用于写入数据库
func (t TokenList) Value() (driver.Value, error) {
	if t == nil {
		return nil, nil
	}
	return json.Marshal(t)
}

// Scan 实现 sql.Scanner 接口，用于从数据库读取
func (t *TokenList) Scan(value interface{}) error {
	if value == nil {
		*t = TokenList{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("failed to unmarshal JSONB value: %v", value)
	}

	return json.Unmarshal(bytes, t)
}

// WalletSummary 钱包摘要信息
type WalletSummary struct {
	ID              int64           `gorm:"primaryKey" json:"id"`
	WalletAddress   string          `gorm:"column:wallet_address;type:varchar(512);not null;uniqueIndex" json:"wallet_address"`
	Avatar          string          `gorm:"column:avatar;type:varchar(512);not null" json:"avatar"` // 头像
	Balance         decimal.Decimal `gorm:"column:balance;type:decimal(50,20);not null;default:0" json:"balance"`
	BalanceUSD      decimal.Decimal `gorm:"column:balance_usd;type:decimal(50,20);not null;default:0" json:"balance_usd"`
	ChainID         uint64          `gorm:"column:chain_id;type:int" json:"chain_id"`
	Tags            pq.StringArray  `gorm:"column:tags;type:varchar(50)[]" json:"tags"` // smart money
	TwitterName     string          `gorm:"column:twitter_name;type:varchar(50)" json:"twitter_name"`
	TwitterUsername string          `gorm:"column:twitter_username;type:varchar(50)" json:"twitter_username"`
	WalletType      int             `gorm:"column:wallet_type;type:int;default:0" json:"wallet_type"`                           // 0:一般聪明钱，1:pump聪明钱，2:moonshot聪明钱
	AssetMultiple   decimal.Decimal `gorm:"column:asset_multiple;type:decimal(50,20);not null;default:0" json:"asset_multiple"` // 盈亏资产倍数
	TokenList       TokenList       `gorm:"column:token_list;type:jsonb" json:"token_list"`                                     // 最近交易过的token(3个)

	// 交易数据 - 30天
	AvgCost30d decimal.Decimal `gorm:"column:avg_cost_30d;type:decimal(50,20);not null;default:0" json:"avg_cost_30d"`
	BuyNum30d  int             `gorm:"column:buy_num_30d" json:"buy_num_30d"`
	SellNum30d int             `gorm:"column:sell_num_30d" json:"sell_num_30d"`
	WinRate30d decimal.Decimal `gorm:"column:win_rate_30d;type:decimal(50,20);not null;default:0" json:"win_rate_30d"`

	// 交易数据 - 7天
	AvgCost7d decimal.Decimal `gorm:"column:avg_cost_7d;type:decimal(50,20);not null;default:0" json:"avg_cost_7d"`
	BuyNum7d  int             `gorm:"column:buy_num_7d" json:"buy_num_7d"`
	SellNum7d int             `gorm:"column:sell_num_7d" json:"sell_num_7d"`
	WinRate7d decimal.Decimal `gorm:"column:win_rate_7d;type:decimal(50,20);not null;default:0" json:"win_rate_7d"`

	// 交易数据 - 1天
	AvgCost1d decimal.Decimal `gorm:"column:avg_cost_1d;type:decimal(50,20);not null;default:0" json:"avg_cost_1d"`
	BuyNum1d  int             `gorm:"column:buy_num_1d" json:"buy_num_1d"`
	SellNum1d int             `gorm:"column:sell_num_1d" json:"sell_num_1d"`
	WinRate1d decimal.Decimal `gorm:"column:win_rate_1d;type:decimal(50,20);not null;default:0" json:"win_rate_1d"`

	// 盈亏数据 - 30天
	PNL30d               decimal.Decimal `gorm:"column:pnl_30d;type:decimal(50,20);not null;default:0" json:"pnl_30d"`
	PNLPercentage30d     decimal.Decimal `gorm:"column:pnl_percentage_30d;type:decimal(50,20);not null;default:0" json:"pnl_percentage_30d"`
	PNLPic30d            string          `gorm:"column:pnl_pic_30d;type:text" json:"pnl_pic_30d"`
	UnrealizedProfit30d  decimal.Decimal `gorm:"column:unrealized_profit_30d;type:decimal(50,20);not null;default:0" json:"unrealized_profit_30d"`
	TotalCost30d         decimal.Decimal `gorm:"column:total_cost_30d;type:decimal(50,20);not null;default:0" json:"total_cost_30d"`
	AvgRealizedProfit30d decimal.Decimal `gorm:"column:avg_realized_profit_30d;type:decimal(50,20);not null;default:0" json:"avg_realized_profit_30d"`

	// 盈亏数据 - 7天
	PNL7d               decimal.Decimal `gorm:"column:pnl_7d;type:decimal(50,20);not null;default:0" json:"pnl_7d"`
	PNLPercentage7d     decimal.Decimal `gorm:"column:pnl_percentage_7d;type:decimal(50,20);not null;default:0" json:"pnl_percentage_7d"`
	UnrealizedProfit7d  decimal.Decimal `gorm:"column:unrealized_profit_7d;type:decimal(50,20);not null;default:0" json:"unrealized_profit_7d"`
	TotalCost7d         decimal.Decimal `gorm:"column:total_cost_7d;type:decimal(50,20);not null;default:0" json:"total_cost_7d"`
	AvgRealizedProfit7d decimal.Decimal `gorm:"column:avg_realized_profit_7d;type:decimal(50,20);not null;default:0" json:"avg_realized_profit_7d"`

	// 盈亏数据 - 1天
	PNL1d               decimal.Decimal `gorm:"column:pnl_1d;type:decimal(50,20);not null;default:0" json:"pnl_1d"`
	PNLPercentage1d     decimal.Decimal `gorm:"column:pnl_percentage_1d;type:decimal(50,20);not null;default:0" json:"pnl_percentage_1d"`
	UnrealizedProfit1d  decimal.Decimal `gorm:"column:unrealized_profit_1d;type:decimal(50,20);not null;default:0" json:"unrealized_profit_1d"`
	TotalCost1d         decimal.Decimal `gorm:"column:total_cost_1d;type:decimal(50,20);not null;default:0" json:"total_cost_1d"`
	AvgRealizedProfit1d decimal.Decimal `gorm:"column:avg_realized_profit_1d;type:decimal(50,20);not null;default:0" json:"avg_realized_profit_1d"`

	// 收益分布数据 - 30天
	DistributionGt500_30d             int             `gorm:"column:distribution_gt500_30d" json:"distribution_gt500_30d"`
	Distribution200to500_30d          int             `gorm:"column:distribution_200to500_30d" json:"distribution_200to500_30d"`
	Distribution0to200_30d            int             `gorm:"column:distribution_0to200_30d" json:"distribution_0to200_30d"`
	DistributionN50to0_30d            int             `gorm:"column:distribution_n50to0_30d" json:"distribution_n50to0_30d"`
	DistributionLt50_30d              int             `gorm:"column:distribution_lt50_30d" json:"distribution_lt50_30d"`
	DistributionGt500Percentage30d    decimal.Decimal `gorm:"column:distribution_gt500_percentage_30d;type:decimal(50,20);not null;default:0" json:"distribution_gt500_percentage_30d"`
	Distribution200to500Percentage30d decimal.Decimal `gorm:"column:distribution_200to500_percentage_30d;type:decimal(50,20);not null;default:0" json:"distribution_200to500_percentage_30d"`
	Distribution0to200Percentage30d   decimal.Decimal `gorm:"column:distribution_0to200_percentage_30d;type:decimal(50,20);not null;default:0" json:"distribution_0to200_percentage_30d"`
	DistributionN50to0Percentage30d   decimal.Decimal `gorm:"column:distribution_n50to0_percentage_30d;type:decimal(50,20);not null;default:0" json:"distribution_n50to0_percentage_30d"`
	DistributionLt50Percentage30d     decimal.Decimal `gorm:"column:distribution_lt50_percentage_30d;type:decimal(50,20);not null;default:0" json:"distribution_lt50_percentage_30d"`

	// 收益分布数据 - 7天
	DistributionGt500_7d             int             `gorm:"column:distribution_gt500_7d" json:"distribution_gt500_7d"`
	Distribution200to500_7d          int             `gorm:"column:distribution_200to500_7d" json:"distribution_200to500_7d"`
	Distribution0to200_7d            int             `gorm:"column:distribution_0to200_7d" json:"distribution_0to200_7d"`
	DistributionN50to0_7d            int             `gorm:"column:distribution_n50to0_7d" json:"distribution_n50to0_7d"`
	DistributionLt50_7d              int             `gorm:"column:distribution_lt50_7d" json:"distribution_lt50_7d"`
	DistributionGt500Percentage7d    decimal.Decimal `gorm:"column:distribution_gt500_percentage_7d;type:decimal(50,20);not null;default:0" json:"distribution_gt500_percentage_7d"`
	Distribution200to500Percentage7d decimal.Decimal `gorm:"column:distribution_200to500_percentage_7d;type:decimal(50,20);not null;default:0" json:"distribution_200to500_percentage_7d"`
	Distribution0to200Percentage7d   decimal.Decimal `gorm:"column:distribution_0to200_percentage_7d;type:decimal(50,20);not null;default:0" json:"distribution_0to200_percentage_7d"`
	DistributionN50to0Percentage7d   decimal.Decimal `gorm:"column:distribution_n50to0_percentage_7d;type:decimal(50,20);not null;default:0" json:"distribution_n50to0_percentage_7d"`
	DistributionLt50Percentage7d     decimal.Decimal `gorm:"column:distribution_lt50_percentage_7d;type:decimal(50,20);not null;default:0" json:"distribution_lt50_percentage_7d"`

	// 时间和状态
	LastTransactionTime int64 `gorm:"column:last_transaction_time" json:"last_transaction_time"` // blocktime
	IsActive            bool  `gorm:"column:is_active;type:boolean" json:"is_active"`

	UpdatedAt int64 `gorm:"column:updated_at;not null" json:"updated_at"` // 毫秒时间戳
	CreatedAt int64 `gorm:"column:created_at;not null" json:"created_at"` // 毫秒时间戳
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
	w.TokenList = w.updateTokenList(TokenInfo{
		TokenAddress: updatedHolding.TokenAddress,
		TokenIcon:    updatedHolding.TokenIcon,
		TokenName:    updatedHolding.TokenName,
	})

	// 更新交易数据
	switch txType {
	case TX_TYPE_BUILD, TX_TYPE_BUY:
		w.BuyNum30d++
		w.BuyNum7d++
		w.BuyNum1d++

		// 更新总花费
		volumeUsd := decimal.NewFromFloat(trade.Event.VolumeUsd)
		w.TotalCost30d = w.TotalCost30d.Add(volumeUsd)
		w.TotalCost7d = w.TotalCost7d.Add(volumeUsd)
		w.TotalCost1d = w.TotalCost1d.Add(volumeUsd)
	case TX_TYPE_SELL, TX_TYPE_CLEAN:
		w.SellNum30d++
		w.SellNum7d++
		w.SellNum1d++

		// 更新盈亏数据
		price := decimal.NewFromFloat(trade.Event.Price)
		fromTokenAmount := decimal.NewFromFloat(trade.Event.FromTokenAmount)
		curTxPnl := price.Sub(updatedHolding.AvgPrice).Mul(fromTokenAmount)
		w.PNL30d = w.PNL30d.Add(curTxPnl)
		w.PNL7d = w.PNL7d.Add(curTxPnl)
		w.PNL1d = w.PNL1d.Add(curTxPnl)

		var curTxPercentage decimal.Decimal
		if updatedHolding.AvgPrice.GreaterThan(decimal.Zero) {
			curTxPercentage = price.Div(updatedHolding.AvgPrice).Sub(decimal.NewFromInt(1)).Mul(decimal.NewFromInt(100))
		} else {
			curTxPercentage = decimal.Zero
		}
		w.PNLPercentage30d = w.PNLPercentage30d.Add(curTxPercentage)
		w.PNLPercentage30d = decimal.Max(decimal.NewFromFloat(-100), w.PNLPercentage30d) // 限制最大亏损在-100%
		w.PNLPercentage7d = w.PNLPercentage7d.Add(curTxPercentage)
		w.PNLPercentage7d = decimal.Max(decimal.NewFromFloat(-100), w.PNLPercentage7d)
		w.PNLPercentage1d = w.PNLPercentage1d.Add(curTxPercentage)
		w.PNLPercentage1d = decimal.Max(decimal.NewFromFloat(-100), w.PNLPercentage1d)

		// 更新盈亏分布数据
		if curTxPercentage.GreaterThan(decimal.NewFromInt(500)) {
			w.DistributionGt500_30d++
			w.DistributionGt500_7d++
		} else if curTxPercentage.GreaterThan(decimal.NewFromInt(200)) {
			w.Distribution200to500_30d++
			w.Distribution200to500_7d++
		} else if curTxPercentage.GreaterThan(decimal.Zero) {
			w.Distribution0to200_30d++
			w.Distribution0to200_7d++
		} else if curTxPercentage.GreaterThan(decimal.NewFromInt(-50)) {
			w.DistributionN50to0_30d++
			w.DistributionN50to0_7d++
		} else {
			w.DistributionLt50_30d++
			w.DistributionLt50_7d++
		}

		// 更新盈亏分布数据百分比
		count := w.DistributionGt500_30d + w.Distribution200to500_30d + w.Distribution0to200_30d + w.DistributionN50to0_30d + w.DistributionLt50_30d
		if count > 0 {
			countDecimal := decimal.NewFromInt(int64(count))
			w.DistributionGt500Percentage30d = decimal.NewFromInt(int64(w.DistributionGt500_30d)).Div(countDecimal)
			w.Distribution200to500Percentage30d = decimal.NewFromInt(int64(w.Distribution200to500_30d)).Div(countDecimal)
			w.Distribution0to200Percentage30d = decimal.NewFromInt(int64(w.Distribution0to200_30d)).Div(countDecimal)
			w.DistributionN50to0Percentage30d = decimal.NewFromInt(int64(w.DistributionN50to0_30d)).Div(countDecimal)
			w.DistributionLt50Percentage30d = decimal.NewFromInt(int64(w.DistributionLt50_30d)).Div(countDecimal)
		}
		count = w.DistributionGt500_7d + w.Distribution200to500_7d + w.Distribution0to200_7d + w.DistributionN50to0_7d + w.DistributionLt50_7d
		if count > 0 {
			countDecimal := decimal.NewFromInt(int64(count))
			w.DistributionGt500Percentage7d = decimal.NewFromInt(int64(w.DistributionGt500_7d)).Div(countDecimal)
			w.Distribution200to500Percentage7d = decimal.NewFromInt(int64(w.Distribution200to500_7d)).Div(countDecimal)
			w.Distribution0to200Percentage7d = decimal.NewFromInt(int64(w.Distribution0to200_7d)).Div(countDecimal)
			w.DistributionN50to0Percentage7d = decimal.NewFromInt(int64(w.DistributionN50to0_7d)).Div(countDecimal)
			w.DistributionLt50Percentage7d = decimal.NewFromInt(int64(w.DistributionLt50_7d)).Div(countDecimal)
		}
	}

	w.AssetMultiple = w.PNLPercentage30d.Div(decimal.NewFromInt(100)) // 资产盈亏倍数

	w.LastTransactionTime = updatedHolding.LastTransactionTime // 毫秒时间戳
	w.IsActive = true
	w.UpdatedAt = time.Now().UnixMilli()
}

func (w *WalletSummary) updateTokenList(tokenInfo TokenInfo) TokenList {
	// 初始化空的TokenList
	tokenList := w.TokenList
	if tokenList == nil {
		tokenList = TokenList{}
	}

	// 如果是空列表,直接返回只包含新token的列表
	if len(tokenList) == 0 {
		return TokenList{tokenInfo}
	}

	// 如果tokenList中包含该token，则更新其信息并移到最后
	for i, token := range tokenList {
		if token.TokenAddress == tokenInfo.TokenAddress {
			// 安全地移除当前位置的token
			if i < len(tokenList)-1 {
				tokenList = append(tokenList[:i], tokenList[i+1:]...)
			} else {
				tokenList = tokenList[:i]
			}
			// 添加到末尾（更新后的信息）
			tokenList = append(tokenList, tokenInfo)
			return tokenList
		}
	}

	// 否则，直接添加到末尾，去掉第一个，最多保留3个
	if len(tokenList) < 3 {
		tokenList = append(tokenList, tokenInfo)
	} else {
		tokenList = append(tokenList[1:], tokenInfo)
	}

	return tokenList
}

// ToESDocument converts WalletSummary to map with float64 values for ES indexing
func (w *WalletSummary) ToESDocument() map[string]interface{} {
	return map[string]interface{}{
		"id":                                   w.ID,
		"wallet_address":                       w.WalletAddress,
		"avatar":                               w.Avatar,
		"balance":                              w.Balance.InexactFloat64(),
		"balance_usd":                          w.BalanceUSD.InexactFloat64(),
		"chain_id":                             w.ChainID,
		"tags":                                 w.Tags,
		"twitter_name":                         w.TwitterName,
		"twitter_username":                     w.TwitterUsername,
		"wallet_type":                          w.WalletType,
		"asset_multiple":                       w.AssetMultiple.InexactFloat64(),
		"token_list":                           w.TokenList,
		"avg_cost_30d":                         w.AvgCost30d.InexactFloat64(),
		"total_num_30d":                        w.BuyNum30d + w.SellNum30d,
		"buy_num_30d":                          w.BuyNum30d,
		"sell_num_30d":                         w.SellNum30d,
		"win_rate_30d":                         w.WinRate30d.InexactFloat64(),
		"avg_cost_7d":                          w.AvgCost7d.InexactFloat64(),
		"total_num_7d":                         w.BuyNum7d + w.SellNum7d,
		"buy_num_7d":                           w.BuyNum7d,
		"sell_num_7d":                          w.SellNum7d,
		"win_rate_7d":                          w.WinRate7d.InexactFloat64(),
		"avg_cost_1d":                          w.AvgCost1d.InexactFloat64(),
		"total_num_1d":                         w.BuyNum1d + w.SellNum1d,
		"buy_num_1d":                           w.BuyNum1d,
		"sell_num_1d":                          w.SellNum1d,
		"win_rate_1d":                          w.WinRate1d.InexactFloat64(),
		"pnl_30d":                              w.PNL30d.InexactFloat64(),
		"pnl_percentage_30d":                   w.PNLPercentage30d.InexactFloat64(),
		"pnl_pic_30d":                          w.PNLPic30d,
		"unrealized_profit_30d":                w.UnrealizedProfit30d.InexactFloat64(),
		"total_cost_30d":                       w.TotalCost30d.InexactFloat64(),
		"avg_realized_profit_30d":              w.AvgRealizedProfit30d.InexactFloat64(),
		"pnl_7d":                               w.PNL7d.InexactFloat64(),
		"pnl_percentage_7d":                    w.PNLPercentage7d.InexactFloat64(),
		"unrealized_profit_7d":                 w.UnrealizedProfit7d.InexactFloat64(),
		"total_cost_7d":                        w.TotalCost7d.InexactFloat64(),
		"avg_realized_profit_7d":               w.AvgRealizedProfit7d.InexactFloat64(),
		"pnl_1d":                               w.PNL1d.InexactFloat64(),
		"pnl_percentage_1d":                    w.PNLPercentage1d.InexactFloat64(),
		"unrealized_profit_1d":                 w.UnrealizedProfit1d.InexactFloat64(),
		"total_cost_1d":                        w.TotalCost1d.InexactFloat64(),
		"avg_realized_profit_1d":               w.AvgRealizedProfit1d.InexactFloat64(),
		"distribution_gt500_30d":               w.DistributionGt500_30d,
		"distribution_200to500_30d":            w.Distribution200to500_30d,
		"distribution_0to200_30d":              w.Distribution0to200_30d,
		"distribution_n50to0_30d":              w.DistributionN50to0_30d,
		"distribution_lt50_30d":                w.DistributionLt50_30d,
		"distribution_gt500_percentage_30d":    w.DistributionGt500Percentage30d.InexactFloat64(),
		"distribution_200to500_percentage_30d": w.Distribution200to500Percentage30d.InexactFloat64(),
		"distribution_0to200_percentage_30d":   w.Distribution0to200Percentage30d.InexactFloat64(),
		"distribution_n50to0_percentage_30d":   w.DistributionN50to0Percentage30d.InexactFloat64(),
		"distribution_lt50_percentage_30d":     w.DistributionLt50Percentage30d.InexactFloat64(),
		"distribution_gt500_7d":                w.DistributionGt500_7d,
		"distribution_200to500_7d":             w.Distribution200to500_7d,
		"distribution_0to200_7d":               w.Distribution0to200_7d,
		"distribution_n50to0_7d":               w.DistributionN50to0_7d,
		"distribution_lt50_7d":                 w.DistributionLt50_7d,
		"distribution_gt500_percentage_7d":     w.DistributionGt500Percentage7d.InexactFloat64(),
		"distribution_200to500_percentage_7d":  w.Distribution200to500Percentage7d.InexactFloat64(),
		"distribution_0to200_percentage_7d":    w.Distribution0to200Percentage7d.InexactFloat64(),
		"distribution_n50to0_percentage_7d":    w.DistributionN50to0Percentage7d.InexactFloat64(),
		"distribution_lt50_percentage_7d":      w.DistributionLt50Percentage7d.InexactFloat64(),
		"last_transaction_time":                w.LastTransactionTime,
		"is_active":                            w.IsActive,
		"updated_at":                           w.UpdatedAt,
		"created_at":                           w.CreatedAt,
	}
}

func (w *WalletSummary) ToESIndex() map[string]interface{} {
	return map[string]interface{}{
		"mappings": map[string]interface{}{
			// 暂时移除routing要求，因为当前ES版本不支持
			// "_routing": map[string]interface{}{
			//	"required": true,
			// },
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
				"token_list": map[string]interface{}{
					"type": "nested",
					"properties": map[string]interface{}{
						"token_address": map[string]interface{}{"type": "keyword"},
						"token_icon":    map[string]interface{}{"type": "keyword", "index": false},
						"token_name": map[string]interface{}{"type": "text", "fields": map[string]interface{}{
							"keyword": map[string]interface{}{"type": "keyword", "ignore_above": 256},
						}},
					},
				},

				// 交易数据 - 30天
				"avg_cost_30d":  map[string]interface{}{"type": "double"},
				"total_num_30d": map[string]interface{}{"type": "integer"},
				"buy_num_30d":   map[string]interface{}{"type": "integer"},
				"sell_num_30d":  map[string]interface{}{"type": "integer"},
				"win_rate_30d":  map[string]interface{}{"type": "double"},

				// 交易数据 - 7天
				"avg_cost_7d":  map[string]interface{}{"type": "double"},
				"total_num_7d": map[string]interface{}{"type": "integer"},
				"buy_num_7d":   map[string]interface{}{"type": "integer"},
				"sell_num_7d":  map[string]interface{}{"type": "integer"},
				"win_rate_7d":  map[string]interface{}{"type": "double"},

				// 交易数据 - 1天
				"avg_cost_1d":  map[string]interface{}{"type": "double"},
				"total_num_1d": map[string]interface{}{"type": "integer"},
				"buy_num_1d":   map[string]interface{}{"type": "integer"},
				"sell_num_1d":  map[string]interface{}{"type": "integer"},
				"win_rate_1d":  map[string]interface{}{"type": "double"},

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
				"last_transaction_time": map[string]interface{}{"type": "date", "format": "epoch_millis"},
				"is_active":             map[string]interface{}{"type": "boolean"},
				"updated_at":            map[string]interface{}{"type": "date", "format": "epoch_millis"},
				"created_at":            map[string]interface{}{"type": "date", "format": "epoch_millis"},
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
