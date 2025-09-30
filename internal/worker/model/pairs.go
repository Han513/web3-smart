package model

import (
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
)

// Pair mapped from table <pairs>
type Pair struct {
	ID                int64            `gorm:"column:id;primaryKey;autoIncrement:true" json:"id"`
	ChainID           int32            `gorm:"column:chain_id;primaryKey" json:"chain_id"`
	Address           string           `gorm:"column:address;primaryKey;comment:pair地址" json:"address"`                             // pair地址
	Base              string           `gorm:"column:base;not null;comment:base token地址" json:"base"`                               // base token地址
	Quote_            string           `gorm:"column:quote;not null;comment:quote token地址" json:"quote"`                            // quote token地址
	BaseBalance       *decimal.Decimal `gorm:"column:base_balance;comment:base token余额 已处理精度" json:"base_balance"`                  // base token余额 已处理精度
	QuoteBalance      *decimal.Decimal `gorm:"column:quote_balance;comment:quote token余额 已处理精度" json:"quote_balance"`               // quote token余额 已处理精度
	FirstBaseBalance  *decimal.Decimal `gorm:"column:first_base_balance;comment:首次base token余额 已处理精度" json:"first_base_balance"`    // 首次base token余额 已处理精度
	FirstQuoteBalance *decimal.Decimal `gorm:"column:first_quote_balance;comment:首次quote token余额 已处理精度" json:"first_quote_balance"` // 首次quote token余额 已处理精度
	Dex               string           `gorm:"column:dex;not null;comment:dex地址" json:"dex"`                                        // dex地址
	InnerProcess      *float64         `gorm:"column:inner_process;comment:curve process" json:"inner_process"`                     // curve process
	InnerCompleteAt   *int64           `gorm:"column:inner_complete_at;comment:内盘完成时间" json:"inner_complete_at"`                    // 内盘完成时间
	MigrationTarget   *datatypes.JSON  `gorm:"column:migration_target;comment:迁移目标详情" json:"migration_target"`                      // 迁移目标详情
	Creater           *string          `gorm:"column:creater;comment:创建者" json:"creater"`                                           // 创建者
	CreatedAt         *int64           `gorm:"column:created_at;default:(EXTRACT(epoch FROM now()) * (1000)" json:"created_at"`
	OpenAt            *int64           `gorm:"column:open_at;comment:开盘时间" json:"open_at"`                // 开盘时间
	Timestamp         *int64           `gorm:"column:timestamp;comment:最新交易时间戳" json:"timestamp"`         // 最新交易时间戳
	LastPrice         *decimal.Decimal `gorm:"column:last_price;comment:币本位报价" json:"last_price"`         // 币本位报价
	LastPriceUsd      *decimal.Decimal `gorm:"column:last_price_usd;comment:U本位报价" json:"last_price_usd"` // U本位报价
	LastSellAt        *int64           `gorm:"column:last_sell_at" json:"last_sell_at"`
	LastSellPrice     *decimal.Decimal `gorm:"column:last_sell_price;comment:币本位最后卖出报价" json:"last_sell_price"`         // 币本位最后卖出报价
	LastSellPriceUsd  *decimal.Decimal `gorm:"column:last_sell_price_usd;comment:U本位最后卖出报价" json:"last_sell_price_usd"` // U本位最后卖出报价
	LastBuyAt         *int64           `gorm:"column:last_buy_at" json:"last_buy_at"`
	LastBuyPrice      *decimal.Decimal `gorm:"column:last_buy_price;comment:币本位最后买入报价" json:"last_buy_price"`         // 币本位最后买入报价
	LastBuyPriceUsd   *decimal.Decimal `gorm:"column:last_buy_price_usd;comment:U本位最后买入报价" json:"last_buy_price_usd"` // U本位最后买入报价
	MarketCapUsd      *decimal.Decimal `gorm:"column:market_cap_usd;comment:U本位市值" json:"market_cap_usd"`             // U本位市值
	Payload           *datatypes.JSON  `gorm:"column:payload;comment:payload" json:"payload"`                         // payload
	TxHash            *string          `gorm:"column:tx_hash;comment:创建交易对的hash" json:"tx_hash"`                      // 创建交易对的hash
	BlockNumber       *int64           `gorm:"column:block_number;comment:创建交易对的block号" json:"block_number"`          // 创建交易对的block号
	BlockTimestamp    *int64           `gorm:"column:block_timestamp;comment:创建交易对的block时间" json:"block_timestamp"`   // 创建交易对的block时间
}

// TableName Pair's table name
func (*Pair) TableName() string {
	return "dex_query_v1.pairs"
}
