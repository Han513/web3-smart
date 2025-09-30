package model

import (
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
)

// Token mapped from table <tokens>
type Token struct {
	ID              int64            `gorm:"column:id;primaryKey;autoIncrement:true" json:"id"`
	ChainID         int32            `gorm:"column:chain_id;primaryKey;comment:链ID" json:"chain_id"`                                       // 链ID
	Address         string           `gorm:"column:address;primaryKey;comment:token地址" json:"address"`                                     // token地址
	Name            string           `gorm:"column:name;not null;comment:token名称" json:"name"`                                             // token名称
	Symbol          string           `gorm:"column:symbol;not null;comment:token符号" json:"symbol"`                                         // token符号
	Decimals        *int32           `gorm:"column:decimals;comment:token精度" json:"decimals"`                                              // token精度
	Supply          *decimal.Decimal `gorm:"column:supply;comment:token总供应量" json:"supply"`                                                // token总供应量
	URI             *string          `gorm:"column:uri;comment:token uri" json:"uri"`                                                      // token uri
	Creater         *string          `gorm:"column:creater;comment:创建者" json:"creater"`                                                    // 创建者
	CreatedAt       *int64           `gorm:"column:created_at;default:(EXTRACT(epoch FROM now()) * (1000);comment:创建时间" json:"created_at"` // 创建时间
	Pairs           *datatypes.JSON  `gorm:"column:pairs;comment:交易对列表" json:"pairs"`                                                      // 交易对列表
	FdvUsd          *decimal.Decimal `gorm:"column:fdv_usd;comment:U本位完全稀释市值" json:"fdv_usd"`                                              // U本位完全稀释市值
	PriceUsd        *decimal.Decimal `gorm:"column:price_usd;comment:U本位价格" json:"price_usd"`                                              // U本位价格
	Logo            *string          `gorm:"column:logo;comment:token logo" json:"logo"`                                                   // token logo
	Banner          *string          `gorm:"column:banner;comment:token banner" json:"banner"`                                             // token banner
	Description     *string          `gorm:"column:description;comment:token描述" json:"description"`                                        // token描述
	Social          *datatypes.JSON  `gorm:"column:social;comment:社交媒体信息" json:"social"`                                                   // 社交媒体信息
	InnerProcess    *float64         `gorm:"column:inner_process;comment:内盘进度" json:"inner_process"`                                       // 内盘进度
	InnerCompleteAt *int64           `gorm:"column:inner_complete_at;comment:内盘完成时间" json:"inner_complete_at"`                             // 内盘完成时间
	Payload         *datatypes.JSON  `gorm:"column:payload" json:"payload"`
	TxHash          *string          `gorm:"column:tx_hash;comment:创建代币的哈希" json:"tx_hash"`                      // 创建代币的哈希
	BlockNumber     *int64           `gorm:"column:block_number;comment:创建代币的block号" json:"block_number"`        // 创建代币的block号
	BlockTimestamp  *int64           `gorm:"column:block_timestamp;comment:创建代币的block时间" json:"block_timestamp"` // 创建代币的block时间
	Timestamp       *int64           `gorm:"column:timestamp;comment:最后更新时间" json:"timestamp"`                   // 最后更新时间
}

// TableName Token's table name
func (*Token) TableName() string {
	return "dex_query_v1.tokens"
}

type SmTokenRet struct {
	Address string           `gorm:"column:address;primaryKey;comment:token地址" json:"address"` // token地址
	Name    string           `gorm:"column:name;not null;comment:token名称" json:"name"`         // token名称
	Symbol  string           `gorm:"column:symbol;not null;comment:token符号" json:"symbol"`     // token符号
	Supply  *decimal.Decimal `gorm:"column:supply;comment:token总供应量" json:"supply"`            // token总供应量
	Creater *string          `gorm:"column:creater;comment:创建者" json:"creater"`                // 创建者
	Logo    string           `gorm:"column:logo;comment:token logo" json:"logo"`               // token logo
}
