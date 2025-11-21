package model

import (
	"github.com/lib/pq"
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
)

type Token struct {
	ID                int64            `gorm:"column:id;primaryKey;autoIncrement:true"`
	Network           string           `gorm:"column:network;not null"`
	ChainID           *int32           `gorm:"column:chain_id"`
	Address           string           `gorm:"column:address;not null"`
	Symbol            string           `gorm:"column:symbol;not null"`
	Name              *string          `gorm:"column:name"`
	Decimals          *int32           `gorm:"column:decimals"`
	Logo              *string          `gorm:"column:logo"`
	Description       *string          `gorm:"column:description"`
	TokenType         *string          `gorm:"column:token_type"`
	PriceUsd          *decimal.Decimal `gorm:"column:price_usd"`
	FdvUsd            *decimal.Decimal `gorm:"column:fdv_usd"`
	TotalReserveUsd   *decimal.Decimal `gorm:"column:total_reserve_in_usd"`
	VolumeUsdH24      *decimal.Decimal `gorm:"column:volume_usd_h24"`
	MarketCapUsd      *decimal.Decimal `gorm:"column:market_cap_usd"`
	Liquidity         *decimal.Decimal `gorm:"column:liquidity"`
	TotalSupply       *decimal.Decimal `gorm:"column:total_supply"`
	SupplyFloat       *decimal.Decimal `gorm:"column:supply_float"`
	Brand             *string          `gorm:"column:brand"`
	Zone              *string          `gorm:"column:zone"`
	Tags              pq.StringArray   `gorm:"column:tags;type:text[]"`
	LastTradeAt       *int64           `gorm:"column:last_trade_at"`
	SocialInfo        *datatypes.JSON  `gorm:"column:social_info"`
	PoolInfo          *datatypes.JSON  `gorm:"column:pool_info"`
	SecurityInfo      *datatypes.JSON  `gorm:"column:security_info"`
	CommunityInfo     *datatypes.JSON  `gorm:"column:community_info"`
	StateInfo         *datatypes.JSON  `gorm:"column:state_info"`
	SmartMoneyInfo    *datatypes.JSON  `gorm:"column:smart_money_info"`
	HolderInfo        *datatypes.JSON  `gorm:"column:holder_info"`
	ContractInfo      *datatypes.JSON  `gorm:"column:contract_info"`
	MarketInfo        *datatypes.JSON  `gorm:"column:market_info"`
	CreatedAt         *int64           `gorm:"column:created_at;default:(EXTRACT(epoch FROM now()) * (1000))"`
	UpdatedAt         *int64           `gorm:"column:updated_at;default:(EXTRACT(epoch FROM now()) * (1000))"`
	IsInner           *bool            `gorm:"column:is_inner"`
	InnerPool         *datatypes.JSON  `gorm:"column:inner_pool"`
	AddressNormalized *string          `gorm:"column:address_normalized"`
	BlockTimestamp    *int64           `gorm:"column:block_timestamp"`
	HeatScoreBackup   *float64         `gorm:"column:heat_score_backup"`
	HeatScore         *datatypes.JSON  `gorm:"column:heat_score"`
	Rank              *datatypes.JSON  `gorm:"column:rank"`
	InnerProcess      *float64         `gorm:"column:inner_process"`
	RepairPool        *datatypes.JSON  `gorm:"column:repair_pool"`
}

func (*Token) TableName() string {
	return "dex_query_v1.web3_tokens"
}

type SmTokenRet struct {
	Address string           `gorm:"column:address;primaryKey;comment:token地址" json:"address"` // token地址
	Name    string           `gorm:"column:name;not null;comment:token名称" json:"name"`         // token名称
	Symbol  string           `gorm:"column:symbol;not null;comment:token符号" json:"symbol"`     // token符号
	Supply  *decimal.Decimal `gorm:"column:supply;comment:token总供应量" json:"supply"`            // token总供应量
	Creater *string          `gorm:"column:creater;comment:创建者" json:"creater"`                // 创建者
	Logo    string           `gorm:"column:logo;comment:token logo" json:"logo"`               // token logo
}
