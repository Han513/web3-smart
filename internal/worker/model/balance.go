package model

type Balance struct {
	Network      string `gorm:"column:network" json:"network"`
	TokenAddress string `gorm:"column:token_address" json:"token_address"`
	Wallet       string `gorm:"column:wallet" json:"wallet"`
	TokenAccount string `gorm:"column:token_account" json:"token_account"`
	Amount       string `gorm:"column:amount" json:"amount"`
	Decimal      int    `gorm:"column:decimal" json:"decimal"`
	BlockNumber  int64  `gorm:"column:block_number" json:"block_number"`
	Version      int64  `gorm:"column:version" json:"version"`
	UpdatedAt    string `gorm:"column:updated_at" json:"updated_at"`
}

// TableName 指定 GORM 写入的表名为 balance
func (Balance) TableName() string {
	return "balance"
}
