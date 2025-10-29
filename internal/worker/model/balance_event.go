package model

type BalanceEvent struct {
	Wallet       string `json:"wallet"`
	TokenAccount string `json:"tokenAccount"`
	TokenAddress string `json:"tokenAddress"` // evm系列同wallet
	Amount       string `json:"amount"`       // 是否需要
	Decimal      uint8  `json:"decimal"`      // 是否需要
}

type BlockBalance struct {
	Network   string         `json:"network"`
	Brand     string         `json:"brand"`
	Hash      string         `json:"hash"`
	Number    uint64         `json:"number"`
	EventTime uint64         `json:"eventTime"`
	Balances  []BalanceEvent `json:"balances"`
}
