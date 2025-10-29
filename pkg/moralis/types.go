package moralis

type TokenHoldersResp struct {
	Cursor      string      `json:"cursor"`
	TotalSupply string      `json:"totalSupply"`
	Page        int         `json:"page"`
	PageSize    int         `json:"page_size"`
	Result      []TokenHold `json:"result"`
}

type TokenHold struct {
	Balance                         string  `json:"balance"`                             // 原始余额字符串
	BalanceFormatted                string  `json:"balance_formatted"`                   // 格式化后的余额字符串（带精度）
	IsContract                      bool    `json:"is_contract"`                         // 是否为合约地址
	OwnerAddress                    string  `json:"owner_address"`                       // 持有者钱包地址
	OwnerAddressLabel               *string `json:"owner_address_label"`                 // 持有者标签（可为null）
	Entity                          *string `json:"entity"`                              // 关联实体（可为null）
	EntityLogo                      *string `json:"entity_logo"`                         // 实体Logo URL（可为null）
	USDValue                        string  `json:"usd_value"`                           // 美元估值字符串（高精度）
	PercentageRelativeToTotalSupply float64 `json:"percentage_relative_to_total_supply"` // 占总供应量百分比
}

type TokenHolderData struct {
	Top100 []TokenHolder `json:"top100"`
}

type TokenHolder struct {
	Type       string  `json:"type"`
	Owner      string  `json:"owner"`
	Balance    string  `json:"balance"`
	Percentage float64 `json:"percentage"`
}

// TokenAnalytics represents the complete token analytics response
type TokenAnalytics struct {
	TotalHolders         int                  `json:"totalHolders"`
	HoldersByAcquisition HoldersByAcquisition `json:"holdersByAcquisition"`
	HolderChange         HolderChange         `json:"holderChange"`
	HolderSupply         HolderSupply         `json:"holderSupply"`
	HolderDistribution   HolderDistribution   `json:"holderDistribution"`
}

// HoldersByAcquisition represents how holders acquired their tokens
type HoldersByAcquisition struct {
	Swap     int `json:"swap"`
	Transfer int `json:"transfer"`
	Airdrop  int `json:"airdrop"`
}

// HolderChange represents holder count changes over different time periods
type HolderChange struct {
	FiveMin   TimePeriodChange `json:"5min"`
	OneHour   TimePeriodChange `json:"1h"`
	SixHour   TimePeriodChange `json:"6h"`
	OneDay    TimePeriodChange `json:"24h"`
	ThreeDay  TimePeriodChange `json:"3d"`
	SevenDay  TimePeriodChange `json:"7d"`
	ThirtyDay TimePeriodChange `json:"30d"`
}

// TimePeriodChange represents the change in holder count for a specific time period
type TimePeriodChange struct {
	Change        int     `json:"change"`
	ChangePercent float64 `json:"changePercent"`
}

// HolderSupply represents supply distribution among top holders
type HolderSupply struct {
	Top10  SupplyInfo `json:"top10"`
	Top25  SupplyInfo `json:"top25"`
	Top50  SupplyInfo `json:"top50"`
	Top100 SupplyInfo `json:"top100"`
	Top250 SupplyInfo `json:"top250"`
	Top500 SupplyInfo `json:"top500"`
}

// SupplyInfo represents supply information for a specific holder tier
type SupplyInfo struct {
	Supply        string  `json:"supply"`
	SupplyPercent float64 `json:"supplyPercent"`
}

// HolderDistribution represents the distribution of holders by balance tiers
type HolderDistribution struct {
	Whales   int `json:"whales"`
	Sharks   int `json:"sharks"`
	Dolphins int `json:"dolphins"`
	Fish     int `json:"fish"`
	Octopus  int `json:"octopus"`
	Crabs    int `json:"crabs"`
	Shrimps  int `json:"shrimps"`
}

// SolanaTokenHoldersResp represents the response structure for Solana token holders
type SolanaHoldersResp struct {
	Cursor      string              `json:"cursor"`
	Page        int                 `json:"page"`
	PageSize    int                 `json:"pageSize"`
	TotalSupply string              `json:"totalSupply"`
	Result      []SolanaTokenHolder `json:"result"`
}

// SolanaTokenHolder represents a single token holder in Solana response
type SolanaTokenHolder struct {
	Balance                         string  `json:"balance"`
	BalanceFormatted                string  `json:"balanceFormatted"`
	IsContract                      bool    `json:"isContract"`
	OwnerAddress                    string  `json:"ownerAddress"`
	USDValue                        string  `json:"usdValue"`
	PercentageRelativeToTotalSupply float64 `json:"percentageRelativeToTotalSupply"`
}
