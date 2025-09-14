package model

const TRADE_EVENT_TYPE = "com.zeroex.web3.core.event.data.TradeEvent"

type TradeEvent struct {
	Event EventDetails `json:"event"`
	Type  string       `json:"type"`
	Raw   []byte       `json:"raw"` // 添加 Raw 字段存储原始消息
}

type EventDetails struct {
	Brand           string  `json:"brand"`
	EventTime       int64   `json:"eventTime"` // 毫秒时间戳
	ID              string  `json:"id"`
	Network         string  `json:"network"`
	TokenAddress    string  `json:"tokenAddress"`
	PoolAddress     string  `json:"poolAddress"`
	H24             float64 `json:"h24"`
	Time            int64   `json:"time"` // 秒时间戳
	Side            string  `json:"side"`
	VolumeUsd       float64 `json:"volumeUsd"`
	TxnValue        float64 `json:"txnValue"`
	FromTokenAmount float64 `json:"fromTokenAmount"`
	ToTokenAmount   float64 `json:"toTokenAmount"`
	Address         string  `json:"address"`
	Hash            string  `json:"hash"`
	Price           float64 `json:"price"`
	PriceNav        float64 `json:"priceNav"`
	Source          string  `json:"source"`
	Timestamp       int64   `json:"timestamp"`

	// ✅ 新增字段
	BaseMint     string  `json:"base_mint"`
	QuoteMint    string  `json:"quote_mint"`
	BaseBalance  float64 `json:"base_balance"`
	QuoteBalance float64 `json:"quote_balance"`
}
