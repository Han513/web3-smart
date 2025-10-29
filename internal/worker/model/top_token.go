package model

type HotToken struct {
	Rank     int    `json:"rank" bson:"rank"`
	Network  string `json:"network" bson:"network"`
	Address  string `json:"address" bson:"address"`
	Decimals int    `json:"decimals" bson:"decimals"`
	Done     bool   `json:"done" bson:"done"`
}

func (HotToken) TableName() string {
	return "hot_tokens"
}
