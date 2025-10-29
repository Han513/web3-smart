package selectdbclient

import (
	"context"
	"strings"
	"testing"
)

func TestNewClient(t *testing.T) {
	c := NewClient("http://selectdb-sg-o51479b4e02.selectdbfe.rds.aliyuncs.com:8080", "smart_money", "test_selectdb_teemo", "x$dKds7f%JqaK")
	if c == nil {
		t.Errorf("NewClient failed")
	}
	data := `[{
  "network": "BSC",
  "token_address": "0x007ea5c0ea75a8e0000000000000000000000000",
  "wallet": "0xEAB9AF1C90270000000000000000000000000000",
  "token_account": "0xEAB9AF1C90270000000000000000000000000000",
  "amount": "-1",
  "decimal": 17,
  "block_number": 65571579,
  "version": 65571579,
  "updated_at": "2024-11-23 08:31:27"
},{
  "network": "SOLANA",
  "token_address": "GWNMwrqX6Ai45EmBEAhXsmiwpC68U7HUREPxzjoFpump",
  "wallet": "2vgQHS4vUTJFiqUvBZWfJdJYD6UjAnnN47Kk5y3mPp2a",
  "token_account": "DNPwMGgRSNm81Lf81aS1UmnhCEtnXPoiTW17aDpEebjm",
  "amount": "356930932916",
  "decimal": 6,
  "block_number": 65571580,
  "version": 65571580,
  "updated_at": "2025-11-23 09:31:27"
}]`
	err := c.StreamLoadBalance(context.Background(), strings.NewReader(data), StreamLoadOptions{Table: "balance", Format: "json"})
	if err != nil {
		t.Errorf("StreamLoadBalance failed: %v", err)
	}
}
