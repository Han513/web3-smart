package moralis

import (
	"context"
	"testing"
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/logger"
)

func TestNewMoralisClient(t *testing.T) {
	c := NewMoralisClient(config.MoralisConfig{
		BaseURL:    "https://deep-index.moralis.io",
		GatewayURL: "https://solana-gateway.moralis.io",
		APIKey:     "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6Ijg4M2Q4OWUwLWM4MDYtNGRjOC04MDE5LTk5NTdiMDcyYzViOSIsIm9yZ0lkIjoiNDM5MTQyIiwidXNlcklkIjoiNDUxNzg1IiwidHlwZUlkIjoiM2U3YjE1MmYtZmU2ZC00Y2Y4LTk3ZGUtMDlhN2RkMzYyZjI3IiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDM1MTgzOTIsImV4cCI6NDg5OTI3ODM5Mn0.6Se9ckq52yREVMUo0EegSkt0pmlxwF1OEYPu7h0Ktg0",
		RateLimit:  300,
		Timeout:    30,
	}, logger.NewLogger("test"))
	if c == nil {
		t.Errorf("NewMoralisClient failed")
	}
	holders, err := c.GetEvmTokenHolders(context.Background(), "BSC", "0x15272209c6996e7dfa88c7463b899f4754794444")
	if err != nil {
		t.Errorf("GetEvmTokenHolders failed: %v", err)
	}
	t.Log(holders)
}
