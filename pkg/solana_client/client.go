package solana_client

import (
	"github.com/gagliardetto/solana-go/rpc"
)

// Init solana client
func Init(rawUrl string) *rpc.Client {
	client := rpc.New(rawUrl)
	return client
}
