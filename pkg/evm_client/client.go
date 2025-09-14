package evm_client

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

// Init evm client
func Init(rawurl string) *ethclient.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := ethclient.DialContext(ctx, rawurl)
	if err != nil {
		panic(fmt.Sprintf("Init evm client error: %v", err))
	}

	return client
}
