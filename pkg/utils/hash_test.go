package utils

import (
	"testing"
)

func TestHashString(t *testing.T) {
	input := "SOLANA_balance"

	b := GetHashBucket(input, 2)
	print(b)
}
