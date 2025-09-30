package holdingutils

import (
	"fmt"
	"web3-smart/internal/worker/model"
)

// 根据wallet_address, token_address, chain_id去重
func DeduplicateHoldings(holdings []model.WalletHolding) []model.WalletHolding {
	deduplicatedHoldings := make([]model.WalletHolding, 0)
	seen := make(map[string]struct{})
	for _, holding := range holdings {
		key := fmt.Sprintf("%s:%s:%d", holding.WalletAddress, holding.TokenAddress, holding.ChainID)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			deduplicatedHoldings = append(deduplicatedHoldings, holding)
		}
	}
	return deduplicatedHoldings
}
