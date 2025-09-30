package walletutils

import (
	"web3-smart/internal/worker/model"
)

// 根据wallet_address去重
func DeduplicateWallets(wallets []model.WalletSummary) []model.WalletSummary {
	deduplicatedWallets := make([]model.WalletSummary, 0)
	seen := make(map[string]struct{})
	for _, wallet := range wallets {
		key := wallet.WalletAddress
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			deduplicatedWallets = append(deduplicatedWallets, wallet)
		}
	}
	return deduplicatedWallets
}
