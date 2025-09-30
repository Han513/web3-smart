package transactionutils

import (
	"fmt"
	"web3-smart/internal/worker/model"
)

// 根据wallet_address, token_address, signature, transaction_time, chain_id去重
func DeduplicateTransactions(transactions []model.WalletTransaction) []model.WalletTransaction {
	deduplicatedTransactions := make([]model.WalletTransaction, 0)
	seen := make(map[string]struct{})
	for _, transaction := range transactions {
		key := fmt.Sprintf("%s:%s:%s:%d:%d",
			transaction.WalletAddress,
			transaction.TokenAddress,
			transaction.Signature,
			transaction.TransactionTime,
			transaction.ChainID)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			deduplicatedTransactions = append(deduplicatedTransactions, transaction)
		}
	}
	return deduplicatedTransactions
}
