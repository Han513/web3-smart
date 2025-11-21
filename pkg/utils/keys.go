package utils

import "fmt"

func HoldingKey(chainId uint64, walletAddress, tokenAddress string) string {
	return fmt.Sprintf("smart_money:holding:%d:%s:%s", chainId, walletAddress, tokenAddress)
}

func WalletSummaryKey(chainId uint64, walletAddress string) string {
	return fmt.Sprintf("smart_money:%d:%s", chainId, walletAddress)
}

func WapperPriceKey(source, target string) string {
	return fmt.Sprintf("BYD:price:%s_%s", source, target)
}

func TokenInfoKey(chainId uint64, tokenAddress string) string {
	return fmt.Sprintf("smart_money:token_info:%d:%s", chainId, tokenAddress)
}

func PairsEarliestBlockTimestampKey(chainId uint64, tokenAddress string) string {
	return fmt.Sprintf("smart_money:pairs_earliest:%d:%s", chainId, tokenAddress)
}

func MissingTokenInfoKey() string {
	return "smart_money:missing_tokeninfo:list"
}
