package smartmoney

import (
	"context"
	"fmt"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/quotecoin"
)

func GetSolanaBalance(ctx context.Context, client *rpc.Client, walletAddress string) (decimal.Decimal, map[string]decimal.Decimal, error) {
	// 获取SOL余额
	solBalance, err := getSOLBalance(ctx, client, walletAddress)
	if err != nil {
		fmt.Printf("获取SOL余额失败: %v\n", err)
	}

	// 获取WSOL余额
	wsolBalance, err := getWSOLBalance(ctx, client, walletAddress)
	if err != nil {
		fmt.Printf("获取WSOL余额失败: %v\n", err)
	}

	// fmt.Printf("钱包地址: %s\n", walletAddress)
	// fmt.Printf("SOL余额: %.9f SOL\n", solBalance)
	// fmt.Printf("WSOL余额: %.9f WSOL\n", wsolBalance)

	m := map[string]decimal.Decimal{
		quotecoin.ID501_WSOL_ADDRESS: decimal.NewFromFloat(wsolBalance),
	}

	return decimal.NewFromFloat(solBalance), m, nil
}

// 获取SOL余额
func getSOLBalance(ctx context.Context, client *rpc.Client, walletAddr string) (float64, error) {
	// 将字符串地址转换为PublicKey
	pubKey, err := solana.PublicKeyFromBase58(walletAddr)
	if err != nil {
		return 0, fmt.Errorf("无效的钱包地址: %v", err)
	}

	// 获取账户信息
	accountInfo, err := client.GetAccountInfo(ctx, pubKey)
	if err != nil {
		return 0, fmt.Errorf("获取账户信息失败: %v", err)
	}

	// 计算SOL余额 (lamports转换为SOL)
	balance := float64(accountInfo.Value.Lamports) / float64(solana.LAMPORTS_PER_SOL)
	return balance, nil
}

// 获取WSOL余额 - 使用更准确的方法
func getWSOLBalance(ctx context.Context, client *rpc.Client, walletAddr string) (float64, error) {
	// 将字符串地址转换为PublicKey
	ownerPubKey, err := solana.PublicKeyFromBase58(walletAddr)
	if err != nil {
		return 0, fmt.Errorf("无效的钱包地址: %v", err)
	}

	// 将WSOL的mint地址转换为PublicKey
	mintPubKey, err := solana.PublicKeyFromBase58(quotecoin.ID501_WSOL_ADDRESS)
	if err != nil {
		return 0, fmt.Errorf("无效的mint地址: %v", err)
	}

	// 获取所有Token账户
	tokenAccounts, err := client.GetTokenAccountsByOwner(
		ctx,
		ownerPubKey,
		&rpc.GetTokenAccountsConfig{
			Mint: &mintPubKey,
		},
		&rpc.GetTokenAccountsOpts{
			Encoding: solana.EncodingBase64,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("获取Token账户失败: %v", err)
	}

	// 如果没有找到WSOL账户，返回0
	if len(tokenAccounts.Value) == 0 {
		return 0, nil
	}

	// 解析Token账户数据
	var totalBalance float64
	for _, account := range tokenAccounts.Value {
		// 使用GetTokenAccountBalance方法获取准确的余额
		balance, err := client.GetTokenAccountBalance(ctx, account.Pubkey, rpc.CommitmentFinalized)
		if err != nil {
			//log.Printf("获取Token账户余额失败: %v", err)
			continue
		}

		// 使用UiAmount字段获取实际的用户界面显示金额
		if balance.Value.UiAmount != nil {
			totalBalance += *balance.Value.UiAmount
			//log.Printf("找到WSOL账户 %s，余额: %f", account.Pubkey.String(), *balance.Value.UiAmount)
		}
	}

	return totalBalance, nil
}
