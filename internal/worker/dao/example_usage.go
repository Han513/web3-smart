package dao

import (
	"context"
	"fmt"
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/elasticsearch"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ExampleUsage 展示如何使用DAO
func ExampleUsage(cfg *config.Config, db *gorm.DB, es *elasticsearch.Client, rdb *redis.Client) {
	ctx := context.Background()

	// 创建DAO管理器
	daoManager := NewDAOManager(cfg, db, es, rdb)

	// 使用HoldingDAO
	wallet := "0x1234567890abcdef1234567890abcdef12345678"
	token := "0xabcdef1234567890abcdef1234567890abcdef12"
	chainID := uint64(9006)
	holding, err := daoManager.HoldingDAO.GetByWalletAndToken(ctx, chainID, wallet, token)
	if err != nil {
		fmt.Printf("查询持仓失败: %v\n", err)
		return
	}
	if holding == nil {
		fmt.Println("未找到持仓记录")
	} else {
		fmt.Printf("找到持仓记录: 钱包=%s, 代币=%s, 数量=%f\n",
			holding.WalletAddress, holding.TokenAddress, holding.Amount)
	}

	// 使用TokenDAO

	tokenInfo, err := daoManager.TokenDAO.GetTokenInfo(ctx, chainID, token)
	if err != nil {
		fmt.Printf("查询代币信息失败: %v\n", err)
		return
	}
	if tokenInfo == nil {
		fmt.Println("未找到代币信息")
	} else {
		fmt.Printf("找到代币信息: 符号=%s, 名称=%s\n",
			tokenInfo.Symbol, tokenInfo.Name)
	}

	// 使用PairsDAO
	blockTimestamp, err := daoManager.PairsDAO.GetEarliestBlockTimestamp(ctx, chainID, token)
	if err != nil {
		fmt.Printf("查询池子创建时间失败: %v\n", err)
		return
	}
	if blockTimestamp == nil {
		fmt.Println("未找到池子创建时间")
	} else {
		fmt.Printf("池子创建时间: %d\n", *blockTimestamp)
	}

	// 使用WalletDAO
	walletInfo, err := daoManager.WalletDAO.GetByWalletAddress(ctx, chainID, wallet)
	if err != nil {
		fmt.Printf("查询钱包信息失败: %v\n", err)
		return
	}
	if walletInfo == nil {
		fmt.Println("未找到钱包信息")
	} else {
		fmt.Printf("找到钱包信息: 地址=%s, 余额=%f USD\n",
			walletInfo.WalletAddress, walletInfo.BalanceUSD)
	}
}
