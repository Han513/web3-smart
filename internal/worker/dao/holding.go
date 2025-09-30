package dao

import (
	"context"
	"web3-smart/internal/worker/model"
)

// HoldingDAO 定义holding数据访问接口
type HoldingDAO interface {
	// GetByWalletAndToken 通过钱包地址和代币地址查询持仓信息
	GetByWalletAndToken(ctx context.Context, chainId uint64, walletAddress, tokenAddress string) (*model.WalletHolding, error)

	// GetByWallet 通过钱包地址查询所有持仓信息
	GetByWallet(ctx context.Context, chainId uint64, walletAddress string) ([]*model.WalletHolding, error)

	// Create 创建新的持仓记录
	Create(ctx context.Context, holding *model.WalletHolding) error

	// Update 更新持仓记录
	Update(ctx context.Context, holding *model.WalletHolding) error

	// Delete 删除持仓记录
	Delete(ctx context.Context, id int) error

	// GetByChainAndWallet 通过链ID和钱包地址查询持仓信息
	GetByChainAndWallet(ctx context.Context, chainID uint64, walletAddress string) ([]*model.WalletHolding, error)

	// GetActiveHoldings 获取未清仓的持仓信息
	GetActiveHoldings(ctx context.Context, walletAddress string) ([]*model.WalletHolding, error)

	// UpdateHoldingCache 更新持仓缓存
	UpdateHoldingCache(ctx context.Context, cacheKey string, holding *model.WalletHolding) error
}
