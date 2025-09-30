package dao

import (
	"context"
	"web3-smart/internal/worker/model"
)

// WalletDAO 定义wallet数据访问接口
type WalletDAO interface {
	// GetByWalletAddress 通过钱包地址查询钱包摘要信息（核心需求）
	GetByWalletAddress(ctx context.Context, chainId uint64, walletAddress string) (*model.WalletSummary, error)

	// GetByID 通过ID查询钱包信息
	GetByID(ctx context.Context, id int) (*model.WalletSummary, error)

	// GetByChain 获取指定链上的钱包列表
	GetByChain(ctx context.Context, chainID uint64, limit, offset int) ([]*model.WalletSummary, error)

	// GetByTags 通过标签查询钱包
	GetByTags(ctx context.Context, tags []string, limit, offset int) ([]*model.WalletSummary, error)

	// GetByWalletType 通过钱包类型查询
	GetByWalletType(ctx context.Context, walletType int, limit, offset int) ([]*model.WalletSummary, error)

	// GetActiveWallets 获取活跃钱包
	GetActiveWallets(ctx context.Context, limit, offset int) ([]*model.WalletSummary, error)

	// GetTopPerformers 获取表现最好的钱包（按PNL排序）
	GetTopPerformers(ctx context.Context, period string, limit, offset int) ([]*model.WalletSummary, error)

	// GetByTwitterUsername 通过Twitter用户名查询钱包
	GetByTwitterUsername(ctx context.Context, twitterUsername string) ([]*model.WalletSummary, error)

	// Create 创建新的钱包记录
	Create(ctx context.Context, wallet *model.WalletSummary) error

	// Update 更新钱包记录
	Update(ctx context.Context, wallet *model.WalletSummary) error

	// Delete 删除钱包记录
	Delete(ctx context.Context, id int) error

	// BatchUpdate 批量更新钱包信息
	BatchUpdate(ctx context.Context, wallets []*model.WalletSummary) error

	// GetWalletStats 获取钱包统计信息
	GetWalletStats(ctx context.Context, walletAddress string) (*model.WalletStats, error)

	// UpdateWalletCache 更新钱包缓存
	UpdateWalletCache(ctx context.Context, cacheKey string, wallet *model.WalletSummary) error
}
