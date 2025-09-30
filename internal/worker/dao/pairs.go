package dao

import (
	"context"
	"web3-smart/internal/worker/model"
)

// PairsDAO 定义pairs数据访问接口
type PairsDAO interface {
	// GetEarliestBlockTimestamp 获取指定base或quote代币的最早区块时间戳
	// 对应SQL: SELECT block_timestamp FROM dex_query_v1.pairs WHERE base = ? OR "quote" = ? ORDER BY block_timestamp ASC limit 1;
	GetEarliestBlockTimestamp(ctx context.Context, chainId uint64, tokenAddress string) (*int64, error)

	// GetByAddress 通过pair地址获取交易对信息
	GetByAddress(ctx context.Context, chainID int32, address string) (*model.Pair, error)

	// GetByBaseAndQuote 通过base和quote代币地址获取交易对
	GetByBaseAndQuote(ctx context.Context, chainID int32, base, quote string) (*model.Pair, error)

	// GetByToken 获取包含指定代币的所有交易对
	GetByToken(ctx context.Context, chainID int32, tokenAddress string) ([]*model.Pair, error)

	// GetByChain 获取指定链上的所有交易对
	GetByChain(ctx context.Context, chainID int32, limit, offset int) ([]*model.Pair, error)

	// Create 创建新的交易对记录
	Create(ctx context.Context, pair *model.Pair) error

	// Update 更新交易对记录
	Update(ctx context.Context, pair *model.Pair) error

	// Delete 删除交易对记录
	Delete(ctx context.Context, id int64) error

	// GetLatestPairs 获取最新创建的交易对
	GetLatestPairs(ctx context.Context, chainID int32, limit int) ([]*model.Pair, error)

	// GetPairsByCreator 获取指定创建者的交易对
	GetPairsByCreator(ctx context.Context, chainID int32, creator string) ([]*model.Pair, error)
}
