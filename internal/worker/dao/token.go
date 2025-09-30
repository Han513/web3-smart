package dao

import (
	"context"
	"web3-smart/internal/worker/model"
)

// TokenDAO 定义token数据访问接口
type TokenDAO interface {
	// GetTokenInfo 获取代币信息
	GetTokenInfo(ctx context.Context, chainID uint64, tokenAddress string) (*model.SmTokenRet, error)

	// GetByAddress 通过地址获取完整token信息
	GetByAddress(ctx context.Context, chainID uint64, tokenAddress string) (*model.Token, error)

	// GetByChain 获取指定链上的所有token
	GetByChain(ctx context.Context, chainID uint64, limit, offset int) ([]*model.Token, error)

	// Create 创建新的token记录
	Create(ctx context.Context, token *model.Token) error

	// Update 更新token记录
	Update(ctx context.Context, token *model.Token) error

	// GetBySymbol 通过symbol查询token
	GetBySymbol(ctx context.Context, chainID uint64, symbol string) ([]*model.Token, error)
}
