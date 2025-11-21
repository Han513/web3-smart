package dao

import (
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/elasticsearch"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// DAOManager 管理所有DAO实例
type DAOManager struct {
	HoldingDAO HoldingDAO
	TokenDAO   TokenDAO
	PairsDAO   PairsDAO
	WalletDAO  WalletDAO
}

// NewDAOManager 创建DAO管理器实例
func NewDAOManager(cfg *config.Config, db *gorm.DB, es *elasticsearch.Client, rds *redis.Client) *DAOManager {
	return &DAOManager{
		HoldingDAO: NewHoldingDAO(cfg, db, rds),
		TokenDAO:   NewTokenDAO(cfg, db, es, rds),
		PairsDAO:   NewPairsDAO(cfg, db, rds),
		WalletDAO:  NewWalletDAO(cfg, db, rds),
	}
}
