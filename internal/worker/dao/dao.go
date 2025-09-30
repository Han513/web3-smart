package dao

import (
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
func NewDAOManager(db *gorm.DB, rds *redis.Client) *DAOManager {
	return &DAOManager{
		HoldingDAO: NewHoldingDAO(db, rds),
		TokenDAO:   NewTokenDAO(db, rds),
		PairsDAO:   NewPairsDAO(db, rds),
		WalletDAO:  NewWalletDAO(db, rds),
	}
}
