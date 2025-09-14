package repository

import (
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type RedisClient = *redis.Client
type DBClient = *gorm.DB
type MQClient = *kafka.Writer

type Repository interface {
	//DB
	GetMainRDB() RedisClient
	GetMetricsRDB() RedisClient
	GetDB() DBClient
	GetMQ() MQClient
	GetSelectDB() DBClient
	GetBscClient() *ethclient.Client
	GetSolanaClient() *rpc.Client
	Close() error
}
