package repository

import (
	"web3-smart/internal/worker/dao"
	"web3-smart/pkg/elasticsearch"
	selectdbclient "web3-smart/pkg/selectdb_client"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	bydrpc "gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/byd_rpc"
	"gorm.io/gorm"
)

type RedisClient = *redis.Client
type DBClient = *gorm.DB
type MQClient = *kafka.Writer

type Repository interface {
	//DB
	GetMainRDB() RedisClient
	GetMetricsRDB() RedisClient
	GetPriceRDB() RedisClient
	GetDB() DBClient
	GetMQ() MQClient
	GetSelectDB() DBClient
	GetSelectDBHttp() *selectdbclient.Client
	GetElasticsearchClient() *elasticsearch.Client
	GetBscClient() *ethclient.Client
	GetSolanaClient() *rpc.Client
	GetBydRpc() *bydrpc.BydRpcClient

	//DAO
	GetDAOManager() *dao.DAOManager

	Close() error
}
