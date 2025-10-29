package repository

import (
	"context"
	"strings"
	"sync"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/dao"
	"web3-smart/internal/worker/model"
	"web3-smart/pkg/database"
	"web3-smart/pkg/elasticsearch"
	"web3-smart/pkg/evm_client"
	selectdbclient "web3-smart/pkg/selectdb_client"
	"web3-smart/pkg/solana_client"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	bydrpc "gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/byd_rpc"
	layeredcahe "gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/layered_cahe"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var once sync.Once
var r *repositoryImpl

func New(cfg config.Config, logger *zap.Logger) Repository {
	once.Do(func() {
		r = &repositoryImpl{
			cfg:    cfg,
			logger: logger,
		}
		r.init()
	})
	return r
}

type repositoryImpl struct {
	cfg          config.Config
	logger       *zap.Logger
	db           *gorm.DB
	selectDB     *gorm.DB
	selectDBHttp *selectdbclient.Client
	esClient     *elasticsearch.Client
	mainRdb      *redis.Client
	metricsRdb   *redis.Client
	mq           *kafka.Writer
	solanaClient *rpc.Client
	bscClient    *ethclient.Client
	bydRpc       *bydrpc.BydRpcClient
	daoManager   *dao.DAOManager
}

func (r *repositoryImpl) init() {
	var err error
	r.db, err = database.InitPG(r.cfg.Postgres.DSN)

	if err != nil {
		panic(err)
	}

	r.selectDB, err = database.InitSelectDB(r.cfg.SelectDB.DSN)
	if err != nil {
		panic(err)
	}
	r.selectDBHttp, err = database.InitSelectDBHttpClient(r.cfg.SelectDB.BaseURL,
		r.cfg.SelectDB.Database,
		r.cfg.SelectDB.Username,
		r.cfg.SelectDB.Password)
	if err != nil {
		panic(err)
	}

	// 初始化selectDB
	//r.selectDB, err = database.InitSelectDB(r.cfg.SelectDB.DSN)
	//if err != nil {
	//	panic(err)
	//}

	// 初始化 Elasticsearch Client
	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: r.cfg.Elasticsearch.Addresses,
		Username:  r.cfg.Elasticsearch.Username,
		Password:  r.cfg.Elasticsearch.Password,
		Indexs: map[string]map[string]interface{}{
			// r.cfg.Elasticsearch.HoldingsIndexName: (*model.WalletHolding)(nil).ToESIndex(),
			r.cfg.Elasticsearch.WalletsIndexName: (*model.WalletSummary)(nil).ToESIndex(),
		},
	}, r.logger)
	if err != nil {
		panic(err)
	}
	r.esClient = esClient

	// 初始化 Main RDB
	r.mainRdb = redis.NewClient(&redis.Options{
		Addr:     r.cfg.Redis.Address,
		Password: r.cfg.Redis.Password,
		DB:       r.cfg.Redis.DB,
		PoolSize: 20,
	})

	if err := r.mainRdb.Ping(context.Background()).Err(); err != nil {
		panic("failed to connect to redis: " + err.Error())
	}

	// 初始化 Metrics RDB
	r.metricsRdb = redis.NewClient(&redis.Options{
		Addr:     r.cfg.Redis.Address,
		Password: r.cfg.Redis.Password,
		DB:       r.cfg.Redis.DBMetrics,
	})

	if err := r.metricsRdb.Ping(context.Background()).Err(); err != nil {
		panic("failed to connect to redis: " + err.Error())
	}

	// 初始化 Byd Rpc
	priceRdb := redis.NewClient(&redis.Options{
		Addr:     r.cfg.Redis.Address,
		Password: r.cfg.Redis.Password,
		DB:       r.cfg.Redis.DBPrice,
	})

	if err := priceRdb.Ping(context.Background()).Err(); err != nil {
		panic("failed to connect to redis: " + err.Error())
	}

	priceCache := layeredcahe.NewWithRedisClient(priceRdb, 30*time.Second, 30*time.Second)
	r.bydRpc = bydrpc.NewBydRpcClient(r.cfg.BydRpcUrl, 30*time.Second, priceCache)

	brokers := strings.Split(r.cfg.Kafka.Brokers, ",")
	r.mq = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{},
		BatchSize:    1000,
		BatchBytes:   1024 * 1024, // 1MB
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
		// 添加连接控制
		MaxAttempts:  5,
		WriteTimeout: 500 * time.Millisecond, // 降低单次写入超时时间
	}

	// 初始化rpc client
	r.bscClient = evm_client.Init(r.cfg.BscClientRawUrl)
	r.solanaClient = solana_client.Init(r.cfg.SolanaClientRawUrl)

	// 初始化 DAO Manager
	r.daoManager = dao.NewDAOManager(r.db, r.mainRdb)
}

func (r *repositoryImpl) GetMainRDB() *redis.Client {
	return r.mainRdb
}

func (r *repositoryImpl) GetMetricsRDB() *redis.Client {
	return r.metricsRdb
}

func (r *repositoryImpl) GetDB() *gorm.DB {
	return r.db
}

func (r *repositoryImpl) GetSelectDB() *gorm.DB {
	return r.selectDB
}

func (r *repositoryImpl) GetSelectDBHttp() *selectdbclient.Client {
	return r.selectDBHttp
}

func (r *repositoryImpl) GetElasticsearchClient() *elasticsearch.Client {
	return r.esClient
}

func (r *repositoryImpl) GetMQ() MQClient {
	return r.mq
}

func (r *repositoryImpl) GetSolanaClient() *rpc.Client {
	return r.solanaClient
}

func (r *repositoryImpl) GetBscClient() *ethclient.Client {
	return r.bscClient
}

func (r *repositoryImpl) GetBydRpc() *bydrpc.BydRpcClient {
	return r.bydRpc
}

func (r *repositoryImpl) GetDAOManager() *dao.DAOManager {
	return r.daoManager
}

func (r *repositoryImpl) Close() error {
	if r.db != nil {
		sqlDB, _ := r.db.DB()
		sqlDB.Close()
	}
	if r.selectDB != nil {
		sqlDB, _ := r.selectDB.DB()
		sqlDB.Close()
	}
	if r.mainRdb != nil {
		r.mainRdb.Close()
	}
	if r.metricsRdb != nil {
		r.metricsRdb.Close()
	}
	if r.mq != nil {
		r.mq.Close()
	}
	if r.bscClient != nil {
		r.bscClient.Close()
	}
	if r.solanaClient != nil {
		r.solanaClient.Close()
	}
	return nil
}
