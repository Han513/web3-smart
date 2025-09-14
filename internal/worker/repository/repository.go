package repository

import (
	"context"
	"strings"
	"sync"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/database"
	"web3-smart/pkg/evm_client"
	"web3-smart/pkg/solana_client"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
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
	mainRdb      *redis.Client
	metricsRdb   *redis.Client
	mq           *kafka.Writer
	solanaClient *rpc.Client
	bscClient    *ethclient.Client
}

func (r *repositoryImpl) init() {
	var err error
	r.db, err = database.InitPG(r.cfg.Postgres.DSN)

	if err != nil {
		panic(err)
	}

	// 初始化selectDB（可选，DSN 为空则跳过）
	if strings.TrimSpace(r.cfg.SelectDB.DSN) != "" {
		r.selectDB, err = database.InitSelectDB(r.cfg.SelectDB.DSN)
		if err != nil {
			r.logger.Warn("failed to connect to selectdb, continue without it", zap.Error(err))
		}
	} else {
		r.logger.Info("selectdb dsn empty, skip selectdb initialization")
	}

	// 初始化 Main RDB
	r.mainRdb = redis.NewClient(&redis.Options{
		Addr:     r.cfg.Redis.Address,
		Password: r.cfg.Redis.Password,
		DB:       r.cfg.Redis.DB,
		PoolSize: 20,
	})

	if err := r.mainRdb.Ping(context.Background()).Err(); err != nil {
		r.logger.Warn("failed to connect to redis, continue", zap.Error(err))
	}

	// 初始化 Metrics RDB
	r.metricsRdb = redis.NewClient(&redis.Options{
		Addr:     r.cfg.Redis.Address,
		Password: r.cfg.Redis.Password,
		DB:       r.cfg.Redis.DBMetrics,
	})

	if err := r.metricsRdb.Ping(context.Background()).Err(); err != nil {
		r.logger.Warn("failed to connect to metrics redis, continue", zap.Error(err))
	}

	brokers := strings.Split(r.cfg.Kafka.Brokers, ",")
	r.mq = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1000,
		BatchBytes:   1024 * 1024, // 1MB
		Async:        true,
		RequiredAcks: kafka.RequireNone,
		Compression:  kafka.Snappy,
		// 添加连接控制
		MaxAttempts:  5,
		WriteTimeout: 500 * time.Millisecond, // 降低单次写入超时时间
	}

	// 初始化rpc client
	r.bscClient = evm_client.Init(r.cfg.BscClientRawUrl)
	r.solanaClient = solana_client.Init(r.cfg.SolanaClientRawUrl)
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

func (r *repositoryImpl) GetMQ() MQClient {
	return r.mq
}

func (r *repositoryImpl) GetSolanaClient() *rpc.Client {
	return r.solanaClient
}

func (r *repositoryImpl) GetBscClient() *ethclient.Client {
	return r.bscClient
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
	return nil
}
