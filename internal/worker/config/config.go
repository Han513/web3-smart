package config

import (
	"fmt"
	"web3-smart/pkg/logger"

	"github.com/fsnotify/fsnotify"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// Config 定义整个配置的结构
type Config struct {
	Log                LogConfig           `mapstructure:"log"`
	Kafka              KafkaConfig         `mapstructure:"kafka"`
	Redis              RedisConfig         `mapstructure:"redis"`
	Postgres           PostgresConfig      `mapstructure:"postgres"`
	SelectDB           SelectDBConfig      `mapstructure:"selectdb"`
	Elasticsearch      ElasticsearchConfig `mapstructure:"elasticsearch"`
	Lark               LarkConfig          `mapstructure:"lark"`
	Worker             WorkerConfig        `mapstructure:"worker"`
	Monitor            MonitorConfig       `mapstructure:"monitor"`
	Moralis            MoralisConfig       `mapstructure:"moralis"`
	BydRpcUrl          string              `mapstructure:"byd_rpc_url"`
	BscClientRawUrl    string              `mapstructure:"bsc_client_rawurl"`
	SolanaClientRawUrl string              `mapstructure:"solana_client_rawurl"`
}

// KafkaConfig Kafka 配置
type KafkaConfig struct {
	Brokers      string `mapstructure:"brokers"`
	TopicTrade   string `mapstructure:"topic_trade"`
	TopicBalance string `mapstructure:"topic_balance"`
	TopicData    string `mapstructure:"topic_data"`
	TopicSM      string `mapstructure:"topic_sm"`
	TopicDev     string `mapstructure:"topic_dev"`
	GroupID      string `mapstructure:"group_id"`
}

// RedisConfig Redis 配置
type RedisConfig struct {
	Address   string `mapstructure:"address"`
	Password  string `mapstructure:"password"`
	DB        int    `mapstructure:"db"`
	DBMetrics int    `mapstructure:"db_metrics"`
	DBPrice   int    `mapstructure:"db_price"`
}

// PostgresConfig PostgreSQL 配置
type PostgresConfig struct {
	DSN string `mapstructure:"dsn"`
}

// SelectDBConfig SelectDB 配置
type SelectDBConfig struct {
	DSN      string `mapstructure:"dsn"`
	BaseURL  string `mapstructure:"base_url"`
	Database string `mapstructure:"database"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type ElasticsearchConfig struct {
	Addresses         []string `mapstructure:"addresses"`
	Username          string   `mapstructure:"username"`
	Password          string   `mapstructure:"password"`
	HoldingsIndexName string   `mapstructure:"holdings_index_name"`
	WalletsIndexName  string   `mapstructure:"wallets_index_name"`
}

// LarkConfig Lark 配置
type LarkConfig struct {
	Webhook string `mapstructure:"webhook"`
}

// LogConfig Log 日志配置
type LogConfig struct {
	Level string `mapstructure:"level"`
}

type WorkerConfig struct {
	WorkerNum int `mapstructure:"worker_num"`
}

type MonitorConfig struct {
	Enable         bool   `mapstructure:"enable"`
	PrometheusAddr string `mapstructure:"prometheus_addr"`
}

type MoralisConfig struct {
	BaseURL    string `mapstructure:"base_url"`
	GatewayURL string `mapstructure:"gateway_url"`
	APIKey     string `mapstructure:"api_key"`
	RateLimit  int    `mapstructure:"rate_limit"`
	Timeout    int    `mapstructure:"timeout"`
}

func InitConfig() Config {
	var config Config

	viper.SetConfigName("config.worker")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config/")

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := mapstructure.Decode(viper.AllSettings(), &config); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	return config
}

func WatchConfig(config *Config) {
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		newConfig := InitConfig()
		*config = newConfig
		logger.SetLogLevel(config.Log.Level)
	})
}
