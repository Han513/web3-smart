package config

import (
	"testing"
)

func TestInitConfig(t *testing.T) {
	cfg := InitConfig()
	t.Logf("cfg redis: %+v", cfg.Redis)
	t.Logf("cfg pg: %+v", cfg.Postgres)
	t.Logf("cfg selectdb: %+v", cfg.SelectDB)
	t.Logf("cfg elasticsearch: %+v", cfg.Elasticsearch)
	t.Logf("cfg moralis: %+v", cfg.Moralis)
}
