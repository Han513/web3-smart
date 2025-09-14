package monitor

import (
	"context"
	"net/http"
	"time"
	"web3-smart/internal/worker/config"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsServer struct {
	cfg    config.MonitorConfig
	server *http.Server
}

func NewMetricsServer(cfg config.MonitorConfig) *MetricsServer {
	if !cfg.Enable || cfg.PrometheusAddr == "" {
		return &MetricsServer{cfg: cfg}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &MetricsServer{
		cfg: cfg,
		server: &http.Server{
			Addr:    cfg.PrometheusAddr,
			Handler: mux,
		},
	}
}

// Run 启动指标暴露服务
func (s *MetricsServer) Run() {
	if s.server == nil {
		return // disabled
	}

	go func() {
		s.server.ListenAndServe()
	}()
}

// Stop 优雅关闭 HTTP 服务
func (s *MetricsServer) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil // disabled
	}

	s.server.SetKeepAlivesEnabled(false)
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return s.server.Shutdown(shutdownCtx)
}
