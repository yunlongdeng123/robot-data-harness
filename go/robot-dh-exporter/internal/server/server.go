package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HealthSnapshot 描述 /healthz 当前观察到的 exporter 健康度。
// 由调用方在每次 scrape 完成后写入；server 端只做线程安全读取。
type HealthSnapshot struct {
	DBConnected     bool      `json:"db_connected"`
	LastScrapeTime  time.Time `json:"last_scrape_time"`
	LastScrapeError string    `json:"last_scrape_error,omitempty"`
}

// HealthProvider 抽象 health 数据来源，避免 server 包反向依赖 metrics 包。
type HealthProvider interface {
	Snapshot() HealthSnapshot
}

// New 返回带 /metrics + /healthz 的 HTTP server。health 允许为 nil（向后兼容旧测试）。
func New(addr string, reg *prometheus.Registry, logger *slog.Logger, health ...HealthProvider) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          reg,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// 没注入 provider 时仍保留最小响应，向后兼容现有 readinessProbe。
		if len(health) == 0 || health[0] == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		snap := health[0].Snapshot()
		// DB 未连接时返回 503，让 readinessProbe 把 Pod 从 endpoint 摘除。
		if !snap.DBConnected {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":            statusFromSnapshot(snap),
			"db_connected":      snap.DBConnected,
			"last_scrape_time":  snap.LastScrapeTime.UTC().Format(time.RFC3339),
			"last_scrape_error": snap.LastScrapeError,
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("robot-dh-exporter\nGET /metrics for Prometheus metrics\nGET /healthz for liveness\n"))
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func statusFromSnapshot(snap HealthSnapshot) string {
	if !snap.DBConnected {
		return "degraded"
	}
	if snap.LastScrapeError != "" {
		return "warn"
	}
	return "ok"
}

// Run 启动 server 并在 ctx 取消时 graceful 关闭。
func Run(ctx context.Context, server *http.Server, logger *slog.Logger) error {
	errCh := make(chan error, 1)
	go func() {
		logger.Info("exporter listening", "addr", server.Addr)
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		return err
	}
}
