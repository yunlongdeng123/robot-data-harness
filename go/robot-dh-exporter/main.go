// robot-dh-exporter
//
// 小型 Go 常驻进程：从 PostgreSQL 读取 robot-data-harness 元数据，
// 暴露 Prometheus 指标在 /metrics（默认 :9108），同时提供 /healthz。
//
// 设计原则：
//   - 不依赖也不修改 Python 项目；仅通过 ROBOT_DH_DB_URI 读 DB。
//   - 表缺失 / 网络故障时仅把 robot_dh_exporter_up=0，不退出进程。
//   - 不在日志中暴露任何凭据。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/robot-data-harness/robot-dh-exporter/internal/config"
	"github.com/robot-data-harness/robot-dh-exporter/internal/db"
	"github.com/robot-data-harness/robot-dh-exporter/internal/metrics"
	"github.com/robot-data-harness/robot-dh-exporter/internal/server"
)

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("robot-dh-exporter v0.1.5")
		return
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(2)
	}

	logger := newLogger(cfg.LogLevel)
	logger.Info("starting robot-dh-exporter",
		"addr", cfg.ListenAddr,
		"scrape_interval", cfg.ScrapeInterval.String(),
		"db", cfg.RedactedDSN(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	querier, err := db.New(ctx, cfg.DBURI, logger)
	if err != nil {
		logger.Error("failed to connect to PostgreSQL; exporter will run with up=0 until DB is reachable", "error", err)
		querier = nil
	}
	if querier != nil {
		defer querier.Close()
	}

	reg := prometheus.NewRegistry()
	collector := metrics.MustRegister(reg, querier, cfg.ScrapeInterval, logger)

	scrapeDone := make(chan struct{})
	if querier != nil {
		go func() {
			defer close(scrapeDone)
			collector.Start(ctx)
		}()
	} else {
		close(scrapeDone)
	}

	healthBridge := newHealthBridge(collector)
	srv := server.New(cfg.ListenAddr, reg, logger, healthBridge)
	if err := server.Run(ctx, srv, logger); err != nil {
		logger.Error("server exited with error", "error", err)
		os.Exit(1)
	}
	<-scrapeDone
	logger.Info("exporter exited cleanly")
}

// healthBridge 适配 metrics.Collector.Snapshot() 到 server.HealthProvider。
// 单独写在 main 避免 metrics ↔ server 互相 import。
type healthBridge struct {
	collector *metrics.Collector
}

func newHealthBridge(c *metrics.Collector) *healthBridge {
	return &healthBridge{collector: c}
}

func (h *healthBridge) Snapshot() server.HealthSnapshot {
	s := h.collector.Snapshot()
	return server.HealthSnapshot{
		DBConnected:     s.DBConnected,
		LastScrapeTime:  s.LastScrapeTime,
		LastScrapeError: s.LastScrapeError,
	}
}

func newLogger(level string) *slog.Logger {
	lvl := slog.LevelInfo
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
