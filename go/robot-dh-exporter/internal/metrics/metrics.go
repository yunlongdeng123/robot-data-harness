package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/robot-data-harness/robot-dh-exporter/internal/db"
)

// Collector 把 robot-dh PG 元数据扫描结果转成 Prometheus 指标。
//
// 采用 "pull + 后台刷新" 模式：HTTP /metrics 请求时返回最近一次的 cached gauge 值，
// 避免每次抓取都打 DB。后台 goroutine 按 scrapeInterval 周期刷新；刷新失败时只把
// robot_dh_exporter_up 设为 0，不抛错。
type Collector struct {
	mu       sync.RWMutex
	logger   *slog.Logger
	querier  *db.Querier
	interval time.Duration

	gauges     *registeredGauges
	gaugesPlat *platformGauges
	gaugesWh   *warehouseGauges

	lastScrape    time.Time
	lastScrapeErr string
	dbConnected   bool
}

type registeredGauges struct {
	datasetsTotal      prometheus.Gauge
	lakeAssetsTotal    *prometheus.GaugeVec
	etlJobsTotal       *prometheus.GaugeVec
	etlFailuresTotal   *prometheus.GaugeVec
	etlDurationSeconds *prometheus.GaugeVec
	etlInputBytes      *prometheus.GaugeVec
	etlOutputBytes     *prometheus.GaugeVec
	benchmarkCases     *prometheus.GaugeVec
	argoWorkflows      *prometheus.GaugeVec
	qualityScoreAvg    prometheus.Gauge
	runtimeEvents      *prometheus.GaugeVec
	exporterUp         prometheus.Gauge
	lastScrapeSec      prometheus.Gauge
	scrapeDuration     prometheus.Gauge
}

// MustRegister 创建并注册指标；返回 collector。
func MustRegister(reg prometheus.Registerer, querier *db.Querier, interval time.Duration, logger *slog.Logger) *Collector {
	g := &registeredGauges{
		datasetsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "robot_dh_datasets_total",
			Help: "Total datasets registered in robot-dh.",
		}),
		lakeAssetsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_lake_assets_total",
			Help: "Lake assets per layer.",
		}, []string{"layer"}),
		etlJobsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_etl_jobs_total",
			Help: "ETL jobs grouped by status and job_type.",
		}, []string{"status", "job_type"}),
		etlFailuresTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_etl_failures_total",
			Help: "Failed ETL perf records grouped by phase.",
		}, []string{"phase"}),
		etlDurationSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_etl_duration_seconds",
			Help: "Sum of duration_sec per phase from etl_perf_runs.",
		}, []string{"phase"}),
		etlInputBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_etl_input_bytes",
			Help: "Sum of input_bytes per phase from etl_perf_runs.",
		}, []string{"phase"}),
		etlOutputBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_etl_output_bytes",
			Help: "Sum of output_bytes per phase from etl_perf_runs.",
		}, []string{"phase"}),
		benchmarkCases: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_benchmark_cases_total",
			Help: "Benchmark cases grouped by passed (true/false/unknown).",
		}, []string{"passed"}),
		argoWorkflows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_argo_workflows_total",
			Help: "Argo workflows grouped by status.",
		}, []string{"status"}),
		qualityScoreAvg: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "robot_dh_quality_score_avg",
			Help: "Average quality_score across quality_snapshots.",
		}),
		runtimeEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_runtime_events_total",
			Help: "Runtime events grouped by event_type.",
		}, []string{"event_type"}),
		exporterUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "robot_dh_exporter_up",
			Help: "1 if the last scrape succeeded, 0 otherwise.",
		}),
		lastScrapeSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "robot_dh_exporter_last_scrape_timestamp_seconds",
			Help: "Unix timestamp of the last successful scrape.",
		}),
		scrapeDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "robot_dh_exporter_scrape_duration_seconds",
			Help: "Wall-clock duration of the most recent scrape.",
		}),
	}
	for _, c := range []prometheus.Collector{
		g.datasetsTotal,
		g.lakeAssetsTotal,
		g.etlJobsTotal,
		g.etlFailuresTotal,
		g.etlDurationSeconds,
		g.etlInputBytes,
		g.etlOutputBytes,
		g.benchmarkCases,
		g.argoWorkflows,
		g.qualityScoreAvg,
		g.runtimeEvents,
		g.exporterUp,
		g.lastScrapeSec,
		g.scrapeDuration,
	} {
		reg.MustRegister(c)
	}
	platGauges := registerPlatform(reg)
	whGauges := registerWarehouse(reg)
	return &Collector{
		logger:     logger,
		querier:    querier,
		interval:   interval,
		gauges:     g,
		gaugesPlat: platGauges,
		gaugesWh:   whGauges,
	}
}

// Snapshot 返回当前 health 快照；供 server /healthz 调用。
func (c *Collector) Snapshot() HealthSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return HealthSnapshot{
		DBConnected:     c.dbConnected,
		LastScrapeTime:  c.lastScrape,
		LastScrapeError: c.lastScrapeErr,
	}
}

// HealthSnapshot 复述 server.HealthSnapshot 字段；避免 metrics 反向依赖 server 包。
type HealthSnapshot struct {
	DBConnected     bool
	LastScrapeTime  time.Time
	LastScrapeError string
}

// Start 启动后台周期刷新；调用方在 ctx 取消时应等待返回。
func (c *Collector) Start(ctx context.Context) {
	if err := c.scrape(ctx); err != nil {
		c.logger.Warn("initial scrape failed", "error", err)
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.scrape(ctx); err != nil {
				c.logger.Warn("scrape failed", "error", err)
			}
		}
	}
}

// scrape 一次性扫描所有目标表，把结果写回 gauges。
func (c *Collector) scrape(ctx context.Context) error {
	if c.querier == nil {
		c.gauges.exporterUp.Set(0)
		return nil
	}
	start := time.Now()
	scrapeCtx, cancel := context.WithTimeout(ctx, c.interval)
	defer cancel()

	ok := true

	if n, has, err := c.querier.CountSingle(scrapeCtx, "datasets"); err == nil && has {
		c.gauges.datasetsTotal.Set(float64(n))
	} else if err != nil {
		c.logger.Warn("datasets count failed", "error", err)
		ok = false
	} else {
		c.gauges.datasetsTotal.Set(0)
	}

	if counts, has, err := c.querier.LabeledCounts(scrapeCtx, "lake_assets", "layer"); err == nil && has {
		c.gauges.lakeAssetsTotal.Reset()
		for layer, n := range counts {
			c.gauges.lakeAssetsTotal.WithLabelValues(layer).Set(float64(n))
		}
	} else if err != nil {
		c.logger.Warn("lake_assets failed", "error", err)
		ok = false
	}

	if buckets, has, err := c.querier.EtlJobBuckets(scrapeCtx); err == nil && has {
		c.gauges.etlJobsTotal.Reset()
		for _, b := range buckets {
			c.gauges.etlJobsTotal.WithLabelValues(b.Status, b.Type).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("etl_jobs failed", "error", err)
		ok = false
	}

	if stats, has, err := c.querier.EtlPerfByPhase(scrapeCtx); err == nil && has {
		c.gauges.etlDurationSeconds.Reset()
		c.gauges.etlInputBytes.Reset()
		c.gauges.etlOutputBytes.Reset()
		for phase, s := range stats {
			c.gauges.etlDurationSeconds.WithLabelValues(phase).Set(s.DurationSec)
			c.gauges.etlInputBytes.WithLabelValues(phase).Set(s.InputBytes)
			c.gauges.etlOutputBytes.WithLabelValues(phase).Set(s.OutputBytes)
		}
	} else if err != nil {
		c.logger.Warn("etl_perf_runs failed", "error", err)
		ok = false
	}

	if failures, has, err := c.querier.EtlFailureCountsByPhase(scrapeCtx); err == nil && has {
		c.gauges.etlFailuresTotal.Reset()
		for phase, n := range failures {
			c.gauges.etlFailuresTotal.WithLabelValues(phase).Set(float64(n))
		}
	} else if err != nil {
		c.logger.Warn("etl_failures failed", "error", err)
		ok = false
	}

	if buckets, has, err := c.querier.BenchmarkCaseBuckets(scrapeCtx); err == nil && has {
		c.gauges.benchmarkCases.Reset()
		for _, b := range buckets {
			c.gauges.benchmarkCases.WithLabelValues(b.Passed).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("benchmark_cases failed", "error", err)
		ok = false
	}

	if counts, has, err := c.querier.LabeledCounts(scrapeCtx, "argo_workflow_runs", "status"); err == nil && has {
		c.gauges.argoWorkflows.Reset()
		for status, n := range counts {
			c.gauges.argoWorkflows.WithLabelValues(status).Set(float64(n))
		}
	} else if err != nil {
		c.logger.Warn("argo_workflow_runs failed", "error", err)
		ok = false
	}

	if avg, has, err := c.querier.AvgQualityScore(scrapeCtx); err == nil && has {
		c.gauges.qualityScoreAvg.Set(avg)
	} else if err != nil {
		c.logger.Warn("quality_snapshots failed", "error", err)
		ok = false
	}

	if counts, has, err := c.querier.LabeledCounts(scrapeCtx, "runtime_events", "event_type"); err == nil && has {
		c.gauges.runtimeEvents.Reset()
		for et, n := range counts {
			c.gauges.runtimeEvents.WithLabelValues(et).Set(float64(n))
		}
	} else if err != nil {
		c.logger.Warn("runtime_events failed", "error", err)
		ok = false
	}

	if !c.scrapePlatform(scrapeCtx) {
		ok = false
	}
	if !c.scrapeWarehouse(scrapeCtx) {
		ok = false
	}
	c.gauges.scrapeDuration.Set(time.Since(start).Seconds())

	c.mu.Lock()
	c.lastScrape = time.Now()
	c.dbConnected = c.querier != nil
	if ok {
		c.lastScrapeErr = ""
	} else {
		c.lastScrapeErr = "one or more table queries failed; see warn logs"
	}
	c.mu.Unlock()

	if ok {
		c.gauges.exporterUp.Set(1)
		c.gauges.lastScrapeSec.Set(float64(time.Now().Unix()))
		return nil
	}
	c.gauges.exporterUp.Set(0)
	return nil
}
