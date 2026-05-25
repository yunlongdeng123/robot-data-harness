package metrics

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// 验证：querier=nil 时 scrape 不 panic、不阻塞、把 exporter_up 设为 0。
func TestScrape_NoQuerier_DoesNotPanic(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	c := MustRegister(reg, nil, time.Second, logger)
	if err := c.scrape(context.Background()); err != nil {
		t.Fatalf("scrape with nil querier should not return error, got %v", err)
	}
	if v := testutil.ToFloat64(c.gauges.exporterUp); v != 0 {
		t.Fatalf("expected exporter_up=0 with nil querier, got %v", v)
	}
}

// 验证：MustRegister 把所有平台层 metric（v1.6 引入的 9 张表对应指标）注册到 registry。
//
// GaugeVec 默认在没有任何 label 取过值时不会出现在 Gather() 里；这里通过
// Registry.Describe 间接拿到 metric 名称，验证注册而不要求实际值。
func TestMustRegister_RegistersPlatformMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	_ = MustRegister(reg, nil, 30*time.Second, logger)

	descs := make(chan *prometheus.Desc, 64)
	go func() {
		defer close(descs)
		// nil 表示遍历所有已注册 collector
		reg.Describe(descs)
	}()
	got := make(map[string]bool, 32)
	for d := range descs {
		s := d.String()
		// Desc 字符串形如 Desc{fqName: "robot_dh_xxx", help: ...}
		if i := strings.Index(s, `fqName: "`); i >= 0 {
			tail := s[i+len(`fqName: "`):]
			if j := strings.Index(tail, `"`); j > 0 {
				got[tail[:j]] = true
			}
		}
	}
	wanted := []string{
		"robot_dh_qc_contracts_total",
		"robot_dh_qc_contract_runs_total",
		"robot_dh_qc_contract_duration_seconds",
		"robot_dh_workflows_total",
		"robot_dh_workflow_steps_total",
		"robot_dh_workflow_step_duration_seconds",
		"robot_dh_asset_profiles_total",
		"robot_dh_asset_profile_bytes",
		"robot_dh_asset_profile_rows",
		"robot_dh_ml_ready_datasets_total",
		"robot_dh_ml_ready_rows",
		"robot_dh_dataset_partitions_total",
		"robot_dh_task_heartbeat_age_seconds",
		"robot_dh_openlineage_events_total",
	}
	for _, name := range wanted {
		if !got[name] {
			t.Errorf("metric %s not registered (got %v)", name, got)
		}
	}
}
