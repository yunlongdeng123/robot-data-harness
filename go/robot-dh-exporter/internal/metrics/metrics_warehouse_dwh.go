package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// v1.8 warehouse / quality-ops Prometheus 指标。
// 与 v1.5 / v1.6 指标完全独立，名字不重叠；表不存在时 silently 跳过，不 panic。
//
// 指标列表（promptC §8）：
//   robot_dh_warehouse_rows_total{table}
//   robot_dh_ads_quality_score{dataset_family,dataset_id}
//   robot_dh_ads_qc_pass_rate{dataset_family,dataset_id}
//   robot_dh_ads_etl_success_rate{dataset_family,dataset_id}
//   robot_dh_backfill_tasks_total{status,phase}
//   robot_dh_sla_checks_total{status,policy_id}

type warehouseGauges struct {
	rowsTotal        *prometheus.GaugeVec // table
	adsQualityScore  *prometheus.GaugeVec // dataset_family, dataset_id
	adsQcPassRate    *prometheus.GaugeVec // dataset_family, dataset_id
	adsEtlSuccessRt  *prometheus.GaugeVec // dataset_family, dataset_id
	adsWfSuccessRate *prometheus.GaugeVec // dataset_family, dataset_id
	backfillTasksT   *prometheus.GaugeVec // status, phase
	slaChecksTotal   *prometheus.GaugeVec // status, policy_id
}

func registerWarehouse(reg prometheus.Registerer) *warehouseGauges {
	g := &warehouseGauges{
		rowsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_warehouse_rows_total",
			Help: "Row count of v1.8 warehouse tables (dim/fact/dws/ads/backfill/sla).",
		}, []string{"table"}),
		adsQualityScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ads_quality_score",
			Help: "ads_quality_dashboard.quality_score for the latest dt, grouped by family/dataset_id.",
		}, []string{"dataset_family", "dataset_id"}),
		adsQcPassRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ads_qc_pass_rate",
			Help: "ads_quality_dashboard.qc_pass_rate for the latest dt, grouped by family/dataset_id.",
		}, []string{"dataset_family", "dataset_id"}),
		adsEtlSuccessRt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ads_etl_success_rate",
			Help: "ads_quality_dashboard.etl_success_rate for the latest dt, grouped by family/dataset_id.",
		}, []string{"dataset_family", "dataset_id"}),
		adsWfSuccessRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ads_workflow_success_rate",
			Help: "ads_quality_dashboard.workflow_success_rate for the latest dt, grouped by family/dataset_id.",
		}, []string{"dataset_family", "dataset_id"}),
		backfillTasksT: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_backfill_tasks_total",
			Help: "Count of backfill_tasks grouped by status and phase.",
		}, []string{"status", "phase"}),
		slaChecksTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_sla_checks_total",
			Help: "Count of sla_checks grouped by status and policy_id.",
		}, []string{"status", "policy_id"}),
	}
	for _, c := range []prometheus.Collector{
		g.rowsTotal, g.adsQualityScore, g.adsQcPassRate, g.adsEtlSuccessRt,
		g.adsWfSuccessRate, g.backfillTasksT, g.slaChecksTotal,
	} {
		reg.MustRegister(c)
	}
	return g
}

// scrapeWarehouse 抓 v1.8 数仓层；任何子查询失败把 ok=false 并 warn，不阻断主流程。
func (c *Collector) scrapeWarehouse(ctx context.Context) bool {
	q := c.querier
	g := c.gaugesWh
	ok := true
	if q == nil || g == nil {
		return ok
	}

	if buckets, has, err := q.WarehouseRowCounts(ctx); err == nil && has {
		g.rowsTotal.Reset()
		for _, b := range buckets {
			g.rowsTotal.WithLabelValues(b.Table).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("warehouse_row_counts failed", "error", err)
		ok = false
	}

	if rows, has, err := q.AdsQualityLatest(ctx); err == nil && has {
		g.adsQualityScore.Reset()
		g.adsQcPassRate.Reset()
		g.adsEtlSuccessRt.Reset()
		g.adsWfSuccessRate.Reset()
		for _, r := range rows {
			g.adsQualityScore.WithLabelValues(r.DatasetFamily, r.DatasetID).Set(r.QualityScore)
			g.adsQcPassRate.WithLabelValues(r.DatasetFamily, r.DatasetID).Set(r.QcPassRate)
			g.adsEtlSuccessRt.WithLabelValues(r.DatasetFamily, r.DatasetID).Set(r.EtlSuccessRate)
			g.adsWfSuccessRate.WithLabelValues(r.DatasetFamily, r.DatasetID).Set(r.WorkflowSuccessRate)
		}
	} else if err != nil {
		c.logger.Warn("ads_quality_dashboard failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.BackfillTaskBuckets(ctx); err == nil && has {
		g.backfillTasksT.Reset()
		for _, b := range buckets {
			g.backfillTasksT.WithLabelValues(b.Status, b.Phase).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("backfill_tasks failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.SlaCheckBuckets(ctx); err == nil && has {
		g.slaChecksTotal.Reset()
		for _, b := range buckets {
			g.slaChecksTotal.WithLabelValues(b.Status, b.PolicyID).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("sla_checks failed", "error", err)
		ok = false
	}
	return ok
}
