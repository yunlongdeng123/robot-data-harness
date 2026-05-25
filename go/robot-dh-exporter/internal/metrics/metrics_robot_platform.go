package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/robot-data-harness/robot-dh-exporter/internal/db"
)

// 平台层（v1.6 引入）的额外 Prometheus 指标；与早期指标共存，名称不重叠。

type platformGauges struct {
	qcContractsTotal       *prometheus.GaugeVec // family, enabled
	qcContractRunsTotal    *prometheus.GaugeVec // family, contract_id, status
	qcContractDurationSec  *prometheus.GaugeVec // family, contract_id, status
	workflowsTotal         *prometheus.GaugeVec // workflow_type, status
	workflowStepsTotal     *prometheus.GaugeVec // step_name, phase
	workflowStepDuration   *prometheus.GaugeVec // step_name, phase
	assetProfilesTotal     *prometheus.GaugeVec // family, asset_format, status
	assetProfileBytes      *prometheus.GaugeVec // family, asset_format
	assetProfileRows       *prometheus.GaugeVec // family, asset_format
	mlReadyDatasetsTotal   *prometheus.GaugeVec // family, status
	mlReadyRows            *prometheus.GaugeVec // family, split
	datasetPartitionsTotal *prometheus.GaugeVec // family, partition_type, status
	taskHeartbeatAge       *prometheus.GaugeVec // phase
	openlineageEventsTotal *prometheus.GaugeVec // event_type
}

func registerPlatform(reg prometheus.Registerer) *platformGauges {
	g := &platformGauges{
		qcContractsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_qc_contracts_total",
			Help: "QC contracts grouped by dataset_family and enabled flag.",
		}, []string{"dataset_family", "enabled"}),
		qcContractRunsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_qc_contract_runs_total",
			Help: "QC contract runs grouped by family / contract_id / status.",
		}, []string{"dataset_family", "contract_id", "status"}),
		qcContractDurationSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_qc_contract_duration_seconds",
			Help: "Sum of duration_sec from qc_contract_runs grouped by family / contract / status.",
		}, []string{"dataset_family", "contract_id", "status"}),
		workflowsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_workflows_total",
			Help: "Workflow runs grouped by workflow_type and status.",
		}, []string{"workflow_type", "status"}),
		workflowStepsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_workflow_steps_total",
			Help: "Workflow steps grouped by step_name and phase.",
		}, []string{"step_name", "phase"}),
		workflowStepDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_workflow_step_duration_seconds",
			Help: "Sum of step duration_sec grouped by step_name and phase.",
		}, []string{"step_name", "phase"}),
		assetProfilesTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_asset_profiles_total",
			Help: "Asset profiles grouped by dataset_family / asset_format / status.",
		}, []string{"dataset_family", "asset_format", "status"}),
		assetProfileBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_asset_profile_bytes",
			Help: "Sum of asset_profiles.bytes grouped by family and asset_format.",
		}, []string{"dataset_family", "asset_format"}),
		assetProfileRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_asset_profile_rows",
			Help: "Sum of asset_profiles.rows grouped by family and asset_format.",
		}, []string{"dataset_family", "asset_format"}),
		mlReadyDatasetsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ml_ready_datasets_total",
			Help: "ML-ready datasets grouped by family / status.",
		}, []string{"dataset_family", "status"}),
		mlReadyRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_ml_ready_rows",
			Help: "ML-ready row counts grouped by family / split (train/val/test).",
		}, []string{"dataset_family", "split"}),
		datasetPartitionsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_dataset_partitions_total",
			Help: "Dataset partitions grouped by family / partition_type / status.",
		}, []string{"dataset_family", "partition_type", "status"}),
		taskHeartbeatAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_task_heartbeat_age_seconds",
			Help: "Age (seconds) of the most recent task_heartbeats row per phase.",
		}, []string{"phase"}),
		openlineageEventsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "robot_dh_openlineage_events_total",
			Help: "OpenLineage events grouped by event_type.",
		}, []string{"event_type"}),
	}
	for _, c := range []prometheus.Collector{
		g.qcContractsTotal, g.qcContractRunsTotal, g.qcContractDurationSec,
		g.workflowsTotal, g.workflowStepsTotal, g.workflowStepDuration,
		g.assetProfilesTotal, g.assetProfileBytes, g.assetProfileRows,
		g.mlReadyDatasetsTotal, g.mlReadyRows,
		g.datasetPartitionsTotal, g.taskHeartbeatAge, g.openlineageEventsTotal,
	} {
		reg.MustRegister(c)
	}
	return g
}

// scrapePlatform 抓平台层（v1.6 引入的 9 张表）；任何失败把 ok=false 并 warn，不阻断主流程。
func (c *Collector) scrapePlatform(ctx context.Context) bool {
	q := c.querier
	g := c.gaugesPlat
	ok := true
	if q == nil || g == nil {
		return ok
	}

	if buckets, has, err := q.QcContractBuckets(ctx); err == nil && has {
		g.qcContractsTotal.Reset()
		for _, b := range buckets {
			g.qcContractsTotal.WithLabelValues(b.DatasetFamily, b.Enabled).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("qc_contracts failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.QcRunBuckets(ctx); err == nil && has {
		g.qcContractRunsTotal.Reset()
		g.qcContractDurationSec.Reset()
		for _, b := range buckets {
			g.qcContractRunsTotal.WithLabelValues(b.DatasetFamily, b.ContractID, b.Status).Set(float64(b.Count))
			g.qcContractDurationSec.WithLabelValues(b.DatasetFamily, b.ContractID, b.Status).Set(b.DurationSec)
		}
	} else if err != nil {
		c.logger.Warn("qc_contract_runs failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.WorkflowRunBuckets(ctx); err == nil && has {
		g.workflowsTotal.Reset()
		for _, b := range buckets {
			g.workflowsTotal.WithLabelValues(b.WorkflowType, b.Status).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("workflow_runs failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.WorkflowStepBuckets(ctx); err == nil && has {
		g.workflowStepsTotal.Reset()
		g.workflowStepDuration.Reset()
		for _, b := range buckets {
			g.workflowStepsTotal.WithLabelValues(b.StepName, b.Phase).Set(float64(b.Count))
			g.workflowStepDuration.WithLabelValues(b.StepName, b.Phase).Set(b.DurationSec)
		}
	} else if err != nil {
		c.logger.Warn("workflow_steps failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.AssetProfileBuckets(ctx); err == nil && has {
		g.assetProfilesTotal.Reset()
		g.assetProfileBytes.Reset()
		g.assetProfileRows.Reset()
		for _, b := range buckets {
			g.assetProfilesTotal.WithLabelValues(b.DatasetFamily, b.AssetFormat, b.Status).Set(float64(b.Count))
			g.assetProfileBytes.WithLabelValues(b.DatasetFamily, b.AssetFormat).Add(b.Bytes)
			g.assetProfileRows.WithLabelValues(b.DatasetFamily, b.AssetFormat).Add(b.Rows)
		}
	} else if err != nil {
		c.logger.Warn("asset_profiles failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.MlReadyBuckets(ctx); err == nil && has {
		g.mlReadyDatasetsTotal.Reset()
		g.mlReadyRows.Reset()
		for _, b := range buckets {
			g.mlReadyDatasetsTotal.WithLabelValues(b.DatasetFamily, b.Status).Set(float64(b.Count))
			g.mlReadyRows.WithLabelValues(b.DatasetFamily, "train").Add(b.NumTrain)
			g.mlReadyRows.WithLabelValues(b.DatasetFamily, "val").Add(b.NumVal)
			g.mlReadyRows.WithLabelValues(b.DatasetFamily, "test").Add(b.NumTest)
		}
	} else if err != nil {
		c.logger.Warn("ml_ready_datasets failed", "error", err)
		ok = false
	}

	if buckets, has, err := q.DatasetPartitionBuckets(ctx); err == nil && has {
		g.datasetPartitionsTotal.Reset()
		for _, b := range buckets {
			g.datasetPartitionsTotal.WithLabelValues(b.DatasetFamily, b.PartitionType, b.Status).Set(float64(b.Count))
		}
	} else if err != nil {
		c.logger.Warn("dataset_partitions failed", "error", err)
		ok = false
	}

	if ages, has, err := q.LatestHeartbeatAgeByPhase(ctx); err == nil && has {
		g.taskHeartbeatAge.Reset()
		for _, a := range ages {
			g.taskHeartbeatAge.WithLabelValues(a.Phase).Set(a.AgeSec)
		}
	} else if err != nil {
		c.logger.Warn("task_heartbeats failed", "error", err)
		ok = false
	}

	if counts, has, err := q.OpenLineageEventCounts(ctx); err == nil && has {
		g.openlineageEventsTotal.Reset()
		for et, n := range counts {
			g.openlineageEventsTotal.WithLabelValues(et).Set(float64(n))
		}
	} else if err != nil {
		c.logger.Warn("openlineage_events failed", "error", err)
		ok = false
	}

	return ok
}

// _ 静态校验：platformGauges 字段名都被引用，避免 lint 误删。
var _ = func(_ *db.Querier) {}
