package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// V1.6 query helpers：QC contracts / contract runs / workflows / steps / asset profiles /
// ml-ready datasets / dataset partitions / heartbeats / openlineage events.
//
// 设计：与 v1.5 一致的 "table 不存在 -> 返回 (zero, false, nil)" 风格；
// 仅查询，不写入；不打印 secret；查询带 ctx timeout 由调用方控制。

// QcContractCount returns counts grouped by (dataset_family, enabled).
type QcContractBucket struct {
	DatasetFamily string
	Enabled       string // "true" / "false"
	Count         int64
}

// QcContractBuckets：按 (dataset_family, enabled) 统计 qc_contracts。
func (q *Querier) QcContractBuckets(ctx context.Context) ([]QcContractBucket, bool, error) {
	has, err := q.HasTable(ctx, "qc_contracts")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(dataset_family,'') AS family,
               CASE WHEN enabled THEN 'true' ELSE 'false' END AS en,
               COUNT(*)
        FROM qc_contracts GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []QcContractBucket
	for rows.Next() {
		var b QcContractBucket
		if err := rows.Scan(&b.DatasetFamily, &b.Enabled, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// QcRunBucket：(dataset_family, contract_id, status) 维度的 qc_contract_runs 统计。
type QcRunBucket struct {
	DatasetFamily string
	ContractID    string
	Status        string
	Count         int64
	DurationSec   float64
}

func (q *Querier) QcRunBuckets(ctx context.Context) ([]QcRunBucket, bool, error) {
	has, err := q.HasTable(ctx, "qc_contract_runs")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(dataset_family,'') AS family,
               COALESCE(contract_id,'') AS contract_id,
               COALESCE(status,'') AS status,
               COUNT(*),
               COALESCE(SUM(duration_sec), 0)
        FROM qc_contract_runs GROUP BY 1, 2, 3`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []QcRunBucket
	for rows.Next() {
		var b QcRunBucket
		if err := rows.Scan(&b.DatasetFamily, &b.ContractID, &b.Status, &b.Count, &b.DurationSec); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// WorkflowRunBucket：按 (workflow_type, status) 维度的 workflow_runs 统计。
type WorkflowRunBucket struct {
	WorkflowType string
	Status       string
	Count        int64
}

func (q *Querier) WorkflowRunBuckets(ctx context.Context) ([]WorkflowRunBucket, bool, error) {
	has, err := q.HasTable(ctx, "workflow_runs")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(workflow_type,'') AS wt,
               COALESCE(status,'') AS s,
               COUNT(*)
        FROM workflow_runs GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []WorkflowRunBucket
	for rows.Next() {
		var b WorkflowRunBucket
		if err := rows.Scan(&b.WorkflowType, &b.Status, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// WorkflowStepBucket：按 (step_name, phase) 维度统计 workflow_steps；同时聚合 duration。
type WorkflowStepBucket struct {
	StepName    string
	Phase       string
	Count       int64
	DurationSec float64
}

func (q *Querier) WorkflowStepBuckets(ctx context.Context) ([]WorkflowStepBucket, bool, error) {
	has, err := q.HasTable(ctx, "workflow_steps")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(step_name,'') AS sn,
               COALESCE(phase,'') AS p,
               COUNT(*),
               COALESCE(SUM(duration_sec), 0)
        FROM workflow_steps GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []WorkflowStepBucket
	for rows.Next() {
		var b WorkflowStepBucket
		if err := rows.Scan(&b.StepName, &b.Phase, &b.Count, &b.DurationSec); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// AssetProfileBucket：按 (dataset_family, asset_format, status) 维度的 asset_profiles 统计；同时聚合 bytes / rows。
type AssetProfileBucket struct {
	DatasetFamily string
	AssetFormat   string
	Status        string
	Count         int64
	Bytes         float64
	Rows          float64
}

func (q *Querier) AssetProfileBuckets(ctx context.Context) ([]AssetProfileBucket, bool, error) {
	has, err := q.HasTable(ctx, "asset_profiles")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(dataset_family,'') AS family,
               COALESCE(asset_format,'') AS af,
               COALESCE(status,'') AS s,
               COUNT(*),
               COALESCE(SUM(bytes), 0),
               COALESCE(SUM(rows), 0)
        FROM asset_profiles GROUP BY 1, 2, 3`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []AssetProfileBucket
	for rows.Next() {
		var b AssetProfileBucket
		if err := rows.Scan(&b.DatasetFamily, &b.AssetFormat, &b.Status, &b.Count, &b.Bytes, &b.Rows); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// MlReadyBucket：按 (dataset_family, status) 维度的 ml_ready_datasets 统计；同时聚合 num_train / num_val / num_test。
type MlReadyBucket struct {
	DatasetFamily string
	Status        string
	Count         int64
	NumTrain      float64
	NumVal        float64
	NumTest       float64
}

func (q *Querier) MlReadyBuckets(ctx context.Context) ([]MlReadyBucket, bool, error) {
	has, err := q.HasTable(ctx, "ml_ready_datasets")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(dataset_family,'') AS family,
               COALESCE(status,'') AS s,
               COUNT(*),
               COALESCE(SUM(num_train), 0),
               COALESCE(SUM(num_val), 0),
               COALESCE(SUM(num_test), 0)
        FROM ml_ready_datasets GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []MlReadyBucket
	for rows.Next() {
		var b MlReadyBucket
		if err := rows.Scan(&b.DatasetFamily, &b.Status, &b.Count, &b.NumTrain, &b.NumVal, &b.NumTest); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// DatasetPartitionBucket：按 (dataset_family, partition_type, status) 统计 dataset_partitions。
type DatasetPartitionBucket struct {
	DatasetFamily string
	PartitionType string
	Status        string
	Count         int64
}

func (q *Querier) DatasetPartitionBuckets(ctx context.Context) ([]DatasetPartitionBucket, bool, error) {
	has, err := q.HasTable(ctx, "dataset_partitions")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(dataset_family,'') AS family,
               COALESCE(partition_type,'') AS pt,
               COALESCE(status,'') AS s,
               COUNT(*)
        FROM dataset_partitions GROUP BY 1, 2, 3`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []DatasetPartitionBucket
	for rows.Next() {
		var b DatasetPartitionBucket
		if err := rows.Scan(&b.DatasetFamily, &b.PartitionType, &b.Status, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// TaskHeartbeatAge：按 phase 维度，取最新一条 heartbeat 距今的 age（秒）。
type TaskHeartbeatAge struct {
	Phase  string
	AgeSec float64
}

func (q *Querier) LatestHeartbeatAgeByPhase(ctx context.Context) ([]TaskHeartbeatAge, bool, error) {
	has, err := q.HasTable(ctx, "task_heartbeats")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(phase, '') AS p,
               EXTRACT(EPOCH FROM (now() - MAX(updated_at))) AS age_sec
        FROM task_heartbeats GROUP BY 1`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []TaskHeartbeatAge
	for rows.Next() {
		var t TaskHeartbeatAge
		if err := rows.Scan(&t.Phase, &t.AgeSec); err != nil {
			return nil, true, err
		}
		out = append(out, t)
	}
	return out, true, rows.Err()
}

// OpenLineageEventCount：按 event_type 统计 openlineage_events。
func (q *Querier) OpenLineageEventCounts(ctx context.Context) (map[string]int64, bool, error) {
	has, err := q.HasTable(ctx, "openlineage_events")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, fmt.Sprintf(
		`SELECT COALESCE(%s, '') AS et, COUNT(*) FROM %s GROUP BY 1`,
		pgx.Identifier{"event_type"}.Sanitize(),
		pgx.Identifier{"openlineage_events"}.Sanitize(),
	))
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	out := make(map[string]int64, 4)
	for rows.Next() {
		var et string
		var n int64
		if err := rows.Scan(&et, &n); err != nil {
			return nil, true, err
		}
		out[strings.ToLower(et)] = n
	}
	return out, true, rows.Err()
}
