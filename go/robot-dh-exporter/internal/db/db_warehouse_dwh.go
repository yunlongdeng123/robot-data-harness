package db

import (
	"context"
)

// v1.8 warehouse / quality-ops 表的查询 helper。
//
// 设计：与 v1.5 / v1.6 一致的 "table 不存在 -> 返回 (zero, false, nil)" 风格；
// 不写入；不打印 secret。表名硬编码在查询 SQL 里，列名固定，与
// postgres/migrations/006_v1_8_warehouse_quality_ops.sql 严格对齐。

// WarehouseRowCount：14 张 v1.8 表行数。
type WarehouseRowCount struct {
	Table string
	Count int64
}

var warehouseTablesV1_8 = []string{
	"dim_dataset",
	"fact_etl_run",
	"fact_qc_rule_result",
	"fact_workflow_step",
	"fact_asset_profile",
	"dws_dataset_quality_daily",
	"dws_rule_failure_daily",
	"dws_workflow_ops_daily",
	"ads_quality_dashboard",
	"ads_workflow_ops_dashboard",
	"backfill_plans",
	"backfill_tasks",
	"sla_policies",
	"sla_checks",
}

// WarehouseRowCounts：14 张表逐表 count(*)，缺表 silently 跳过。
func (q *Querier) WarehouseRowCounts(ctx context.Context) ([]WarehouseRowCount, bool, error) {
	out := make([]WarehouseRowCount, 0, len(warehouseTablesV1_8))
	anyHas := false
	for _, t := range warehouseTablesV1_8 {
		n, has, err := q.CountSingle(ctx, t)
		if err != nil {
			return out, anyHas, err
		}
		if !has {
			continue
		}
		anyHas = true
		out = append(out, WarehouseRowCount{Table: t, Count: n})
	}
	return out, anyHas, nil
}

// AdsQualityRow：ads_quality_dashboard 最近一日的关键字段。
type AdsQualityRow struct {
	DatasetFamily       string
	DatasetID           string
	Version             string
	QualityScore        float64
	QcPassRate          float64
	EtlSuccessRate      float64
	WorkflowSuccessRate float64
}

// AdsQualityLatest：取 ads_quality_dashboard 最新 dt 这一天的所有行。
// 返回 (rows, has, err)。has=false 表示表不存在或表里没数据。
func (q *Querier) AdsQualityLatest(ctx context.Context) ([]AdsQualityRow, bool, error) {
	has, err := q.HasTable(ctx, "ads_quality_dashboard")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        WITH latest AS (SELECT MAX(dt) AS d FROM ads_quality_dashboard)
        SELECT COALESCE(dataset_family,''),
               COALESCE(dataset_id,''),
               COALESCE(version,''),
               COALESCE(quality_score, 0),
               COALESCE(qc_pass_rate, 0),
               COALESCE(etl_success_rate, 0),
               COALESCE(workflow_success_rate, 0)
        FROM ads_quality_dashboard a
        JOIN latest l ON a.dt = l.d`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []AdsQualityRow
	for rows.Next() {
		var r AdsQualityRow
		if err := rows.Scan(
			&r.DatasetFamily, &r.DatasetID, &r.Version,
			&r.QualityScore, &r.QcPassRate, &r.EtlSuccessRate, &r.WorkflowSuccessRate,
		); err != nil {
			return nil, true, err
		}
		out = append(out, r)
	}
	return out, true, rows.Err()
}

// BackfillTaskBucket：(status, phase) 维度的 backfill_tasks 计数。
type BackfillTaskBucket struct {
	Status string
	Phase  string
	Count  int64
}

func (q *Querier) BackfillTaskBuckets(ctx context.Context) ([]BackfillTaskBucket, bool, error) {
	has, err := q.HasTable(ctx, "backfill_tasks")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(status,'') AS s,
               COALESCE(phase,'')  AS p,
               COUNT(*)
        FROM backfill_tasks GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []BackfillTaskBucket
	for rows.Next() {
		var b BackfillTaskBucket
		if err := rows.Scan(&b.Status, &b.Phase, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// SlaCheckBucket：(status, policy_id) 维度的 sla_checks 计数。
type SlaCheckBucket struct {
	Status   string
	PolicyID string
	Count    int64
}

func (q *Querier) SlaCheckBuckets(ctx context.Context) ([]SlaCheckBucket, bool, error) {
	has, err := q.HasTable(ctx, "sla_checks")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(status,'')    AS s,
               COALESCE(policy_id,'') AS pid,
               COUNT(*)
        FROM sla_checks GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []SlaCheckBucket
	for rows.Next() {
		var b SlaCheckBucket
		if err := rows.Scan(&b.Status, &b.PolicyID, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}
