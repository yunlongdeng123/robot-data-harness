package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier 是 db 包对外暴露的查询入口：通过 pgxpool 复用连接，
// 暴露用于 metrics 抓取的若干表级查询方法。
type Querier struct {
	pool          *pgxpool.Pool
	logger        *slog.Logger
	tableCache    map[string]bool
	tableCacheTTL time.Duration
	tableCacheAt  time.Time
	mu            sync.RWMutex
}

// New 用 DSN 建立连接池；通过 ping 兜底验证可用性。
func New(ctx context.Context, dsn string, logger *slog.Logger) (*Querier, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Querier{
		pool:          pool,
		logger:        logger,
		tableCacheTTL: time.Minute,
	}, nil
}

// Close 释放连接池。
func (q *Querier) Close() {
	if q == nil || q.pool == nil {
		return
	}
	q.pool.Close()
}

// Ping 简单连通性探测。
func (q *Querier) Ping(ctx context.Context) error {
	if q == nil || q.pool == nil {
		return errors.New("querier not initialized")
	}
	return q.pool.Ping(ctx)
}

// tablesPresent 缓存 information_schema 中目标 schema 内现存的表。
func (q *Querier) tablesPresent(ctx context.Context) (map[string]bool, error) {
	q.mu.RLock()
	if q.tableCache != nil && time.Since(q.tableCacheAt) < q.tableCacheTTL {
		out := q.tableCache
		q.mu.RUnlock()
		return out, nil
	}
	q.mu.RUnlock()

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.tableCache != nil && time.Since(q.tableCacheAt) < q.tableCacheTTL {
		return q.tableCache, nil
	}
	rows, err := q.pool.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]bool, 16)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[strings.ToLower(name)] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	q.tableCache = out
	q.tableCacheAt = time.Now()
	return out, nil
}

// HasTable 返回当前 schema 中是否存在指定表（带 1 分钟缓存）。
func (q *Querier) HasTable(ctx context.Context, name string) (bool, error) {
	present, err := q.tablesPresent(ctx)
	if err != nil {
		return false, err
	}
	return present[strings.ToLower(name)], nil
}

func (q *Querier) hasColumn(ctx context.Context, table, column string) (bool, error) {
	row := q.pool.QueryRow(ctx, `
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = current_schema()
            AND table_name = $1
            AND column_name = $2
        )`, table, column)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

// CountSingle 返回 `SELECT COUNT(*) FROM <table>` 的结果；表不存在时返回 (0, false, nil)。
func (q *Querier) CountSingle(ctx context.Context, table string) (int64, bool, error) {
	has, err := q.HasTable(ctx, table)
	if err != nil {
		return 0, false, err
	}
	if !has {
		return 0, false, nil
	}
	row := q.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, pgx.Identifier{table}.Sanitize()))
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, true, err
	}
	return n, true, nil
}

// LabeledCounts 把 `SELECT <label_col>, COUNT(*)` 的结果以 map[string]int64 返回。
// 表不存在或字段为空时返回 nil, false, nil。
func (q *Querier) LabeledCounts(ctx context.Context, table, labelCol string) (map[string]int64, bool, error) {
	has, err := q.HasTable(ctx, table)
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	sql := fmt.Sprintf(`SELECT COALESCE(CAST(%s AS TEXT), '') AS label, COUNT(*) FROM %s GROUP BY 1`,
		pgx.Identifier{labelCol}.Sanitize(),
		pgx.Identifier{table}.Sanitize())
	rows, err := q.pool.Query(ctx, sql)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	out := make(map[string]int64, 8)
	for rows.Next() {
		var label string
		var n int64
		if err := rows.Scan(&label, &n); err != nil {
			return nil, true, err
		}
		out[label] = n
	}
	return out, true, rows.Err()
}

// EtlPhaseStats 聚合 etl_perf_runs 的 phase->{rows,bytes,duration}。
type EtlPhaseStats struct {
	DurationSec float64
	InputBytes  float64
	OutputBytes float64
}

// EtlPerfByPhase 把每个 phase 聚合到 EtlPhaseStats，求 sum。
func (q *Querier) EtlPerfByPhase(ctx context.Context) (map[string]EtlPhaseStats, bool, error) {
	has, err := q.HasTable(ctx, "etl_perf_runs")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(phase,'') AS phase,
               COALESCE(SUM(duration_sec), 0) AS d,
               COALESCE(SUM(input_bytes), 0) AS ib,
               COALESCE(SUM(output_bytes), 0) AS ob
        FROM etl_perf_runs
        GROUP BY 1`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	out := make(map[string]EtlPhaseStats, 8)
	for rows.Next() {
		var phase string
		var d, ib, ob float64
		if err := rows.Scan(&phase, &d, &ib, &ob); err != nil {
			return nil, true, err
		}
		out[phase] = EtlPhaseStats{DurationSec: d, InputBytes: ib, OutputBytes: ob}
	}
	return out, true, rows.Err()
}

// EtlFailureCountsByPhase 仅统计 etl_perf_runs.status='FAIL' 的数量，按 phase 分组。
func (q *Querier) EtlFailureCountsByPhase(ctx context.Context) (map[string]int64, bool, error) {
	has, err := q.HasTable(ctx, "etl_perf_runs")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(phase,'') AS phase, COUNT(*) FROM etl_perf_runs
        WHERE status='FAIL'
        GROUP BY 1`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	out := make(map[string]int64, 4)
	for rows.Next() {
		var phase string
		var n int64
		if err := rows.Scan(&phase, &n); err != nil {
			return nil, true, err
		}
		out[phase] = n
	}
	return out, true, rows.Err()
}

// EtlJobsByStatusType returns counts grouped by (status, job_type).
type EtlJobBucket struct {
	Status string
	Type   string
	Count  int64
}

// EtlJobBuckets returns (status,job_type) buckets from etl_jobs.
func (q *Querier) EtlJobBuckets(ctx context.Context) ([]EtlJobBucket, bool, error) {
	has, err := q.HasTable(ctx, "etl_jobs")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}
	rows, err := q.pool.Query(ctx, `
        SELECT COALESCE(status,'') AS status, COALESCE(job_type,'') AS jt, COUNT(*)
        FROM etl_jobs GROUP BY 1, 2`)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []EtlJobBucket
	for rows.Next() {
		var b EtlJobBucket
		if err := rows.Scan(&b.Status, &b.Type, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// BenchmarkCasePassFail counts benchmark_cases by pass/fail field (true/false/null).
type BenchmarkCaseBucket struct {
	Passed string
	Count  int64
}

// BenchmarkCaseBuckets returns the per-case pass/fail aggregate.
func (q *Querier) BenchmarkCaseBuckets(ctx context.Context) ([]BenchmarkCaseBucket, bool, error) {
	has, err := q.HasTable(ctx, "benchmark_cases")
	if err != nil {
		return nil, false, err
	}
	if !has {
		return nil, false, nil
	}

	// 云端 v1.5 初始表使用 passed；当前本地模型使用 match。两者共存时优先新写入的 match。
	hasPassed, err := q.hasColumn(ctx, "benchmark_cases", "passed")
	if err != nil {
		return nil, true, err
	}
	hasMatch, err := q.hasColumn(ctx, "benchmark_cases", "match")
	if err != nil {
		return nil, true, err
	}
	boolExpr := ""
	switch {
	case hasPassed && hasMatch:
		boolExpr = "COALESCE(" + pgx.Identifier{"match"}.Sanitize() + ", " + pgx.Identifier{"passed"}.Sanitize() + ")"
	case hasMatch:
		boolExpr = pgx.Identifier{"match"}.Sanitize()
	case hasPassed:
		boolExpr = pgx.Identifier{"passed"}.Sanitize()
	default:
		return nil, true, nil
	}

	sql := fmt.Sprintf(`
        SELECT
          CASE WHEN %s IS TRUE THEN 'true'
               WHEN %s IS FALSE THEN 'false'
               ELSE 'unknown'
          END AS passed,
          COUNT(*)
        FROM benchmark_cases GROUP BY 1`,
		boolExpr,
		boolExpr)
	rows, err := q.pool.Query(ctx, sql)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()
	var out []BenchmarkCaseBucket
	for rows.Next() {
		var b BenchmarkCaseBucket
		if err := rows.Scan(&b.Passed, &b.Count); err != nil {
			return nil, true, err
		}
		out = append(out, b)
	}
	return out, true, rows.Err()
}

// AvgQualityScore 返回 quality_snapshots.quality_score 的平均值。
func (q *Querier) AvgQualityScore(ctx context.Context) (float64, bool, error) {
	has, err := q.HasTable(ctx, "quality_snapshots")
	if err != nil {
		return 0, false, err
	}
	if !has {
		return 0, false, nil
	}
	row := q.pool.QueryRow(ctx, `SELECT COALESCE(AVG(quality_score), 0) FROM quality_snapshots`)
	var v float64
	if err := row.Scan(&v); err != nil {
		return 0, true, err
	}
	return v, true, nil
}
