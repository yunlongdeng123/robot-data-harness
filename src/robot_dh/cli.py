from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path

from robot_dh.benchmark import (
    apply_mutation,
    list_supported_mutations,
    run_benchmark,
)
from robot_dh.benchmark.report import render_summary_from_dir
from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.ads import build_ads
from robot_dh.etl.features import build_features
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.etl.runner import etl_run, etl_scan
from robot_dh.gate import evaluate_report, write_gate_report
from robot_dh.infra import parse_check_list, render_doctor_human, run_infra_doctor
from robot_dh.lake.commands import (
    lake_audit,
    lake_init,
    lake_list,
    lake_manifest,
    render_audit_human,
    render_init_human,
    render_list_human,
)
from robot_dh.logging_utils import configure_logging
from robot_dh.perf import (
    emit_perf_records,
    perf_records_from_etl_run,
    reingest_pending_perf_records,
)
from robot_dh.perf.io_stats import measure_uri_bytes
from robot_dh.pipeline import compare_reports, run_validation
from robot_dh.registry import RegistryService
from robot_dh.runtime.events import RuntimeEventLogger
from robot_dh.scan import scan_datasets
from robot_dh.sharding.io import read_json_uri, write_json_uri
from robot_dh.sharding.merge import merge_shard_summaries
from robot_dh.sharding.models import EtlPlan
from robot_dh.sharding.planner import plan_etl
from robot_dh.sharding.shard_runner import run_shard
from robot_dh.warehouse.service import WarehouseService


def _print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _print_dataset_rows(rows: list[object]) -> None:
    print("dataset_id\tversion\tlast_status\tlast_run_id\tupdated_at")
    for row in rows:
        print(
            f"{row.dataset_id}\t{row.version}\t{row.last_status or '-'}\t{row.last_run_id or '-'}\t{row.updated_at}"
        )


def _print_run_rows(rows: list[object]) -> None:
    print("run_id\tdataset_id\tversion\tstatus\tstarted_at")
    for row in rows:
        print(f"{row.run_id}\t{row.dataset_id}\t{row.dataset_version}\t{row.status}\t{row.started_at}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="robot-dh")
    parser.add_argument("--version", action="store_true", dest="show_version", help="Print package version")

    subparsers = parser.add_subparsers(dest="command")

    generate_parser = subparsers.add_parser("generate-demo", help="Generate a synthetic demo dataset")
    generate_parser.add_argument("--output", type=Path, required=True)
    generate_parser.add_argument("--duration-sec", type=float, default=46.0)
    generate_parser.add_argument("--fps", type=int, default=30)
    generate_parser.add_argument("--num-buttons", type=int, default=5)
    generate_parser.add_argument("--num-presses", type=int, default=25)

    validate_parser = subparsers.add_parser("validate", help="Validate a robot dataset")
    validate_parser.add_argument("--dataset", type=Path, required=True)
    validate_parser.add_argument("--config", type=Path, default=Path("configs/button_press.yaml"))
    validate_parser.add_argument("--output", type=Path, required=True)
    validate_parser.add_argument("--run-id", type=str, default=None)
    validate_parser.add_argument("--record-to-registry", action="store_true")
    validate_parser.add_argument("--gate-policy", type=Path, default=None)
    validate_parser.add_argument("--artifact-store", choices=("local", "s3"), default=None)
    validate_parser.add_argument("--artifact-prefix", type=str, default=None)
    validate_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    compare_parser = subparsers.add_parser("compare", help="Compare two validation reports")
    compare_parser.add_argument("--baseline", type=Path, required=True)
    compare_parser.add_argument("--candidate", type=Path, required=True)

    gate_parser = subparsers.add_parser("gate", help="Evaluate a report against a quality gate policy")
    gate_parser.add_argument("--report", type=Path, required=True)
    gate_parser.add_argument("--policy", type=Path, required=True)
    gate_parser.add_argument("--output", type=Path, default=None)

    scan_parser = subparsers.add_parser("scan", help="Scan a dataset root and validate discovered datasets")
    scan_parser.add_argument("--root", type=Path, required=True)
    scan_parser.add_argument("--config", type=Path, default=Path("configs/button_press.yaml"))
    scan_parser.add_argument("--output-root", type=Path, required=True)
    scan_parser.add_argument("--registry", action="store_true")
    scan_parser.add_argument("--only-new", action="store_true")
    scan_parser.add_argument("--gate-policy", type=Path, default=None)
    scan_parser.add_argument("--artifact-store", choices=("local", "s3"), default=None)
    scan_parser.add_argument("--artifact-prefix", type=str, default=None)
    scan_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    dataset_parser = subparsers.add_parser("dataset", help="Dataset registry commands")
    dataset_subparsers = dataset_parser.add_subparsers(dest="dataset_command")
    dataset_register_parser = dataset_subparsers.add_parser("register", help="Register a dataset")
    dataset_register_parser.add_argument("--dataset", type=Path, required=True)
    dataset_register_parser.add_argument("--dataset-id", type=str, required=True)
    dataset_register_parser.add_argument("--version", type=str, required=True)
    dataset_register_parser.add_argument("--storage-uri", type=str, required=True)
    dataset_register_parser.add_argument("--task-type", type=str, default=None)
    dataset_register_parser.add_argument("--robot-type", type=str, default=None)
    dataset_register_parser.add_argument("--pose-format", type=str, default="eexyzxyzw")
    dataset_subparsers.add_parser("list", help="List datasets")
    dataset_show_parser = dataset_subparsers.add_parser("show", help="Show dataset details")
    dataset_show_parser.add_argument("--dataset-id", type=str, required=True)
    dataset_show_parser.add_argument("--version", type=str, default=None)

    runs_parser = subparsers.add_parser("runs", help="Run-history commands")
    runs_subparsers = runs_parser.add_subparsers(dest="runs_command")
    runs_subparsers.add_parser("list", help="List validation runs")
    runs_show_parser = runs_subparsers.add_parser("show", help="Show a validation run")
    runs_show_parser.add_argument("--run-id", type=str, required=True)

    infra_parser = subparsers.add_parser("infra", help="Infrastructure connectivity commands")
    infra_subparsers = infra_parser.add_subparsers(dest="infra_command")
    infra_doctor_parser = infra_subparsers.add_parser("doctor", help="Check DB / S3 / Redis / Lake connectivity")
    infra_doctor_parser.add_argument("--output", choices=("human", "json"), default="human")
    infra_doctor_parser.add_argument("--check", type=str, default="db,s3,redis,lake")

    lake_parser = subparsers.add_parser("lake", help="v1.4 data lake commands")
    lake_subparsers = lake_parser.add_subparsers(dest="lake_command")
    lake_init_parser = lake_subparsers.add_parser("init", help="Probe lake bucket / prefixes / lake metadata tables (does not create resources)")
    lake_init_parser.add_argument("--output", choices=("human", "json"), default="human")
    lake_list_parser = lake_subparsers.add_parser("list", help="List lake assets by layer")
    lake_list_parser.add_argument("--layer", choices=("raw", "ods", "dwd", "ads", "lineage", "tmp"), default=None)
    lake_list_parser.add_argument("--lake-root", dest="lake_root", type=str, default=None)
    lake_list_parser.add_argument("--include", dest="include", action="append", default=None, help="glob filter applied to slice keys (multi-valued)")
    lake_list_parser.add_argument("--exclude", dest="exclude", action="append", default=None, help="glob filter excluding slice keys (multi-valued)")
    lake_list_parser.add_argument("--output", choices=("human", "json"), default="human")
    lake_audit_parser = lake_subparsers.add_parser("audit", help="Audit lake (bucket, prefixes, manifest completeness, PG tables)")
    lake_audit_parser.add_argument("--output", choices=("human", "json"), default="human")
    lake_manifest_parser = lake_subparsers.add_parser("manifest", help="Print _manifest.json under a layer URI")
    lake_manifest_parser.add_argument("--uri", type=str, required=True)

    normalize_parser = subparsers.add_parser("normalize", help="raw -> ods: normalize a dataset into the lake")
    normalize_parser.add_argument("--dataset", type=str, required=True, help="raw dataset URI (local path or s3://...)")
    normalize_parser.add_argument("--output", type=str, required=True, help="ods slice URI to write")
    normalize_parser.add_argument("--dataset-id", type=str, default=None)
    normalize_parser.add_argument("--version", type=str, default=None)
    normalize_parser.add_argument("--lake-root", type=str, default=None, help="Optional lake root for lineage JSONL")
    normalize_parser.add_argument("--job-id", type=str, default=None)
    normalize_parser.add_argument("--resume", dest="resume", action="store_true", default=True)
    normalize_parser.add_argument("--no-resume", dest="resume", action="store_false")
    normalize_parser.add_argument("--force", action="store_true")
    normalize_parser.add_argument("--heartbeat-interval-sec", type=float, default=30.0)
    normalize_parser.add_argument("--progress-log-interval-sec", type=float, default=30.0)
    normalize_parser.add_argument("--workflow-name", type=str, default=None)
    normalize_parser.add_argument("--perf-dir", type=Path, default=None)
    normalize_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    features_parser = subparsers.add_parser("build-features", help="ods -> dwd: build feature parquets")
    features_parser.add_argument("--input", type=str, required=True, help="ods slice URI")
    features_parser.add_argument("--output", type=str, required=True, help="dwd slice URI to write")
    features_parser.add_argument("--config", type=Path, default=Path("configs/etl_default.yaml"))
    features_parser.add_argument("--lake-root", type=str, default=None)
    features_parser.add_argument("--job-id", type=str, default=None)

    ads_parser = subparsers.add_parser("build-ads", help="dwd -> ads: build dataset quality summaries")
    ads_parser.add_argument("--input-root", type=str, required=True, help="dwd root URI (e.g. s3://robot-lake/dwd)")
    ads_parser.add_argument("--output", type=str, required=True, help="ads slice URI (e.g. s3://robot-lake/ads/quality)")
    ads_parser.add_argument("--config", type=Path, default=Path("configs/etl_default.yaml"))
    ads_parser.add_argument("--lake-root", type=str, default=None)
    ads_parser.add_argument("--job-id", type=str, default=None)
    # 与其他子命令对齐；当前实现始终走 _print_json，"human" 仅占位，避免 v1.7
    # local devscale workflow 模板的 `--log-format json` 触发 argparse error。
    ads_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    etl_parser = subparsers.add_parser("etl", help="v1.4 ETL orchestration")
    etl_subparsers = etl_parser.add_subparsers(dest="etl_command")
    etl_run_parser = etl_subparsers.add_parser("run", help="Run normalize + build-features (+ optional build-ads) for one dataset")
    etl_run_parser.add_argument("--dataset", type=str, required=True)
    etl_run_parser.add_argument("--dataset-id", type=str, default=None)
    etl_run_parser.add_argument("--version", type=str, default=None)
    etl_run_parser.add_argument("--lake-root", type=str, required=True, help="Lake root URI (e.g. s3://robot-lake/)")
    etl_run_parser.add_argument("--build-ads", action="store_true")
    etl_run_parser.add_argument("--features-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_run_parser.add_argument("--ads-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_run_parser.add_argument("--summary-dir", type=Path, default=None)
    etl_run_parser.add_argument("--job-id", type=str, default=None)
    etl_run_parser.add_argument("--perf-dir", type=Path, default=None, help="write v1.5 perf JSON next to summary")
    etl_run_parser.add_argument("--log-format", choices=("human", "json"), default="human")
    etl_run_parser.add_argument("--phase", choices=("normalize", "features", "ads", "all"), default="all")
    etl_run_parser.add_argument("--resume", dest="resume", action="store_true", default=True)
    etl_run_parser.add_argument("--no-resume", dest="resume", action="store_false")
    etl_run_parser.add_argument("--force", action="store_true")
    etl_run_parser.add_argument("--heartbeat-interval-sec", type=float, default=30.0)
    etl_run_parser.add_argument("--progress-log-interval-sec", type=float, default=30.0)
    etl_run_parser.add_argument("--workflow-name", type=str, default=None)

    etl_scan_parser = etl_subparsers.add_parser("scan", help="Discover datasets and run etl run for each")
    etl_scan_parser.add_argument("--root", type=str, required=True, help="Data root (e.g. s3://robot-datasets)")
    etl_scan_parser.add_argument("--lake-root", type=str, required=True)
    etl_scan_parser.add_argument("--limit", type=int, default=None)
    etl_scan_parser.add_argument("--force", action="store_true")
    etl_scan_parser.add_argument("--build-ads", action="store_true")
    etl_scan_parser.add_argument("--features-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_scan_parser.add_argument("--ads-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_scan_parser.add_argument("--summary-dir", type=Path, default=None)
    etl_scan_parser.add_argument("--include", action="append", default=None, help="glob filter on dataset_id (multi-valued)")
    etl_scan_parser.add_argument("--exclude", action="append", default=None, help="glob filter excluding dataset_id (multi-valued)")
    etl_scan_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.5: etl plan
    etl_plan_parser = etl_subparsers.add_parser("plan", help="Plan sharded ETL: discover datasets and partition them into shards")
    etl_plan_parser.add_argument("--root", type=str, required=True, help="raw root URI (e.g. s3://robot-datasets/raw)")
    etl_plan_parser.add_argument("--lake-root", type=str, required=True)
    etl_plan_parser.add_argument("--output", type=str, required=True, help="plan JSON output path (local) or s3:// URI")
    etl_plan_parser.add_argument("--target-shard-size-gb", type=float, default=5.0)
    etl_plan_parser.add_argument("--max-shards", type=int, default=16)
    etl_plan_parser.add_argument("--include", action="append", default=None)
    etl_plan_parser.add_argument("--exclude", action="append", default=None)
    etl_plan_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.5: etl run-shard
    etl_run_shard_parser = etl_subparsers.add_parser("run-shard", help="Execute a single shard of an etl plan")
    etl_run_shard_parser.add_argument("--plan", type=str, required=True, help="plan JSON local path or s3:// URI")
    etl_run_shard_parser.add_argument("--shard-id", required=True, help="shard index (int) or shard_id (string)")
    etl_run_shard_parser.add_argument("--lake-root", type=str, default=None)
    etl_run_shard_parser.add_argument("--output", type=Path, required=True, help="local work dir for shard outputs")
    etl_run_shard_parser.add_argument("--summary-uri", type=str, default=None, help="optional s3:// URI to upload shard_summary.json")
    etl_run_shard_parser.add_argument("--max-workers", type=int, default=1)
    etl_run_shard_parser.add_argument("--fail-fast", action="store_true")
    etl_run_shard_parser.add_argument("--build-ads", action="store_true")
    etl_run_shard_parser.add_argument("--features-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_run_shard_parser.add_argument("--ads-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_run_shard_parser.add_argument("--work-dir", type=Path, default=None)
    etl_run_shard_parser.add_argument("--tmp-dir", type=Path, default=None)
    etl_run_shard_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.5: etl merge-summary
    etl_merge_parser = etl_subparsers.add_parser("merge-summary", help="Aggregate shard summaries into a plan-level summary")
    etl_merge_parser.add_argument("--plan", type=str, required=True)
    etl_merge_parser.add_argument("--shard-results", type=str, required=True, help="dir or s3:// prefix containing shard_summary.json files")
    etl_merge_parser.add_argument("--output", type=str, required=True)
    etl_merge_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.6.3: ml-ready export / list / show
    mlr_parser = subparsers.add_parser("ml-ready", help="v1.6 ML-ready dataset export")
    mlr_subparsers = mlr_parser.add_subparsers(dest="ml_ready_command")

    mlr_export_parser = mlr_subparsers.add_parser("export", help="Build train/val/test parquet + dataset_card")
    mlr_export_parser.add_argument("--input-root", type=str, required=True)
    mlr_export_parser.add_argument("--quality-root", type=str, default=None)
    mlr_export_parser.add_argument("--qc-root", type=str, default=None)
    mlr_export_parser.add_argument("--output", type=str, required=True)
    mlr_export_parser.add_argument("--quality-threshold", type=float, default=80.0)
    mlr_export_parser.add_argument("--split", type=str, default="0.8,0.1,0.1")
    mlr_export_parser.add_argument("--dataset-id", type=str, default="ml_ready")
    mlr_export_parser.add_argument("--version", type=str, default="v1")
    mlr_export_parser.add_argument("--dataset-family", type=str, default="all")
    mlr_export_parser.add_argument("--min-episode-length", type=int, default=None)
    mlr_export_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    mlr_subparsers.add_parser("list", help="List ml-ready datasets registered in PG")
    mlr_show_parser = mlr_subparsers.add_parser("show", help="Show one ml-ready dataset row from PG")
    mlr_show_parser.add_argument("--dataset-id", type=str, required=True)
    mlr_show_parser.add_argument("--version", type=str, default="v1")

    # v1.6.2: qc contract / profile / report
    qc_parser = subparsers.add_parser("qc", help="v1.6 QC contract layer")
    qc_subparsers = qc_parser.add_subparsers(dest="qc_command")

    qc_contract_parser = qc_subparsers.add_parser("contract", help="contract operations")
    qc_contract_subparsers = qc_contract_parser.add_subparsers(dest="qc_contract_command")

    qc_contract_subparsers.add_parser("list", help="List built-in contracts")

    qc_contract_run_parser = qc_contract_subparsers.add_parser("run", help="Run a contract on a dataset")
    qc_contract_run_parser.add_argument("--dataset-family", type=str, required=True)
    qc_contract_run_parser.add_argument("--dataset-uri", type=str, required=True)
    qc_contract_run_parser.add_argument("--dataset-id", type=str, required=True)
    qc_contract_run_parser.add_argument("--version", type=str, required=True)
    qc_contract_run_parser.add_argument("--output", type=str, required=True)
    qc_contract_run_parser.add_argument("--contract", type=Path, default=None)
    qc_contract_run_parser.add_argument("--layer", type=str, default=None)
    qc_contract_run_parser.add_argument("--log-format", choices=("human", "json"), default="human")
    # v1.7：bridge / robomimic 容灾参数；统一转成 env，下沉到 profile.py / parquet_probe.py / hdf5_probe.py
    qc_contract_run_parser.add_argument("--max-workers", type=int, default=None,
        help="HDF5 / parquet 探针并发；映射到 ROBOT_DH_QC_PROBE_CONCURRENCY")
    qc_contract_run_parser.add_argument("--file-timeout-sec", type=float, default=None,
        help="单文件探针硬 timeout（秒），robomimic 路径生效；映射到 ROBOT_DH_QC_FILE_TIMEOUT_SEC")
    qc_contract_run_parser.add_argument("--probe-timeout-sec", type=float, default=None,
        help="bridge 远端 lazy probe 硬 timeout（秒）；映射到 ROBOT_DH_QC_PROBE_TIMEOUT_SEC")
    qc_contract_run_parser.add_argument("--max-retries", type=int, default=None,
        help="bridge / robomimic 远端 probe 最大重试；映射到 ROBOT_DH_QC_MAX_RETRIES")
    qc_contract_run_parser.add_argument("--disable-remote-lazy", action="store_true",
        help="bridge：禁用 S3 lazy 探针（仅本地 file URI 可用）；映射到 ROBOT_DH_QC_DISABLE_REMOTE_LAZY=1")
    qc_contract_run_parser.add_argument("--fail-fast", action="store_true",
        help="任一文件探针失败立即终止；映射到 ROBOT_DH_QC_FAIL_FAST=1")

    qc_profile_parser = qc_subparsers.add_parser("profile", help="Profile a dataset (asset_profile.json only)")
    qc_profile_parser.add_argument("--dataset-uri", type=str, required=True)
    qc_profile_parser.add_argument("--dataset-family", type=str, default="universal")
    qc_profile_parser.add_argument("--dataset-id", type=str, default=None)
    qc_profile_parser.add_argument("--version", type=str, default=None)
    qc_profile_parser.add_argument("--output", type=str, required=True)
    qc_profile_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    qc_report_parser = qc_subparsers.add_parser("report", help="Render markdown summary from a contract_report.json")
    qc_report_parser.add_argument("--input", type=str, required=True)
    qc_report_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.6: partition plan / list / run-normalize
    partition_parser = subparsers.add_parser("partition", help="v1.6 partition planning for large datasets")
    partition_subparsers = partition_parser.add_subparsers(dest="partition_command")

    partition_plan_parser = partition_subparsers.add_parser("plan", help="Plan dataset partitions")
    partition_plan_parser.add_argument("--dataset", type=str, required=True)
    partition_plan_parser.add_argument("--dataset-id", type=str, required=True)
    partition_plan_parser.add_argument("--version", type=str, required=True)
    partition_plan_parser.add_argument("--output", type=str, required=True)
    partition_plan_parser.add_argument("--target-partition-size-gb", type=float, default=2.0)
    partition_plan_parser.add_argument("--family", type=str, default=None)
    partition_plan_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    partition_list_parser = partition_subparsers.add_parser("list", help="List partitions in a plan")
    partition_list_parser.add_argument("--plan", type=str, required=True)
    partition_list_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    partition_run_normalize_parser = partition_subparsers.add_parser(
        "run-normalize", help="Run normalize for a single partition by index",
    )
    partition_run_normalize_parser.add_argument("--plan", type=str, required=True)
    partition_run_normalize_parser.add_argument("--partition-index", type=int, required=True)
    partition_run_normalize_parser.add_argument("--output", type=str, required=True)
    partition_run_normalize_parser.add_argument("--lake-root", type=str, default=None)
    partition_run_normalize_parser.add_argument("--heartbeat-interval-sec", type=float, default=30.0)
    partition_run_normalize_parser.add_argument("--progress-log-interval-sec", type=float, default=30.0)
    partition_run_normalize_parser.add_argument("--resume", dest="resume", action="store_true", default=True)
    partition_run_normalize_parser.add_argument("--no-resume", dest="resume", action="store_false")
    partition_run_normalize_parser.add_argument("--force", action="store_true")
    partition_run_normalize_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.6.4: argo sync / lineage report ; v1.7 argo logs index
    argo_parser = subparsers.add_parser("argo", help="v1.6 Argo workflow metadata sync")
    argo_subparsers = argo_parser.add_subparsers(dest="argo_command")
    argo_sync_parser = argo_subparsers.add_parser("sync", help="Sync workflow status from kubectl into PG")
    argo_sync_parser.add_argument("--workflow-name", type=str, required=True)
    argo_sync_parser.add_argument("--namespace", type=str, default="robot-dh")
    argo_sync_parser.add_argument("--from-json", type=Path, default=None,
        help="Read workflow JSON from a local file instead of calling kubectl")
    argo_sync_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    argo_logs_parser = argo_subparsers.add_parser("logs", help="v1.7 archive log index operations")
    argo_logs_sub = argo_logs_parser.add_subparsers(dest="argo_logs_command")
    argo_logs_index_parser = argo_logs_sub.add_parser(
        "index", help="Derive archive_log_uri per step from workflow JSON and write into PG workflow_steps.metrics"
    )
    argo_logs_index_parser.add_argument("--workflow-name", type=str, required=True)
    argo_logs_index_parser.add_argument("--namespace", type=str, default="robot-dh")
    argo_logs_index_parser.add_argument("--archive-root", type=str,
        default="s3://robot-dh-artifacts/argo-logs")
    argo_logs_index_parser.add_argument("--container-name", type=str, default="main")
    argo_logs_index_parser.add_argument("--from-json", type=Path, default=None,
        help="Read workflow JSON from a local file instead of calling kubectl")
    argo_logs_index_parser.add_argument("--dry-run", action="store_true")
    argo_logs_index_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.7: local runtime / datasets
    local_parser = subparsers.add_parser("local", help="v1.7 local-first runtime commands")
    local_subparsers = local_parser.add_subparsers(dest="local_command")

    local_runtime_parser = local_subparsers.add_parser("runtime", help="local runtime config / doctor")
    local_runtime_sub = local_runtime_parser.add_subparsers(dest="local_runtime_command")
    local_runtime_doctor_parser = local_runtime_sub.add_parser(
        "doctor", help="Health-check local data root, devscale manifests, total size",
    )
    local_runtime_doctor_parser.add_argument("--config", type=Path,
        default=Path("configs/devscale_runtime.yaml"))
    local_runtime_doctor_parser.add_argument("--devscale-config", type=Path,
        default=Path("configs/devscale_datasets.yaml"))
    local_runtime_doctor_parser.add_argument("--allow-over-limit", action="store_true")
    local_runtime_doctor_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    local_datasets_parser = local_subparsers.add_parser("datasets", help="devscale datasets")
    local_datasets_sub = local_datasets_parser.add_subparsers(dest="local_datasets_command")
    local_datasets_list = local_datasets_sub.add_parser("list", help="List devscale datasets")
    local_datasets_list.add_argument("--config", type=Path,
        default=Path("configs/devscale_runtime.yaml"))
    local_datasets_list.add_argument("--devscale-config", type=Path,
        default=Path("configs/devscale_datasets.yaml"))
    local_datasets_list.add_argument("--log-format", choices=("human", "json"), default="human")
    local_datasets_verify = local_datasets_sub.add_parser("verify",
        help="Verify devscale datasets against plan / manifest")
    local_datasets_verify.add_argument("--config", type=Path,
        default=Path("configs/devscale_runtime.yaml"))
    local_datasets_verify.add_argument("--devscale-config", type=Path,
        default=Path("configs/devscale_datasets.yaml"))
    local_datasets_verify.add_argument("--plan", type=Path, default=None,
        help="manifests/devscale_plan.json; if omitted, fallback to per-dataset _manifest.json")
    local_datasets_verify.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.7: adapter detect / probe / list
    adapter_parser = subparsers.add_parser("adapter", help="v1.7 dataset adapter registry")
    adapter_subparsers = adapter_parser.add_subparsers(dest="adapter_command")
    adapter_subparsers.add_parser("list", help="List registered adapter families")
    adapter_detect_parser = adapter_subparsers.add_parser("detect",
        help="Detect best adapter for a dataset URI")
    adapter_detect_parser.add_argument("--dataset-uri", type=str, required=True)
    adapter_detect_parser.add_argument("--dataset-id", type=str, default=None)
    adapter_detect_parser.add_argument("--all", action="store_true",
        help="Print confidence of every adapter")
    adapter_detect_parser.add_argument("--log-format", choices=("human", "json"), default="human")
    adapter_probe_parser = adapter_subparsers.add_parser("probe",
        help="Probe a dataset via adapter (schema / file counts / errors)")
    adapter_probe_parser.add_argument("--dataset-uri", type=str, required=True)
    adapter_probe_parser.add_argument("--dataset-id", type=str, default=None)
    adapter_probe_parser.add_argument("--family", type=str, default=None,
        help="Override detected family; defaults to detect_adapter")
    adapter_probe_parser.add_argument("--sample-limit", type=int, default=32)
    adapter_probe_parser.add_argument("--option", action="append", default=[],
        help="Adapter probe option in key=value form (e.g. probe_timeout_sec=60)")
    adapter_probe_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.7: runtime heartbeat check
    runtime_parser = subparsers.add_parser("runtime", help="v1.7 runtime utilities")
    runtime_subparsers = runtime_parser.add_subparsers(dest="runtime_command")
    runtime_heartbeat = runtime_subparsers.add_parser("heartbeat", help="heartbeat utilities")
    runtime_heartbeat_sub = runtime_heartbeat.add_subparsers(dest="runtime_heartbeat_command")
    runtime_heartbeat_check = runtime_heartbeat_sub.add_parser(
        "check", help="Check whether any phase heartbeat is stale",
    )
    runtime_heartbeat_check.add_argument("--workflow-name", type=str, default=None)
    runtime_heartbeat_check.add_argument("--stale-after-sec", type=float, default=300.0)
    runtime_heartbeat_check.add_argument("--warn-after-sec", type=float, default=120.0)
    runtime_heartbeat_check.add_argument("--events-dir", type=Path, default=None,
        help="Override ROBOT_DH_EVENTS_DIR / runs/events")
    runtime_heartbeat_check.add_argument("--log-format", choices=("human", "json"), default="human")
    runtime_heartbeat_check.add_argument("--fail-on", choices=("warn", "stale", "never"),
        default="stale",
        help="Exit non-zero when overall status reaches the threshold (default stale)")

    lineage_parser = subparsers.add_parser("lineage", help="v1.6 lineage report")
    lineage_subparsers = lineage_parser.add_subparsers(dest="lineage_command")
    lineage_report_parser = lineage_subparsers.add_parser("report", help="Build a lineage report for a workflow")
    lineage_report_parser.add_argument("--workflow-name", type=str, required=True)
    lineage_report_parser.add_argument("--namespace", type=str, default="robot-dh")
    lineage_report_parser.add_argument("--output", type=str, required=True)
    lineage_report_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.5: mutate
    mutate_parser = subparsers.add_parser("mutate", help="Apply a benchmark mutation to a local dataset")
    mutate_parser.add_argument("--dataset", type=Path, required=True)
    mutate_parser.add_argument("--output", type=Path, required=True)
    mutate_parser.add_argument("--mutation", type=str, required=True, choices=list_supported_mutations())
    mutate_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    # v1.5: benchmark
    benchmark_parser = subparsers.add_parser("benchmark", help="v1.5 benchmark commands")
    benchmark_subparsers = benchmark_parser.add_subparsers(dest="benchmark_command")
    benchmark_run_parser = benchmark_subparsers.add_parser("run", help="Run a benchmark suite")
    benchmark_run_parser.add_argument("--suite", type=Path, required=True)
    # 接 file:// 或裸路径，由 to_local_path 统一解析；v1.7 模板传 file:///...
    benchmark_run_parser.add_argument("--output", type=str, required=True)
    benchmark_run_parser.add_argument("--record-to-registry", action="store_true")
    benchmark_run_parser.add_argument("--config", type=Path, default=Path("configs/button_press.yaml"))
    benchmark_run_parser.add_argument("--gate-policy", type=Path, default=None)
    benchmark_run_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    benchmark_report_parser = benchmark_subparsers.add_parser("report", help="Render markdown summary from a benchmark dir")
    benchmark_report_parser.add_argument("--benchmark-dir", type=Path, required=True)
    benchmark_report_parser.add_argument("--log-format", choices=("human", "json"), default="human")

    perf_parser = subparsers.add_parser("perf", help="v1.6 perf record maintenance")
    perf_subparsers = perf_parser.add_subparsers(dest="perf_command")
    perf_reingest_parser = perf_subparsers.add_parser(
        "reingest-pending",
        help=(
            "Re-ingest pending perf records (deferred by schema drift) into PG "
            "and archive them locally (+ S3 mirror when configured)."
        ),
    )
    perf_reingest_parser.add_argument(
        "--pending-dir",
        type=Path,
        default=None,
        help="Local pending dir; defaults to ROBOT_DH_PERF_PENDING_DIR or ~/.cache/robot-dh/perf-records-pending",
    )
    perf_reingest_parser.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="Local archive dir; defaults to ROBOT_DH_PERF_ARCHIVE_DIR or ~/.cache/robot-dh/perf-records-archived",
    )
    perf_reingest_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan but do not write to DB / move files",
    )

    return parser


def _emit_runner_boot(argv: list[str] | None) -> None:
    """在所有业务 import 之后、解析 argparse 之前打第一行 JSON 到 stderr 并 flush。

    这一行不依赖任何 robot_dh 内部模块，是 Argo step pod 0B archive log 的兜底
    诊断信号——只要这条 print 出现在 archive log 顶部，就能证明 python 解释器至少
    成功跑到了 ``main()`` 入口；如果 archive log 仍然 0B，那就是 image 启动 /
    runtime hook 层面的问题，与业务代码无关。

    **必须**走 stderr：很多 CLI 子命令 stdout 是结构化 JSON 给下游消费（``_print_json``），
    不能在前面塞额外 JSON 把"整段 stdout 都是合法 JSON"的契约破坏。K8s container log
    合并 stdout+stderr，archive log 一样能抓到。
    """
    import sys
    import time

    try:
        payload = {
            "event": "runner_boot",
            "argv": list(argv) if argv is not None else sys.argv[1:],
            "python": sys.version.split()[0],
            "ts": time.time(),
            "env_keys": sorted(k for k in os.environ if k.startswith("ROBOT_DH_")),
        }
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)
    except Exception:
        # 兜底自己不能再爆错；让 main 继续走
        pass


def main(argv: list[str] | None = None) -> int:
    _emit_runner_boot(argv)
    try:
        return _main_impl(argv)
    except SystemExit:
        raise
    except BaseException:
        # 任何未捕获异常（含 KeyboardInterrupt / 子线程冒上来的 OOM）都至少留下 traceback,
        # 让 Argo archive log 不再是 0B。重抛交给上层默认 stderr 路径。
        import sys
        import traceback

        traceback.print_exc()
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        raise


def _main_impl(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(log_format=getattr(args, "log_format", "human"))
    if args.show_version:
        from robot_dh import __version__

        print(__version__)
        return 0

    if args.command == "generate-demo":
        dataset_dir = generate_demo_dataset(
            output_dir=args.output,
            duration_sec=args.duration_sec,
            fps=args.fps,
            num_buttons=args.num_buttons,
            num_presses=args.num_presses,
        )
        print(str(dataset_dir))
        return 0

    if args.command == "validate":
        report = run_validation(
            dataset_path=args.dataset,
            config_path=args.config,
            output_dir=args.output,
            run_id=args.run_id,
            record_to_registry=args.record_to_registry,
            gate_policy_path=args.gate_policy,
            artifact_store_type=args.artifact_store,
            artifact_prefix=args.artifact_prefix,
        )
        if report.gate.get("status") == "FAIL":
            return 1
        fail_on_warning = bool(report.config.get("quality_gate", {}).get("fail_on_warning", False))
        if report.status == "FAIL":
            return 2
        if report.status == "WARN" and fail_on_warning:
            return 1
        return 0

    if args.command == "compare":
        comparison = compare_reports(args.baseline, args.candidate)
        for line in comparison["lines"]:
            print(line)
        return 1 if comparison["has_failures"] else 0

    if args.command == "gate":
        gate_report = evaluate_report(args.report, args.policy)
        output_path = args.output or (args.report.parent / "gate_report.json")
        write_gate_report(gate_report, output_path)
        if gate_report["status"] == "FAIL":
            print(f"[FAIL] quality gate failed: {', '.join(gate_report['failed_rules'])}")
            return 1
        if gate_report["status"] == "WARN":
            print(f"[WARN] quality gate warnings: {', '.join(gate_report['warning_rules'])}")
            return 0
        print("[PASS] quality gate passed")
        return 0

    if args.command == "scan":
        summary = scan_datasets(
            root=args.root,
            config_path=args.config,
            output_root=args.output_root,
            use_registry=args.registry,
            only_new=args.only_new,
            gate_policy_path=args.gate_policy,
            artifact_store_type=args.artifact_store,
            artifact_prefix=args.artifact_prefix,
        )
        _print_json(summary)
        return 1 if summary["failed"] > 0 else 0

    if args.command == "dataset":
        registry = RegistryService()
        if args.dataset_command == "register":
            record = registry.register_dataset_path(
                dataset_path=args.dataset,
                dataset_id=args.dataset_id,
                version=args.version,
                storage_uri=args.storage_uri,
                task_type=args.task_type,
                robot_type=args.robot_type,
                pose_format=args.pose_format,
            )
            _print_json(asdict(record))
            return 0
        if args.dataset_command == "list":
            _print_dataset_rows(registry.list_datasets())
            return 0
        if args.dataset_command == "show":
            record = registry.get_dataset(args.dataset_id, args.version)
            if record is None:
                print(f"Dataset not found: {args.dataset_id}")
                return 1
            _print_json(asdict(record))
            return 0

    if args.command == "runs":
        registry = RegistryService()
        if args.runs_command == "list":
            _print_run_rows(registry.list_runs())
            return 0
        if args.runs_command == "show":
            record = registry.get_run(args.run_id)
            if record is None:
                print(f"Run not found: {args.run_id}")
                return 1
            _print_json(asdict(record))
            return 0

    if args.command == "infra" and args.infra_command == "doctor":
        payload = run_infra_doctor(checks=parse_check_list(args.check))
        if args.output == "json":
            _print_json(payload)
        else:
            print(render_doctor_human(payload))
        return 1 if payload["status"] == "FAIL" else 0

    if args.command == "lake":
        if args.lake_command == "init":
            payload = lake_init(output=args.output)
            if args.output == "json":
                _print_json(payload)
            else:
                print(render_init_human(payload))
            return 1 if payload["status"] == "FAIL" else 0
        if args.lake_command == "list":
            payload = lake_list(
                layer=args.layer,
                lake_root_uri=args.lake_root,
                include=args.include,
                exclude=args.exclude,
            )
            if args.output == "json":
                _print_json(payload)
            else:
                print(render_list_human(payload))
            return 0
        if args.lake_command == "audit":
            payload = lake_audit(output=args.output)
            if args.output == "json":
                _print_json(payload)
            else:
                print(render_audit_human(payload))
            return 1 if payload["status"] == "FAIL" else 0
        if args.lake_command == "manifest":
            payload = lake_manifest(uri=args.uri)
            _print_json(payload)
            return 0
        parser.print_help()
        return 0

    if args.command == "normalize":
        result = normalize_dataset(
            dataset_uri=args.dataset,
            output_uri=args.output,
            dataset_id=args.dataset_id,
            version=args.version,
            job_id=args.job_id,
            lake_root_uri=args.lake_root,
            resume=getattr(args, "resume", True),
            force=getattr(args, "force", False),
            heartbeat_interval_sec=getattr(args, "heartbeat_interval_sec", 30.0),
            progress_log_interval_sec=getattr(args, "progress_log_interval_sec", 30.0),
            workflow_name=getattr(args, "workflow_name", None),
            perf_dir=getattr(args, "perf_dir", None),
        )
        _print_json({
            "dataset_id": result.dataset_id,
            "version": result.version,
            "output_uri": result.output_uri,
            "manifest_uri": result.manifest_uri,
            "num_samples": result.num_samples,
            "duration_sec": result.duration_sec,
            "job_id": result.job_id,
            "job_duration_sec": result.duration_job_sec,
            "status": result.status,
            "completed_steps": result.completed_steps,
        })
        return 0

    if args.command == "build-features":
        result = build_features(
            input_uri=args.input,
            output_uri=args.output,
            config_path=args.config,
            job_id=args.job_id,
            lake_root_uri=args.lake_root,
        )
        _print_json({
            "dataset_id": result.dataset_id,
            "version": result.version,
            "output_uri": result.output_uri,
            "manifest_uri": result.manifest_uri,
            "num_press_events": result.num_press_events,
            "cluster_silhouette": result.cluster_silhouette,
            "job_id": result.job_id,
            "job_status": result.job_status,
            "duration_sec": result.duration_job_sec,
        })
        return 0 if result.job_status != "FAIL" else 2

    if args.command == "build-ads":
        result = build_ads(
            input_root_uri=args.input_root,
            output_uri=args.output,
            config_path=args.config,
            job_id=args.job_id,
            lake_root_uri=args.lake_root,
        )
        _print_json({
            "output_uri": result.output_uri,
            "manifest_uri": result.manifest_uri,
            "num_episodes": result.num_episodes,
            "num_datasets": result.num_datasets,
            "job_id": result.job_id,
            "duration_sec": result.duration_job_sec,
        })
        return 0

    if args.command == "etl":
        if args.etl_command == "run":
            result = etl_run(
                dataset_uri=args.dataset,
                dataset_id=args.dataset_id,
                version=args.version,
                lake_root_uri=args.lake_root,
                build_ads_layer=args.build_ads,
                features_config_path=args.features_config,
                ads_config_path=args.ads_config,
                job_id=args.job_id,
                summary_dir=args.summary_dir,
                phase=getattr(args, "phase", "all"),
                resume=getattr(args, "resume", True),
                force=getattr(args, "force", False),
                heartbeat_interval_sec=getattr(args, "heartbeat_interval_sec", 30.0),
                progress_log_interval_sec=getattr(args, "progress_log_interval_sec", 30.0),
                workflow_name=getattr(args, "workflow_name", None),
                perf_dir=args.perf_dir,
            )
            try:
                input_bytes_estimate = measure_uri_bytes(args.dataset)
            except Exception:
                input_bytes_estimate = 0
            perf_records = perf_records_from_etl_run(
                etl_result=result,
                run_id=result.job_id,
                input_bytes_estimate=input_bytes_estimate,
            )
            perf_dir = args.perf_dir or args.summary_dir
            emit_perf_records(perf_records, work_dir=perf_dir)
            _print_json(result.to_dict())
            return 0 if result.status in {"OK", "WARN"} else 1
        if args.etl_command == "scan":
            result = etl_scan(
                root_uri=args.root,
                lake_root_uri=args.lake_root,
                limit=args.limit,
                build_ads_layer=args.build_ads,
                force=args.force,
                features_config_path=args.features_config,
                ads_config_path=args.ads_config,
                summary_dir=args.summary_dir,
                include_patterns=args.include,
                exclude_patterns=args.exclude,
            )
            _print_json(result.to_dict())
            return 0 if result.failed == 0 else 1
        if args.etl_command == "plan":
            plan = plan_etl(
                root_uri=args.root,
                lake_root=args.lake_root,
                target_shard_size_gb=args.target_shard_size_gb,
                max_shards=args.max_shards,
                include_patterns=args.include,
                exclude_patterns=args.exclude,
            )
            output_path = write_json_uri(args.output, plan.to_dict())
            warehouse = WarehouseService(soft=True)
            events = RuntimeEventLogger(warehouse=warehouse)
            events.emit(
                "etl_plan_created",
                payload={
                    "plan_id": plan.plan_id,
                    "output_uri": output_path,
                    "total_datasets": plan.total_datasets,
                    "total_bytes": plan.total_bytes,
                    "num_shards": len(plan.shards),
                },
                run_id=plan.plan_id,
            )
            _print_json({"plan_path": output_path, "plan": plan.to_dict()})
            return 0
        if args.etl_command == "run-shard":
            plan_raw = read_json_uri(args.plan)
            plan = EtlPlan.from_dict(plan_raw)
            shard_arg: int | str
            if args.shard_id.isdigit():
                shard_arg = int(args.shard_id)
            else:
                shard_arg = args.shard_id
            work_dir = args.work_dir or args.output
            if args.tmp_dir is not None:
                args.tmp_dir.mkdir(parents=True, exist_ok=True)
                # pyarrow / tempfile 都优先读这些变量，便于 K8s 指向 emptyDir。
                os.environ["TMPDIR"] = args.tmp_dir.as_posix()
                os.environ["TEMP"] = args.tmp_dir.as_posix()
                os.environ["TMP"] = args.tmp_dir.as_posix()
            summary = run_shard(
                plan=plan,
                shard_id=shard_arg,
                lake_root=args.lake_root,
                work_dir=Path(work_dir),
                output_summary_uri=args.summary_uri,
                max_workers=args.max_workers,
                fail_fast=args.fail_fast,
                features_config_path=args.features_config,
                ads_config_path=args.ads_config,
                build_ads_layer=args.build_ads,
            )
            _print_json(summary.to_dict())
            if summary.status == "FAIL":
                return 1
            return 0
        if args.etl_command == "merge-summary":
            plan_raw = read_json_uri(args.plan)
            plan = EtlPlan.from_dict(plan_raw)
            payload = merge_shard_summaries(
                plan=plan,
                shard_results_uri=args.shard_results,
                output_uri=args.output,
            )
            _print_json(payload)
            return 0 if payload.get("failed", 0) == 0 else 1
        parser.print_help()
        return 0

    if args.command == "ml-ready":
        from robot_dh.ml_ready import export_ml_ready as _export_ml_ready
        from robot_dh.warehouse.robot_platform import PlatformWarehouse

        if args.ml_ready_command == "export":
            try:
                split_parts = tuple(float(x) for x in args.split.split(","))
            except ValueError:
                print(f"invalid --split={args.split!r}")
                return 1
            if len(split_parts) != 3:
                print(f"--split expects 3 floats; got {split_parts}")
                return 1
            family_filter = None
            if args.dataset_family and args.dataset_family != "all":
                family_filter = [s.strip() for s in args.dataset_family.split(",") if s.strip()]
            result = _export_ml_ready(
                input_root=args.input_root,
                output_uri=args.output,
                quality_root=args.quality_root,
                qc_root=args.qc_root,
                dataset_id=args.dataset_id,
                version=args.version,
                quality_threshold=args.quality_threshold,
                split=(split_parts[0], split_parts[1], split_parts[2]),
                family_filter=family_filter,
                min_episode_length=args.min_episode_length,
            )
            wh_plat = PlatformWarehouse(soft=True)
            wh_plat.record_ml_ready_dataset(
                dataset_id=result.dataset_id,
                version=result.version,
                output_uri=result.output_uri,
                train_uri=result.train_uri,
                val_uri=result.val_uri,
                test_uri=result.test_uri,
                dataset_card_uri=result.dataset_card_uri,
                feature_schema_uri=result.feature_schema_uri,
                quality_filter_uri=result.quality_filter_uri,
                lineage_uri=result.lineage_uri,
                quality_threshold=args.quality_threshold,
                num_train=result.num_train,
                num_val=result.num_val,
                num_test=result.num_test,
                status="OK",
                metrics=result.metrics,
            )
            _print_json(result.to_dict())
            return 0
        if args.ml_ready_command == "list":
            wh_plat = PlatformWarehouse(soft=False)
            try:
                rows = wh_plat.list_ml_ready_datasets()
            except Exception as err:
                print(f"DB unavailable: {err}")
                return 1
            _print_json(rows)
            return 0
        if args.ml_ready_command == "show":
            wh_plat = PlatformWarehouse(soft=False)
            try:
                row = wh_plat.get_ml_ready_dataset(dataset_id=args.dataset_id, version=args.version)
            except Exception as err:
                print(f"DB unavailable: {err}")
                return 1
            if row is None:
                print("not found")
                return 1
            _print_json(row)
            return 0
        parser.print_help()
        return 0

    if args.command == "qc":
        from robot_dh.qc import (
            list_contracts as _list_contracts,
            run_contract as _run_contract,
            profile_dataset as _profile_dataset,
            write_report as _write_report,
        )
        from robot_dh.warehouse.robot_platform import PlatformWarehouse

        if args.qc_command == "contract":
            if args.qc_contract_command == "list":
                _print_json(_list_contracts())
                return 0
            if args.qc_contract_command == "run":
                # v1.7：把 CLI 容灾参数翻译成 env，profile.py / parquet_probe.py / robomimic adapter 已能消费
                if args.max_workers is not None:
                    os.environ["ROBOT_DH_QC_PROBE_CONCURRENCY"] = str(max(1, args.max_workers))
                if args.file_timeout_sec is not None:
                    os.environ["ROBOT_DH_QC_FILE_TIMEOUT_SEC"] = str(args.file_timeout_sec)
                if args.probe_timeout_sec is not None:
                    os.environ["ROBOT_DH_QC_PROBE_TIMEOUT_SEC"] = str(args.probe_timeout_sec)
                if args.max_retries is not None:
                    os.environ["ROBOT_DH_QC_MAX_RETRIES"] = str(max(0, args.max_retries))
                if args.disable_remote_lazy:
                    os.environ["ROBOT_DH_QC_DISABLE_REMOTE_LAZY"] = "1"
                if args.fail_fast:
                    os.environ["ROBOT_DH_QC_FAIL_FAST"] = "1"
                report, profile = _run_contract(
                    dataset_uri=args.dataset_uri,
                    dataset_family=args.dataset_family,
                    dataset_id=args.dataset_id,
                    version=args.version,
                    layer=args.layer,
                )
                artifacts = _write_report(report=report, profile=profile, output_uri=args.output)
                wh_plat = PlatformWarehouse(soft=True)
                wh_plat.upsert_qc_contract(
                    contract_id=report.contract_id,
                    dataset_family=report.dataset_family,
                    version="v1",
                    rules={"items": [r.to_dict() for r in (lambda: __import__("robot_dh.qc.registry", fromlist=["get_contract_runner"]).get_contract_runner(report.dataset_family))()[0]]},
                )
                wh_plat.record_qc_contract_run(
                    run_id=report.run_id,
                    contract_id=report.contract_id,
                    status=report.status,
                    dataset_id=report.dataset_id,
                    version=report.version,
                    dataset_family=report.dataset_family,
                    dataset_uri=report.dataset_uri,
                    duration_sec=report.duration_sec,
                    metrics=report.metrics,
                    failed_rules=report.failed_rules,
                    warning_rules=report.warning_rules,
                    artifacts_uri=artifacts.get("report_uri"),
                )
                wh_plat.record_asset_profile(
                    profile_id=profile.profile_id,
                    asset_uri=profile.asset_uri,
                    dataset_id=profile.dataset_id,
                    version=profile.version,
                    dataset_family=profile.dataset_family,
                    asset_format=profile.asset_format,
                    layer=profile.layer,
                    bytes_=profile.bytes,
                    rows=profile.rows,
                    files_count=profile.files_count,
                    episodes_count=profile.episodes_count,
                    videos_count=profile.videos_count,
                    schema_hash=profile.schema_hash,
                    null_rate=profile.null_rate,
                    profile=profile.profile,
                    status=profile.status,
                )
                _print_json({
                    "run_id": report.run_id,
                    "contract_id": report.contract_id,
                    "status": report.status,
                    "artifacts": artifacts,
                    "metrics": report.metrics,
                })
                return 0 if report.status != "FAIL" else 1
            parser.print_help()
            return 0
        if args.qc_command == "profile":
            profile = _profile_dataset(
                dataset_uri=args.dataset_uri,
                dataset_id=args.dataset_id,
                version=args.version,
                dataset_family=args.dataset_family,
            )
            from robot_dh.lake.store import create_lake_store as _cs
            from robot_dh.lake.uri import join_uri as _ju
            store = _cs(args.output)
            uri = _ju(args.output, "asset_profile.json")
            store.write_json(uri, profile.to_dict())
            wh_plat = PlatformWarehouse(soft=True)
            wh_plat.record_asset_profile(
                profile_id=profile.profile_id,
                asset_uri=profile.asset_uri,
                dataset_id=profile.dataset_id,
                version=profile.version,
                dataset_family=profile.dataset_family,
                asset_format=profile.asset_format,
                bytes_=profile.bytes,
                rows=profile.rows,
                files_count=profile.files_count,
                episodes_count=profile.episodes_count,
                videos_count=profile.videos_count,
                schema_hash=profile.schema_hash,
                null_rate=profile.null_rate,
                profile=profile.profile,
                status=profile.status,
            )
            _print_json({"profile_uri": uri, "profile": profile.to_dict()})
            return 0
        if args.qc_command == "report":
            from robot_dh.lake.store import create_lake_store as _cs
            store = _cs(args.input)
            payload = store.read_json(args.input)
            print("contract_id:", payload.get("contract_id"))
            print("status:", payload.get("status"))
            print("failed_rules:", len(payload.get("failed_rules") or []))
            print("warning_rules:", len(payload.get("warning_rules") or []))
            for r in payload.get("rules") or []:
                print(f"  [{r['status']}] {r['rule_id']} {r['metric']} {r['op']} {r['threshold']} actual={r['actual']}")
            return 0
        parser.print_help()
        return 0

    if args.command == "partition":
        from robot_dh.partition import plan_dataset_partitions
        from robot_dh.partition.models import PartitionPlan
        from robot_dh.warehouse.robot_platform import PlatformWarehouse

        if args.partition_command == "plan":
            plan = plan_dataset_partitions(
                dataset_uri=args.dataset,
                dataset_id=args.dataset_id,
                version=args.version,
                target_partition_size_gb=args.target_partition_size_gb,
                family_hint=args.family,
            )
            output_path = write_json_uri(args.output, plan.to_dict())
            wh_plat = PlatformWarehouse(soft=True)
            for part in plan.partitions:
                wh_plat.record_dataset_partition(
                    partition_id=part.partition_id,
                    dataset_id=plan.dataset_id,
                    version=plan.version,
                    dataset_uri=plan.dataset_uri,
                    partition_type=plan.partition_type,
                    partition_index=part.partition_index,
                    partition_uri=part.partition_uri,
                    dataset_family=plan.dataset_family,
                    input_bytes=part.input_bytes,
                    estimated_rows=part.estimated_rows,
                    status="PLANNED",
                    metrics=part.metrics,
                )
            _print_json({"plan_path": output_path, "plan": plan.to_dict()})
            return 0
        if args.partition_command == "list":
            payload = read_json_uri(args.plan)
            plan = PartitionPlan.from_dict(payload)
            _print_json(plan.to_dict())
            return 0
        if args.partition_command == "run-normalize":
            payload = read_json_uri(args.plan)
            plan = PartitionPlan.from_dict(payload)
            if args.partition_index < 0 or args.partition_index >= len(plan.partitions):
                print(f"partition_index out of range: {args.partition_index} (n={len(plan.partitions)})")
                return 1
            partition = plan.partitions[args.partition_index]
            wh_plat = PlatformWarehouse(soft=True)
            wh_plat.record_dataset_partition(
                partition_id=partition.partition_id,
                dataset_id=plan.dataset_id,
                version=plan.version,
                dataset_uri=plan.dataset_uri,
                partition_type=plan.partition_type,
                partition_index=partition.partition_index,
                partition_uri=partition.partition_uri,
                dataset_family=plan.dataset_family,
                input_bytes=partition.input_bytes,
                estimated_rows=partition.estimated_rows,
                status="RUNNING",
            )
            result = normalize_dataset(
                dataset_uri=partition.partition_uri or partition.dataset_uri,
                output_uri=args.output,
                dataset_id=plan.dataset_id,
                version=plan.version,
                lake_root_uri=args.lake_root,
                resume=args.resume,
                force=args.force,
                heartbeat_interval_sec=args.heartbeat_interval_sec,
                progress_log_interval_sec=args.progress_log_interval_sec,
                warehouse_v16=wh_plat,
            )
            wh_plat.record_dataset_partition(
                partition_id=partition.partition_id,
                dataset_id=plan.dataset_id,
                version=plan.version,
                dataset_uri=plan.dataset_uri,
                partition_type=plan.partition_type,
                partition_index=partition.partition_index,
                partition_uri=args.output,
                dataset_family=plan.dataset_family,
                input_bytes=partition.input_bytes,
                estimated_rows=partition.estimated_rows,
                status="OK" if result.status in {"OK", "RESUMED", "SKIPPED"} else "FAIL",
                metrics={"manifest_uri": result.manifest_uri, "num_samples": result.num_samples},
            )
            _print_json({
                "partition_id": partition.partition_id,
                "output_uri": result.output_uri,
                "manifest_uri": result.manifest_uri,
                "status": result.status,
                "num_samples": result.num_samples,
            })
            return 0
        parser.print_help()
        return 0

    if args.command == "argo":
        from robot_dh.argo import sync_workflow, sync_from_kubectl
        if args.argo_command == "sync":
            try:
                if args.from_json:
                    payload = json.loads(args.from_json.read_text())
                    result = sync_workflow(payload=payload)
                else:
                    result = sync_from_kubectl(args.workflow_name, namespace=args.namespace)
            except Exception as err:
                print(f"argo sync failed: {err}")
                return 1
            _print_json({
                "workflow_name": result.workflow_name,
                "workflow_namespace": result.workflow_namespace,
                "status": result.status,
                "steps": result.steps,
            })
            return 0
        if args.argo_command == "logs":
            if args.argo_logs_command == "index":
                from robot_dh.argo import index_archive_logs
                try:
                    result = index_archive_logs(
                        workflow_name=args.workflow_name,
                        namespace=args.namespace,
                        archive_root=args.archive_root,
                        container_name=args.container_name,
                        from_json_path=args.from_json,
                        dry_run=args.dry_run,
                    )
                except Exception as err:
                    print(f"argo logs index failed: {err}")
                    return 1
                _print_json(result.to_dict())
                return 0
            parser.print_help()
            return 0
        parser.print_help()
        return 0

    if args.command == "local":
        from robot_dh.local_runtime import (
            load_runtime_config,
            load_devscale_registry,
            runtime_doctor,
            verify_local_datasets,
        )

        if args.local_command == "runtime":
            if args.local_runtime_command == "doctor":
                cfg = load_runtime_config(config_path=args.config)
                report = runtime_doctor(
                    runtime_config=cfg,
                    devscale_config_path=args.devscale_config,
                    allow_over_limit=args.allow_over_limit,
                )
                _print_json(report.to_dict())
                return 0 if report.status == "ok" else 1
            parser.print_help()
            return 0
        if args.local_command == "datasets":
            cfg = load_runtime_config(config_path=args.config)
            registry = load_devscale_registry(
                config_path=args.devscale_config, runtime_config=cfg,
            )
            if args.local_datasets_command == "list":
                _print_json(registry.to_dict())
                return 0
            if args.local_datasets_command == "verify":
                report = verify_local_datasets(registry=registry, plan_path=args.plan)
                _print_json(report.to_dict())
                return 0 if report.status == "ok" else 1
            parser.print_help()
            return 0
        parser.print_help()
        return 0

    if args.command == "adapter":
        from robot_dh.adapters import (
            detect_adapter,
            get_adapter,
            list_adapters,
            load_adapter_registry,
        )

        if args.adapter_command == "list":
            reg = load_adapter_registry()
            _print_json({
                "families": list_adapters(),
                "overrides": reg.yaml_overrides,
            })
            return 0
        if args.adapter_command == "detect":
            reg = load_adapter_registry()
            if args.all:
                results = reg.detect_all(args.dataset_uri, dataset_id=args.dataset_id)
                _print_json([r.to_dict() for r in results])
            else:
                res = detect_adapter(args.dataset_uri, dataset_id=args.dataset_id)
                _print_json(res.to_dict())
            return 0
        if args.adapter_command == "probe":
            family = args.family
            if not family:
                det = detect_adapter(args.dataset_uri, dataset_id=args.dataset_id)
                family = det.family
            adapter = get_adapter(family)
            options: dict[str, object] = {}
            for kv in args.option or []:
                if "=" not in kv:
                    continue
                k, v = kv.split("=", 1)
                # 简易类型推断：bool / int / float / str
                lower = v.lower()
                if lower in ("true", "false"):
                    options[k] = (lower == "true")
                else:
                    try:
                        options[k] = int(v)
                    except ValueError:
                        try:
                            options[k] = float(v)
                        except ValueError:
                            options[k] = v
            result = adapter.probe(
                args.dataset_uri,
                sample_limit=args.sample_limit,
                options=options,
            )
            _print_json(result.to_dict())
            return 0 if result.status != "FAIL" else 1
        parser.print_help()
        return 0

    if args.command == "runtime":
        from robot_dh.progress.stale import check_stale_heartbeats

        if args.runtime_command == "heartbeat":
            if args.runtime_heartbeat_command == "check":
                report = check_stale_heartbeats(
                    workflow_name=args.workflow_name,
                    stale_after_sec=args.stale_after_sec,
                    warn_after_sec=args.warn_after_sec,
                    events_dir=args.events_dir,
                )
                _print_json(report.to_dict())
                if args.fail_on == "never":
                    return 0
                if args.fail_on == "warn" and report.status in ("warn", "stale"):
                    return 1
                if args.fail_on == "stale" and report.status == "stale":
                    return 1
                return 0
            parser.print_help()
            return 0
        parser.print_help()
        return 0

    if args.command == "lineage":
        from robot_dh.lineage import build_lineage_report, write_lineage_report
        if args.lineage_command == "report":
            report = build_lineage_report(
                workflow_name=args.workflow_name,
                workflow_namespace=args.namespace,
            )
            uri = write_lineage_report(report, args.output)
            _print_json({"lineage_report_uri": uri, "summary": {
                "workflow_status": report.workflow_status,
                "steps_total": report.steps_total,
                "steps_failed": report.steps_failed,
            }})
            return 0
        parser.print_help()
        return 0

    if args.command == "mutate":
        target = apply_mutation(
            source_dataset=args.dataset,
            output_dataset=args.output,
            mutation=args.mutation,
        )
        _print_json({"output": target.as_posix(), "mutation": args.mutation})
        return 0

    if args.command == "perf":
        if args.perf_command == "reingest-pending":
            stats = reingest_pending_perf_records(
                pending_dir=args.pending_dir,
                archive_dir=args.archive_dir,
                dry_run=args.dry_run,
            )
            _print_json(stats)
            return 0 if stats["failed"] == 0 else 1
        parser.print_help()
        return 0

    if args.command == "benchmark":
        if args.benchmark_command == "run":
            from robot_dh.lake.uri import to_local_path
            report = run_benchmark(
                suite_path=args.suite,
                output_dir=to_local_path(args.output),
                record_to_registry=args.record_to_registry,
                default_config_path=args.config,
                gate_policy_path=args.gate_policy,
            )
            _print_json(report.to_dict())
            return 0 if report.failed == 0 else 1
        if args.benchmark_command == "report":
            text = render_summary_from_dir(args.benchmark_dir)
            print(text)
            return 0
        parser.print_help()
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
