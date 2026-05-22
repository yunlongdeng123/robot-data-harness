from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path

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
from robot_dh.pipeline import compare_reports, run_validation
from robot_dh.registry import RegistryService
from robot_dh.scan import scan_datasets


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

    etl_scan_parser = etl_subparsers.add_parser("scan", help="Discover datasets and run etl run for each")
    etl_scan_parser.add_argument("--root", type=str, required=True, help="Data root (e.g. s3://robot-datasets)")
    etl_scan_parser.add_argument("--lake-root", type=str, required=True)
    etl_scan_parser.add_argument("--limit", type=int, default=None)
    etl_scan_parser.add_argument("--force", action="store_true")
    etl_scan_parser.add_argument("--build-ads", action="store_true")
    etl_scan_parser.add_argument("--features-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_scan_parser.add_argument("--ads-config", type=Path, default=Path("configs/etl_default.yaml"))
    etl_scan_parser.add_argument("--summary-dir", type=Path, default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
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
            payload = lake_list(layer=args.layer, lake_root_uri=args.lake_root)
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
            )
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
            )
            _print_json(result.to_dict())
            return 0 if result.failed == 0 else 1
        parser.print_help()
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
