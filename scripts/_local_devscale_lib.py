"""v1.7 Local-First Data Runtime - devscale 共用工具。

仅供 scripts/local_plan_devscale_sync.sh / local_sync_devscale.sh /
local_verify_devscale.sh 等 shell 入口调用，不是 robot_dh CLI 的一部分。

子命令：
  plan     生成 devscale_plan.json / .md（不下载）
  sync-pre 给定 plan，按文件输出 "src\tdst\tsize" 三列，供 shell 循环 mc cp
  sync-post 写 dataset 级 _manifest.json + 汇总 devscale_sync_report.json
  verify   读取 plan + 目标目录，写 devscale_verify_report.json + .md
  summary  打印 devscale 数据集摘要（dataset_id / family / size / status）

设计约束：
  1. include / exclude 是 fnmatch 风格 + 显式支持 `**`（任意层级）。
     `meta/**` 匹配 `meta/info.json` / `meta/episodes/000.json`。
     `data/*.parquet` 仅匹配根下一层。
  2. include 命中后还要再检 exclude，exclude 命中则丢弃。
  3. max_files 按对象 key 字典序取前 N（稳定）。
  4. max_bytes 按上面 max_files 截断后，按字典序累加，超过即截断（含被截 0 文件）。
  5. 不调 boto3 / mc；mc 由 shell 层负责。本模块只做纯计算 + JSON I/O。
  6. 路径换算：
       source_uri    = s3://<bucket>/<prefix>
       target_local  = file:///mnt/d/robot-dh-local/raw/<dataset_id>/<version>
       relative_key  = mc 列出的 key 去掉 <prefix>（不含开头 /）
       dst_local     = target_local + "/" + relative_key
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


# ----------------------------- 通用 -----------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"ERROR: 配置文件不存在: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """拆 s3://bucket/prefix（prefix 不含开头 /，可能为空）。"""
    if not uri.startswith("s3://"):
        raise SystemExit(f"ERROR: source_uri 必须以 s3:// 开头: {uri}")
    rest = uri[len("s3://") :].rstrip("/")
    if "/" in rest:
        bucket, prefix = rest.split("/", 1)
    else:
        bucket, prefix = rest, ""
    return bucket, prefix


def _parse_file_uri(uri: str) -> Path:
    if not uri.startswith("file://"):
        raise SystemExit(f"ERROR: target_local_uri 必须以 file:// 开头: {uri}")
    return Path(uri[len("file://") :])


def _resolve_local_root_override(uri: str, override_root: Path | None) -> Path:
    """如果用户用 --root 覆盖了 ROBOT_DH_LOCAL_DATA_ROOT，
    把 yaml 里的默认 /mnt/d/robot-dh-local 前缀替换成新 root。"""
    base = _parse_file_uri(uri)
    if override_root is None:
        return base
    default_root = Path("/mnt/d/robot-dh-local")
    try:
        rel = base.relative_to(default_root)
        return override_root / rel
    except ValueError:
        # yaml 路径不在默认 root 下，按原样返回（dev 自定义 layout）
        return base


# --------------------- include/exclude matching ---------------------


def _glob_to_regex(pat: str) -> re.Pattern[str]:
    """fnmatch + `**` 通配。

    - `**` 匹配任意（含空）路径片段（即 0+ 个 `/` 分隔的段）；
    - `*`  匹配单个路径段内的任意字符（不含 `/`）；
    - `?`  匹配单字符（不含 `/`）；
    - 其它字符按字面量转义。
    """
    out = ["^"]
    i = 0
    while i < len(pat):
        c = pat[i]
        if c == "*":
            if i + 1 < len(pat) and pat[i + 1] == "*":
                # `**` 或 `**/`：吃掉任意层级
                if i + 2 < len(pat) and pat[i + 2] == "/":
                    out.append("(?:.*/)?")
                    i += 3
                else:
                    out.append(".*")
                    i += 2
                continue
            out.append("[^/]*")
            i += 1
            continue
        if c == "?":
            out.append("[^/]")
            i += 1
            continue
        out.append(re.escape(c))
        i += 1
    out.append("$")
    return re.compile("".join(out))


def _match_any(pat_list: list[str], key: str) -> bool:
    if not pat_list:
        return False
    for pat in pat_list:
        if _glob_to_regex(pat).match(key):
            return True
        # fnmatch 作为兜底（覆盖 `*.hdf5` 单段语义）
        if fnmatch.fnmatchcase(key, pat):
            return True
    return False


# --------------------- mc ls --json 解析 ---------------------


def _read_mc_objects(jsonl_path: Path, prefix: str) -> list[dict[str, Any]]:
    """读 mc ls --json --recursive 的 JSONL 输出。

    每行示例:
      {"status":"success","type":"file","key":"raw/x/v1/meta/info.json","size":123,...}

    返回 [{rel_key, size_bytes, src_key}]，rel_key 去掉 prefix 头。
    """
    if not jsonl_path.exists():
        raise SystemExit(f"ERROR: mc 列对象输出不存在: {jsonl_path}")
    norm_prefix = prefix.strip("/")
    items: list[dict[str, Any]] = []
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("status") != "success" or obj.get("type") != "file":
            continue
        # mc 不同版本 key 字段名：key | name；用宽容兜底
        raw_key = obj.get("key") or obj.get("name") or ""
        # mc ls 在某些版本里返回相对 prefix 的路径（已经不含 bucket/prefix）
        # 这里统一：如果 raw_key 以 prefix 开头则削掉，否则原样
        rel = raw_key
        if norm_prefix and rel.startswith(norm_prefix):
            rel = rel[len(norm_prefix) :].lstrip("/")
        else:
            rel = rel.lstrip("/")
        if not rel:
            continue
        try:
            size = int(obj.get("size", 0))
        except (TypeError, ValueError):
            size = 0
        items.append({"rel_key": rel, "size_bytes": size, "src_key": raw_key})
    # 按 rel_key 字典序稳定
    items.sort(key=lambda r: r["rel_key"])
    return items


def _filter_for_dataset(
    objects: list[dict[str, Any]],
    *,
    include: list[str],
    exclude: list[str],
    max_files: int | None,
    max_bytes: int | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """返回 (selected, stats)。"""
    stage1 = [o for o in objects if _match_any(include, o["rel_key"])]
    stage2 = [o for o in stage1 if not _match_any(exclude, o["rel_key"])]
    after_exclude = len(stage2)

    truncated_by_files = False
    if max_files is not None and len(stage2) > max_files:
        stage2 = stage2[:max_files]
        truncated_by_files = True

    truncated_by_bytes = False
    total = 0
    final: list[dict[str, Any]] = []
    # 记录第 1 个就超 cap 的尺寸，便于 plan json 直接看出"为什么 0 文件"。
    first_oversize_bytes: int | None = None
    for o in stage2:
        size = int(o["size_bytes"])
        if max_bytes is not None and total + size > max_bytes:
            truncated_by_bytes = True
            if not final and first_oversize_bytes is None:
                first_oversize_bytes = size
            break
        final.append(o)
        total += size

    stats = {
        "candidates_after_include": len(stage1),
        "candidates_after_exclude": after_exclude,
        "candidates_after_max_files": len(stage2),
        "selected_count": len(final),
        "selected_bytes": total,
        "truncated_by_max_files": truncated_by_files,
        "truncated_by_max_bytes": truncated_by_bytes,
        "first_oversize_bytes": first_oversize_bytes,
    }
    return final, stats


# --------------------- plan ---------------------


def cmd_plan(args: argparse.Namespace) -> int:
    cfg = _load_yaml(Path(args.config))
    total_cap = int(cfg.get("total_max_bytes", 3_000_000_000))
    datasets_cfg = cfg.get("datasets") or []
    if not datasets_cfg:
        raise SystemExit("ERROR: 配置中没有 datasets")

    override_root = Path(args.root) if args.root else None
    plan: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "config_path": str(Path(args.config).resolve()),
        "robot_dh_local_data_root": str(override_root) if override_root else None,
        "total_max_bytes": total_cap,
        "datasets": [],
        "totals": {},
    }

    grand_total_bytes = 0
    grand_total_files = 0

    for ds in datasets_cfg:
        dataset_id = ds["dataset_id"]
        family = ds["family"]
        version = ds.get("version", "v1")
        source_uri = ds["source_uri"]
        target_uri = ds["target_local_uri"]
        max_bytes = ds.get("max_bytes")
        max_files = ds.get("max_files")
        include = ds.get("include", [])
        exclude = ds.get("exclude", [])

        bucket, prefix = _parse_s3_uri(source_uri)
        target_path = _resolve_local_root_override(target_uri, override_root)

        listing_path = Path(args.listings_dir) / f"{dataset_id}.jsonl"
        objects = _read_mc_objects(listing_path, prefix)

        selected, stats = _filter_for_dataset(
            objects,
            include=include,
            exclude=exclude,
            max_files=max_files,
            max_bytes=max_bytes,
        )

        files_out = []
        for o in selected:
            rel = o["rel_key"]
            src = f"s3://{bucket}/{prefix.rstrip('/')}/{rel}".replace("//", "/").replace(
                "s3:/", "s3://"
            )
            dst = str(target_path / rel)
            files_out.append(
                {
                    "rel_key": rel,
                    "size_bytes": int(o["size_bytes"]),
                    "src_uri": src,
                    "dst_path": dst,
                }
            )

        ds_entry = {
            "dataset_id": dataset_id,
            "family": family,
            "version": version,
            "source_uri": source_uri,
            "source_bucket": bucket,
            "source_prefix": prefix,
            "target_local_uri": str(target_path.as_uri()),
            "target_local_path": str(target_path),
            "max_bytes": max_bytes,
            "max_files": max_files,
            "include": include,
            "exclude": exclude,
            "stats": stats,
            "files": files_out,
        }
        plan["datasets"].append(ds_entry)
        grand_total_bytes += stats["selected_bytes"]
        grand_total_files += stats["selected_count"]

    plan["totals"] = {
        "selected_files": grand_total_files,
        "selected_bytes": grand_total_bytes,
        "total_max_bytes": total_cap,
        "over_limit": grand_total_bytes > total_cap,
    }

    out_json = Path(args.output_json)
    out_md = Path(args.output_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
    out_md.write_text(_plan_to_md(plan), encoding="utf-8")

    if plan["totals"]["over_limit"] and not args.allow_over_limit:
        print(
            f"ERROR: 计划总量 {grand_total_bytes/1e9:.2f} GB 超过上限 "
            f"{total_cap/1e9:.2f} GB；如确需，请追加 --allow-over-limit。",
            file=sys.stderr,
        )
        return 1

    print(
        f"plan ok: files={grand_total_files} bytes={grand_total_bytes} "
        f"cap={total_cap}",
    )
    print(f"json={out_json}")
    print(f"md  ={out_md}")
    return 0


def _plan_to_md(plan: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# devscale 下载计划")
    lines.append("")
    lines.append(f"- 生成时间: {plan['generated_at']}")
    lines.append(f"- 配置文件: {plan['config_path']}")
    if plan.get("robot_dh_local_data_root"):
        lines.append(f"- ROBOT_DH_LOCAL_DATA_ROOT 覆盖: {plan['robot_dh_local_data_root']}")
    totals = plan["totals"]
    lines.append(
        f"- 全局: files={totals['selected_files']}  "
        f"bytes={totals['selected_bytes']}  "
        f"cap={totals['total_max_bytes']}  "
        f"over_limit={totals['over_limit']}"
    )
    lines.append("")
    lines.append("| dataset_id | family | files | size (MiB) | source_uri | target |")
    lines.append("|---|---|---:|---:|---|---|")
    for ds in plan["datasets"]:
        size_mib = ds["stats"]["selected_bytes"] / (1024 * 1024)
        lines.append(
            f"| {ds['dataset_id']} | {ds['family']} | "
            f"{ds['stats']['selected_count']} | {size_mib:.1f} | "
            f"{ds['source_uri']} | {ds['target_local_path']} |"
        )
    lines.append("")
    return "\n".join(lines)


# --------------------- sync-pre ---------------------


def cmd_sync_pre(args: argparse.Namespace) -> int:
    """把 plan 里的所有文件按 dataset 顺序输出三列：src\tdst\tsize_bytes。

    shell 端读这份输出做 mc cp 循环。"""
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        for ds in plan["datasets"]:
            for f in ds["files"]:
                fh.write(f"{ds['dataset_id']}\t{f['src_uri']}\t{f['dst_path']}\t{f['size_bytes']}\n")
    print(f"sync-pre wrote {out}")
    return 0


# --------------------- sync-post ---------------------


def cmd_sync_post(args: argparse.Namespace) -> int:
    """根据 sync 结果文件（src\tdst\tsize\tstatus）汇总 dataset manifest 与 report。"""
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    results: dict[str, dict[str, Any]] = {}
    results_path = Path(args.results)
    if not results_path.exists():
        raise SystemExit(f"ERROR: 同步结果文件不存在: {results_path}")
    for line in results_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        dataset_id, src, dst, size, status = parts[:5]
        results.setdefault(dataset_id, {"files": [], "ok": 0, "fail": 0, "bytes": 0})
        results[dataset_id]["files"].append(
            {"src_uri": src, "dst_path": dst, "size_bytes": int(size), "status": status}
        )
        if status == "ok":
            results[dataset_id]["ok"] += 1
            results[dataset_id]["bytes"] += int(size)
        else:
            results[dataset_id]["fail"] += 1

    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "plan_path": str(Path(args.plan).resolve()),
        "datasets": [],
        "totals": {"ok": 0, "fail": 0, "bytes": 0},
    }

    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]
        target_path = Path(ds["target_local_path"])
        target_path.mkdir(parents=True, exist_ok=True)
        info = results.get(ds_id, {"files": [], "ok": 0, "fail": 0, "bytes": 0})

        manifest = {
            "schema_version": 1,
            "dataset_id": ds_id,
            "family": ds["family"],
            "version": ds["version"],
            "source_uri": ds["source_uri"],
            "local_uri": ds["target_local_uri"],
            "created_at": _now_iso(),
            "sync_tool": "mc",
            "status": "ok" if info["fail"] == 0 and info["ok"] > 0 else (
                "partial" if info["ok"] > 0 else "empty"
            ),
            "size_bytes": info["bytes"],
            "file_count": info["ok"],
            "files": info["files"],
        }
        (target_path / "_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        report["datasets"].append(
            {
                "dataset_id": ds_id,
                "family": ds["family"],
                "version": ds["version"],
                "ok": info["ok"],
                "fail": info["fail"],
                "bytes": info["bytes"],
                "manifest_status": manifest["status"],
                "target_local_path": str(target_path),
            }
        )
        report["totals"]["ok"] += info["ok"]
        report["totals"]["fail"] += info["fail"]
        report["totals"]["bytes"] += info["bytes"]

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"sync-post wrote {out}")
    if report["totals"]["fail"] > 0:
        print(f"WARNING: 有 {report['totals']['fail']} 个文件失败", file=sys.stderr)
        return 1
    return 0


# --------------------- verify ---------------------


def cmd_verify(args: argparse.Namespace) -> int:
    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": _now_iso(),
        "plan_path": str(Path(args.plan).resolve()),
        "datasets": [],
        "totals": {"present": 0, "missing": 0, "wrong_size": 0, "bytes": 0},
    }
    fatal = False
    for ds in plan["datasets"]:
        ds_id = ds["dataset_id"]
        target_path = Path(ds["target_local_path"])
        manifest_path = target_path / "_manifest.json"
        manifest_present = manifest_path.exists()

        missing: list[str] = []
        wrong_size: list[dict[str, Any]] = []
        present_count = 0
        present_bytes = 0
        for f in ds["files"]:
            dst = Path(f["dst_path"])
            if not dst.exists():
                missing.append(f["rel_key"])
                continue
            try:
                actual = dst.stat().st_size
            except OSError:
                missing.append(f["rel_key"])
                continue
            if actual != int(f["size_bytes"]):
                wrong_size.append(
                    {
                        "rel_key": f["rel_key"],
                        "expected": int(f["size_bytes"]),
                        "actual": actual,
                    }
                )
                continue
            present_count += 1
            present_bytes += actual

        ds_report = {
            "dataset_id": ds_id,
            "family": ds["family"],
            "version": ds["version"],
            "target_local_path": str(target_path),
            "manifest_present": manifest_present,
            "plan_file_count": len(ds["files"]),
            "present_files": present_count,
            "missing_files": missing,
            "wrong_size_files": wrong_size,
            "present_bytes": present_bytes,
            "status": "ok"
            if not missing and not wrong_size and manifest_present
            else "fail",
        }
        if ds_report["status"] != "ok":
            fatal = True
        report["datasets"].append(ds_report)
        report["totals"]["present"] += present_count
        report["totals"]["missing"] += len(missing)
        report["totals"]["wrong_size"] += len(wrong_size)
        report["totals"]["bytes"] += present_bytes

    out_json = Path(args.output_json)
    out_md = Path(args.output_md)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    out_md.write_text(_verify_to_md(report), encoding="utf-8")
    print(f"verify wrote {out_json}")
    if fatal:
        print("FAIL: missing 或 wrong_size > 0，或 _manifest.json 缺失", file=sys.stderr)
        return 1
    return 0


def _verify_to_md(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# devscale 校验报告")
    lines.append("")
    lines.append(f"- 生成时间: {report['generated_at']}")
    lines.append(f"- plan: {report['plan_path']}")
    t = report["totals"]
    lines.append(
        f"- 全局: present={t['present']}  missing={t['missing']}  "
        f"wrong_size={t['wrong_size']}  bytes={t['bytes']}"
    )
    lines.append("")
    lines.append("| dataset_id | family | status | present | missing | wrong_size | bytes | manifest |")
    lines.append("|---|---|---|---:|---:|---:|---:|---|")
    for ds in report["datasets"]:
        lines.append(
            f"| {ds['dataset_id']} | {ds['family']} | {ds['status']} | "
            f"{ds['present_files']} | {len(ds['missing_files'])} | "
            f"{len(ds['wrong_size_files'])} | {ds['present_bytes']} | "
            f"{ds['manifest_present']} |"
        )
    lines.append("")
    return "\n".join(lines)


# --------------------- summary ---------------------


def cmd_summary(args: argparse.Namespace) -> int:
    cfg = _load_yaml(Path(args.config))
    override_root = Path(args.root) if args.root else None
    out_lines: list[str] = []
    rows: list[dict[str, Any]] = []
    for ds in cfg.get("datasets", []):
        target_path = _resolve_local_root_override(ds["target_local_uri"], override_root)
        manifest_path = target_path / "_manifest.json"
        manifest_status = "missing"
        size_bytes = 0
        file_count = 0
        if manifest_path.exists():
            try:
                m = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest_status = m.get("status", "unknown")
                size_bytes = int(m.get("size_bytes", 0))
                file_count = int(m.get("file_count", 0))
            except json.JSONDecodeError:
                manifest_status = "corrupt"
        # 推荐的 Argo 参数：local-only dataset_uri
        recommended = {
            "dataset_id": ds["dataset_id"],
            "family": ds["family"],
            "version": ds.get("version", "v1"),
            "dataset_uri": f"file:///mnt/local-data/robot-dh-local/raw/{ds['dataset_id']}/{ds.get('version', 'v1')}",
        }
        rows.append(
            {
                "dataset_id": ds["dataset_id"],
                "family": ds["family"],
                "version": ds.get("version", "v1"),
                "target_local_path": str(target_path),
                "manifest_status": manifest_status,
                "size_bytes": size_bytes,
                "file_count": file_count,
                "recommended_argo_params": recommended,
            }
        )

    if args.output_json:
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(
            json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    # 控制台 markdown
    out_lines.append("# devscale 本地数据集摘要")
    out_lines.append("")
    out_lines.append(
        "| dataset_id | family | version | files | size (MiB) | manifest | local path |"
    )
    out_lines.append("|---|---|---|---:|---:|---|---|")
    for r in rows:
        out_lines.append(
            f"| {r['dataset_id']} | {r['family']} | {r['version']} | "
            f"{r['file_count']} | {r['size_bytes']/(1024*1024):.1f} | "
            f"{r['manifest_status']} | {r['target_local_path']} |"
        )
    out_lines.append("")
    out_lines.append("推荐的 Argo workflow 入参（dataset_uri 走本地 hostPath 挂载）：")
    out_lines.append("")
    for r in rows:
        p = r["recommended_argo_params"]
        out_lines.append(
            f"- {p['dataset_id']} ({p['family']}): "
            f"dataset_uri={p['dataset_uri']}"
        )
    print("\n".join(out_lines))
    return 0


# --------------------- main ---------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="v1.7 devscale 共用工具")
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("plan")
    pp.add_argument("--config", required=True)
    pp.add_argument("--listings-dir", required=True)
    pp.add_argument("--output-json", required=True)
    pp.add_argument("--output-md", required=True)
    pp.add_argument("--root", default=None)
    pp.add_argument("--allow-over-limit", action="store_true")

    sp = sub.add_parser("sync-pre")
    sp.add_argument("--plan", required=True)
    sp.add_argument("--output", required=True)

    spo = sub.add_parser("sync-post")
    spo.add_argument("--plan", required=True)
    spo.add_argument("--results", required=True)
    spo.add_argument("--output", required=True)

    vp = sub.add_parser("verify")
    vp.add_argument("--plan", required=True)
    vp.add_argument("--output-json", required=True)
    vp.add_argument("--output-md", required=True)

    sm = sub.add_parser("summary")
    sm.add_argument("--config", required=True)
    sm.add_argument("--root", default=None)
    sm.add_argument("--output-json", default=None)

    args = p.parse_args(argv)
    dispatch = {
        "plan": cmd_plan,
        "sync-pre": cmd_sync_pre,
        "sync-post": cmd_sync_post,
        "verify": cmd_verify,
        "summary": cmd_summary,
    }
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
