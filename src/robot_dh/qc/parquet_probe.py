"""parquet 探针：读取 schema / rows / null_rate。

v1.6.5 起新增 lazy 路径 ``probe_parquet_s3``：直接通过 s3fs + PyArrow footer 读取，
不再把整个文件下载到 /tmp。这对 droid 这种 82 MiB 单 shard 的 profile 是结构性优化，
对未来上 GiB 级 parquet 更关键。失败时把底层 exception 的具体类型暴露到 ``error`` /
``cause`` 字段，停止吞成单行 "Max Retries Exceeded"。
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

LOG = logging.getLogger(__name__)

# 与 bridge adapter 复用：识别 episode 列名 + nested language 字段路径。
_EPISODE_COL_CANDIDATES = ("episode_idx", "episode_index", "episode")
_FRAME_COL_CANDIDATES = ("step_idx", "frame_index", "frame_idx", "frame", "index")
_LANGUAGE_HINTS = ("language", "language_instruction", "instruction", "task_description")
_LANGUAGE_EMBEDDING_HINTS = ("language_embedding", "language_emb", "lang_embedding")


def _summarize_exception(err: BaseException) -> dict[str, Any]:
    """暴露 type / message / cause 链，让 contract_report / argo-logs 能拿到根因。

    v1.6.7：
    - 对 ``botocore.exceptions.RetriesExceededError`` 这种 ``raise`` 不带 ``from`` 的
      异常，``__cause__`` 永远是 None；fallback 到 ``__context__``（隐式异常链上一环）。
    - cause 仍空时把 ``repr(err)`` 写进 cause；``traceback`` 字段记最近一次堆栈。

    v1.6.8（fvx5z F2 修复）：
    - 沿 ``__cause__`` / ``__context__`` 链一直回溯，**跳过与 err 同类型的祖先**——
      botocore 的 download_file 流（s3transfer + 内层 client）会把 RetriesExceededError
      互相包成 context，导致 ``cause_type == error_type`` 的自引用，排障毫无信息量。
      最多回溯 8 层，找到第一个不同类型的祖先才算找到 cause；都同类型就保留 None
      并依赖 ``traceback`` 字段兜底。
    """
    import traceback

    err_type = type(err)
    visited: set[int] = {id(err)}
    cause: BaseException | None = None
    cur: BaseException = err
    for _ in range(8):
        nxt = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
        if nxt is None or id(nxt) in visited:
            break
        visited.add(id(nxt))
        if type(nxt) is not err_type:
            cause = nxt
            break
        cur = nxt
    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__))
    out: dict[str, Any] = {
        "error_type": type(err).__name__,
        "error": str(err),
        "cause_type": type(cause).__name__ if cause is not None else None,
        "cause": str(cause) if cause is not None else repr(err),
    }
    if tb and tb.strip() != "NoneType: None":
        out["traceback"] = tb[-2000:]
    return out


def probe_parquet(path: Path) -> dict[str, Any]:
    """对单个本地 parquet 取 schema / rows / null 比例 / 嵌套字段统计。"""
    out: dict[str, Any] = {
        "uri": path.as_posix(),
        "size_bytes": int(path.stat().st_size),
        "readable": False,
        "row_count": 0,
        "schema_columns": [],
        "schema_hash": None,
        "null_rate": None,
    }
    try:
        pf = pq.ParquetFile(str(path))
        _fill_metadata(out, pf)
        # core metric 必须先于 nice-to-have null_rate 调用：null_rate 探针在 lazy s3
        # 下偶发会让 ParquetFile 内部 fobj 状态损坏（ContentLengthError 等），导致
        # 后续 read([episode_idx]) 静默失败，bridge_metrics 回退到 num_rows = traj_len 失真。
        _fill_bridge_metrics(out, pf)
        _maybe_fill_null_rate(out, pf, source=path.as_posix())
    except Exception as err:  # noqa: BLE001
        out.update(_summarize_exception(err))
    return out


def probe_parquet_s3(s3_uri: str, *, sample_rows: bool = True) -> dict[str, Any]:
    """直接在 S3 上 footer-level 读 parquet schema / row_count / row_group 元数据。

    ``sample_rows=True`` 时会再读一次第一个 row_group 做 null_rate 估算（要拉一次
    page 数据，但 row_group 通常远小于整文件）；profile 大量 shard 时可以传 False
    跳过、只读 footer。

    v1.6.5 起 nested column path + per-episode 切分 + language nested 缺失率
    随 sample_rows=True 一并返回，给 bridge_metrics 等下游用。

    v1.6.8（fvx5z F1 修复）：footer / metadata 走默认 s3fs（300s read_timeout，覆盖
    大 row_group 的 sample_rows 路径）；**bridge enrichment 单独走 fast s3fs**
    （read_timeout=10s × max_attempts=3 standard），把 fvx5z 单 step 1849s 拖死
    收敛到 < 30s——根因是 s3fs 默认 ``adaptive`` retry 在 `aiohttp ContentLengthError`
    上累计指数退避能跑半小时。
    """
    out: dict[str, Any] = {
        "uri": s3_uri,
        "size_bytes": None,
        "readable": False,
        "row_count": 0,
        "schema_columns": [],
        "schema_hash": None,
        "null_rate": None,
    }
    try:
        from robot_dh.lake.s3_fs import get_s3fs, split_s3_uri

        fs = get_s3fs()
        bucket, key = split_s3_uri(s3_uri)
        try:
            info = fs.info(f"{bucket}/{key}")
            out["size_bytes"] = int(info.get("size") or 0)
        except Exception as err:  # noqa: BLE001
            # info 失败不致命，size 缺失就保持 None
            LOG.debug("s3fs.info failed for %s: %s", s3_uri, err)
        with fs.open(f"{bucket}/{key}", "rb") as fobj:
            pf = pq.ParquetFile(fobj)
            _fill_metadata(out, pf)
            if sample_rows:
                # core 在前、nice-to-have 在后：详见 probe_parquet 同款注释。
                # bridge enrichment 用 fast s3fs（独立短超时），失败时被 except 兜底，
                # 绝不让单次 enrichment 内部 retry 把 step 拖到 30 min（fvx5z F1）。
                _fill_bridge_metrics_s3_fast(out, s3_uri)
                _maybe_fill_null_rate(out, pf, source=s3_uri)
    except Exception as err:  # noqa: BLE001
        out.update(_summarize_exception(err))
    return out


def _fill_bridge_metrics_s3_fast(out: dict[str, Any], s3_uri: str) -> None:
    """bridge enrichment 单独开一个 fast s3fs ``ParquetFile``，避开默认 client 的
    ``adaptive`` retry 30 min 长尾。失败直接走 ``_summarize_exception``，**不**
    回退到 row_count（保证 contract `episode_count_min` rule 能 FAIL）。
    """
    try:
        from robot_dh.lake.s3_fs import get_s3fs_fast, split_s3_uri

        fs = get_s3fs_fast()
        bucket, key = split_s3_uri(s3_uri)
        with fs.open(f"{bucket}/{key}", "rb") as fobj:
            pf_fast = pq.ParquetFile(fobj)
            _fill_bridge_metrics(out, pf_fast)
    except Exception as err:  # noqa: BLE001
        summary = _summarize_exception(err)
        out["bridge_metrics_error_type"] = summary["error_type"]
        out["bridge_metrics_cause_type"] = summary["cause_type"]
        out["bridge_metrics_error"] = summary["error"]
        LOG.warning(
            "bridge metrics enrichment (fast) failed for %s: error_type=%s cause_type=%s error=%s",
            s3_uri,
            summary["error_type"],
            summary["cause_type"],
            summary["error"],
        )


def _fill_bridge_metrics(out: dict[str, Any], pf: pq.ParquetFile) -> None:
    """把 per-episode 长度 + language nested 缺失率写到 probe 输出。

    Bridge / DROID 之类按 ``episode_idx`` 切 trajectory 的 dataset，v1.6 之前 QC 把
    单个 parquet 当作一条 trajectory 统计 traj_len，p50=p95=row_count；这里读一列
    episode_idx + 一列 language 修正这个口径。

    v1.6.7：失败时改 LOG.warning（之前 LOG.debug 被吞掉），并把 ``error_type / cause_type``
    写到 probe dict 的 ``bridge_metrics_error_type / bridge_metrics_cause_type``，让
    contract aggregator 能直接看到 core metric 失败的根因，**绝不回退到 num_rows**。
    """
    try:
        schema = pf.schema_arrow
        names_lower = {n.lower(): n for n in schema.names}
        ep_col = next((names_lower[c] for c in _EPISODE_COL_CANDIDATES if c in names_lower), None)
        if ep_col is not None:
            ep_values = pf.read([ep_col]).column(ep_col).to_pylist()
            from collections import Counter

            counter = Counter(ep_values)
            out["per_episode_lengths"] = [counter[k] for k in sorted(counter)]
            out["episode_column"] = ep_col

        lang_text_col = None
        for cand in _LANGUAGE_HINTS:
            match = next((n for n in schema.names if cand in n.lower()), None)
            if match is not None:
                lang_text_col = match
                break
        if lang_text_col is not None:
            values = pf.read([lang_text_col]).column(lang_text_col).to_pylist()
            total = len(values)
            if total:
                missing = sum(
                    1 for v in values if v is None or (isinstance(v, str) and not v.strip())
                )
                out["language_missing_rate"] = float(missing) / float(total)
                out["language_path"] = lang_text_col
                return

        paths = out.get("nested_columns") or _flatten_schema_paths(schema)
        emb_path = _find_dotted_path(paths, _LANGUAGE_EMBEDDING_HINTS)
        if emb_path is None:
            return
        parts = emb_path.split(".")
        root_col = parts[0]
        col = pf.read([root_col]).column(root_col).combine_chunks()
        leaf = col
        for p in parts[1:]:
            if pa.types.is_struct(leaf.type):
                leaf = leaf.field(p)
            else:
                break
        values = leaf.to_pylist()
        if not values:
            return
        missing = sum(1 for v in values if v is None or (hasattr(v, "__len__") and len(v) == 0))
        out["language_missing_rate"] = float(missing) / float(len(values))
        out["language_path"] = emb_path
    except Exception as err:  # noqa: BLE001
        summary = _summarize_exception(err)
        out["bridge_metrics_error_type"] = summary["error_type"]
        out["bridge_metrics_cause_type"] = summary["cause_type"]
        out["bridge_metrics_error"] = summary["error"]
        LOG.warning(
            "bridge metrics enrichment failed for %s: error_type=%s cause_type=%s error=%s",
            out.get("uri"),
            summary["error_type"],
            summary["cause_type"],
            summary["error"],
        )


def _fill_metadata(out: dict[str, Any], pf: pq.ParquetFile) -> None:
    schema = pf.schema_arrow
    names = list(schema.names)
    out["readable"] = True
    out["row_count"] = int(pf.metadata.num_rows)
    out["num_row_groups"] = int(pf.num_row_groups)
    out["schema_columns"] = names
    out["nested_columns"] = _flatten_schema_paths(schema)
    out["schema_hash"] = hashlib.sha256(
        "|".join(f"{n}:{str(schema.field(n).type)}" for n in names).encode()
    ).hexdigest()


def _flatten_schema_paths(schema: pa.Schema) -> list[str]:
    """递归把 struct/list 子字段拍平成 dotted-path：

    例如 ``state: struct<end_effector_pose: struct<x,y,z>, language_embedding: list<double>>``
    -> ``["state", "state.end_effector_pose", "state.end_effector_pose.x", ...,
          "state.language_embedding", "state.language_embedding.element"]``。

    用于 QC 在不读全表的前提下识别嵌套 schema 里的 language / pose 字段，
    解决 v1.6 bridgedata_v2 contract `language_missing_rate=1.0` 假阳性。
    """
    out: list[str] = []

    def walk(field: pa.Field, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        out.append(path)
        ftype = field.type
        if pa.types.is_struct(ftype):
            for i in range(ftype.num_fields):
                walk(ftype.field(i), path)
        elif pa.types.is_list(ftype) or pa.types.is_large_list(ftype) or pa.types.is_fixed_size_list(ftype):
            value_field = ftype.value_field
            if pa.types.is_struct(value_field.type):
                for i in range(value_field.type.num_fields):
                    walk(value_field.type.field(i), path)
            # 不递归到 element 标量；上层只需要知道列存在。

    for name in schema.names:
        walk(schema.field(name), "")
    return out


def _find_dotted_path(paths: list[str], leaf_hints: tuple[str, ...]) -> str | None:
    """在拍平后的 column path 里找首个 leaf 名称匹配的 dotted-path。"""
    for path in paths:
        leaf = path.rsplit(".", 1)[-1].lower()
        if any(h == leaf or h in leaf for h in leaf_hints):
            return path
    return None


def collect_per_episode_lengths_from_path(path: Path) -> list[int]:
    """本地 parquet：按 episode 列（episode_idx/episode_index 等）切分，返回长度列表。

    没识别到 episode 列时返回 ``[num_rows]`` 兜底，与改造前等价。
    """
    try:
        schema = pq.read_schema(path)
    except Exception:
        return []
    ep_col = next(
        (
            name
            for name in schema.names
            for cand in _EPISODE_COL_CANDIDATES
            if name.lower() == cand
        ),
        None,
    )
    if ep_col is None:
        try:
            num_rows = int(pq.ParquetFile(str(path)).metadata.num_rows)
        except Exception:
            num_rows = 0
        return [num_rows] if num_rows else []
    try:
        col = pq.read_table(str(path), columns=[ep_col]).column(ep_col).to_pylist()
    except Exception:
        return []
    from collections import Counter

    counter = Counter(col)
    # 输出按 episode_id 排序的列表，便于跨 shard 稳定 percentile
    return [counter[k] for k in sorted(counter)]


def collect_language_missing_rate_from_path(path: Path) -> float | None:
    """本地 parquet：探测 nested language 字段的缺失率。

    优先：
    1. 顶层 ``language_instruction`` / ``language`` 等字符串列：null 或空串 -> missing
    2. 嵌套 ``state.language_embedding`` list<double>：长度 0 -> missing
    返回 None 表示 schema 里没有任何 language 字段（与"完全缺失"区分开）。
    """
    try:
        schema = pq.read_schema(path)
    except Exception:
        return None

    # 1) 顶层文本列
    for cand in _LANGUAGE_HINTS:
        match = next((n for n in schema.names if cand in n.lower()), None)
        if match is not None:
            try:
                values = pq.read_table(str(path), columns=[match]).column(match).to_pylist()
            except Exception:
                continue
            total = len(values)
            if not total:
                return None
            missing = sum(1 for v in values if v is None or (isinstance(v, str) and not v.strip()))
            return float(missing) / float(total)

    # 2) state.language_embedding（list<double>）
    paths = _flatten_schema_paths(schema)
    emb_path = _find_dotted_path(paths, _LANGUAGE_EMBEDDING_HINTS)
    if emb_path is None:
        return None
    parts = emb_path.split(".")
    root_col = parts[0]
    try:
        col = pq.read_table(str(path), columns=[root_col]).column(root_col)
    except Exception:
        return None
    leaf = col.combine_chunks()
    for p in parts[1:]:
        if pa.types.is_struct(leaf.type):
            leaf = leaf.field(p)
        else:
            # 已经到 list 层，不再下钻
            break
    try:
        values = leaf.to_pylist()
    except Exception:
        return None
    if not values:
        return None
    missing = sum(1 for v in values if v is None or (hasattr(v, "__len__") and len(v) == 0))
    return float(missing) / float(len(values))


def _maybe_fill_null_rate(out: dict[str, Any], pf: pq.ParquetFile, *, source: str) -> None:
    """null_rate 走 footer row-group statistics，不再二次 GET row_group 数据。

    v1.6.7：之前用 ``pf.read_row_group(0)`` 在 lazy s3fs 路径下偶发抛 IndexError
    或 ``ContentLengthError 400 Not enough data to satisfy content length header``，
    然后被 except 吞掉，并且会把 ParquetFile 内部 fobj 状态搞坏，进而拖坏后续
    ``_fill_bridge_metrics`` 的 read([episode_idx]) 调用，最终 traj_p50 = num_rows
    回归 dls4z 失真。footer statistics 在 ParquetFile 构造时已经一次性拉到，
    不会触发二次 IO；失败时 LOG.warning + cause，绝不静默吞掉。
    """
    try:
        meta = pf.metadata
        if meta is None or meta.num_rows <= 0 or meta.num_columns <= 0:
            return
        total_cells = int(meta.num_rows) * int(meta.num_columns)
        null_cells = 0
        ok = True
        for g in range(meta.num_row_groups):
            rg = meta.row_group(g)
            for c in range(meta.num_columns):
                stats = rg.column(c).statistics
                if stats is None or not getattr(stats, "has_null_count", False):
                    ok = False
                    break
                null_cells += int(stats.null_count)
            if not ok:
                break
        if ok and total_cells > 0:
            out["null_rate"] = float(null_cells) / float(total_cells)
        else:
            out["null_rate"] = None
    except Exception as err:  # noqa: BLE001
        summary = _summarize_exception(err)
        out["null_rate"] = None
        LOG.warning(
            "parquet null_rate probe failed for %s: error_type=%s cause_type=%s error=%s",
            source, summary["error_type"], summary["cause_type"], summary["error"],
        )
