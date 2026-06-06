# bridge-qc metric 回归 + robomimic hdf5 probe 回归 + droid-normalize 卡死 修复需求

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：WSL 侧 `robot-data-harness` 主项目
> 优先级：P1（v1.6 `multisource-scale30` 三支 fanout 全部各自卡住一处，workflow 整体不可能到达 ml-ready）
> 关联：
>
> - [`docs/v1_6_droid_robomimic_qc_request.md`](v1_6_droid_robomimic_qc_request.md)（droid-qc / robomimic 上一份需求，§4.2.2 lerobot v2 lazy 已闭环；§5.1/§5.2 hdf5 路径**未生效或回归**）
> - [`docs/v1_6_fhkvr_step_failures_request.md`](v1_6_fhkvr_step_failures_request.md) §5.1.2（`profile_hdf5` 用 boto3 materialize-first 的最早要求；本次 ddbfb 再次违反）
> - [`docs/v1_6_bridgedata_v2_normalize_adapter_request.md`](v1_6_bridgedata_v2_normalize_adapter_request.md) §3 B 类（bridge-qc traj/episode 切分；本次 metric 回归到 dls4z 时代）
> - [`docs/v1_6_etl_perf_runs_schema_align_request.md`](v1_6_etl_perf_runs_schema_align_request.md)（perf fallback；本次 ddbfb 又落 2 条 pending，累计 4 条）
> - 本次完整 step 归档：[`docs/runs/20260525/robot-dh-multisource-scale30-ddbfb/INDEX.md`](runs/20260525/robot-dh-multisource-scale30-ddbfb/INDEX.md)

## 1. 背景

`robot-dh-multisource-scale30-ddbfb` 是 wsl 侧 v1.6 修复 PR（实现 [`v1_6_droid_robomimic_qc_request.md`](v1_6_droid_robomimic_qc_request.md) §4.2 几条）上线后**第一次重新提交**的 multisource-scale30 workflow。它把 qptk9 列出的 droid-qc 0B fail 完全消化（droid-qc 0.86s PASS、95658 episodes、156 parquet、14 videos），但**同时把 bridge-qc 和 robomimic-qc 的稳定版本退坏了**：

```text
✅ runner_boot 首行 print 兜底         （qptk9 §4.2.1 闭环：9 个 pod 全打出 argv + env_keys）
✅ droid-qc 用 lerobot v2 lazy footer  （qptk9 §4.2.2 闭环：profile_dataset: detected lerobot v2 layout, using lazy footer path）
✅ droid contract 落地                 （qptk9 §4.2.3 闭环：contract_id=droid_multimodal_v1）
❌ bridge-qc metric 回归到 dls4z 时代  （traj_p50=314, p95=314, episode_count=0，duration 2005s 内部 retry loop）
❌ robomimic hdf5 probe 走 fsspec    （16 次 RetriesExceededError，cause_type=None；fhkvr §5.1.2 要的 materialize-first + cause 暴露都没生效）
🆕 droid-normalize 卡死 RUNNING      （ods/_checkpoint.json 写了 RUNNING 之后 2.5h 无进展；archive log 完全缺失）
```

完整时间线与对账见 [INDEX.md §综述表](runs/20260525/robot-dh-multisource-scale30-ddbfb/INDEX.md#综述v16-修了-1-个新洞回归了-2-个旧洞新爆-1-个洞)。

## 2. 错误清单与优先级

| # | 错误 | 致命？ | 阻塞下游？ | 责任方 |
|---|------|--------|-----------|--------|
| R1 | **bridge-qc metric 回归 dls4z**：`traj_p50/p95 = num_rows`、`episode_count=0`、duration 2005s 内部 retry loop | 否（status PASS） | **是**（ml-ready / dashboard 拿到错误统计） | robot-data-harness（bridge contract aggregator + null_rate probe 异常处理） |
| R2 | **robomimic hdf5 probe 退到 fsspec**：26 个 hdf5 全 `RetriesExceededError`、`cause_type=None`、无 final json、无新 contract_report | **是** | **是**（robomimic 通路 100% FAIL） | robot-data-harness（`profile_hdf5` 改回 boto3 materialize-first + 暴露 `__cause__`） |
| R3 | **droid-normalize 卡死 RUNNING**：`_checkpoint.json` 写 RUNNING 后 2.5h 无进展；archive log 完全缺失（pod stdout 0 行也没归档） | **是** | **是**（droid 通路在 normalize 阶段 100% FAIL） | robot-data-harness（normalize 入口的 `runner_boot` 兜底；lerobot v2 normalize adapter；partition fanout 编排） |

> infra 侧（`robot-dh-infra`）所有检查项均通过：raw 数据完整可达（`mc stat` 秒返）、`robotdhapp` policy 含 `robot-datasets/*` GetObject、archive log 链路同 workflow 内其他 pod 都正常归档（bridge/lake-list/partition-plan/etl-phase 9 个 pod 都拿到）。详见 §3。

## 3. infra 端零改动证明

| 检查项 | 结论 | 证据 |
|--------|------|------|
| droid raw 18 GiB 完整 | ✅ | `mc du rdh/robot-datasets/raw/droid_lerobot_scale30/v1/` → 18 GiB / 546 obj；同 workflow 内 **droid-qc 0.86s 跑通**，证明 droid 路径下整条 IO 链可用 |
| robomimic raw 6.5 GiB 完整 | ✅ | `mc stat rdh/robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/low_dim_dense_v15.hdf5` 立即返回 1.1 GiB + ETag |
| bridge raw 227 MiB 完整 | ✅ | 同 workflow 内 `bridge-normalize` `materialize_input` 历史能拉完 227 MiB；ddbfb 这次 SKIP via manifest 也证明 ods 端可读 |
| `robotdhapp` policy | ✅ | `s3:GetObject arn:aws:s3:::robot-datasets/*` 已在；`bridge-qc` 同 endpoint 同 contract template 同步 retry 之后能 PASS |
| MinIO endpoint 可达 | ✅ | 同 workflow 内 9 pod 中 8 个能正常 GET/PUT；只有 robomimic-qc 在 hdf5 probe 走异常路径 |
| `archiveLogs` 链路 | ✅ | bridge/droid/robomimic/lake-list/partition-plan/etl-phase 9 个 pod 都正常归档；只有 droid-normalize pod 完全缺失 ← 是 **pod 没创建 / OOM SIGKILL 前未启动业务进程**，**不是** archive 链路问题 |
| infra 侧 `etl_perf_runs` 列 | ❌ 已知漂移，**走 fallback 不阻塞业务** | qptk9 / ddbfb 4 条 pending records 已落 `s3://robot-dh-artifacts/perf-records-pending/`，等 `006_etl_perf_runs_align.sql` 上线后 `robot-dh perf reingest-pending` 一次性回填（**与本需求 PR 互不依赖**） |

→ ddbfb 同一条 workflow 内 8 个 pod 都正常跑通 + 写出归档 log，**R1 / R2 / R3 都不是** policy / bucket / endpoint / 文件存在性 / 网络拓扑层面的问题，全归 `robot-data-harness` 主项目。

## 4. 错误 R1：`bridge-qc` metric 回归 dls4z + duration 2005s 内部 retry loop

### 4.1 现场对账（一张表读完）

| 字段 | dls4z (5/24 早) | jddlp (5/24 中) | qptk9 (5/24 晚 PASS) | **ddbfb (5/25 凌晨 PASS)** | 真值（episode_idx 切 group） |
|------|-----------------|------------------|----------------------|----------------------------|------------------------------|
| `traj_len_p50` | 314 | **108** | **108** | **314** ← 回归 | 108 |
| `traj_len_p95` | 314 | **131** | **131** | **314** ← 回归 | 131 |
| `episode_count` | (missing) | **3** | **3** | **0** ← 比 dls4z 更差 | 3 |
| `language_missing_rate` | 1.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| duration_sec | ? | < 1s | < 1s | **2005s ≈ 33min** | < 1s |
| 失败前的 WARN | adapter mismatch | (无) | (无) | `tuple index out of range` → `ContentLengthError 400 Not enough data to satisfy content length header` | — |

3 处同时**完全复刻 dls4z 行为**，强烈提示 wsl 端在 qptk9 之后引入的某个 PR（疑似"加 `parquet null_rate probe` 异常软降级"）把 metric aggregator 又踢回**整 parquet 当 1 traj** 的失真路径。

### 4.2 复现日志（关键 2 行）

```text
# ddbfb 第 1 次 bridge-qc（pod 1610040161）：
{"message": "parquet null_rate probe failed for s3://robot-datasets/raw/bridgedata_v2_scale30/v1/data/shard_0-00000-of-00001.parquet: tuple index out of range", "level": "WARNING"}
# 然后 step exit 非 0，无 final json

# ddbfb 第 2 次 bridge-qc retry（pod 1140413924）：
{"message": "parquet null_rate probe failed for s3://...shard_0-00000-of-00001.parquet: Response payload is not completed: <ContentLengthError: 400, message='Not enough data to satisfy content length header.'>", "level": "WARNING"}
{ "status": "PASS", "metrics": {"traj_len_p50": 314, "traj_len_p95": 314, "episode_count": 0, ...} }
```

### 4.3 根因推断

3 个独立证据指向**同一个 `null_rate probe` 错误处理路径**：

1. **`tuple index out of range`**（IndexError）：通常发生在 `arr.shape[0]` 但 `arr.shape == ()`（标量），或者 `result[0]` 但 result 是空 tuple
2. **`ContentLengthError 400 Not enough data to satisfy content length header`**：aiohttp / `s3fs` lazy footer read 拿到的 body 不足，常见于 `pq.ParquetFile(fs.open(uri)).schema_arrow` 后再次 `pf.read_row_group(0)` 的二次 GET，aiohttp connection pool 复用了一个被对端半关的连接
3. **`episode_count=0` + `traj_p50=314`**：probe 异常被 except 吞掉之后 metric aggregator 回退到 default：把整 parquet 看成"1 个长度 314 的 trajectory"且不调用 `groupby('episode_idx')`

### 4.4 修复方案

#### 4.4.1 **绝对不要**把 probe 异常软降级成"metric default"

```python
# 错误模式（疑似 ddbfb 引入的新 PR 行为）：
def aggregate_bridge_v2(parquet_uri: str) -> dict:
    metrics = _default_bridge_metrics()  # traj_p50 = num_rows, episode_count = 0
    try:
        metrics.update(_probe_null_rate(parquet_uri))   # ★ 这里抛 IndexError 后被吞
        metrics.update(_compute_traj_metrics(parquet_uri))
    except Exception as exc:
        logger.warning(f"parquet null_rate probe failed for {parquet_uri}: {exc}")
        # ★ default 被保留，traj_p50 = num_rows = 314 → 失真 PASS
    return metrics

# 正确模式：
def aggregate_bridge_v2(parquet_uri: str) -> dict:
    # null_rate probe 是 nice-to-have，traj/episode 切分是 core，两者必须分开 try
    try:
        null_rate = _probe_null_rate(parquet_uri)
    except Exception as exc:
        logger.warning(f"parquet null_rate probe failed for {parquet_uri}: {exc} "
                       f"(cause_type={type(exc.__cause__).__name__})")
        null_rate = {"null_rate": None}    # 报告里写 None / "n/a"，不是 0
    
    # ★ traj/episode 必须算出来，算不出就 FAIL，绝不静默回退
    traj_metrics = _compute_traj_metrics_by_episode_idx(parquet_uri)
    if traj_metrics["episode_count"] == 0:
        raise ValueError(
            f"bridge contract aggregator could not extract any episode from {parquet_uri}; "
            f"expected groupby('episode_idx') to yield >=1 group; "
            f"got num_rows={traj_metrics.get('num_rows')}"
        )
    return {**null_rate, **traj_metrics}
```

要点：

- `traj_len` / `episode_count` 是 **bridge contract 的 core metric**，统计算不出来必须 FAIL，**不能**回退到 num_rows
- `null_rate` 是 nice-to-have，可以失败但**必须**在 JSON 中写 `null_rate: null` 而不是 `null_rate: 0`，避免"0.0 看上去像合法值"
- WARN 行必须带 `cause_type={type(exc.__cause__).__name__}`（与 fhkvr §5.1.1 cause 暴露同款）

#### 4.4.2 给 `null_rate probe` 加 connection retry + reuse-safe

`ContentLengthError 400 Not enough data` 是 aiohttp pool 复用半关连接的典型表现。建议：

```python
# 不要复用 s3fs / aiohttp client；profile 一个 parquet 一个 session
def _probe_null_rate(parquet_uri: str) -> dict:
    fs = s3fs.S3FileSystem(
        ...,
        config_kwargs={
            "connect_timeout": 10,
            "read_timeout": 60,
            "retries": {"max_attempts": 5, "mode": "adaptive"},
        },
        # ★ 单次 probe 创建独立 session，避免长 run 复用半关连接
        client_kwargs={"endpoint_url": ..., "use_ssl": False},
        # ★ 关闭 aiohttp keepalive（s3fs/aiobotocore 默认 keepalive 60s，对短探测连接没意义）
        skip_instance_cache=True,
    )
    with fs.open(parquet_uri.removeprefix("s3://"), "rb") as fobj:
        pf = pq.ParquetFile(fobj)
        # 一次 read footer，整列 null_count 由 footer 统计直接拿，不要再 read_row_group
        col_null_counts = {
            pf.schema_arrow.field(i).name:
                sum(pf.metadata.row_group(g).column(i).statistics.null_count
                    for g in range(pf.num_row_groups))
            for i in range(pf.schema_arrow.num_fields)
        }
        return {"null_rate": col_null_counts}
```

要点：

- 走 footer statistics 拿 null_count，**不二次 GET row_group**，避免 `ContentLengthError`
- 单 probe 单 session，结束就 close

#### 4.4.3 加退避 cap：duration 2005s 太长

主项目的 retry 模板（疑似 wsl 端在 step 模板里写了 step-level retry，外加 boto3 retries=10，叠加后 16min × 2 = 33min）建议：

```yaml
# Argo workflow template
- name: qc-contract-run
  retryStrategy:
    limit: 2
    retryPolicy: OnFailure
    backoff:
      duration: 30s
      factor: 2
      maxDuration: 5m            # ★ cap，不能让 2 次 retry 跑 33min
  activeDeadlineSeconds: 1800    # ★ 30 分钟硬上限，否则就 SIGTERM
```

→ 期望优化后单 bridge-qc < 30s。

## 5. 错误 R2：`robomimic-qc` hdf5 probe 退到 fsspec / 26 文件全 `RetriesExceededError`

### 5.1 复现日志

```text
{"event": "runner_boot", "argv": ["qc", "contract", "run", "--dataset-family", "robomimic", ...], ...}
{"message": "hdf5 probe failed for s3://robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/low_dim_dense_v15.hdf5: error_type=RetriesExceededError error=Max Retries Exceeded cause_type=None cause=None", "level": "WARNING"}
{"message": "hdf5 probe failed for s3://robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/demo_v15.hdf5: error_type=RetriesExceededError error=Max Retries Exceeded cause_type=None cause=None", "level": "WARNING"}
... 共 16 行（13 个 hdf5 文件，每条相隔 3–12 分钟）...
# 然后 step exit 非 0，contract_report 没产出
```

时间分布（节选）：

| 行 | timestamp | hdf5 | 距上一行 |
|---|-----------|------|----------|
| 1 | 18:40:26 | can/mg/low_dim_dense | — |
| 2 | 18:40:27 | can/mg/demo | 1s |
| 3 | 18:47:53 | can/mg/low_dim_sparse | **7m26s** |
| 4 | 19:07:46 | can/ph/demo | **19m53s** |
| 5 | 19:19:31 | can/ph/low_dim | 11m45s |
| ... | ... | ... | ... |
| 16 | 20:17:50 | tool_hang/ph/low_dim | 4m |

→ 单文件 ~7–20 分钟 retry，**完全串行**（fhkvr §5.2 / qptk9 §5.2 G2 并发要求未生效）。

### 5.2 根因推断

`error_type=RetriesExceededError` + `cause_type=None` 强烈指向 wsl 端 hdf5 probe **没有按 fhkvr §5.1.2 要求改回 `boto3.download_file → /tmp → h5py.File(local)` 模式**，而是依然走 **fsspec / h5py 远端 random read**：

| fhkvr §5.1.2 要求 | ddbfb 现状 |
|-------------------|------------|
| `boto3.client('s3', config=Config(retries={'max_attempts': 10, 'mode': 'adaptive'}))` | ❌ 用了 `s3fs` / aiobotocore 默认 retry（5 次 standard） |
| `download_file(bucket, key, /tmp/xxx.hdf5)` 整文件落地 | ❌ 用了 `h5py.File(fsspec_open(uri))` 远端 read-by-range |
| 失败时 `logger.warning('cause=%r', exc.__cause__)` | ❌ `cause_type=None cause=None`，吞了底层异常 |
| 26 文件并发 download | ❌ 串行循环 |

### 5.3 修复方案

#### 5.3.1 hdf5 probe 必须走 materialize-first

```python
# robot_data_harness/qc/profile_hdf5.py
import tempfile
import boto3
import h5py
from botocore.config import Config
from urllib.parse import urlparse
from pathlib import Path

_BOTO3_CFG = Config(
    connect_timeout=10,
    read_timeout=300,
    retries={"max_attempts": 10, "mode": "adaptive"},
)

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["ROBOT_DH_S3_ENDPOINT_URL"],
        region_name=os.environ.get("ROBOT_DH_S3_REGION", "us-east-1"),
        aws_access_key_id=os.environ["ROBOT_DH_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ROBOT_DH_S3_SECRET_KEY"],
        config=_BOTO3_CFG,
    )

def profile_hdf5(s3_uri: str) -> dict:
    parsed = urlparse(s3_uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    with tempfile.TemporaryDirectory(prefix="robot-dh-qc-") as tmpdir:
        local = Path(tmpdir) / Path(key).name
        try:
            _s3().download_file(bucket, key, str(local))     # ★ 整文件落地
        except Exception as exc:
            # ★ 必须暴露 cause
            logger.warning(
                "hdf5 download failed for %s: error_type=%s cause_type=%s cause=%r",
                s3_uri, type(exc).__name__,
                type(exc.__cause__).__name__ if exc.__cause__ else None,
                exc.__cause__,
            )
            return {"status": "FAILED", "uri": s3_uri,
                    "error_type": type(exc).__name__,
                    "cause_type": type(exc.__cause__).__name__ if exc.__cause__ else None,
                    "error": str(exc)}
        try:
            with h5py.File(local, "r") as f:
                episode_lens: list[int] = []
                for demo_name in f["data"].keys():
                    actions = f[f"data/{demo_name}/actions"]
                    episode_lens.append(int(actions.shape[0]))   # ★ 上次 qptk9 §5.1 也要求过
                import numpy as np
                return {
                    "status": "OK",
                    "uri": s3_uri,
                    "demo_count": len(episode_lens),
                    "episode_lens": episode_lens,
                    "episode_len_p50": int(np.percentile(episode_lens, 50)) if episode_lens else 0,
                    "episode_len_p95": int(np.percentile(episode_lens, 95)) if episode_lens else 0,
                    "has_obs": "obs" in f.get("data/" + list(f["data"].keys())[0], {}),
                    "has_actions": True,
                    "has_rewards": "rewards" in f.get("data/" + list(f["data"].keys())[0], {}),
                }
        except Exception as exc:
            logger.warning("hdf5 open failed for %s: %r", local, exc)
            return {"status": "FAILED", "uri": s3_uri, "error": str(exc)}
```

要点：

- **整文件 download 到 `/tmp`，h5py 本地打开**（fhkvr §5.1.2 / qptk9 §5.1 重复要求第三次）
- `cause_type` / `cause` 必须 `repr(exc.__cause__)`，不能 `None`
- 顺手把 qptk9 §5.1 G1 的 `episode_lens` 也落上

#### 5.3.2 26 个 hdf5 并发 + 单文件不重复 download

```python
def run_qc_robomimic(dataset_uri: str) -> dict:
    files = list_hdf5_files(dataset_uri)
    # ★ max_workers=4：MinIO 单 worker ~30 MiB/s，4 路并发 ~120 MiB/s 撞带宽
    with ThreadPoolExecutor(max_workers=4) as ex:
        profiles = list(ex.map(profile_hdf5, files))
    return aggregate_robomimic(profiles)
```

期望 26 文件 × ~250 MiB ÷ 120 MiB/s = ~55s download + ~2s × 26 = ~52s h5py 解析 ≈ **2 分钟跑完**（vs 当前 2 小时 + 全失败）。

#### 5.3.3 单元测试守门

```python
# tests/test_profile_hdf5.py
def test_profile_hdf5_returns_cause_type():
    # 故意制造一个不可达 endpoint
    with mock_unreachable_endpoint():
        r = profile_hdf5("s3://nonexistent/x.hdf5")
        assert r["status"] == "FAILED"
        assert r["cause_type"] is not None    # ← 卡这一行，防止再退回 cause_type=None
        assert "RetriesExceededError" not in r.get("error_type", "") or r["cause_type"]
```

## 6. 错误 R3：`droid-normalize` 卡死 RUNNING 状态、archive log 完全缺失

### 6.1 现场

```bash
$ mc cat rdh/robot-lake/ods/droid_lerobot_scale30/v1/_checkpoint.json
{
  "dataset_id": "droid_lerobot_scale30",
  "version": "v1",
  "phase": "normalize",
  "source_uri": "s3://robot-datasets/raw/droid_lerobot_scale30/v1",
  "output_uri": "s3://robot-lake/ods/droid_lerobot_scale30/v1",
  "status": "RUNNING",
  "completed_steps": [],
  "files": {},
  "metrics": {},
  "schema_version": "1.6",
  "updated_at": "2026-05-24T18:27:37Z"      ← 02:27:37 CST，已经 2.5h 无更新
}

$ mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/
# 只有 _checkpoint.json 365B，没有 _manifest.json / pose.parquet / video_meta.parquet / episode_meta.parquet

$ mc ls rdh/robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-ddbfb/ | grep -i normalize
# 完全为空，没有任何 etl-phase pod 对应 droid-normalize
```

partition-plan 已经把 droid 切成 **6 个 partition**（25–31 文件/分片，~2 GiB/分片，estimated_rows 5.1M–8.0M），理论上应该 fanout 出 6 个 normalize pod。但 archive log 里**一个都没有**。

### 6.2 根因 3 选 1

| 嫌疑 | 证据 | 验证方法 |
|------|------|----------|
| ① pod 启动了但被 SIGKILL（OOMKilled / DeadlineExceeded），stdout buffer 未 flush，与 qptk9 droid-qc 0B 同款 | `_checkpoint.json` 写了 RUNNING 说明 normalize 进入主循环；之后无任何写入说明 pod 死了 | `kubectl get pods -l workflows.argoproj.io/workflow=robot-dh-multisource-scale30-ddbfb -o wide`，看 `Last State.reason` 是否 OOMKilled |
| ② Argo DAG 模板没把 partition-plan 的 6 个 partition fanout 到 normalize | 只看到 1 个 RUNNING checkpoint（如果 6 个 partition 都启动应该看到 6 个 worker 时间戳交错） | `kubectl -n robot-dh get workflows ddbfb -o yaml | yq '.spec.templates[] | select(.name=="droid-normalize-fanout")'` 看是否有 `withParam: "{{tasks.partition-plan.outputs.parameters.partitions}}"` |
| ③ normalize 启动后命中第一个 partition `materialize_input`（2.1 GiB 下载到 `/tmp`），step pod ephemeral storage 撑爆被 evict | bridge 单 partition 是 227 MiB 没问题；droid 单 partition 2.1 GiB 比 bridge 大 10× | `kubectl describe pod <droid-normalize-pod>` 看 `Events: Evicted: ephemeral-storage usage exceeded ...` |

### 6.3 修复方案

#### 6.3.1 normalize 入口同样要打 `runner_boot` 首行（与 qc 路径平齐）

`runner_boot` 在 lake-list / qc-contract-run / partition-plan / etl-phase（bridge 通路）4 类入口都已经覆盖，**但 droid-normalize 这个 pod 没有任何 stdout 行**，说明 etl-phase entrypoint 在某些路径下**没走到 `print(runner_boot)`** 就 SIGKILL 了。

```python
# robot_data_harness/etl/run.py 顶部（建议保证 import 之前）
def _emit_runner_boot():
    import sys, json, os, time
    print(json.dumps({
        "event": "runner_boot",
        "argv": sys.argv,
        "python": sys.version.split()[0],
        "ts": time.time(),
        "env_keys": sorted([k for k in os.environ if k.startswith("ROBOT_DH_")]),
    }), flush=True)

_emit_runner_boot()    # ★ 模块顶层，import 副作用，最早执行

# ... 其他 import / business logic ...
```

要点：

- **模块顶层**而不是 `if __name__ == "__main__":` 里——后者会在 import 阶段抛 ImportError 时 print 不到
- `flush=True` 强制刷盘，SIGKILL 不会丢

#### 6.3.2 step container 加 ephemeral-storage limit + emptyDir 兜底

```yaml
# Argo workflow template - etl-phase for droid
- name: etl-phase
  container:
    resources:
      requests:
        memory: 2Gi
        cpu: 1
        ephemeral-storage: 4Gi      # ← droid 单 partition 2.1 GiB
      limits:
        memory: 8Gi                 # ← 给 h5py / pyarrow 留 buffer
        cpu: 4
        ephemeral-storage: 16Gi     # ← materialize_input 2.1 GiB + load_bundles 中间态 ≤ 16 GiB
    volumeMounts:
      - name: workdir
        mountPath: /tmp/robot-dh
  volumes:
    - name: workdir
      emptyDir:
        sizeLimit: 16Gi
```

要点：

- 用 emptyDir 替代写 `/tmp`，**让 ephemeral-storage limit 真正生效**
- sizeLimit + ephemeral-storage limit 双层兜底，避免 droid 2 GiB partition 把 node disk 撑爆触发 NodePressure 全 pod evict

#### 6.3.3 normalize 进度心跳（避免 RUNNING 卡死无感知）

`_checkpoint.json` 一次写 RUNNING 之后整段 normalize 都不更新 → 看不到进度。建议：

```python
# 每完成一个 internal step 就 PUT 一次 _checkpoint.json
def normalize(partition_uri: str, ods_uri: str):
    ckpt = Checkpoint(ods_uri)
    ckpt.update(status="RUNNING", completed_steps=[])
    
    materialize_input(partition_uri)
    ckpt.update(completed_steps=["materialize_input"])     # ← 每完成一步就 PUT
    
    bundles = load_bundles()
    ckpt.update(completed_steps=["materialize_input", "load_bundles"])
    
    write_pose_parquet(bundles)
    ckpt.update(completed_steps=[..., "write_pose_parquet"])
    
    # ... 等等 ...
    
    ckpt.update(status="OK", manifest_uri=...)
```

要点：

- bridge 的 normalize 已经写了 `completed_steps=[materialize_input, load_bundles, ..., write_manifest]` 7 步 → 模板已经存在
- droid 的 normalize 写了 RUNNING + `completed_steps=[]` 后没下文 → **完全没走到第一步 `materialize_input`**，更怀疑是 §6.2 ① OOM / ② fanout 没起 / ③ ephemeral 撑爆

#### 6.3.4 给 droid lerobot v2 写 normalize adapter（与 §4.2.2 qc 路径平齐）

qptk9 §4.2.2 的 lerobot v2 lazy 修复**只在 qc profile 路径生效**。normalize 路径是 lerobot v2 数据写出"pose / video_meta / episode_meta"三份 parquet 到 ods，**正交于 qc**。如果 wsl 端的 normalize adapter 仅识别 bridge / robomimic 而 droid 走默认 LeRobot adapter 但 lerobot v2 与 LeRobot v1（pose 扁平 7-float）schema 不匹配，会 fail-fast 抛 `ValueError`——但**这条 ValueError 也应该在 archive log 里能看到**。当前 log 完全为空说明根本没走到 adapter 这一步，确实指向 OOM / Evicted / pending 调度。

建议 wsl 侧：

```python
@register_normalize_adapter("droid_lerobot_scale30")
def adapt_droid_lerobot_v2(table: pa.Table) -> NormalizedEpisodes:
    # droid lerobot v2 列结构（与 chunk parquet 一致）：
    #   episode_index, frame_index, timestamp, 
    #   observation.state, observation.images.<camera>,
    #   action (struct or list[7]), reward, done
    # → groupby(episode_index) 拆 episode，pose 走 observation.state 或 from action
    ...
```

→ 即便 §6.3.1 / §6.3.2 解决了"pod 起不来 / 没 log"问题，**droid lerobot v2 normalize 本身还需要这个 adapter** 才能跑通 load_bundles。

## 7. 验收清单

| 项 | 责任方 | 通过标准 |
|----|--------|----------|
| bridge-qc metric 不再 traj=314 | robot-data-harness | `mc cat rdh/robot-lake/qc/bridgedata_v2_scale30/v1/contract_report.json \| jq '.metrics.traj_len_p50'` 返回 108；`.episode_count` 返回 3 |
| bridge-qc duration 收敛 | robot-data-harness | `.duration_sec` < 30 |
| bridge-qc probe 失败必带 cause_type | robot-data-harness | log 中若出现 `null_rate probe failed`，必须含 `cause_type=<具体类>`，不能 `cause_type=None` |
| robomimic-qc 不再 26 文件全 RetriesExceededError | robot-data-harness | log 中 `hdf5 probe failed` 行数 ≤ 1，且必带 `cause_type=<ReadTimeoutError\|ConnectionResetError\|...>` 具体类 |
| robomimic-qc duration | robot-data-harness | `mc cat rdh/.../robomimic_scale30/v1/contract_report.json \| jq '.duration_sec'` < 1200（20min） |
| robomimic episode_len 非 0 | robot-data-harness | `.metrics.episode_len_p50` >= 5（同 qptk9 §6 验收） |
| droid-normalize 不再卡 RUNNING | robot-data-harness | `mc cat rdh/robot-lake/ods/droid_lerobot_scale30/v1/_checkpoint.json \| jq '.status'` 不为 `RUNNING`（要 OK / FAILED 二选一） |
| droid-normalize 至少有 archive log | robot-data-harness（WorkflowTemplate runner_boot） | `mc ls rdh/robot-dh-artifacts/argo-logs/robot-dh/<workflow>/ \| grep droid-normalize` size > 0；首行必有 `{"event":"runner_boot",...}` |
| droid ods 工件落地 | robot-data-harness | `mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/` 至少含 `_manifest.json` + `pose.parquet` + `episode_meta.parquet` |
| 完整 multisource-scale30 跑到 Succeeded | robot-data-harness + WSL/kind | `kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-XXXXX -o jsonpath='{.status.phase}'` 返回 `Succeeded` |
| 4 条 pending perf records 回填（infra 完成后） | infra 完成 `006_etl_perf_runs_align.sql` apply → 主项目跑 `robot-dh perf reingest-pending` | `psql -c "SELECT count(*) FROM etl_perf_runs WHERE job_id LIKE 'etl-run-%' AND created_at > '2026-05-24';"` ≥ 4 |

## 8. 复现 / 排障最小命令

```bash
# A. 复现 bridge-qc metric 失真（核心：traj 必须按 episode_idx 切 group）
python -c "
import pyarrow.parquet as pq, s3fs, pandas as pd
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'},
                      key='robotdhapp', secret='***')
t = pq.read_table(fs.open('robot-datasets/raw/bridgedata_v2_scale30/v1/data/shard_0-00000-of-00001.parquet'))
df = t.to_pandas()
print('total rows:', len(df))                # 应该 = 314
print('episode_idx column:', 'episode_idx' in df.columns)
if 'episode_idx' in df.columns:
    lens = df.groupby('episode_idx').size().sort_values()
    print('episode_count:', len(lens), 'lens:', lens.tolist())   # 应该 3 个 [75, 108, 131]
    import numpy as np
    print('traj_p50:', int(np.percentile(lens, 50)))             # 应该 108
    print('traj_p95:', int(np.percentile(lens, 95)))             # 应该 131
"

# B. 复现 robomimic hdf5 probe cause=None（必须改成 materialize-first 才能拿到 cause）
python -c "
import boto3, h5py, tempfile, os
from botocore.config import Config
from pathlib import Path
s3 = boto3.client('s3', endpoint_url=os.environ['ROBOT_DH_S3_ENDPOINT_URL'],
                  aws_access_key_id=os.environ['ROBOT_DH_S3_ACCESS_KEY'],
                  aws_secret_access_key=os.environ['ROBOT_DH_S3_SECRET_KEY'],
                  config=Config(retries={'max_attempts': 10, 'mode': 'adaptive'}))
with tempfile.TemporaryDirectory() as td:
    local = Path(td) / 'demo.hdf5'
    s3.download_file('robot-datasets', 'raw/robomimic_scale30/v1/v1.5/can/mh/demo_v15.hdf5', str(local))
    with h5py.File(local, 'r') as f:
        first_demo = list(f['data'].keys())[0]
        print('demo count:', len(f['data'].keys()))     # 应该 ~300
        print('actions shape:', f[f'data/{first_demo}/actions'].shape)   # 应该 (N, 7)
"

# C. 看 droid-normalize 实际 pod 状态（必须 wsl 端执行）
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow=robot-dh-multisource-scale30-ddbfb -o wide
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow=robot-dh-multisource-scale30-ddbfb \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.containerStatuses[0].lastState.terminated.reason}{"\t"}{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}{end}'
# 如果看到 OOMKilled → §6.3.2 加 memory limit
# 如果看到 Evicted → §6.3.2 加 ephemeral-storage limit + emptyDir
# 如果完全没有 droid-normalize 这个 pod → §6.2 ②，DAG 模板没把 partition fanout 起来

# D. workflow 拓扑诊断（看 partition fanout 是否成功）
kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-ddbfb \
  -o jsonpath='{range .status.nodes[*]}{.displayName}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}' \
  | sort | grep -i droid
```

## 9. infra 侧并行 follow-up（不在本需求 PR 内）

| 项 | 当前状态 | 处理 |
|----|---------|------|
| `etl_perf_runs` 加 `started_at` / `finished_at` 列 | infra 端待 apply（[`v1_6_etl_perf_runs_schema_align_request.md`](v1_6_etl_perf_runs_schema_align_request.md)） | infra 落 `006_v1_6_etl_perf_runs_align.sql` migration |
| 4 条 pending perf records 回填 | jddlp 2 + qptk9 2 + ddbfb 2 = 4 条；本次 ddbfb 累计 + 重复（同一 job_id 不同 pod）= 4 条独立的 | infra schema 上线后跑 `robot-dh perf reingest-pending` |
| droid contract 入 `qc_contracts` 表 | qptk9 已通过 `contract_id=droid_multimodal_v1` 写入；ddbfb 复用 | 验证 `psql -c "SELECT contract_id, dataset_family FROM qc_contracts WHERE contract_id='droid_multimodal_v1';"` 返回 1 行 |
| `bridge-features status=WARN` 的 `input_bytes=0` | qptk9 + ddbfb 都出现；不阻塞 | wsl 侧复核 `compute_input_bytes` 是否未消费 ods 读取字节计数（可与本 PR 一起改） |

## 10. 时间窗口建议

| 阶段 | 估计耗时 | 备注 |
|------|----------|------|
| R1: bridge-qc null_rate probe 异常处理 + null_rate footer-only + 单测 | 0.5 day | 不让 probe 异常软降级；改 footer statistics |
| R2: robomimic hdf5 probe 改回 materialize-first + 暴露 cause + 并发 + 单测守门 | 1 day | 与 fhkvr §5.1.2 / qptk9 §5.1+§5.2 重复要求，需确认上次 PR 是否真的合进了 |
| R3.6.3.1: etl runner_boot 模块顶层 print | < 0.5h | 改一处入口 |
| R3.6.3.2: WorkflowTemplate 加 ephemeral-storage + emptyDir | < 1h | 改 yaml |
| R3.6.3.4: droid lerobot v2 normalize adapter | 1–2 day | 列名探测（episode_index / frame_index / observation.state / action）+ 单测 |
| 联调一次 multisource-scale30 | 1 day | 期望首条端到端 Succeeded |

---

> 收到 wsl 侧的修复 PR + 联调通过截图（含三份 contract_report.json 的 metric 值 + `mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/` 输出）后，本文档可以标 **「已闭环」**，同步更新到 [`docs/runs/20260525/robot-dh-multisource-scale30-ddbfb/INDEX.md`](runs/20260525/robot-dh-multisource-scale30-ddbfb/INDEX.md) 引用处。
>
> **三条等同 P1**：R2 robomimic 是 fhkvr 时代第 3 次重复要求（fhkvr §5.1.2 → qptk9 §5.1 → ddbfb R2），如果再次回归，建议 wsl 侧在 CI 流水线加上 §5.3.3 单测**强制门**：`cause_type is None` 的 retries 错误必须让 CI red，防止后续 PR 再退回。
