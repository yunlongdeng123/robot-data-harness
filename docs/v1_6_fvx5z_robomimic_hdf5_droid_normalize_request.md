# robomimic hdf5 probe 持续未修 + droid-normalize 卡死持续未修 + bridge-qc duration 收敛 修复需求

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：WSL 侧 `robot-data-harness` 主项目
> 优先级：P1（v1.6 `multisource-scale30` 第 5 次提交，robomimic + droid-normalize 两条通路仍 100% FAIL）
> 关联：
>
> - [`docs/v1_6_ddbfb_qc_regression_and_droid_normalize_request.md`](v1_6_ddbfb_qc_regression_and_droid_normalize_request.md)（上一份需求；§4 bridge-qc metric **已闭环**，§4.4.3 duration cap **未生效**，§5 robomimic + §6 droid-normalize **完全没动**）
> - [`docs/v1_6_droid_robomimic_qc_request.md`](v1_6_droid_robomimic_qc_request.md) §5.1 / §5.2（hdf5 path 第 2 次重复要求）
> - [`docs/v1_6_fhkvr_step_failures_request.md`](v1_6_fhkvr_step_failures_request.md) §5.1.2（`profile_hdf5` 用 boto3 materialize-first 的最早要求；fvx5z 是**第 4 次违反**）
> - 本次完整 step 归档：[`docs/runs/20260525/robot-dh-multisource-scale30-fvx5z/INDEX.md`](runs/20260525/robot-dh-multisource-scale30-fvx5z/INDEX.md)

## 1. 背景

`robot-dh-multisource-scale30-fvx5z` 是 wsl 侧消化 [`v1_6_ddbfb_qc_regression_and_droid_normalize_request.md`](v1_6_ddbfb_qc_regression_and_droid_normalize_request.md) 之后**第一次重新提交**的 multisource-scale30 workflow。结果：

```text
✅ R1 (bridge-qc metric) 闭环      （traj_p50=108, p95=131, episode_count=3 真值）
✅ bridge-qc probe cause 暴露生效  （cause_type=ContentLengthError）
⚠ R1.2 (bridge-qc duration) 未收敛 （仍 1849s = 30.8min，§4.4.3 step cap + boto3 backoff cap 没生效）
❌ R2 (robomimic hdf5 probe) 完全没动 + cause 暴露**做错了**：
    - cause_type=RetriesExceededError （= error_type，是 exc 自己当 cause；不是 exc.__cause__ 的底层异常类）
    - 仍然走 fsspec / h5py 远端 read，没改成 boto3.download_file 整文件落地
    - 仍然串行，20 文件 1.5h 全失败
❌ R3 (droid-normalize) 完全没动：
    - _checkpoint.json 写了 RUNNING 之后 2h+ 无更新
    - 6 个 partition pod 仍然 0 archive log（runner_boot 模块顶层 print 没接）
🆕 robomimic-qc 失败文件从 ddbfb 16 个增加到 fvx5z 20 个（更糟）
```

完整对账与时间线见 [INDEX.md §综述表](runs/20260525/robot-dh-multisource-scale30-fvx5z/INDEX.md#综述v16-修了-1-处旧洞回归--误修-2-处旧洞1-处旧洞继续卡死)。

## 2. 错误清单与优先级

| # | 错误 | 致命？ | 阻塞下游？ | 责任方 |
|---|------|--------|-----------|--------|
| F1 | **bridge-qc duration 仍 1849s**：单次 enrichment 内部 retry loop 跑 30min；step cap + backoff cap 没生效 | 否（status PASS、metric 正确） | 否（仅浪费算力） | robot-data-harness（probe 单次超时 cap + step `activeDeadlineSeconds`） |
| F2 | **robomimic hdf5 probe 仍走 fsspec + 仍串行 + cause 暴露错** | **是** | **是**（robomimic 通路 100% FAIL，contract_report 没产出） | robot-data-harness（**第 4 次**重复要求 `profile_hdf5` 改 boto3 materialize-first + 修正 cause 暴露用 `exc.__cause__` + 加 `ThreadPoolExecutor(max_workers=4)`） |
| F3 | **droid-normalize 卡死 RUNNING + 0 archive log** | **是** | **是**（droid 通路在 normalize 阶段 100% FAIL） | robot-data-harness（**第 2 次**重复要求 `runner_boot` 模块顶层 print + WorkflowTemplate ephemeral-storage + lerobot v2 normalize adapter） |

> infra 侧（`robot-dh-infra`）所有检查项均通过。同 workflow 内 droid-qc 0.86s ~ 40s PASS、bridge-qc PASS、bridge-normalize/features PASS、partition-plan 全 PASS，证明 endpoint + policy + raw 数据完整性都没问题。详见 §3。

## 3. infra 端零改动证明

| 检查项 | 结论 | 证据 |
|--------|------|------|
| droid raw 18.6 GiB 完整 | ✅ | 同 workflow 内 `droid-qc` 跑通 95658 episodes / 156 parquet / 14 videos；`droid-partition-plan` 跑通 6 个 partition × ~2 GiB |
| robomimic raw 6.5 GiB 完整 | ✅ | `mc stat rdh/robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/low_dim_dense_v15.hdf5` 立即返回 1.1 GiB + ETag |
| bridge raw 227 MiB 完整 | ✅ | `bridge-qc` 同 workflow 内 PASS 且 metric 正确 |
| `robotdhapp` policy | ✅ | 同 workflow 内 bridge / droid / robomimic 三家用同一份 secret，bridge / droid 两家正常 GET，证明 policy 通 |
| MinIO endpoint 可达 | ✅ | 同 workflow 内 8/8 pod 都能写 stdout 到 `s3://robot-dh-artifacts/argo-logs/` |
| `archiveLogs` 链路 | ✅ | bridge / droid / lake-list / partition-plan / etl-phase 8 个 pod 都归档；只有 `droid-normalize` 6 个 partition pod 缺 ← 是 **pod 没创建 / 启动后被 SIGKILL 前没 flush stdout** |
| infra 侧 `etl_perf_runs` 列 | ❌ 已知漂移，**走 fallback 不阻塞业务** | 累计 6 条 pending records，等 `006_etl_perf_runs_align.sql` 上线后 `robot-dh perf reingest-pending` 一次性回填（**与本需求 PR 互不依赖**） |

→ F1 / F2 / F3 全部归 `robot-data-harness` 主项目。

## 4. 错误 F1：`bridge-qc` duration 仍 1849s

### 4.1 现场对账

| 字段 | qptk9 PASS | ddbfb PASS | **fvx5z PASS** | 目标 |
|------|-----------|------------|----------------|------|
| status | PASS | PASS | PASS | PASS |
| traj_len_p50 | 108 | 314 ❌ | **108 ✅** | 108 |
| traj_len_p95 | 131 | 314 ❌ | **131 ✅** | 131 |
| episode_count | 3 | 0 ❌ | **3 ✅** | 3 |
| duration_sec | < 1s | 2005s | **1849s** ❌ | < 30s |
| enrichment WARN 行数 | 0 | 2 | **1** | 0–1 |
| cause_type 暴露 | n/a | None ❌ | **ContentLengthError ✅** | 具体类 |

**正面变化**：

- traj/episode 切分修对了（§4.4.1 已闭环）
- cause 暴露修对了（cause_type=ContentLengthError）
- WARN 行数从 2 降到 1（probe 不再 retry 自身）

**仍未修复**：

- 单次 enrichment 跑 1849s 才放弃
- 推断在 enrichment 内部某个 `s3.get_object` 走了 **boto3 默认 `mode=adaptive` 的累计指数退避**，从 1s → 2s → 4s → ... 一路 30min 才 throw `ClientPayloadError`

### 4.2 复现日志

```text
# fvx5z bridge-qc (pod 2371171254)：
{"event": "runner_boot", "argv": ["qc", "contract", "run", "--dataset-family", "bridge", ...], "ts": 1779659836.94}  # 21:57:16Z
{"message": "bridge metrics enrichment failed for s3://...shard_0-00000-of-00001.parquet: error_type=ClientPayloadError cause_type=ContentLengthError error=Response payload is not completed: <ContentLengthError: 400, message='Not enough data to satisfy content length header.'>", "timestamp": "2026-05-24T22:28:06.984487+00:00", "level": "WARNING"}
{"status": "PASS", "metrics": {..., "traj_len_p50": 108, "traj_len_p95": 131, "episode_count": 3}}
# 22:28:06Z - 21:57:16Z = 1849s
```

只有 1 行 WARN 但 step 内部跑了 30min，**说明单 enrichment 内部的 boto3 retry 跑了 30min**。

### 4.3 修复方案

#### 4.3.1 单次 enrichment 加 timeout cap

```python
# robot_data_harness/qc/profile_bridge.py 或 similar
import asyncio
from contextlib import contextmanager

@contextmanager
def _enrichment_timeout(seconds: int = 30):
    """单次 bridge enrichment 不允许超过 30s。"""
    loop_old = asyncio.get_event_loop()
    # 简化版：用 boto3 client 的 read_timeout 而不是 asyncio
    yield

def enrich_bridge_metrics(parquet_uri: str) -> dict:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["ROBOT_DH_S3_ENDPOINT_URL"],
        config=Config(
            connect_timeout=5,
            read_timeout=10,                            # ★ 单次 GET 10s 超时
            retries={"max_attempts": 3, "mode": "standard"},  # ★ 不要 adaptive
        ),
    )
    try:
        # ... read parquet footer + columns ...
        pass
    except (botocore.exceptions.ClientError,
            aiohttp.client_exceptions.ClientPayloadError) as exc:
        # ★ cause 必须用 __cause__，§4.4 验收已通过这里不动
        logger.warning(
            f"bridge metrics enrichment failed for {parquet_uri}: "
            f"error_type={type(exc).__name__} "
            f"cause_type={type(exc.__cause__).__name__ if exc.__cause__ else type(exc).__name__} "
            f"error={exc}"
        )
        return {}   # ★ traj/episode 切分已在外层做了，enrichment 是 nice-to-have
```

要点：

- 单次 GET `read_timeout=10s`、最多 `max_attempts=3 × 10s = 30s`，远比当前 30min 合理
- `mode="standard"` 而不是 `"adaptive"`：后者会做 token-bucket 等待，在被对端持续半关连接的情况下会一直退避
- bridge enrichment 在 §4.4.1 已经是 try/except 外层 metric 不受影响，所以 enrichment 出错直接返回空 dict 即可，**不要再 retry**

#### 4.3.2 同时在 WorkflowTemplate 加 step-level cap（与 ddbfb §4.4.3 同款，**重复要求第 2 次**）

```yaml
# Argo workflow template - qc-contract-run
- name: qc-contract-run
  retryStrategy:
    limit: 1                       # ★ ddbfb §4.4.3 说 2，本次降到 1（与 bridge enrichment 已修对照）
    retryPolicy: OnError
    backoff:
      duration: 10s
      factor: 2
      maxDuration: 1m              # ★ 不要让外层 retry 也跑 30min
  activeDeadlineSeconds: 600       # ★ 单 step 10 分钟硬上限（droid lazy v2 < 1min；bridge 应 < 30s；robomimic 期望 < 10min，见 §5）
```

要点：

- 与 ddbfb §4.4.3 重复要求一遍；之前 `activeDeadlineSeconds: 1800` 没生效，本次降到 600s
- 期望 fvx5z 下一轮 bridge-qc < 30s（与 qptk9 < 1s 对齐）

## 5. 错误 F2：`robomimic-qc` hdf5 probe 完全没修 + cause 暴露做错了（**第 4 次重复要求**）

### 5.1 复现日志

```text
{"event": "runner_boot", "argv": ["qc", "contract", "run", "--dataset-family", "robomimic", ...]}
{"message": "hdf5 probe failed for s3://robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/low_dim_sparse_v15.hdf5: error_type=RetriesExceededError error=Max Retries Exceeded cause_type=RetriesExceededError cause=Max Retries Exceeded", "timestamp": "2026-05-24T22:24:20.962597+00:00", "level": "WARNING"}
# ... 共 20 行同款，每条相隔 30s ~ 8min ...
{"message": "hdf5 probe failed for s3://...transport/mh/low_dim_v15.hdf5: ... cause_type=RetriesExceededError cause=Max Retries Exceeded", "timestamp": "2026-05-24T23:55:16.623583+00:00", "level": "WARNING"}
# 然后 step exit 非 0，contract_report 没产出（s3 上仍是 qptk9 那份 finished_at=2026-05-24T17:11:45Z）
```

### 5.2 与 fhkvr / qptk9 / ddbfb 历史对账（**第 4 次重复**）

| 维度 | fhkvr §5.1.2 要求 | qptk9 §5.1 / §5.2 重复要求 | ddbfb §5.3 重复要求 | **fvx5z 现状** |
|------|-------------------|----------------------------|---------------------|----------------|
| `boto3.download_file` 整文件落地 `/tmp` | ✅ 要求 | ✅ 要求 | ✅ 要求 | ❌ **仍走 fsspec / h5py 远端 read** |
| `Config(retries={'max_attempts': 10, 'mode': 'adaptive'})` | ✅ 要求 | ✅ 要求 | ✅ 要求 | ❌ 未应用（如果用了应该 20 文件 ~20min 而不是 1.5h；单文件 ~4.6min vs adaptive 应 ~10s × 10 = ~2min/file） |
| `cause_type=type(exc.__cause__).__name__` | ✅ 要求 | ✅ 要求 | ✅ 要求 | ❌ **`cause_type=RetriesExceededError` = error_type，是 exc 自己当 cause** |
| 26 文件并发 download | ⚪ 隐含（fhkvr 没明写） | ✅ G2 显式要求 | ✅ §5.3.2 显式要求 | ❌ 串行（4 → 30s+ → 8min 间隔） |
| 单测守门 `cause_type is None / == error_type` 让 CI red | ⚪ | ⚪ | ✅ §5.3.3 显式要求 | ❌ 如果有单测就不会让 cause_type 写成 exc 自引用 |

### 5.3 根因推断（按可能性排序）

#### 5.3.1 cause 暴露做错的最可能实现

```python
# 疑似当前 wsl 端实现（错误：把 exc 自己当 cause）
except Exception as exc:
    logger.warning(
        f"hdf5 probe failed for {uri}: "
        f"error_type={type(exc).__name__} "
        f"error={exc} "
        f"cause_type={type(exc).__name__} "          # ★ BUG：应该是 type(exc.__cause__)
        f"cause={exc}"                                # ★ BUG：应该是 exc.__cause__
    )

# 或者另一种错误实现（用了 `exc.__class__` 别名）
except Exception as exc:
    cause = getattr(exc, "__cause__", exc)            # ★ 如果 __cause__ 是 None，fallback 到 exc，等于自引用
    logger.warning(f"... cause_type={type(cause).__name__}")
```

**修正：**

```python
except Exception as exc:
    cause = exc.__cause__                              # ★ 不要 fallback；__cause__ is None 就明确写 None
    logger.warning(
        f"hdf5 probe failed for {uri}: "
        f"error_type={type(exc).__name__} "
        f"error={exc} "
        f"cause_type={type(cause).__name__ if cause is not None else 'None'} "
        f"cause={cause!r}"                            # ★ 用 !r 才能看到 cause 类
    )
```

→ 修对之后，`RetriesExceededError.__cause__` 应该指向具体的网络异常（`ReadTimeoutError` / `ConnectionResetError` / `EndpointConnectionError`），那条信息才是排障核心。

#### 5.3.2 probe 走 fsspec 而不是 boto3.download_file（**第 4 次**）

ddbfb §5.3.1 已经把完整代码贴出来，本次原样保留：

```python
# robot_data_harness/qc/profile_hdf5.py
import os
import tempfile
import boto3
import h5py
import numpy as np
from botocore.config import Config
from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def profile_hdf5(s3_uri: str, tmpdir: Path) -> dict:
    """单个 hdf5 文件 profile：materialize-first via boto3.download_file。"""
    parsed = urlparse(s3_uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    local = tmpdir / Path(key).name

    try:
        _s3().download_file(bucket, key, str(local))     # ★ 整文件落地，单次重试 boto3 顶 10 次
    except Exception as exc:
        cause = exc.__cause__
        logger.warning(
            "hdf5 download failed for %s: error_type=%s cause_type=%s cause=%r",
            s3_uri, type(exc).__name__,
            type(cause).__name__ if cause is not None else "None",
            cause,
        )
        return {
            "status": "FAILED", "uri": s3_uri,
            "error_type": type(exc).__name__,
            "cause_type": type(cause).__name__ if cause is not None else None,
            "error": str(exc),
        }

    try:
        with h5py.File(local, "r") as f:
            demo_names = list(f["data"].keys())
            episode_lens = [int(f[f"data/{d}/actions"].shape[0]) for d in demo_names]
            return {
                "status": "OK",
                "uri": s3_uri,
                "demo_count": len(demo_names),
                "episode_lens": episode_lens,
                "episode_len_p50": int(np.percentile(episode_lens, 50)) if episode_lens else 0,
                "episode_len_p95": int(np.percentile(episode_lens, 95)) if episode_lens else 0,
            }
    except Exception as exc:
        logger.warning("hdf5 open failed for %s: %r", local, exc)
        return {"status": "FAILED", "uri": s3_uri, "error": str(exc)}
    finally:
        local.unlink(missing_ok=True)                    # ★ 当下文件下载完就立刻删，控制 /tmp 占用

def run_qc_robomimic(dataset_uri: str) -> dict:
    files = list_hdf5_files(dataset_uri)                  # 26 文件
    with tempfile.TemporaryDirectory(prefix="robot-dh-qc-") as td:
        td_path = Path(td)
        # ★ ddbfb §5.3.2 要求 max_workers=4；本次保留同款
        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = {ex.submit(profile_hdf5, f, td_path): f for f in files}
            profiles = []
            for fut in as_completed(futures):
                profiles.append(fut.result())
    return aggregate_robomimic(profiles)
```

要点：

- **不要复用 fsspec/s3fs**：fsspec 的 h5py random-read 路径会触发几千次 small range GET，单 GET 受 botocore 默认 5 次 retry，叠加 26 文件 × 几千 GET 就是「`RetriesExceededError` 永远在 retry 顶层」
- **每 worker 独立 download，每文件 download 完立刻删**：26 × 1.1 GiB 平均，最坏单时刻磁盘占用 = workers × 单文件 = 4 × ~250 MiB ≈ 1 GiB（hdf5 v1.5 大多 250 MiB 左右，只有 transport/mh/image_v15.hdf5 1.1 GiB，**避免一次性把 26 个都拉到 /tmp**）
- `episode_len_p50/p95` 落地（qptk9 §5.1 G1 至今未生效，仍 0/0）

#### 5.3.3 单测守门：本次必须加（**ddbfb §5.3.3 同款，重复要求**）

```python
# tests/test_profile_hdf5.py
import pytest
from unittest.mock import patch

def test_profile_hdf5_cause_type_must_not_be_self_referential():
    """防止 cause_type 写成 error_type（exc 自引用）的回归。"""
    with patch_unreachable_s3():
        result = profile_hdf5("s3://nonexistent/x.hdf5", tmp_path)
        assert result["status"] == "FAILED"
        # ★ cause_type 必须是具体网络异常类，绝不能 == error_type
        assert result["cause_type"] != result["error_type"], (
            f"cause_type leaks error_type: {result['cause_type']} == {result['error_type']} "
            f"this means we wrote exc instead of exc.__cause__"
        )
        # ★ cause_type 应该是 ReadTimeout / ConnectionError 之类具体类
        assert result["cause_type"] in {
            "ReadTimeoutError", "ConnectionError", "EndpointConnectionError",
            "ConnectTimeoutError", "NewConnectionError",
        }, f"unexpected cause_type: {result['cause_type']}"

def test_profile_hdf5_uses_boto3_download_file_not_fsspec():
    """防止再退回 fsspec / h5py 远端 read。"""
    with patch("boto3.client") as mock_client:
        mock_client.return_value.download_file.return_value = None
        profile_hdf5("s3://bucket/key.hdf5", tmp_path)
        mock_client.return_value.download_file.assert_called_once()

def test_profile_hdf5_concurrency_uses_thread_pool_executor():
    """防止再退回串行循环。"""
    with patch("robot_data_harness.qc.profile_hdf5.ThreadPoolExecutor") as mock_pool:
        run_qc_robomimic("s3://bucket/prefix/")
        mock_pool.assert_called_once_with(max_workers=4)
```

要点：

- **本次必须把这 3 条单测加进 CI 强制门**，否则下次 PR 还会回归
- ddbfb §5.3.3 说过一次，**这次再说一次，把 cause 自引用单独拎出来**

### 5.4 期望

| 项 | 当前 | 期望 |
|----|------|------|
| 失败文件数 | 20/20（全失败） | 0/26（全部 OK） |
| cause_type | RetriesExceededError（自引用） | `ReadTimeoutError` / `ConnectionResetError` / 具体类（如果还有失败的话） |
| 串行 → 并发 | 串行 1.5h | 4 路并发 < 10min |
| contract_report 产出 | 仍是 qptk9 那份 | fvx5z 新版本 `episode_len_p50 >= 5` |
| step exit code | 非 0 | 0 |

## 6. 错误 F3：`droid-normalize` 卡死 RUNNING + 0 archive log（**第 2 次重复要求**）

### 6.1 现场（与 ddbfb 完全一致）

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
  "updated_at": "2026-05-24T22:10:52Z"   ← 06:10:52 CST，partition-plan 完成 06:10:28 之后 24s
}

$ mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/
[2026-05-25 06:10:52 CST]   365B STANDARD _checkpoint.json
# 仍然只有 _checkpoint.json，没有 _manifest.json / pose.parquet / video_meta.parquet / episode_meta.parquet

$ mc ls rdh/robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-fvx5z/ | grep -i normalize
# 仍然 0 个 droid-normalize pod；只有 bridge 的 2 个 etl-phase pod
```

| 维度 | ddbfb | **fvx5z** |
|------|-------|-----------|
| `_checkpoint.json updated_at` | 2026-05-24T18:27:37Z | **2026-05-24T22:10:52Z** |
| 之后无更新时长 | 2.5h | **2h+ ~ 仍未恢复** |
| 6 个 partition 中归档了几个 pod | 0 | **0**（同款） |
| ddbfb §6.3.1 `runner_boot` 模块顶层 print | ❌ | ❌ **仍然没接** |
| ddbfb §6.3.2 ephemeral-storage limit + emptyDir | ❌ | ❌ **仍然没接** |
| ddbfb §6.3.3 normalize 进度心跳 | ❌ | ❌ **仍然没接**（_checkpoint 一直 `completed_steps=[]`） |
| ddbfb §6.3.4 lerobot v2 normalize adapter | ❌ | ❌ **未知**（pod 没起来无法验证） |

### 6.2 核心要求（**与 ddbfb §6.3 完全一致，重复要求**）

#### 6.2.1 `runner_boot` 模块顶层 print（**P0：先把"看不到 log"问题解决**）

```python
# robot_data_harness/etl/run.py 顶部（必须在 import 副作用层最早执行）
import sys, json, os, time

print(json.dumps({
    "event": "runner_boot",
    "argv": sys.argv,
    "python": sys.version.split()[0],
    "ts": time.time(),
    "env_keys": sorted([k for k in os.environ if k.startswith("ROBOT_DH_")]),
}), flush=True)

# ... 其他 import 与业务代码 ...
```

要点：

- **模块顶层**（不是 `if __name__ == "__main__":` 里）：后者在 import 阶段抛 `ImportError` / `ModuleNotFoundError` 时不会执行
- `flush=True`：SIGKILL 不会丢
- 其他 `argv` 路径（lake-list / qc-contract-run / partition-plan）都已经接上，**只剩 etl/run.py 这一条没接**

#### 6.2.2 给 etl-phase WorkflowTemplate 加 ephemeral-storage + emptyDir

```yaml
# Argo workflow template - etl-phase for droid normalize
- name: etl-phase
  container:
    resources:
      requests:
        memory: 2Gi
        cpu: 1
        ephemeral-storage: 4Gi          # ← droid 单 partition 2.1 GiB
      limits:
        memory: 8Gi                     # ← 给 h5py / pyarrow 留 buffer
        cpu: 4
        ephemeral-storage: 16Gi         # ← materialize_input 2.1 GiB + load_bundles 中间态 ≤ 16 GiB
    volumeMounts:
      - name: workdir
        mountPath: /tmp/robot-dh
  volumes:
    - name: workdir
      emptyDir:
        sizeLimit: 16Gi
  env:
    - name: ROBOT_DH_INPUT_CACHE_DIR
      value: /tmp/robot-dh/input-cache   # ★ 让 materialize_input 写到 emptyDir 而不是 root fs
```

要点：

- emptyDir + `mountPath: /tmp/robot-dh` 让 ephemeral-storage limit 真正生效
- 同时把 `ROBOT_DH_INPUT_CACHE_DIR` 指过去（fvx5z bridge-normalize log 显示该变量已经在 env，但 droid 路径要确认）

#### 6.2.3 normalize 进度心跳

```python
def normalize(partition_uri: str, ods_uri: str):
    ckpt = Checkpoint(ods_uri)
    ckpt.update(status="RUNNING", completed_steps=[])

    materialize_input(partition_uri)
    ckpt.update(completed_steps=["materialize_input"])      # ★ 每完成一步就 PUT

    bundles = load_bundles()
    ckpt.update(completed_steps=["materialize_input", "load_bundles"])

    # ...
    ckpt.update(status="OK", manifest_uri=...)
```

bridge 的 normalize 已经写了 7 步 completed_steps，模板存在；**droid 这一支没接到**。

#### 6.2.4 droid lerobot v2 normalize adapter

§4.2.2 lazy v2 修复**仅在 qc profile 路径生效**；normalize 路径需要单独写 adapter：

```python
@register_normalize_adapter("droid_lerobot_scale30")
def adapt_droid_lerobot_v2(table: pa.Table) -> NormalizedEpisodes:
    """droid lerobot v2 列结构：
       episode_index, frame_index, timestamp,
       observation.state, observation.images.<camera>,
       action (struct or list[7]), reward, done
    """
    # groupby(episode_index) 拆 episode，pose 走 observation.state
    ...
```

→ 实际能不能跑到 adapter 这一步取决于 §6.2.1 / §6.2.2 是否生效，先让 pod 起来再说。

### 6.3 排障最小命令（**wsl 端必须先跑这条**）

```bash
# 1. 看 fvx5z 这次 droid-normalize 的实际 pod 状态
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow=robot-dh-multisource-scale30-fvx5z -o wide

kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow=robot-dh-multisource-scale30-fvx5z \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.containerStatuses[0].lastState.terminated.reason}{"\t"}{.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}{end}'

# 期望输出会落到下列三类之一：
# 类型 ①: <pod-name>  Failed  OOMKilled  137                         → §6.2.2 加 memory limit
# 类型 ②: <pod-name>  Failed  Evicted    137 (ephemeral-storage)     → §6.2.2 加 ephemeral-storage limit + emptyDir
# 类型 ③: <pod-name>  Pending  ContainersNotInitialized              → §6.2.2 资源配额不足，看 PendingReason
# 类型 ④: 完全没有任何 droid-normalize 这个 pod                       → Argo DAG 模板没把 partition fanout 起来

# 2. 看 workflow DAG 节点状态
kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-fvx5z \
  -o jsonpath='{range .status.nodes[*]}{.displayName}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}' \
  | sort | grep -i droid

# 3. 看 archive log 完整列表确认 droid-normalize 真的一个都没产
mc ls -r rdh/robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-fvx5z/ | grep -v -E '(bridge|lake-list|droid-qc|droid-partition)'
```

## 7. 验收清单

| 项 | 责任方 | 通过标准 |
|----|--------|----------|
| bridge-qc duration 收敛 | robot-data-harness | `mc cat rdh/robot-lake/qc/bridgedata_v2_scale30/v1/contract_report.json \| jq '.duration_sec'` < 30 |
| bridge-qc metric 保持正确 | robot-data-harness（回归保护） | `.metrics.traj_len_p50` == 108、`.metrics.episode_count` == 3 |
| **robomimic-qc cause_type ≠ error_type**（防回归） | robot-data-harness | log 中若仍有 `hdf5 probe failed`，必须含 `cause_type=<ReadTimeoutError\|ConnectionResetError\|...>`，**绝不能** `cause_type=RetriesExceededError`（即 == error_type） |
| robomimic-qc 失败文件数 | robot-data-harness | `≤ 1`（理想 0；偶发 1 个可接受） |
| robomimic episode_len 非 0 | robot-data-harness | `.metrics.episode_len_p50 >= 5` |
| robomimic-qc 走 boto3.download_file | robot-data-harness | log 不含 `RetriesExceededError`；新单测 `test_profile_hdf5_uses_boto3_download_file_not_fsspec` 入 CI |
| robomimic-qc 并发 | robot-data-harness | duration < 600s（10min） |
| robomimic-qc contract_report 时间戳 | robot-data-harness | `mc stat rdh/robot-lake/qc/robomimic_scale30/v1/contract_report.json` 显示 `Last Modified` > 2026-05-25 06:00 CST |
| droid-normalize 至少有 archive log | robot-data-harness（runner_boot 模块顶层 + WorkflowTemplate） | `mc ls -r rdh/robot-dh-artifacts/argo-logs/robot-dh/<workflow>/ \| grep droid \| grep etl-phase` size > 0；首行必有 `{"event":"runner_boot",...}` |
| droid-normalize 不再卡 RUNNING | robot-data-harness | `mc cat rdh/robot-lake/ods/droid_lerobot_scale30/v1/_checkpoint.json \| jq '.status'` 不为 `RUNNING`（要 OK / FAILED 二选一） |
| droid ods 工件落地 | robot-data-harness | `mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/` 至少含 `_manifest.json` + `pose.parquet` + `episode_meta.parquet` |
| 完整 multisource-scale30 跑到 Succeeded | robot-data-harness + WSL/kind | `kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-XXXXX -o jsonpath='{.status.phase}'` 返回 `Succeeded` |
| 6 条 pending perf records 回填（infra 完成后） | infra 完成 `006_etl_perf_runs_align.sql` → 主项目跑 `robot-dh perf reingest-pending` | `psql -c "SELECT count(*) FROM etl_perf_runs WHERE job_id LIKE 'etl-run-%' AND created_at > '2026-05-24';"` ≥ 6 |

## 8. CI 强制门（**必须本次 PR 实现**）

ddbfb §5.3.3 已经口头要求过一次，本次因为 cause 自引用 + 仍走 fsspec + 仍串行 三个老问题再次出现，**必须把以下 3 条作为 CI required check 加进流水线**：

```python
# tests/test_profile_hdf5.py 必须全过

def test_profile_hdf5_cause_type_must_not_be_self_referential():
    """防回归 #1：cause_type 不能 == error_type（防止 exc 自引用）。"""
    ...

def test_profile_hdf5_uses_boto3_download_file_not_fsspec():
    """防回归 #2：必须走 boto3.download_file，不能用 fsspec / h5py 远端 read。"""
    ...

def test_profile_hdf5_concurrency_uses_thread_pool_executor():
    """防回归 #3：必须用 ThreadPoolExecutor(max_workers=4)，不能串行。"""
    ...

def test_runner_boot_emitted_at_module_top_of_etl_run():
    """防回归 #4：etl/run.py 必须在模块顶层 print runner_boot，不能放 if __name__ 里。"""
    import importlib, io, sys
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        importlib.reload(importlib.import_module("robot_data_harness.etl.run"))
    output = buf.getvalue()
    assert '"event": "runner_boot"' in output, (
        "etl/run.py must emit runner_boot at module top, not in __main__"
    )
```

→ 这 4 条单测**必须在本次 PR 的 CI 配置里标记为 required**，否则下一轮 PR 再次回归到 fvx5z 同款。

## 9. 复现 / 排障最小命令

```bash
# A. 验证 bridge-qc duration 是否收敛（应该 < 30s）
mc cat rdh/robot-lake/qc/bridgedata_v2_scale30/v1/contract_report.json | jq '.duration_sec'
# 当前: 1849.99 → 期望: < 30

# B. 验证 robomimic-qc 是否产出新 contract_report
mc stat rdh/robot-lake/qc/robomimic_scale30/v1/contract_report.json | grep "Last modified"
# 当前: 2026-05-25 01:11:45 CST (qptk9 那份)
# 期望: > 2026-05-25 08:30 CST (本次 fvx5z 之后的)

# C. 验证 robomimic-qc cause 暴露不是自引用
mc cp rdh/robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-XXXXX/<robomimic-pod>/main.log/main.log /tmp/robomimic-next.log
jq -r 'select(.message | contains("hdf5 probe failed")) | .message' /tmp/robomimic-next.log | head -5
# 当前: cause_type=RetriesExceededError (= error_type)
# 期望: cause_type=ReadTimeoutError / ConnectionResetError 之类底层异常类

# D. 验证 droid-normalize 至少产生 archive log
mc ls -r rdh/robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-XXXXX/ | grep -i droid | grep etl-phase
# 当前: 0 行
# 期望: ≥ 1 行（理想 6 行 × 6 个 partition）

# E. 验证 droid ods 工件
mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/
# 当前: 仅 _checkpoint.json
# 期望: _manifest.json + pose.parquet + video_meta.parquet + episode_meta.parquet

# F. 复现 robomimic cause 暴露 bug（用 Python）
python -c "
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
try:
    s3 = boto3.client('s3', endpoint_url='http://nonexistent:9000',
                      config=Config(connect_timeout=1, read_timeout=1,
                                    retries={'max_attempts': 2, 'mode': 'standard'}))
    s3.get_object(Bucket='x', Key='y')
except Exception as exc:
    print('error_type:', type(exc).__name__)
    print('cause_type (correct, via __cause__):', type(exc.__cause__).__name__ if exc.__cause__ else 'None')
    print('cause (correct):', repr(exc.__cause__))
"
# 期望输出 cause_type=ReadTimeoutError 或 EndpointConnectionError 之类具体类
```

## 10. infra 侧并行 follow-up（不在本需求 PR 内）

| 项 | 当前状态 | 处理 |
|----|---------|------|
| `etl_perf_runs` 加 `started_at` / `finished_at` 列 | infra 端待 apply（[`v1_6_etl_perf_runs_schema_align_request.md`](v1_6_etl_perf_runs_schema_align_request.md)） | infra 落 `006_v1_6_etl_perf_runs_align.sql` migration |
| 6 条 pending perf records 回填 | jddlp 2 + qptk9 2 + ddbfb 2 + fvx5z 2（部分文件重叠取最新）实际 s3 上 6 条独立记录 | infra schema 上线后跑 `robot-dh perf reingest-pending` |
| droid contract 入 `qc_contracts` 表 | ✅ 已通过 `contract_id=droid_multimodal_v1` 落库；fvx5z 复用 | 持续观察 |
| `bridge-features status=WARN` 的 `input_bytes=0` | qptk9 + ddbfb + fvx5z 三次都出现；不阻塞 | wsl 侧复核 `compute_input_bytes` 是否未消费 ods 读取字节计数（可与本 PR 一起改） |

## 11. 时间窗口建议

| 阶段 | 估计耗时 | 备注 |
|------|----------|------|
| F1: bridge-qc enrichment 单次 timeout cap（5+10s read_timeout，max_attempts=3） + WorkflowTemplate `activeDeadlineSeconds=600` | 0.5 day | 改两处配置 |
| F2.1: hdf5 probe 改 boto3.download_file（**第 4 次**） | 0.5 day | 代码已在 §5.3.2 给出 |
| F2.2: cause 暴露用 `exc.__cause__`（**新发现 bug**） | < 0.5h | 1 行改对，单测守门 |
| F2.3: ThreadPoolExecutor 并发 + /tmp 即下即删 | 0.5 day | 与 §5.3.2 同 PR |
| F2.4: 单测 4 条加 CI required check | < 0.5h | 测试用例已在 §5.3.3 / §8 给出 |
| F3.1: `runner_boot` 模块顶层 print 加到 etl/run.py（**第 2 次**） | < 0.5h | 一行 import 副作用 |
| F3.2: WorkflowTemplate ephemeral-storage + emptyDir | < 1h | 改 yaml |
| F3.3: normalize 进度心跳 PUT _checkpoint | 0.5 day | bridge 已有模板，照着接 droid |
| F3.4: droid lerobot v2 normalize adapter | 1–2 day | 需要 F3.1 + F3.2 先解决"pod 起不来"才能调试 |
| 联调一次 multisource-scale30 | 1 day | 期望首条端到端 Succeeded |

总计 ~4–5 天。

---

> 收到 wsl 侧的修复 PR + 联调通过截图（含三份 contract_report.json metric 值 + `mc ls rdh/robot-lake/ods/droid_lerobot_scale30/v1/` 输出 + `mc ls -r rdh/.../argo-logs/<workflow>/ \| grep droid` 输出）后，本文档可以标 **「已闭环」**，同步更新到 [`docs/runs/20260525/robot-dh-multisource-scale30-fvx5z/INDEX.md`](runs/20260525/robot-dh-multisource-scale30-fvx5z/INDEX.md) 引用处。
>
> **三条等同 P1**：F2 是 fhkvr → qptk9 → ddbfb → **fvx5z 第 4 次重复要求**；F3 是 ddbfb → **fvx5z 第 2 次重复要求**。**强烈建议本次 PR 把 §8 的 CI 强制门一起合并**，否则下次还会回归。
>
> 特别注意：fvx5z 这次把 cause 暴露写成了 `cause = exc`（自引用），这是一个**修了一半反而误导排障**的实现。请用 §5.3.1 给出的正确代码替换，并加单测 `test_profile_hdf5_cause_type_must_not_be_self_referential` 防止下次再次写错。
