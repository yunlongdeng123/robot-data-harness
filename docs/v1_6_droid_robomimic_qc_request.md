# `droid-qc` 0B fail + `robomimic-qc` metric/perf 修复需求

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：WSL 侧 `robot-data-harness` 主项目（`robot_dh.qc.*` profile + contract runner、step container 镜像）
> 优先级：P1（v1.6 `multisource-scale30` 多源 fanout 跑通后，**droid 通路 100% FAIL**，且 robomimic contract 报表统计字段失真）
> 关联：
>
> - [`docs/v1_6_fhkvr_step_failures_request.md`](v1_6_fhkvr_step_failures_request.md) §2 A、§5.1.1 / §5.1.2（`profile_parquet` lazy + `profile_hdf5` materialize-first 的初版方案）
> - [`docs/v1_6_etl_perf_runs_schema_align_request.md`](v1_6_etl_perf_runs_schema_align_request.md) §4（perf fallback 已闭环，本次复用 fallback 思路）
> - [`docs/v1_6_bridgedata_v2_normalize_adapter_request.md`](v1_6_bridgedata_v2_normalize_adapter_request.md)（bridge 通路已闭环，模板）
> - 本次完整 7-step log 归档：[`docs/runs/20260524/robot-dh-multisource-scale30-qptk9/INDEX.md`](runs/20260524/robot-dh-multisource-scale30-qptk9/INDEX.md)

## 1. 背景

`robot-dh-multisource-scale30-qptk9` 是 v1.6 多源 fanout（`discover-assets` → `droid-qc` ‖ `bridge-qc` ‖ `robomimic-qc`）**首条端到端跑起来**的 workflow，bridge 通路第一次完整跑通到 dwd（见 INDEX §B），同时也是 droid / robomimic QC 第一次被 Argo 真实驱动。结果：

| 通路 | 状态 | 关键证据 |
|------|------|----------|
| **bridge-qc → partition → normalize → features** | ✅ 全 PASS | `dwd/bridgedata_v2_scale30/v1/{pose_feature,press_event,trajectory_segment,episode_feature}.parquet` 落齐 |
| **robomimic-qc** | ⚠ **PASS 但 metric 失真 + 耗时偏长** | `contract_report.json status=PASS`、`demo_count=21000` 正确，但 `episode_len_p50=0` / `episode_len_p95=0` 完全没消费 `actions_shape[0]`；duration 7189.94s ≈ 2h |
| **droid-qc** | ❌ **FAIL（0B pod log）** | `qc-contract-run.2368294939.log` size = 0 字节；`s3://robot-lake/qc/droid_lerobot_scale30/v1/contract_report.json` **不存在**；step pod stdout/stderr 整段为空 |

> 备注：fhkvr 那次 `droid-qc` / `robomimic-qc` 都是单行 `WARNING: profile_parquet failed for ... Max Retries Exceeded`（exit 0，静默漏检）；本次 `robomimic-qc` 已经按 fhkvr §5.1.2 的 materialize-first 模式跑通，**说明 fhkvr 的 A 类修复对 robomimic 路径有效**。`droid-qc` 这次连一行日志都没有，是**新失败模式**。

## 2. 错误清单与优先级

| # | 错误 | 致命？ | 阻塞下游？ | 责任方 |
|---|------|--------|-----------|--------|
| F | `droid-qc` 0B pod log，step FAIL，无 contract_report 产物 | **是** | **是**（多源 fanout 任何一支 FAIL → workflow Failed → ml-ready 不触发） | robot-data-harness（profile 路径 / lerobot adapter / 资源限制 / log 早出） |
| G1 | `robomimic-qc` 报表 `episode_len_p50/p95 = 0` 失真（21000 demos 实际不可能） | 否（不阻塞 step） | **是**（下游 `ml_ready_datasets` / dashboard 拉到错误 metric） | robot-data-harness（`robomimic_hdf5_v1` profile / contract rule） |
| G2 | `robomimic-qc` 耗时 7189s ≈ 2h（26 个 HDF5 串行 materialize-first） | 否 | 是（multisource workflow 平均拖到 2h+，影响 cron 节奏） | robot-data-harness（并发模型） |

> infra 侧（`robot-dh-infra`）所有检查项均通过：raw 18 GiB / 546 objects 完整、`robotdhapp` policy 含 `robot-datasets/*` GetObject、同 workflow 内 `bridge-qc` / `bridge-normalize` 都能正常访问 MinIO。详见 §3。

## 3. infra 端零改动证明

| 检查项 | 结论 | 证据 |
|--------|------|------|
| droid raw 是否完整 | ✅ 完整 | `mc du rdh/robot-datasets/raw/droid_lerobot_scale30/v1/` → 18 GiB / 546 objects；`mc stat rdh/.../data/chunk-000/file-000.parquet` 秒返 82 MiB + ETag |
| robomimic raw 是否完整 | ✅ 完整 | `mc du rdh/robot-datasets/raw/robomimic_scale30/v1/` → 84 个 HDF5，单文件 50 MiB–1.1 GiB；`asset_profile.json` 已经成功读到 26 个文件的 `actions_shape` |
| `robotdhapp` 是否有 `robot-datasets/*` GetObject | ✅ 是 | `minio/policies/robot_dh_readwrite.json` 含 `s3:GetObject arn:aws:s3:::robot-datasets/*` |
| MinIO endpoint 是否可达 | ✅ 是 | 同一 qptk9 workflow，**`bridge-qc` 同样走 `profile_*` 入口能成功**（685B contract report）；`bridge-normalize` 能从 raw 全量读 + 写 ods + 写 dwd |
| step pod 是否拿到了正确的 `ROBOT_DH_S3_*` 环境变量 | ✅ 是 | 否则 bridge 通路也会全失败；事实上只有 droid / robomimic profile 路径出问题 |
| `archiveLogs` 是否正常 | ✅ 是 | bridge 系列 6 个 pod 都有 KiB 级别日志；只有 droid 那个 pod **本身就没输出**（0B 是 Argo 上传"空 stdout"的正常表现，不是 archive 链路坏） |

> 也就是说：**同一条 qptk9 workflow 的不同 step pod 既能成功 profile bridge parquet、又能成功 download robomimic 6.5 GiB HDF5**。`droid-qc` FAIL 与 `robomimic-qc` metric 失真**都不是** policy / bucket / endpoint / 文件存在性 / 网络拓扑层面的问题，全归 `robot-data-harness` 主项目。

## 4. 错误 F：`droid-qc` 0B 失败 — 详细分析与修复

### 4.1 现场拆解

```text
qc-contract-run.2368294939.log:  0 bytes
contract_report.json:            does not exist
asset_profile.json:              does not exist
pod 创建时间 vs 失败时间:        与 robomimic-qc PASS 同一时刻（01:11:50 CST）
```

0B 是 Argo controller 在 pod 进入 terminated 后立即从 pod stdout 拉取一次的产物——业务进程**在第一行 print/log 之前**就被结束。可能根因（按嫌疑排序，wsl 侧请按下面顺序复现）：

1. **OOM**：`profile_parquet` 如果使用 `pq.read_table(uri).to_pandas()` 之类**非 lazy** 入口，单 droid parquet 82 MiB 解压成 Arrow 后会膨胀到 ~500 MiB（droid lerobot v2 含 video frame bytes 列）；181 个文件累计能爆 step container（kind 默认 limit 通常 2 Gi）
2. **fhkvr §5.1.1 lazy 修复未走到 droid 多 chunk 入口**：fhkvr 给的方案是单文件 `pq.ParquetFile(fs.open(uri))`，对 bridge 单 shard `shard_0-00000-of-00001.parquet` 完美；但 droid 是 lerobot v2 **目录式** dataset，profile_runner 可能用了：
   - `lerobot.LeRobotDataset.from_hub(...)` 这类高阶 API，回触 huggingface_hub 网络 / `pyav` / `torchvision` 依赖
   - 或者循环 181 个文件全跑 `read_metadata`，每个文件 ~1 GET footer，累计 90s+ 一般 OK；但如果错用 `read_table` 就直接死
3. **缺依赖**：droid 走 lerobot adapter 时若 `lerobot` 或其依赖（`pyav` / `torchcodec` / `decord`）**不在 step container 镜像**，import 阶段就 `ModuleNotFoundError`，且 logger 还没初始化时 print 就被默认 `stderr=PIPE` 吞掉
4. **未捕获异常 + 默认 `logger.basicConfig` 在 main 入口之后**：profile 在 `from robot_data_harness... import ...` 阶段就抛了（例如 `from lerobot.datasets import LeRobotDataset` 失败），CPython 默认会 print traceback 到 stderr，但如果 step container 用 `python -O -c "..."` 类入口、或 `subprocess.check_call` 包一层并 `stderr=subprocess.DEVNULL`，traceback 就消失了
5. **activeDeadlineSeconds**：robomimic 已经 7189s 才完成，如果 deadline 也是 ~7200s（2h），droid 18 GiB > robomimic 6.5 GiB，可能 SIGKILL 来时 stdout 还没 flush；但**首发 retry(0) 也是 0B**说明并不是 deadline 触发

### 4.2 修复方案（必做）

#### 4.2.1 在 `robot-dh qc contract run --dataset droid_lerobot_scale30` 入口**第一行**就打可见日志

```python
# robot_data_harness/qc/runner.py（建议路径）
def main():
    # 必须在所有 import lerobot / pyav 之前
    import sys, json, os, time
    print(json.dumps({
        "event": "qc_runner_boot",
        "argv": sys.argv,
        "env_keys": sorted([k for k in os.environ if k.startswith("ROBOT_DH_")]),
        "python": sys.version,
        "ts": time.time(),
    }), flush=True)
    try:
        from robot_data_harness.qc.profile_router import run
        ...
    except BaseException:
        import traceback
        traceback.print_exc()        # ★ 让 0B pod log 至少有 traceback
        raise
```

要点：

- 即使第一行就 `ImportError`，`qc_runner_boot` 这条 print 也已经 flush 到 stdout，**0B pod log 直接消失**
- `except BaseException` 覆盖 `SystemExit` / `KeyboardInterrupt`，让 SIGTERM 也能打出栈
- `flush=True` 防止 OOM 被 SIGKILL 时 stdout buffer 没刷盘

#### 4.2.2 给 droid lerobot v2 写专属 profile 入口（与 fhkvr §5.1.1 并行）

droid_lerobot_scale30 是 lerobot v2 dataset：

```
raw/droid_lerobot_scale30/v1/
├── _manifest.json     (44 KiB，包含 181 个 files)
├── README.md          (11 KiB)
├── data/chunk-000/file-000.parquet ... file-180.parquet    (各 81–84 MiB)
├── meta/info.json     (10 KiB)
├── meta/stats.json    (44 KiB)
├── meta/tasks.parquet (1.4 MiB)
└── videos/...         (~10 GiB)
```

profile 入口建议（**不要**用 `lerobot.LeRobotDataset.from_hub`，避免 HF Hub 回源）：

```python
# robot_data_harness/qc/profile_droid_lerobot.py（建议路径）
import json
import pyarrow.parquet as pq
import s3fs
from concurrent.futures import ThreadPoolExecutor, as_completed

def profile_droid_lerobot(dataset_uri: str, *, max_parquet: int = 8) -> dict:
    """
    profile droid lerobot v2 dataset。只读元数据，整 parquet 不下载。
    max_parquet=8：只采样前 8 个 chunk parquet 做 schema/row_count 校验，
                   避免 181 文件 × 1 GET footer ≈ 3min 的串行 IO；
                   仍然 read 所有 meta/info.json + meta/stats.json + meta/tasks.parquet。
    """
    fs = _get_s3fs()
    base = dataset_uri.removeprefix("s3://").rstrip("/")

    # 1. 必读元数据（小文件，串行）
    with fs.open(f"{base}/meta/info.json", "rb") as fobj:
        info = json.load(fobj)
    with fs.open(f"{base}/meta/stats.json", "rb") as fobj:
        stats = json.load(fobj)
    with fs.open(f"{base}/_manifest.json", "rb") as fobj:
        manifest = json.load(fobj)

    # 2. 抽样 parquet schema（并发 lazy，整文件不下载）
    chunk_files = sorted(fs.ls(f"{base}/data/chunk-000"))[:max_parquet]
    def _profile_one(uri: str) -> dict:
        with fs.open(uri, "rb") as fobj:
            pf = pq.ParquetFile(fobj)
            return {
                "uri": "s3://" + uri,
                "num_rows": pf.metadata.num_rows,
                "schema_hash": str(hash(pf.schema_arrow.to_string())),
            }
    with ThreadPoolExecutor(max_workers=8) as ex:
        sampled = list(ex.map(_profile_one, chunk_files))

    return {
        "status": "OK",
        "asset_format": "lerobot_v2_parquet",
        "dataset_family": "droid_lerobot",
        "episodes_count": info.get("total_episodes"),
        "frames_count": info.get("total_frames"),
        "fps": info.get("fps"),
        "chunk_files_total": len(manifest.get("files", [])),
        "sampled_parquet": sampled,
        "stats_keys": sorted(list(stats.keys()))[:50],
    }
```

要点：

- **不 import lerobot 包**，纯 pyarrow + s3fs；规避 pyav/torchcodec 镜像缺依赖问题
- 整 parquet **不下载**，只 GET footer；181 文件抽样 8 个，**单 step < 30s**
- 失败时返回 `{"status": "FAILED", "error": str(exc), "cause": repr(exc.__cause__)}`（与 fhkvr §5.1.1 同款）

#### 4.2.3 注册 contract `droid_lerobot_v1` 并接入 router

```python
# robot_data_harness/qc/profile_router.py
PROFILE_REGISTRY = {
    "bridgedata_v2_v1": profile_parquet_bridge,
    "robomimic_hdf5_v1": profile_hdf5_robomimic,
    "droid_lerobot_v1": profile_droid_lerobot,    # ★ 新增
}
```

infra 侧 `qc_contracts` 表已经支持 `dataset_family='droid_lerobot'` 这个值，主项目落 contract 时写：

```python
register_contract(
    contract_id="droid_lerobot_v1",
    dataset_family="droid_lerobot",
    rules_json={"rules": [
        {"rule_id": "parquet_valid_rate", "metric": "parquet_valid_rate", "op": ">=", "threshold": 0.95, "severity": "fail"},
        {"rule_id": "episode_count_min",  "metric": "episodes_count",     "op": ">=", "threshold": 1,    "severity": "fail"},
        {"rule_id": "schema_consistency", "metric": "schema_hash_unique", "op": "==", "threshold": 1,    "severity": "fail"},
    ]},
)
```

#### 4.2.4 step container 资源 limit 兜底

WorkflowTemplate 里 `qc-contract-run` 加：

```yaml
resources:
  requests:
    memory: 1Gi
    cpu: 500m
  limits:
    memory: 4Gi     # droid 路径如果有 1 GiB+ 中间态，OOMKilled 会带 events，比 0B 容易排障
    cpu: 2
```

> 4 Gi 是兜底；按 §4.2.2 lazy 实现后理论 < 500 MiB peak。

### 4.3 修复方案（可选 / 排障）

#### 4.3.1 给 step pod 加 `terminationGracePeriodSeconds: 30`

让 SIGKILL 之前有 30s 把 stdout buffer 刷干净，避免 OOMKilled / DeadlineExceeded 时 archive 0B：

```yaml
spec:
  templates:
    - name: qc-contract-run
      terminationGracePeriodSeconds: 30
      ...
```

#### 4.3.2 stderr 兜底：用 `tee` 把整个 step 输出双写到 emptyDir

```yaml
container:
  command: ["/bin/bash", "-c"]
  args:
    - |
      set -o pipefail
      python -m robot_data_harness.qc.runner "$@" 2>&1 | tee /events/qc-stdout.log
volumeMounts:
  - name: events
    mountPath: /events
volumes:
  - name: events
    emptyDir: {}
```

后续在 Argo `outputs.artifacts` 把 `/events/qc-stdout.log` 上传到 `s3://robot-dh-artifacts/qc-stdout/<pod.name>.log`，**与 Argo archiveLogs 互补**：archiveLogs 拿 controller 看到的 stdout，artifacts 拿 emptyDir 里的真实文件，两者交叉验证 0B 是不是 archive 链路问题。

## 5. 错误 G：`robomimic-qc` 报表失真 + 性能 — 详细分析与修复

### 5.1 G1：`episode_len_p50/p95 = 0`

contract_report.json 里 `metrics`：

```json
{
  "demo_count": 21000,            // ★ 正确：26 个 HDF5 文件 demo_count 求和
  "num_hdf5_files": 26,
  "action_present_rate": 1.0,
  "obs_next_obs_mismatch_rate": 0.0,
  "reward_done_length_mismatch_rate": 0.0,
  "action_range_violation_rate": 0.0,
  "episode_len_p50": 0,           // ★★ 失真
  "episode_len_p95": 0            // ★★ 失真
}
```

asset_profile.json 里**已经拿到** `actions_shape: [N, 7]`，N 就是单 demo 的 episode_len。但 contract 的 metric aggregator 没消费这条字段——疑似把 `actions_shape[0]` 当成"所有 demo 的 action 总长"而不是"单 demo 的 length"，或者 contract 规则里写的是 `episode_len = demo_attrs.get("episode_len", 0)` 而 HDF5 里这条 attr 根本不存在。

修复（建议在 `profile_hdf5_robomimic` 里**显式遍历每个 demo group 取 `actions.shape[0]`**）：

```python
# robot_data_harness/qc/profile_hdf5.py
def profile_hdf5_robomimic(s3_uri: str) -> dict:
    local = _download(s3_uri)  # fhkvr §5.1.2 已有
    with h5py.File(local, "r") as f:
        episode_lens: list[int] = []
        # robomimic v1.5 顶层 `data/demo_X/actions` shape = (T, 7)
        for demo_name in f["data"].keys():
            actions = f[f"data/{demo_name}/actions"]
            episode_lens.append(int(actions.shape[0]))
        import numpy as np
        return {
            "status": "OK",
            "demo_count": len(episode_lens),
            "episode_lens": episode_lens,                    # ★ 让 contract aggregator 能 reduce
            "episode_len_p50": int(np.percentile(episode_lens, 50)) if episode_lens else 0,
            "episode_len_p95": int(np.percentile(episode_lens, 95)) if episode_lens else 0,
            ...
        }
```

然后 contract aggregator 在多文件 reduce 时 **concat `episode_lens` 列表再算 percentile**，而不是对每文件的 `episode_len_p50` 再 percentile（统计学口径错位）：

```python
# robot_data_harness/qc/aggregate.py
def aggregate_robomimic(per_file_profiles: list[dict]) -> dict:
    all_lens: list[int] = []
    for p in per_file_profiles:
        all_lens.extend(p.get("episode_lens", []))
    import numpy as np
    return {
        "demo_count": sum(p["demo_count"] for p in per_file_profiles),
        "num_hdf5_files": len(per_file_profiles),
        "episode_len_p50": int(np.percentile(all_lens, 50)) if all_lens else 0,
        "episode_len_p95": int(np.percentile(all_lens, 95)) if all_lens else 0,
        ...
    }
```

同时 contract `robomimic_hdf5_v1` 的 `rules_json` 建议加：

```json
{"rule_id": "episode_len_p50_min", "metric": "episode_len_p50", "op": ">=", "threshold": 5,  "severity": "warn"},
{"rule_id": "episode_len_p95_max", "metric": "episode_len_p95", "op": "<=", "threshold": 2000, "severity": "warn"}
```

→ 防止下次再回归到 p50=0。

### 5.2 G2：耗时 7189s ≈ 2h（性能）

26 个 HDF5 × 平均 250 MiB，按 fhkvr §5.1.2 单文件 download ~30s + h5py open ~10s = ~40s，串行 ~17min；实际 7189s **比期望慢 7×**。可能原因：

1. **没并发**：`for uri in hdf5_uris: profile_hdf5(uri)` 串行循环
2. **遍历 demo 内容**：如果 §5.1 修复是 `actions[:]` 把整张 action 表读到内存，单 demo ~70 KiB × 21000 demos = 1.4 GiB / 文件，乘以 26 文件就是 36 GiB，纯 IO 主导
3. **没复用 download 缓存**：若 contract `rules` 列表里写了 `action_present_rate` / `obs_next_obs_mismatch_rate` / `reward_done_length_mismatch_rate` 三条规则各自调用 `profile_hdf5()`，会**重复 download 3 次**

修复：

```python
# robot_data_harness/qc/profile_router.py
def run_qc_robomimic(dataset_uri: str) -> dict:
    files = list_hdf5_files(dataset_uri)
    # ★ 并发 download + profile，max_workers=4 让 26 HDF5 4 路并行
    with ThreadPoolExecutor(max_workers=4) as ex:
        profiles = list(ex.map(profile_hdf5_robomimic, files))
    return aggregate_robomimic(profiles)
```

要点：

- `max_workers=4`：MinIO 端口 9000 单 worker pull 大概 ~30 MiB/s，4 路并发 ~120 MiB/s 已经撞带宽；不建议 >8
- 全 `actions.shape[0]` 只读 chunk header，不实际读 byte（h5py 已经支持），不需要 `[:]`
- **同一 HDF5 文件只 download 一次**：多条规则共享 `profile_hdf5` 的 cached profile，不重复 download

> 期望优化后单 dataset < 15min。

## 6. 验收清单

| 项 | 责任方 | 通过标准 |
|----|--------|----------|
| `droid-qc` 不再 0B FAIL | robot-data-harness | 复跑 multisource-scale30，`qc-contract-run-<droid pod>` archive log size > 0；若失败必带 traceback + `cause=` 字段 |
| droid contract_report 落地 | robot-data-harness | `mc stat rdh/robot-lake/qc/droid_lerobot_scale30/v1/contract_report.json` 存在，`status` ∈ {PASS, WARN, FAIL}（**不能是 0B / 不存在**） |
| droid asset_profile 落地 | robot-data-harness | `mc cat rdh/robot-lake/qc/droid_lerobot_scale30/v1/asset_profile.json \| jq '.dataset_family'` 返回 `droid_lerobot`；`.profile.parquet[]` 至少 8 条采样 |
| robomimic `episode_len_p50/p95` 不再 0 | robot-data-harness | `mc cat rdh/robot-lake/qc/robomimic_scale30/v1/contract_report.json \| jq '.metrics.episode_len_p50'` ≥ 5 |
| robomimic-qc 耗时收敛 | robot-data-harness | contract_report `duration_sec` < 1200（20min） |
| step pod 资源 limit | robot-data-harness（WorkflowTemplate） | `kubectl -n robot-dh get pods <pod> -o jsonpath='{.spec.containers[0].resources.limits.memory}'` 不为空 |
| qc_contracts 表里 `droid_lerobot_v1` 已注册 | robot-data-harness | `psql -c "SELECT contract_id, enabled FROM qc_contracts WHERE dataset_family='droid_lerobot';"` 至少 1 行 enabled=true |
| 完整 multisource-scale30 workflow 跑到 Succeeded | robot-data-harness + WSL/kind | `kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-XXXXX -o jsonpath='{.status.phase}'` 返回 `Succeeded` |
| infra 侧 PG `etl_perf_runs` 加列后 pending 回填 | infra（已并行处理）+ robot-data-harness | `robot-dh perf reingest-pending` 后 `SELECT count(*) FROM etl_perf_runs WHERE job_id LIKE 'etl-run-bridgedata_v2_scale30%';` ≥ 2 |

## 7. 复现 / 排障最小命令

```bash
# A. 复现 droid-qc 0B（在 step container 镜像里直接跑入口）
docker run --rm -it robot-data-harness:v1.6 \
  bash -c 'python -m robot_data_harness.qc.runner \
    --dataset droid_lerobot_scale30 --version v1 \
    --src s3://robot-datasets/raw/droid_lerobot_scale30/v1 \
    --contract droid_lerobot_v1'
# 如果第一行没有 qc_runner_boot 这条 JSON，说明 §4.2.1 没 patch 进入口

# B. 本地探测 droid 真实 schema（不走 lerobot 包，纯 pyarrow + s3fs）
python -c "
import pyarrow.parquet as pq, s3fs
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'},
                      key='robotdhapp', secret='***')
pf = pq.ParquetFile(fs.open(
    'robot-datasets/raw/droid_lerobot_scale30/v1/data/chunk-000/file-000.parquet'
))
print(pf.schema_arrow)
print('rows:', pf.metadata.num_rows)
print('size MiB:', pf.metadata.serialized_size / 1024 / 1024)
"

# C. 复现 robomimic episode_len_p50=0 metric 失真
python -c "
import h5py, tempfile, boto3, os
from urllib.parse import urlparse
s3 = boto3.client('s3', endpoint_url=os.environ['ROBOT_DH_S3_ENDPOINT_URL'],
                  aws_access_key_id=os.environ['ROBOT_DH_S3_ACCESS_KEY'],
                  aws_secret_access_key=os.environ['ROBOT_DH_S3_SECRET_KEY'])
uri = 's3://robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mh/low_dim_v15.hdf5'
p = urlparse(uri)
with tempfile.NamedTemporaryFile() as tf:
    s3.download_fileobj(p.netloc, p.path.lstrip('/'), tf)
    tf.flush()
    with h5py.File(tf.name, 'r') as f:
        lens = [f['data'][k]['actions'].shape[0] for k in list(f['data'].keys())[:50]]
        print('first 50 demo lens:', lens)        # 应该看到 100+ 的整数，而不是 0
"

# D. 检查 qptk9 当前实际状态（哪些 step 还在 retry）
kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-qptk9 \
  -o jsonpath='{range .status.nodes[*]}{.displayName}{"\t"}{.phase}{"\t"}{.message}{"\n"}{end}' \
  | sort
```

排障决策树（每条**先 mc 直读**，能读到 = 不是 infra 的事）：

1. `mc stat rdh/robot-datasets/raw/droid_lerobot_scale30/v1/data/chunk-000/file-000.parquet` 是否秒返
   - 不能 → 升 infra issue
   - 能 → 与 infra 无关，继续走 2
2. 同 workflow 其他 step 是否在同一时间窗失败
   - 普遍失败 → 网络 / 凭据
   - 个别失败 → 模块代码（go 走 3）
3. `kubectl describe pod <qc-contract-run-droid-pod>` 看 `Last State` 是否 OOMKilled / DeadlineExceeded
   - OOMKilled → §4.2.4 加 memory limit；§4.2.2 改 lazy
   - DeadlineExceeded → §4.2.2 改 lazy + §4.3.1 加 grace period
   - Error（exit code ≠ 0 但非 137/124）→ §4.2.1 让第一行 print 兜底 traceback

## 8. infra 侧并行 follow-up（不在本需求 PR 内）

| 项 | 当前状态 | 处理 |
|----|---------|------|
| `etl_perf_runs` 加 `started_at` / `finished_at` 列 | infra 端待 apply（见 [`v1_6_etl_perf_runs_schema_align_request.md`](v1_6_etl_perf_runs_schema_align_request.md)） | infra 落 `006_v1_6_etl_perf_runs_align.sql` migration |
| pending perf records 回填 | qptk9 已落 2 条 pending | infra schema 上线后跑 `robot-dh perf reingest-pending` |
| droid contract 入 `qc_contracts` 表 | infra schema 已支持 `dataset_family='droid_lerobot'` | 主项目 §4.2.3 `register_contract` 一次性写入 |
| `bridge-features status=WARN` 的 `input_bytes=0` 统计口径 | qptk9 已发生，不阻塞 | 主项目复核 `compute_input_bytes` 是否未消费 `materialize_input` 的 download 字节 |

## 9. 时间窗口建议

| 阶段 | 估计耗时 | 备注 |
|------|----------|------|
| §4.2.1 qc_runner_boot 兜底 print + traceback | < 1h | 改一处入口 + 单测 |
| §4.2.2 droid lerobot v2 lazy profile + §4.2.3 注册 contract | 1 day | 列名探测 + 映射 + 单测；可顺手把 dry-run schema 检查也加上 |
| §4.2.4 WorkflowTemplate 加 memory limit | < 1h | 改一处 yaml |
| §5.1 robomimic episode_len 统计修复 + §5.2 并发 | 1 day | 改 profile + aggregate + 单测 |
| 联调一次 multisource-scale30 | 1 day | 看新 `argo-logs/` 里的归档 log 验证 |

收到 wsl 侧的修复 PR + 联调通过截图（含 `mc ls -r robot-dh-artifacts/argo-logs/robot-dh/robot-dh-multisource-scale30-XXXXX/` 输出 + 三份 contract_report.json）后，本文档可以标 **「已闭环」**，同步更新到 [`docs/runs/20260524/robot-dh-multisource-scale30-qptk9/INDEX.md`](runs/20260524/robot-dh-multisource-scale30-qptk9/INDEX.md) 引用处。
