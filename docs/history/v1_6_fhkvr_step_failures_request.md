# `robot-dh-multisource-scale30-fhkvr` step 失败归因 / 修复需求

> 提交方：`robot-dh-infra`（云端 PostgreSQL / MinIO / Redis）
> 接收方：`robot-data-harness` 主项目（step container 镜像、CLI、ETL / QC 模块）
> 协同方：WSL/kind 项目（argo 控制面；本次不涉及，只是 step pod 运行环境）
> 关联：
>
> - [`docs/v1_6_argo_log_archive_request.md`](v1_6_argo_log_archive_request.md) §8
> - [`docs/v1_6_argo_log_archive_handoff.md`](v1_6_argo_log_archive_handoff.md) §7
> - [`docs/v1_6_storage_and_deadline_notes.md`](v1_6_storage_and_deadline_notes.md)
> - [`docs/lake_layout.md`](lake_layout.md)
>
> 优先级：P1（v1.6 multisource-scale30 当前会 normalize / QC 双断）

## 1. 现场

来源：`workflow.argoproj.io/robot-dh-multisource-scale30-fhkvr`，2026-05-24 在 WSL/kind 上提交。3 段 stdout 已经被人工复制到云端 host：

```text
log_robot-dh-multisource-scale30-fhkvr_20260524/
├── droid-qc.log         # 1 行 WARNING
├── robomimic-qc.log     # 1 行 WARNING
└── bridge-normalize.log # 72 行，含 INFO / WARNING / ERROR + summary JSON
```

## 2. 三类错误现象（一句话版）

| # | step | 现象 | 原始行 |
|---|------|------|--------|
| A | droid-qc / robomimic-qc | `profile_parquet` / `profile_hdf5` 失败 → 单行 `WARNING`，把底层 exception 吞成 `Max Retries Exceeded` | `profile_parquet failed for s3://...file-000.parquet: Max Retries Exceeded` |
| B | bridge-normalize | `bridgedata_v2_scale30` schema 不被 normalize adapter 识别，跑两次都 `phase=normalize.load_bundles` 抛 `ValueError` | `Unable to extract pose episodes from HuggingFace-style dataset ... Add an explicit adapter mapping for this dataset schema.` |
| C | bridge-normalize（持续） | heartbeat 写本地 jsonl 被拒（容器内 `/app/runs/events/` 不可写） | `heartbeat jsonl write failed: [Errno 13] Permission denied: '/app/runs/events/heartbeats_20260524.jsonl'` |

A 是 WARNING 但 step exit 0（QC profile **静默缺失**）；B 让 normalize step 整个 FAIL；C 全程不影响退出码但让 v1.6 `task_heartbeats` 拿不到 step 心跳。

## 3. 归因：不是 `robot-dh-infra` 的问题

| 检查项 | 结论 | 证据 |
|--------|------|------|
| `robotdhapp` 是否有 `robot-datasets/*` GetObject | 是 | `mc admin policy info rdh robot-dh-readwrite` 中含 `s3:GetObject` on `arn:aws:s3:::robot-datasets/*` |
| 失败对象是否真的存在 | 是 | `mc stat rdh/robot-datasets/raw/droid_lerobot_scale30/v1/data/chunk-000/file-000.parquet` 立刻返回 82 MiB + ETag；`demo_v15.hdf5` 798 MiB 同样可见 |
| MinIO endpoint 是否可达 | 是（公网入口由 WSL/kind 走） | **关键交叉证据**：同一条 fhkvr workflow、同时间窗（07:41–07:59），bridge-normalize 的 `materialize_input` 阶段从 `s3://robot-datasets/raw/bridgedata_v2_scale30/v1` 完整下载 227 MiB shard 到本地 `/tmp/`，`done` 标记齐全 |
| step pod 容器目录权限 | 否（归镜像，与 infra 无关） | `/app/runs/events/` 在 step container 内不可写；这条目录的 `mkdir / chown` 在镜像构建期决定 |
| normalize adapter 列映射 | 否（归代码，与 infra 无关） | 错误信息明文要求 "Add an explicit adapter mapping for this dataset schema" |

> 也就是说：**同一条 fhkvr workflow 的 step pod 既能成功访问 MinIO，又能成功下载几百 MB 的对象**。三类错误**都不是** policy / bucket / endpoint / 文件存在性 / 网络拓扑层面的问题，全归 `robot-data-harness` 主项目。

## 4. 技术选型评估（针对用户提议）

`robot-dh-infra` 侧讨论过几个方案：

1. 把裸 boto3 换成 `fsspec + s3fs` 做流式读取
2. 引入 Apache Iceberg 或 Delta Lake 替换 raw → ods → dwd → ads 分层
3. 引入 Ray Data 做分布式处理
4. 用 Great Expectations 或 Soda Core 取代现有 QC 规则引擎

下面按 "对当前 3 类 bug 的针对性 / 落地成本 / 演进收益" 三个维度做客观评估。

### 4.1 `fsspec + s3fs` ✅ **部分采纳（Parquet 走 lazy；HDF5 走 materialize-first）**

- **能解决什么**：PyArrow + s3fs 的组合允许 `pq.ParquetFile(fs.open(uri))` 真正 lazy open，**profile schema 完全不用整文件下载**，QC 只读 footer + page metadata 即可。这对错误 A 的 Parquet 路径是结构性修复。`fsspec` 同时把 storage_options（`config_kwargs={'connect_timeout': 10, 'read_timeout': 300, 'retries': {...}}`）的语义梳理得比裸 boto3 干净。
- **不能解决什么**：HDF5 不能这么干。h5py + fsspec 的官方说法（[PyNWB streaming tutorial](https://pynwb.readthedocs.io/en/dev/tutorials/advanced_io/streaming.html)）是：

  > "fsspec is not optimized for reading HDF5 files, and so streaming data using this method can be slow. `remfile` may be a faster alternative."

  原因：HDF5 是 chunked binary 格式，元数据散布在文件多个位置，fsspec 的 read-by-range 会引发非常多次 small GET + 重新 seek，单文件 798 MiB 实测会比直接下载慢一个数量级。HDF5 真正的 S3 streaming 方案是：

  - `h5py` 的 [ROS3 driver](https://pynwb.readthedocs.io/en/dev/tutorials/advanced_io/streaming.html#streaming-with-the-ros3-driver)：原生 HDF5 库的 S3 backend；但**只有 conda 安装的 h5py 自带，pip wheel 不带**。本仓库 `environment.yml` 用的是 pip h5py，部署成本不低
  - [`h5coro`](https://github.com/SlideRuleEarth/h5coro)：SlideRule 团队的纯 Python cloud-optimized HDF5 reader，自带 block cache + 并发 GET；适合**TB 级遥感数据集**，对 798 MiB Robomimic HDF5 是大炮打蚊子
  - `remfile`：轻量级 streaming，比 fsspec 快但比 ROS3 慢

  对当前规模（798 MiB / dataset），最务实的方案是**沿用 normalize 已有的 materialize-first 模式**：先 `boto3.client('s3').download_fileobj(bucket, key, fileobj)` 到 `/tmp/robot-dh-qc-<rand>/`，再 `h5py.File('/tmp/.../demo_v15.hdf5')`。带 `Config(connect_timeout, read_timeout=300, retries={'max_attempts': 10, 'mode': 'adaptive'})`，下载阶段的 retry/timeout 由 botocore 控制。同时把底层 exception 直接 log，停止吃成 "Max Retries Exceeded"。

- **结论**：v1.6 立即采纳，Parquet 路径直接换 `pyarrow + s3fs` lazy；HDF5 路径保留 `boto3 download → h5py.File()`，加 botocore Config + 显式 exception。**不必把所有 IO 一刀切换 fsspec**。

### 4.2 Apache Iceberg / Delta Lake ⏸ **暂缓，作为 v2.x roadmap 项目**

- **2026 年现状**：PyIceberg 0.11.1（2026-03 release）成熟可用，且 [Faceberg](https://github.com/kszucs/faceberg) 允许把 HuggingFace 仓库的 parquet **不拷贝、只生成 manifest** 暴露成 Iceberg 表（REST catalog 部署在 HF Space）；`iceberg-rust` 也合并了 HF Hub storage backend PR。技术栈层面**门槛降到很低**。
- **真正解决什么问题**：schema evolution（向上下兼容地加列）、time travel（按 snapshot ID 回放）、atomic commit（避免 ods 半写）、分区裁剪（query engine 自动 prune）。这些都是**未来 v1.7+ 的迭代价值**，但和当前 fhkvr 的 3 个 bug 完全无关。
- **落地成本**：
  - 重写 normalize / build_features / build_ads 作业（写入要 `table.append(arrow_table)` 而不是 `put_object`）
  - 部署 REST catalog（最便宜的是 nessie / 自建 Postgres-backed REST，或者用 Faceberg 的 HF Space）
  - 现有 `lake_assets / dataset_versions / qc_contracts` PG 表要么报废、要么变成 Iceberg manifest 的镜像视图
  - 数据规模上 PyIceberg 单机适配 **GB 级**（官方语：[Using Apache Iceberg with Python and MPP Query Engines](https://dev.to/alexmercedcoder/using-apache-iceberg-with-python-and-mpp-query-engines-1d0)），TB 级要上 Spark/Dremio。当前 27 GiB 总量在甜区，但**演进到 100 GiB+ 后才能展现 Iceberg 真正的价值**
- **结论**：v1.6 不引入，作为 v2.x 大版本规划的核心方向。建议先做一个 **POC 分支**（不动 main），用 Faceberg 把 `robot-datasets/raw/*_scale30/v1/` parquet 暴露成 Iceberg 表，再用 PyIceberg 做一次 schema 查询，验证可行性。

### 4.3 Ray Data ❌ **不引入**

- **设计假设**：Ray Data 是多节点分布式数据处理框架。它最佳的使用场景是 10+ worker 节点 / batch inference / 大规模 shuffle。
- **当前环境**：WSL/kind 是**单节点单 worker** 的开发环境；prod 之后大概率会跑在云端 GPU 单机 / 单 K8s namespace 多 pod。Argo DAG 已经提供 step 级并行，每个 step 是独立 pod，状态、重试、隔离由 Argo 控制。
- **错配点**：在单节点跑 Ray Data 等于在 boto3 / pyarrow 之上多套一层调度抽象。性能没收益，反而引入 Ray cluster 启动开销、daemon 进程、内存占用上限调参等额外维护负担。
- **结论**：当前规模不引入。若未来扩到 100+ TB 级训练数据、需要在云端 GPU 多机并行做 batch encoding / featurization，再单独立项评估。

### 4.4 Great Expectations / Soda Core ⏸ **暂缓，作为 v1.7 QC engine 多 backend 演进**

- **2026 年现状**：根据 [Modern DataTools](https://www.modern-datatools.com/compare/soda-vs-great-expectations) / [Dataworkers](https://dataworkers.io/resources/data-quality-great-expectations-vs-soda-vs-ai-agents/) 等多份对比：
  - **GE**：Python-first，Expectation Suites = code as contract，自带 Data Docs HTML 报告，社区最大；**对 Pandas / Spark / Arrow backend 原生支持**，适合 Parquet on S3 场景
  - **Soda Core**：YAML 声明 + SodaCL，主要走数据仓库 SQL（Snowflake / BigQuery / Postgres / Spark SQL），对 S3 Parquet 要走 Spark/Dask backend；非 Python 工程师更友好
- **本仓库现状**：`postgres/migrations/005_v1_6_robot_platform.sql` 已经创建了 `qc_contracts` 表，字段 `rules_json jsonb` 描述 schema / range / domain check，配套 `qc_contract_runs` 落每次执行结果（`failed_rules_json` / `warning_rules_json` / `metrics_json` / `artifacts_uri`）。**这本质就是一个自研版 Expectation Suite + run history**。
- **真正解决什么问题**：标准化的 expectation 词汇（`expect_column_values_to_be_in_set` 这类），HTML Data Docs，社区维护的 expectation 库（statistical drift / regex 等不用自己写）。
- **落地路径**（推荐渐进而非替换）：
  - 在 `qc_contracts` 上新加列 `qc_engine text NOT NULL DEFAULT 'native'`，允许同一份 contract 在 `'native'` / `'ge'` / `'soda'` 三种 runtime 之间切换
  - 主项目实现 `qc_engine='ge'` backend：把 `rules_json` 翻译成 GE expectation suite，跑完把 `validation_result.results[]` 拆进现有 `failed_rules_json` / `warning_rules_json`，让下游 `39_qc_contract_report.sh` 完全不感知
  - 先在 contract-qc workflow 上做 A/B：同一份 dataset 跑 native + GE 两遍，差异收敛后再切默认
- **结论**：v1.6 不替换，v1.7 起作为 backend 多元化方向规划。**不要重写 `qc_contracts` 表**——它的字段设计已经足够通用承载 GE / Soda 两种 backend 的结果。

### 4.5 决策汇总

| 方案 | v1.6 决定 | 触发 v1.7+ 引入的先决条件 |
|------|-----------|---------------------------|
| `fsspec + s3fs` （Parquet lazy） | ✅ **采纳** | — |
| HDF5 走 fsspec / ROS3 / h5coro | ❌ 当前不需要，沿用 materialize-first | 单 HDF5 > 5 GiB 或需要 lazy slicing |
| Iceberg / Delta Lake | ⏸ 暂缓 | 总数据量 > 100 GiB / 需要 schema evolution / 多读者并发查询 |
| Ray Data | ❌ 不引入 | 多节点 worker 集群 + TB 级 batch inference |
| GE / Soda Core | ⏸ 暂缓 | `qc_contracts` 上加 `qc_engine` 字段后做 GE backend POC |

## 5. v1.6 立即修复方案（具体到代码级别）

> 路径名以仓库实际为准；下面是建议的最小变更范围，便于 PR review。

### 5.1 错误 A：QC profile 模块换 lazy / 加 retry / 暴露底层异常

#### 5.1.1 `profile_parquet`：直接 lazy

```python
# qc/profile_parquet.py（建议路径）
import os
import pyarrow.parquet as pq
import s3fs
from botocore.config import Config

_S3FS: s3fs.S3FileSystem | None = None

def _get_fs() -> s3fs.S3FileSystem:
    global _S3FS
    if _S3FS is None:
        _S3FS = s3fs.S3FileSystem(
            key=os.environ["ROBOT_DH_S3_ACCESS_KEY"],
            secret=os.environ["ROBOT_DH_S3_SECRET_KEY"],
            client_kwargs={
                "endpoint_url": os.environ["ROBOT_DH_S3_ENDPOINT_URL"],
                "region_name": os.environ.get("ROBOT_DH_S3_REGION", "us-east-1"),
            },
            # 复用 botocore 的 retry / timeout 配置
            config_kwargs={
                "connect_timeout": 10,
                "read_timeout": 300,
                "retries": {"max_attempts": 10, "mode": "adaptive"},
            },
        )
    return _S3FS

def profile_parquet(s3_uri: str) -> dict:
    # 只读 schema + row_group 元数据，整个文件不下载
    fs = _get_fs()
    path = s3_uri.removeprefix("s3://")
    try:
        with fs.open(path, "rb") as fobj:
            pf = pq.ParquetFile(fobj)
            return {
                "schema": pf.schema_arrow.to_string(),
                "num_rows": pf.metadata.num_rows,
                "num_row_groups": pf.num_row_groups,
                "column_types": {
                    f.name: str(f.type) for f in pf.schema_arrow
                },
                "status": "OK",
            }
    except Exception as exc:
        logger.warning(
            "profile_parquet failed for %s: %s (cause=%r)",
            s3_uri, exc, exc.__cause__,
        )
        return {"status": "FAILED", "error": str(exc), "cause": repr(exc.__cause__)}
```

要点：

- `pq.ParquetFile(fs.open(uri))` 真正 lazy，整个 82 MiB parquet 不下载
- `config_kwargs` 把 retry / timeout 显式传给底层 botocore
- `exc.__cause__` 露出底层 `ReadTimeoutError` / `IncompleteRead` / `ConnectionResetError`，停止吞成单行 "Max Retries Exceeded"
- 失败时返回 `{"status": "FAILED", ...}`，让 gate_report.json 感知，不再静默缺失

#### 5.1.2 `profile_hdf5`：materialize-first（复用 normalize 模式）

```python
# qc/profile_hdf5.py（建议路径）
import tempfile, boto3, h5py
from botocore.config import Config
from urllib.parse import urlparse

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
    # HDF5 本身不友好做远端 random read：fsspec 慢、ROS3 需要 conda 安装。
    # 当前 798 MiB 量级用 materialize-first 是最稳的方案，与 normalize.materialize_input 同模式。
    parsed = urlparse(s3_uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    with tempfile.TemporaryDirectory(prefix="robot-dh-qc-") as tmpdir:
        local = Path(tmpdir) / Path(key).name
        try:
            _s3().download_file(bucket, key, str(local))
        except Exception as exc:
            logger.warning(
                "profile_hdf5 download failed for %s: %s (cause=%r)",
                s3_uri, exc, exc.__cause__,
            )
            return {"status": "FAILED", "error": str(exc), "cause": repr(exc.__cause__)}
        try:
            with h5py.File(local, "r") as f:
                # 例：robomimic v1.5 顶层有 `data/demo_0/...`
                groups = {k: list(v.keys()) if hasattr(v, "keys") else None
                          for k, v in f.items()}
                return {
                    "status": "OK",
                    "top_level_groups": list(f.keys()),
                    "sample_subgroups": groups,
                    "file_size_bytes": local.stat().st_size,
                }
        except Exception as exc:
            logger.warning("profile_hdf5 open failed for %s: %s", local, exc)
            return {"status": "FAILED", "error": str(exc)}
```

要点：

- `boto3 download_file` 带 retry/timeout，stable
- HDF5 打开在本地 `/tmp/`，原生速度，不引入 fsspec 慢路径
- profile 失败也写明 status，gate_report.json 一致

> 未来若有 > 5 GiB HDF5 / 需要 lazy slicing，再单独评估 `h5coro` 或 conda 装 `ROS3`。

### 5.2 错误 B：给 `bridgedata_v2_scale30` 注册 normalize adapter

normalize 默认走 LeRobot/HuggingFace 标准列名（`action.*`、`observation.state.*`、`pose.*`）。BridgeData V2 的 parquet 列结构与 LeRobot 不同，需要在 adapter registry 显式注册。

复现 / 列结构勘察（不依赖 K8s）：

```bash
# 在本机看 BridgeData V2 真实列
python -c "
import pyarrow.parquet as pq, s3fs
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'},
                      key='robotdhapp', secret='***')
pf = pq.ParquetFile(fs.open('robot-datasets/raw/bridgedata_v2_scale30/v1/data/shard_0-00000-of-00001.parquet'))
print(pf.schema_arrow)
print('rows:', pf.metadata.num_rows)
"
```

主项目修复方向：

- 在 normalize adapter registry 注册 `bridgedata_v2_scale30` 的列映射，至少包含 `episode_id` / `frame_idx` / `pose_xyz` / `pose_rotation` / `gripper_state`（如有）
- 加 dry-run schema 探测：normalize 进入 `load_bundles` 之前先 `pyarrow.parquet.read_schema()`，列不全时 fail-fast，给出"哪些列缺、有哪些列可选"的友好诊断
- adapter 注册接口建议：

  ```python
  @register_normalize_adapter("bridgedata_v2_scale30")
  def adapt_bridgedata_v2(table: pa.Table) -> NormalizedEpisodes:
      # column mapping 由这里定义
      ...
  ```

### 5.3 错误 C：heartbeat 写路径修复

镜像内 `/app/runs/events/` 不可写。建议三选一（推荐前两个组合）：

1. **Dockerfile 建目录 + 改 owner**（最干净）：

   ```Dockerfile
   RUN useradd -u 1000 -m -s /bin/bash appuser \
    && mkdir -p /app/runs/events \
    && chown -R appuser:appuser /app /var/cache/robot-dh
   USER appuser
   ```

2. **环境变量覆盖默认路径**：暴露 `ROBOT_DH_EVENTS_DIR`，Argo step template 挂 `emptyDir` 进去：

   ```yaml
   volumes:
     - name: events
       emptyDir: {}
   containers:
     - volumeMounts:
         - name: events
           mountPath: /data/runs/events
       env:
         - name: ROBOT_DH_EVENTS_DIR
           value: /data/runs/events
   ```

3. **fallback 兜底**：写本地 jsonl 失败时，至少 `print(json.dumps(...), file=sys.stderr)` 一行，让 v1.6 的 `argo-logs/` 归档能拿到事后心跳。

> 第 1 + 2 个落地后，v1.6 `task_heartbeats` 表就能正常拿到 step 级心跳，与本仓库 `005_v1_6_robot_platform.sql` 的设计对齐。

## 6. v1.7+ 数据栈演进 backlog（仅供 robot-data-harness 长期规划）

不要为了"用上新工具"在 v1.6 落地，但建议主项目在 v1.7 起按下列顺序评估：

| 顺位 | 项目 | 触发条件 | 入门成本 | 建议第一步 |
|------|------|----------|----------|------------|
| 1 | `qc_engine='ge'` backend | `qc_contracts.rules_json` 表达力开始觉得不够，或需要 statistical drift / 分布对齐这类高级期望 | 中（GE Python API + result → PG 解析） | `qc_contracts` 加 `qc_engine` 列；对一个 contract 做 native + GE 双跑 A/B |
| 2 | Faceberg / PyIceberg POC | 数据量预计 > 100 GiB，或需要 time travel / atomic commit / 多读者并发 | 高（catalog 部署 + 写入路径重构） | 用 Faceberg 把 `robot-datasets/raw/*_scale30/v1/` parquet 暴露成只读 Iceberg 表，跑 PyIceberg 0.11+ scan + schema evolution 测试 |
| 3 | HDF5 ROS3 / h5coro | 单 HDF5 > 5 GiB，或 QC 只看少数 group / dataset 的 metadata 时 | 中（conda 装 / 部署） | 选 5 个有代表性的 HDF5，跑 materialize-first vs ROS3 vs h5coro 三种 profile 模式，对比 wallclock |
| 4 | Ray Data | 多节点 GPU worker 集群上线后做 batch inference / featurization | 高（cluster 部署 + 心跳监控） | 在 v2.x infra 规划时单独立项 |
| 5 | Iceberg + Spark MPP | 数据量进入 TB 级 / 需要外部查询引擎 | 极高 | 与 #2 合流 |

每一项的"是否引入"建议都按"先 POC、再 A/B、再切默认"三阶段走，不要直接替换现有 runtime。

## 7. 验收清单

| 项 | 责任方 | 通过标准 |
|----|--------|----------|
| profile_parquet 用 lazy 模式，且失败时带 `cause=<具体类>` | robot-data-harness | 复跑 `contract-qc`，对 droid `chunk-000/file-000.parquet`（82 MiB）profile 成功；`gate_report.json` 里 `profile_status: OK` |
| profile_hdf5 用 materialize-first，且失败时带 `cause=<具体类>` | robot-data-harness | 复跑 `contract-qc`，对 robomimic `demo_v15.hdf5`（798 MiB）profile 成功；失败时 log 至少含 `cause=ReadTimeoutError` 之类具体类，不再只有 `Max Retries Exceeded` |
| `bridgedata_v2_scale30` normalize 不再 ValueError | robot-data-harness | 复跑 multisource-scale30，bridge-normalize 至少产出 `ods/bridgedata_v2_scale30/v1/{pose.parquet, episode_meta.parquet, _manifest.json}` |
| `heartbeat jsonl write failed` 不再出现 | robot-data-harness | 三条 normalize（droid / robomimic / bridge）log 全程不再出现 `[Errno 13] Permission denied` |
| v1.6 `task_heartbeats` 表能看到上述 step 的心跳 | infra 侧自验 | `psql -c "SELECT count(*) FROM task_heartbeats WHERE task_id LIKE 'etl-run-bridgedata_v2_scale30%';"` ≥ 1 |
| 完整 fhkvr workflow 跑到 Succeeded | robot-data-harness + WSL/kind | `kubectl -n robot-dh get workflows.argoproj.io/robot-dh-multisource-scale30-XXXXX -o jsonpath='{.status.phase}'` 返回 `Succeeded` |

## 8. 复现 + 排障最小命令

```bash
# A. 复现 normalize adapter 缺失（最快）
python -m robot_data_harness.etl normalize \
  --dataset bridgedata_v2_scale30 --version v1 \
  --src s3://robot-datasets/raw/bridgedata_v2_scale30/v1 \
  --dst s3://robot-lake/ods/bridgedata_v2_scale30/v1 --no-resume

# B. 在本地探测 BridgeData V2 真实列（写 adapter 前必须看一眼）
python -c "
import pyarrow.parquet as pq, s3fs
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'},
                      key='robotdhapp', secret='***')
print(pq.ParquetFile(fs.open(
    'robot-datasets/raw/bridgedata_v2_scale30/v1/data/shard_0-00000-of-00001.parquet'
)).schema_arrow)"

# C. 复现 profile_hdf5 报 Max Retries Exceeded（加重 log 后能拿到 cause=）
python -c "
from robot_data_harness.qc.profile_hdf5 import profile_hdf5
print(profile_hdf5('s3://robot-datasets/raw/robomimic_scale30/v1/v1.5/can/mg/demo_v15.hdf5'))
"

# D. 复现 heartbeat 容器内写不进
docker run --rm -it robot-data-harness:v1.6 \
  bash -c 'touch /app/runs/events/x.jsonl'   # Permission denied
```

排障决策树（每条**先 mc 直读**，能读到 = 不是 infra 的事）：

1. `mc stat rdh/robot-datasets/raw/.../<file>` 是否秒返
   - 不能 → 升 infra issue
   - 能 → 与 infra 无关，继续走 2
2. 同 workflow 其他 step 是否在同一时间窗失败
   - 普遍失败 → 网络 / 凭据
   - 个别失败 → 模块代码（go 走 3）
3. 看 step container 内目录权限 / 进程 user 是否符合预期

## 9. 时间窗口建议

| 阶段 | 估计耗时 | 备注 |
|------|----------|------|
| QC profile 换 fsspec lazy (parquet) + boto3 download (hdf5) + 暴露 cause | < 1 day | 改两个函数 + 单测 |
| bridgedata_v2 normalize adapter | 1–2 day | 列名探测 + 映射 + 单测；可以顺手把 dry-run schema 检查也加上 |
| heartbeat 写路径修复 + 镜像重建 + WorkflowTemplate volume 挂载 | < 1 day | Dockerfile + step template 同改 |
| 联调一次 multisource-scale30 | 1 day | 看新 `argo-logs/` 里的归档 log 验证 |

收到 robot-data-harness 这边的修复 PR + 联调通过截图（含 `mc ls -r robot-dh-artifacts/argo-logs/` 输出）后，本文档可以标 **「已闭环」**，同步更新到 `docs/v1_6_argo_log_archive_request.md` §8 与 `docs/v1_6_argo_log_archive_handoff.md` §7 引用处。
