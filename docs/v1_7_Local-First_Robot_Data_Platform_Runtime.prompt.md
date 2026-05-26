你是资深机器人数据平台工程师、Python 数据工程师、Kubernetes 批处理工程师。当前项目 robot-data-harness 已完成 v1.6：

- Argo 多源 DAG
- QC Contract
- DROID / robomimic / BridgeData 适配
- heartbeat / checkpoint / partition
- archive logs
- workflow_steps / qc_contract_runs / asset_profiles
- ML-ready export
- Go exporter
- FastAPI 查询接口

v1.7 的目标是：
Local-First Robot Data Platform Runtime

当前主要问题：
1. 本地 kind 不适合直接从远端腾讯云 MinIO 拉 18GB DROID / 6GB robomimic。
2. 用户已经决定先做 <=3GB devscale 数据，存放在 Windows D 盘，通过 kind extraMounts 挂载到 pod。
3. Argo workflow 必须先检测本地数据完整，再启动 QC / normalize / features / ADS / ml-ready。
4. 默认本地 workflow 不再读远端 scale30，而是读 file:///mnt/local-data/robot-dh-local/raw。
5. scale30 仍保留为手动压测路径。

============================================================
一、核心目标
============================================================

新增或增强：

1. Local dataset URI support：
   file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1

2. Devscale dataset registry：
   识别 droid_lerobot_dev1g、robomimic_dev1g、bridgedata_v2_dev。

3. Local-first QC / ETL：
   qc contract / normalize / features / ads / ml-ready 都能基于 local file URI 跑。

4. High-performance local input：
   本地路径不走 S3 download，不复制大文件到 /tmp。
   尽量原地读取或轻量 materialize。

5. Robust failure handling：
   - 硬 timeout
   - 明确 retry
   - 明确 cause
   - archive log URI
   - heartbeat stale detection
   - skip / resume / force

6. Adapter 深化：
   体现对 DROID / LeRobot、robomimic、BridgeData 格式理解。

============================================================
二、新增模块 / 修改
============================================================

新增或修改：

src/robot_dh/local_runtime/
  __init__.py
  paths.py
  devscale.py
  preflight.py
  verification.py

src/robot_dh/adapters/
  __init__.py
  base.py
  droid_lerobot.py
  robomimic.py
  bridgedata.py
  registry.py

src/robot_dh/qc/
  droid.py       # 增强
  robomimic.py   # 增强
  bridge.py      # 增强

src/robot_dh/etl/
  normalize.py   # 增强 local path fast path
  features.py
  runner.py

src/robot_dh/lake/
  uri.py         # 增强 file URI / local path

configs/
  devscale_runtime.yaml
  dataset_adapters.yaml

tests/
  test_local_runtime_paths.py
  test_devscale_registry.py
  test_local_file_uri_etl.py
  test_adapter_droid_lerobot.py
  test_adapter_robomimic.py
  test_adapter_bridge.py
  test_local_qc_contract.py
  test_heartbeat_stale.py

============================================================
三、Local runtime path
============================================================

实现：

LocalRuntimeConfig:
  host_data_root
  k8s_data_root
  raw_root
  lake_root
  cache_root
  workdir_root

默认从 env 读取：
  ROBOT_DH_LOCAL_DATA_ROOT=/mnt/local-data/robot-dh-local
  ROBOT_DH_DEV_DATA_ROOT=file:///mnt/local-data/robot-dh-local/raw
  ROBOT_DH_DEV_LAKE_ROOT=file:///mnt/local-data/robot-dh-local/lake
  ROBOT_DH_INPUT_CACHE_DIR=/mnt/local-data/robot-dh-local/cache/input-cache

新增 CLI：

robot-dh local runtime doctor

检查：
  - local root exists
  - raw dirs exist
  - lake dirs writable
  - cache dir writable
  - devscale manifests exist
  - DROID / robomimic / Bridge dev dataset exist
  - 总大小 <= 3GB，除非 --allow-over-limit

新增：

robot-dh local datasets list
robot-dh local datasets verify

输出 JSON 和 human readable。

============================================================
四、Adapter Registry
============================================================

新增统一 Adapter 接口：

class RobotDatasetAdapter:
  family: str
  can_handle(uri) -> bool
  probe(uri) -> AssetProfile
  list_episodes(uri) -> list[EpisodeRef]
  normalize(uri, output_uri, options) -> NormalizeResult
  contract(uri, output_uri, options) -> ContractResult

实现：

DroidLeRobotAdapter:
  - 识别 data/*.parquet
  - 识别 meta/*.json / meta/*.jsonl
  - 识别 videos/**.mp4
  - 支持 LeRobot v2 结构
  - normalize 默认只读 data/meta，不默认拉全部 videos
  - QC 可选 decode 一小段视频

RobomimicAdapter:
  - 识别 *.hdf5
  - 支持 data/demo_* group
  - 支持 actions / obs / next_obs / rewards / dones
  - 支持多个 hdf5 文件
  - 本地 file URI 下直接 h5py 读取，不复制
  - S3 URI 下整文件 download_file 到 cache

BridgeDataAdapter:
  - 识别 parquet shard
  - 识别 language/action/state/image ref columns
  - 支持 lazy metadata probe，但必须有硬 timeout
  - 本地 file URI 下直接 pyarrow/pandas 读取

新增命令：

robot-dh adapter probe --dataset-uri ...
robot-dh adapter list
robot-dh adapter detect --dataset-uri ...

============================================================
五、Local file URI fast path
============================================================

当前 S3 dataset 会 materialize 到本地 tmp。
v1.7 要求：

如果 dataset_uri 是 file:// 或本地路径：
  - 不复制整个 dataset。
  - 直接把 local path 作为 input root。
  - checkpoint / manifest 写 output。
  - 只有需要修改/生成的文件写到 lake/cache。
  - 对 HDF5 不要复制到 tmp。
  - 对 Parquet 不要复制到 tmp。
  - 对视频 QC 只读取 metadata 或小段 decode。

实现：
  materialize_input(uri):
    if local:
      return MaterializedInput(path=local_path, mode="direct")
    if s3:
      use cache/download

日志必须明确：
  "using local direct input, no download"

============================================================
六、Bridge QC timeout / retry hardening
============================================================

Bridge Parquet lazy enrichment 曾出现 ContentLengthError / retry 退避拖很久。

要求：
1. BridgeDataAdapter / bridge QC 支持：
   --probe-timeout-sec 默认 120
   --max-retries 默认 2
   --disable-remote-lazy 默认 false
2. 对 S3 远端 parquet：
   - 尽量先 HEAD / size check
   - 小文件可以 download 到 cache 再读
   - 大文件 remote lazy 必须有超时
3. 如果超时，输出明确 cause：
   cause=REMOTE_PARQUET_TIMEOUT
4. 不允许一个 probe 卡 30 分钟。
5. Contract 报告中写 warning_rules。

============================================================
七、Robomimic 并发 QC
============================================================

robomimic QC 要支持：
  --max-workers 4
  --file-timeout-sec 300
  --fail-fast false

行为：
1. 每个 HDF5 文件独立任务。
2. 本地 file URI 直接读。
3. S3 URI 下载整文件到 cache 后读。
4. 每个文件输出：
   - file_uri
   - status
   - demo_count
   - duration
   - cause
5. 并发失败不能吞异常。
6. Contract report 汇总每个文件状态。

============================================================
八、DROID normalize dev path
============================================================

DROID / LeRobot dev 数据默认 <=1GB。

normalize 要：
1. 只读取 data/meta。
2. 视频只做 metadata probe，不进入 pose normalize。
3. 输出 ODS pose / episode_meta / video_meta。
4. 如果没有完整 videos，也可以 PASS/WARN，不 FAIL。
5. 对 meta / parquet 结构不一致要给明确 warning。
6. 语义上不要把视频当 trajectory 主输入。

============================================================
九、Heartbeat stale detection
============================================================

新增命令：

robot-dh runtime heartbeat check \
  --workflow-name <name> \
  --stale-after-sec 300

行为：
1. 查询 Postgres task_heartbeats 或本地 heartbeat JSONL。
2. 如果某 phase 超过 stale-after-sec 未更新，输出 WARN/FAIL。
3. 可用于 Argo step 或 watcher。
4. 输出 JSON。

============================================================
十、Archive log URL / workflow step enrich
============================================================

增强已有 argo sync / workflow step sync。

workflow_steps 增加字段如果 DB schema 支持：
  pod_name
  pod_uid
  node_id
  node_type
  template_name
  container_name
  container_state
  container_reason
  exit_code
  restart_count
  archive_log_uri
  archive_log_url
  retry_attempt
  started_at
  finished_at

如果 DB 表还没这些列：
  - 不失败
  - warning 提示 infra schema 需要升级

新增命令：

robot-dh argo logs index \
  --workflow-name <name> \
  --namespace robot-dh \
  --archive-root s3://robot-dh-artifacts/argo-logs

把 workflow step -> archive log URI 写入 PostgreSQL。

============================================================
十一、CLI 验收命令
============================================================

新增命令必须可用：

robot-dh local runtime doctor
robot-dh local datasets list
robot-dh local datasets verify

robot-dh adapter list
robot-dh adapter detect --dataset-uri file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1
robot-dh adapter probe --dataset-uri file:///mnt/local-data/robot-dh-local/raw/robomimic_dev1g/v1

robot-dh qc contract run \
  --dataset-family droid \
  --dataset-uri file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1 \
  --dataset-id droid_lerobot_dev1g \
  --version v1 \
  --output file:///mnt/local-data/robot-dh-local/lake/qc/droid_lerobot_dev1g/v1

robot-dh etl run \
  --dataset file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1 \
  --dataset-id droid_lerobot_dev1g \
  --version v1 \
  --lake-root file:///mnt/local-data/robot-dh-local/lake \
  --phase normalize \
  --resume

============================================================
十二、测试
============================================================

新增测试必须在无远端服务下通过。

测试重点：
1. file URI parse。
2. local direct input 不复制。
3. fake LeRobot adapter detect。
4. fake robomimic HDF5 adapter detect + QC。
5. fake Bridge parquet adapter detect。
6. Bridge timeout mock。
7. robomimic 并发 QC。
8. local runtime doctor。
9. heartbeat stale check。
10. archive log URL enrich 解析 mock Argo workflow JSON。

============================================================
十三、README 更新
============================================================

新增 v1.7 章节：

标题：
  v1.7 Local-First Robot Data Platform Runtime

内容：
1. 为什么本地 kind 不跑 30GB scale。
2. devscale <=3GB 策略。
3. D 盘数据路径。
4. local file URI 示例。
5. adapter registry。
6. 本地 QC / normalize / ml-ready 示例。
7. Bridge / robomimic / DROID 具体优化。
8. 容灾：
   - timeout
   - retry
   - checkpoint
   - heartbeat
   - archive log
   - local cache
9. 常见故障。

============================================================
十四、Makefile
============================================================

新增：

make local-runtime-doctor
make local-datasets-list
make local-datasets-verify
make local-adapter-probe
make local-qc-devscale
make local-etl-devscale
make local-ml-ready-devscale
make v1-7-local-smoke

v1-7-local-smoke：
  - local runtime doctor
  - datasets verify
  - adapter detect 三类数据
  - QC contract 三类数据
  - DROID normalize
  - build ADS
  - ml-ready export

============================================================
十五、验收命令
============================================================

用户手动执行：

kubectl config use-context kind-robot-dh-dev

make test
make local-runtime-doctor
make local-datasets-verify
make local-adapter-probe
make local-qc-devscale
make local-etl-devscale
make local-ml-ready-devscale

验收标准：
  - 不访问远端 S3 raw 也能跑本地 devscale QC。
  - DROID normalize 不下载 videos。
  - robomimic QC 本地直接读 HDF5。
  - Bridge QC 不会远端 lazy 卡死。
  - 产物写入 /mnt/local-data/robot-dh-local/lake。
  - 总数据 <=3GB。
  - 所有错误有明确 cause。

请开始实现。不要重写整个项目。不要引入 Kafka。不要做 Go Operator。不要留 TODO，不要写伪代码。