你是资深机器人数据平台工程师、Python 数据工程师、Kubernetes 批处理工程师。当前仓库是 robot-data-harness，已经完成 v1.4：

- robot-dh CLI
- PostgreSQL registry / lake metadata
- MinIO S3 artifact store
- Redis connectivity doctor
- raw / ods / dwd / ads 数据湖分层
- normalize
- build-features
- build-ads
- etl run
- etl scan
- FastAPI lake 查询
- K8s Job / CronJob
- Dockerfile
- pytest

远端 infra 当前已有：
- PostgreSQL / MinIO / Redis
- robot-datasets bucket
- robot-dh-artifacts bucket
- robot-lake bucket
- v1.4 lake metadata schema
- v1.5 将新增 scale benchmark schema
- 30GB 级 scale 数据资产已经在 robot-datasets/raw 下：
    droid_lerobot_scale30
    robomimic_scale30
    bridgedata_v2_scale30

本次目标是升级到 v1.5：

  Scale Benchmark + Sharded ETL + Runtime Profiling + Failure Benchmark

不要重写 v1.4，不要破坏已有命令。所有新增功能必须向后兼容。Argo Workflow 相关 YAML 由下一阶段 Prompt C 实现，本 Prompt 只实现 CLI / Python 核心能力，为 Argo 提供可调用命令。

============================================================
一、v1.5 功能目标
============================================================

新增能力：

1. ETL performance profiler
   记录 normalize / build-features / build-ads / etl scan 每一步：
     - input_bytes
     - output_bytes
     - input_rows
     - output_rows
     - duration_sec
     - download_duration_sec
     - upload_duration_sec
     - compute_duration_sec
     - peak_memory_mb
     - status
     - metrics_json

2. Sharded ETL
   新增：
     robot-dh etl plan
     robot-dh etl run-shard
     robot-dh etl merge-summary

3. Scale benchmark
   新增：
     robot-dh mutate
     robot-dh benchmark run
     robot-dh benchmark report

4. Runtime event logging
   每个关键动作写入 runtime_events 表或 JSONL。

5. Secure batch runtime support
   CLI 支持：
     --work-dir
     --tmp-dir
     --max-workers
     --fail-fast
     --log-format json

6. PostgreSQL v1.5 表写入
   写入：
     etl_perf_runs
     etl_shards
     benchmark_runs
     benchmark_cases
     runtime_events

如果远端 v1.5 表不存在，给出清晰错误，提示先在 infra 项目执行：
  ./scripts/29_pg_apply_v1_5_schema.sh

============================================================
二、新增模块结构
============================================================

新增或修改：

src/robot_dh/perf/
  __init__.py
  profiler.py
  memory.py
  io_stats.py
  writer.py

src/robot_dh/sharding/
  __init__.py
  planner.py
  shard_runner.py
  merge.py
  models.py

src/robot_dh/benchmark/
  __init__.py
  mutations.py
  suite.py
  runner.py
  report.py
  models.py

src/robot_dh/runtime/
  __init__.py
  events.py
  ids.py
  jsonlog.py

src/robot_dh/warehouse/
  service.py   # 扩展 v1.5 写入方法
  models.py    # 扩展 v1.5 模型或 SQL helpers

configs/
  benchmark_suite.yaml
  scale30_etl.yaml
  runtime_default.yaml

tests/
  test_perf_profiler.py
  test_shard_planner.py
  test_run_shard_local.py
  test_merge_summary.py
  test_mutations.py
  test_benchmark_runner.py
  test_runtime_events.py
  test_scale30_discovery_optional.py
  test_postgres_v1_5_optional.py

============================================================
三、Performance Profiler
============================================================

实现 EtlProfiler。

要求：
1. 可作为 context manager 使用。
2. 记录 wall-clock duration。
3. 尽量记录 peak memory。
   - 可用 psutil，如果不想增加依赖，也可以用 resource / tracemalloc。
   - 推荐增加 psutil 依赖。
4. 记录 S3 download / upload 时间。
5. 记录 input / output 文件大小。
6. 记录 parquet row count。
7. 输出 PerfRecord dataclass。
8. 写入本地 JSON：
   _perf.json
9. 如果 DB 可用，写入 etl_perf_runs 表。
10. 如果 DB 表不存在，不影响本地模式，但要 warning。

PerfRecord 字段：
  job_id
  run_id
  dataset_id
  version
  phase
  input_uri
  output_uri
  input_bytes
  output_bytes
  input_rows
  output_rows
  duration_sec
  download_duration_sec
  upload_duration_sec
  compute_duration_sec
  peak_memory_mb
  worker_id
  status
  error_message
  metrics

集成点：
  normalize
  build-features
  build-ads
  etl run
  etl scan

============================================================
四、Sharded ETL
============================================================

新增命令：

robot-dh etl plan \
  --root s3://robot-datasets/raw \
  --lake-root s3://robot-lake \
  --output runs/plans/scale30_plan.json \
  --target-shard-size-gb 5 \
  --max-shards 16

行为：
1. 发现含 endpose.pt、parquet、hdf5、mp4 或 meta 的 dataset prefix。
2. 支持 dataset pattern：
   --include "*scale30*"
   --exclude "*sample*"
3. 估算每个 dataset 的 input bytes。
4. 按 target-shard-size-gb 将 dataset 分配到多个 shard。
5. 输出 plan JSON。

plan JSON schema：
  plan_id
  created_at
  root_uri
  lake_root
  target_shard_size_bytes
  total_datasets
  total_bytes
  shards:
    - shard_id
      datasets:
        - dataset_id
          version
          dataset_uri
          input_bytes
      total_bytes
      status

新增命令：

robot-dh etl run-shard \
  --plan runs/plans/scale30_plan.json \
  --shard-id 0 \
  --lake-root s3://robot-lake \
  --output runs/shards/plan_x/shard_0 \
  --max-workers 2

行为：
1. 读取 plan。
2. 只执行指定 shard。
3. 对 shard 中每个 dataset 执行 etl run。
4. 支持 max-workers 控制并发。
5. 每个 dataset 失败不一定导致整个 shard 失败，除非 --fail-fast。
6. 输出 shard_summary.json。
7. 写入 etl_shards。
8. 写入 etl_perf_runs。
9. 写入 runtime_events。

新增命令：

robot-dh etl merge-summary \
  --plan runs/plans/scale30_plan.json \
  --shard-results runs/shards/plan_x \
  --output runs/plans/scale30_summary.json

行为：
1. 汇总所有 shard_summary.json。
2. 输出：
   total
   succeeded
   failed
   skipped
   duration
   failed_datasets
   per_shard_stats
3. 写入 runtime_events。
4. 可选写入 PostgreSQL。

============================================================
五、Scale30 discovery
============================================================

增强 robot-dh lake list / etl scan。

要求支持：

robot-dh lake list \
  --layer raw \
  --include "*scale30*" \
  --output json

robot-dh etl scan \
  --root s3://robot-datasets/raw \
  --lake-root s3://robot-lake \
  --include "*scale30*" \
  --limit 100

当前 scale30 raw prefix 可能是：
  s3://robot-datasets/raw/droid_lerobot_scale30/v1
  s3://robot-datasets/raw/robomimic_scale30/v1
  s3://robot-datasets/raw/bridgedata_v2_scale30/v1

不要硬编码，只允许在测试或 README 中举例。

============================================================
六、Benchmark / Mutation
============================================================

新增命令：

robot-dh mutate \
  --dataset samples/button_press_001 \
  --output samples/button_press_bad_velocity \
  --mutation velocity_spike

支持 mutation：
  velocity_spike
  quat_drift
  missing_press
  xy_cluster_collapse
  timestamp_jitter
  schema_missing_column
  nan_injection
  video_missing

要求：
1. 对本地 demo dataset 必须支持。
2. 对 S3 dataset 可选支持，若实现成本高，先下载到临时目录再上传。
3. 每个 mutation 输出 meta.yaml 中记录 mutation_type。
4. 不覆盖原始 dataset。

新增 configs/benchmark_suite.yaml：

suite_name: robot_data_quality_v1_5
cases:
  - case_id: clean_demo
    dataset: samples/button_press_001
    expected_status: PASS
  - case_id: velocity_spike
    source_dataset: samples/button_press_001
    mutation: velocity_spike
    expected_status: FAIL
    expected_failed_validators:
      - velocity_jump
  - case_id: quat_drift
    source_dataset: samples/button_press_001
    mutation: quat_drift
    expected_status: FAIL
    expected_failed_validators:
      - quaternion
  - case_id: missing_press
    source_dataset: samples/button_press_001
    mutation: missing_press
    expected_status: FAIL
    expected_failed_validators:
      - press_event

新增命令：

robot-dh benchmark run \
  --suite configs/benchmark_suite.yaml \
  --output runs/benchmark/v1_5 \
  --record-to-registry

行为：
1. 读取 suite。
2. 对需要 mutation 的 case 先生成 mutated dataset。
3. 执行 validate / gate。
4. 比较 expected_status 和 actual_status。
5. 比较 expected_failed_validators 和 actual_failed_validators。
6. 输出 benchmark_report.json 和 benchmark_report.html。
7. 写入 benchmark_runs 和 benchmark_cases。
8. 进程 exit code：
   - 全部 case 通过预期，exit 0
   - 任一 case 不符合预期，exit 1

新增命令：

robot-dh benchmark report \
  --benchmark-dir runs/benchmark/v1_5

生成 Markdown / HTML 汇总。

============================================================
七、Runtime events
============================================================

新增 runtime event 统一记录：

事件类型：
  etl_plan_created
  etl_shard_started
  etl_shard_finished
  dataset_etl_started
  dataset_etl_finished
  benchmark_started
  benchmark_case_finished
  benchmark_finished
  argo_workflow_submitted
  argo_workflow_finished

输出：
1. 本地 JSONL：
   runs/events/runtime_events_YYYYmmdd.jsonl
2. 如果 DB 可用，写入 runtime_events 表。
3. 每条事件必须有 event_id、event_type、created_at、payload_json。

============================================================
八、CLI 兼容与入口
============================================================

确保 robot-dh --help 能看到：

robot-dh etl plan
robot-dh etl run-shard
robot-dh etl merge-summary
robot-dh mutate
robot-dh benchmark run
robot-dh benchmark report

所有命令支持：
  --log-format human|json

ETL 相关支持：
  --max-workers
  --work-dir
  --tmp-dir
  --fail-fast

============================================================
九、FastAPI 增强
============================================================

新增只读接口：

GET /etl/perf
  filters:
    dataset_id
    version
    phase
    status

GET /etl/shards
  filters:
    plan_id
    status

GET /benchmark/runs
GET /benchmark/runs/{benchmark_id}
GET /events
  filters:
    event_type
    run_id
    job_id

这些接口从 PostgreSQL 读取。
DB 不可用返回 503。

不要在 API 中执行 benchmark 或大 ETL。

============================================================
十、Dockerfile / 依赖
============================================================

新增依赖：
  psutil
  beautifulsoup4 可选，用于 HTML benchmark report 不强制
  jinja2 如果已有则复用

确保 Docker image 能运行：
  robot-dh etl plan
  robot-dh etl run-shard
  robot-dh benchmark run

============================================================
十一、测试
============================================================

要求：
1. 无远端服务时 make test 通过。
2. 本地 synthetic demo 可以完整跑 benchmark。
3. mutate 生成的数据能被 validate 识别出异常。
4. shard planner 对 fake dataset list 能生成合理 shards。
5. run-shard 可在本地 file:// 模式跑通。
6. PerfRecord 可以写入本地 JSON。
7. PostgreSQL v1.5 optional test 通过 env 开关控制。

新增测试：
  tests/test_perf_profiler.py
  tests/test_shard_planner.py
  tests/test_run_shard_local.py
  tests/test_merge_summary.py
  tests/test_mutations.py
  tests/test_benchmark_runner.py
  tests/test_runtime_events.py
  tests/test_postgres_v1_5_optional.py

============================================================
十二、README 更新
============================================================

README 新增 v1.5 章节：

1. v1.5 定位：
   Scale Benchmark + Sharded ETL + Runtime Profiling。

2. 本地 benchmark：
   make demo-data
   robot-dh benchmark run --suite configs/benchmark_suite.yaml --output runs/benchmark/v1_5

3. scale30 ETL plan：
   source client/robot-dh-v1-5.env
   robot-dh etl plan --root s3://robot-datasets/raw --lake-root s3://robot-lake --include "*scale30*" --output runs/plans/scale30_plan.json

4. run shard：
   robot-dh etl run-shard --plan runs/plans/scale30_plan.json --shard-id 0 --lake-root s3://robot-lake

5. merge summary：
   robot-dh etl merge-summary ...

6. performance metrics 解释：
   input_bytes
   output_bytes
   duration_sec
   peak_memory_mb
   download_duration_sec
   upload_duration_sec

7. PostgreSQL 表：
   etl_perf_runs
   etl_shards
   benchmark_runs
   benchmark_cases
   runtime_events

8. 常见故障：
   v1.5 schema missing
   S3 endpoint 失败
   scale30 prefix 发现不到
   shard 某些 dataset 失败
   pyarrow OOM
   benchmark expected_failed_validators 不匹配

============================================================
十三、Makefile
============================================================

新增 target：

make benchmark-local
make etl-plan-scale30
make etl-run-shard-0
make etl-merge-scale30
make perf-query
make v1-5-smoke

要求不写真实 secret。
远端相关命令假设用户已经 source client/robot-dh-v1-5.env。

============================================================
十四、验收命令
============================================================

本地：

make test
make demo-data
make benchmark-local

远端：

source client/robot-dh-v1-5.env

robot-dh infra doctor
robot-dh lake audit

robot-dh etl plan \
  --root s3://robot-datasets/raw \
  --lake-root s3://robot-lake \
  --include "*scale30*" \
  --target-shard-size-gb 5 \
  --output runs/plans/scale30_plan.json

robot-dh etl run-shard \
  --plan runs/plans/scale30_plan.json \
  --shard-id 0 \
  --lake-root s3://robot-lake \
  --output runs/shards/scale30/shard_0 \
  --max-workers 2

robot-dh etl merge-summary \
  --plan runs/plans/scale30_plan.json \
  --shard-results runs/shards/scale30 \
  --output runs/plans/scale30_summary.json

robot-dh benchmark run \
  --suite configs/benchmark_suite.yaml \
  --output runs/benchmark/v1_5 \
  --record-to-registry

请开始实现。代码必须模块化、类型清晰、错误信息明确。不要留 TODO，不要写伪代码。