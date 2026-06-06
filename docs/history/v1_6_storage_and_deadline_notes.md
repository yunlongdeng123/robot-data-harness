# robot-dh-infra v1.6 存储与 deadline 背景

本文档记录 v1.5 期间 Argo scale30 ETL 因 `activeDeadlineSeconds=7200` 失败的现场细节，并解释 v1.6 元数据 schema（heartbeat / partition / step / workflow / progress）为何被设计成现在这样。同时再次说明：**v1.6 不自动迁移 `/dev/vdb`**。

## 1. v1.5 deadline failure 背景

### 1.1 现象

- Argo `scale30` ETL 跑到第 ~2h 被 controller 杀掉
- workflow level 配置 `activeDeadlineSeconds=7200`
- `run-shard-0` / `run-shard-1` 停在 `normalize` 阶段
- 没有 Python traceback
- 没有 schema drift 提示
- 没有 adapter 层明确异常
- 主项目侧只在 `etl_perf_runs` 留下一条 `status='running'` 的孤儿记录；shard 终态没写

### 1.2 根因方向

由于 v1.5 没有以下可观测性，根因目前只能基于推测：

- 没有 normalize 内部进度（哪一个 episode / frame 卡住）
- 没有 worker 心跳（worker 是否还在跑还是已被 OOM/kill 但 controller 没收到信号）
- 没有 partition 级别 metadata（如果中途死掉，不能按 partition resume，只能整 dataset 重跑）
- 没有 step 级别状态时序（哪一阶段慢、哪一阶段卡）
- runtime_events 是项目自定义 event_type，没有标准的 START / COMPLETE / FAIL 切换

经验上常见原因：
1. 单个 episode 视频解码慢 + worker 没设 per-episode timeout
2. parquet → in-memory dataframe 时 column / batch 过大，触发 swap
3. 容器层面 GC pause（HDF5 / numpy mmap 大文件）
4. MinIO 网络抖动；S3 client 没设 `read_timeout`，无限阻塞

无论真因是哪一种，**没有运行时的细粒度状态 + 心跳 + 进度，就不可能定位**。

## 2. v1.6 为什么这么设计

| 设计点 | 解决什么问题 |
|--------|--------------|
| `workflow_steps(phase, dataset_id, version)` | 让 `38_workflow_metadata_report.sh` 能看出 "哪一类数据 / 哪一个 step 卡得最多" |
| `task_heartbeats(task_id, progress_*, updated_at)` | 长任务每 10–60s 发心跳，监控可知道是 "活着但慢" 还是 "已死" |
| `dataset_partitions(partition_type, partition_index, status)` | 让 normalize 可以按 partition 续跑；不再整 dataset 重来 |
| `qc_contract_runs(status, failed_rules_json)` | 让 normalize 之后的 QC 失败原因机器可读，不再靠拷贝日志 |
| `asset_profiles(rows, bytes, schema_hash, null_rate)` | 让 schema drift / 大对象 / 高空率自动落库；deadline 之前能预警 |
| `openlineage_events(event_type, event_time, run_id)` | 标准化 START / COMPLETE / FAIL 序列；任何 OpenLineage 兼容工具都能消费 |
| `ml_ready_datasets(output_uri, quality_threshold, num_train/val/test)` | 训练侧消费的数据集元信息一处可查 |

> 也就是说，v1.6 不是 "新增 9 张表"，而是把 v1.5 deadline 失败时 "没有任何运行时证据可看" 这件事，从结构上变成 "Postgres 一查就有"。

## 3. 客户端 / worker 侧的最小改造

v1.6 仅在 `robot-dh-infra` 侧建表 + 授权。要让上述表真正发挥作用，主项目 `robot-data-harness` 需要：

1. **worker 写心跳**：normalize / feature / contract / benchmark worker 每 30s INSERT 一行 `task_heartbeats`，至少包含 `task_id / phase / progress_current / progress_total / progress_unit`
2. **plan 阶段写 partitions**：ETL plan 阶段先按 episode 划分写 `dataset_partitions`，worker 按 partition 取活
3. **Argo sync 写 workflow_runs / workflow_steps**：定时 / 事件触发同步 Argo API → Postgres
4. **QC runner 写 qc_contract_runs**：每次跑完写一条；`failed_rules_json` 提供 actionable 的失败原因
5. **OpenLineage emitter**：主项目 ETL 在 START / COMPLETE / FAIL 三个边界发 OpenLineage 事件

这些写入逻辑由主项目实现；本仓库只负责 schema + 验收。

## 4. /dev/vdb 与 root filesystem

### 4.1 当前状态

- `/dev/vdb` 仍存在但**未挂载**
- `/data/robot-dh` 仍在 root filesystem (`/dev/vda2`)
- v1.5 `25_storage_pressure_report.sh` / `26_plan_vdb_migration.sh` 仍是仅有的迁移入口

### 4.2 v1.6 不做什么

v1.6 **不会**：

- 自动 `mkfs.ext4 /dev/vdb`
- 自动 `mount /dev/vdb`
- 自动写 `/etc/fstab`
- 自动 `rsync /data/robot-dh -> /mnt/data`

v1.6 任何脚本（35–41）执行后，磁盘布局、挂载点、文件系统**都不会被修改**。

### 4.3 root filesystem 空间风险

| 维度 | 数据 |
|------|------|
| root filesystem 容量 | ~118 GiB |
| 已用 | ~63 GiB（含 scale30 24.3 GiB + MinIO 27 GiB + Postgres / 系统） |
| 当前 ETL "理论上" 还能跑的 30GB 级 ETL | ≈ 1 次（按 3× 头roomspace 估算需要 ≈ 90 GiB） |

`./scripts/25_storage_pressure_report.sh` 在 `root_filesystem.avail_bytes < 30 GiB` 时会输出 `WARNING`。v1.6 验收流程**强烈建议**先跑 25 号脚本再跑 37 号脚本，以同时拿到 "硬件维度的空间紧张" 与 "数据库维度的元数据增长"。

### 4.4 迁移到 /dev/vdb 的人工流程（v1.6 之外）

留作未来手动执行，本仓库永远不会自动跑：

```bash
# 1. 计划
./scripts/26_plan_vdb_migration.sh

# 2. 人工确认无误后：
#    - mkfs.ext4 /dev/vdb（必须有应急回滚预案）
#    - mount /dev/vdb /mnt/data
#    - 停 docker-compose
#    - rsync -aHAX /data/robot-dh/ /mnt/data/robot-dh/
#    - 调整 /etc/fstab
#    - 重启容器并跑 06 / 37 验证
```

> 26 号脚本只生成迁移命令草案；它不执行任何破坏性命令，也不修改 `/etc/fstab`。

## 5. tmp 清理与 lifecycle 分工

| 入口 | 用途 |
|------|------|
| `28_minio_lifecycle_plan.sh --apply` | 写 ILM 规则（lifecycle policy）：`robot-lake/tmp/` 与 `robot-dh-artifacts/tmp/` 7d expire |
| `40_storage_tmp_lifecycle_audit.sh --apply-cleanup` | 即时执行 `mc rm --older-than` 一次性清理 |

二者并存，分工：

- 28 号是稳态规则，部署一次后 MinIO 自己执行
- 40 号是按需诊断 / 强清；老对象的 mtime 不被 ILM 抓住时（例如 versioning 残留）可以临时介入
- 两者都**严格只允许动 tmp/**；任何对 `raw / ods / dwd / ads / lineage / manifests / runs` 的引用都会被立即拒绝

## 6. v1.6 之后的下一步建议（不属于本次交付）

- 主项目接入心跳 / partition / step / OpenLineage emitter
- Argo workflow 把 `activeDeadlineSeconds` 拆到 step 级别，避免 7200s 一刀切
- 增加 `task_heartbeats` 30 天清理 cron
- 把 `dataset_partitions.status` 在 normalize 失败时设置为 `failed`，并由 resume worker 选取 `pending / failed` 重跑
- Go exporter 把 `qc_contract_runs` / `workflow_steps` / `task_heartbeats` 暴露给 Prometheus
- 评估把 `/dev/vdb` 挂到 `/mnt/data/robot-dh/postgres` 与 `/mnt/data/robot-dh/minio`，减少 root filesystem 压力
