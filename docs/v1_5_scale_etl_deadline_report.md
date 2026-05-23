# v1.5 scale30 ETL 超时失败报告

## 1. 结论

`robot-dh-scale30-etl-474k8` 本次运行最终失败，直接原因是 Argo / Kubernetes 的 deadline 到期：

- Workflow phase：`Failed`
- 失败节点：`run-shard-0`、`run-shard-1`
- Pod reason：`DeadlineExceeded`
- Pod message：`Pod was active on the node longer than the specified deadline`
- 主容器退出码：`143`，表示收到终止信号后退出
- 原模板超时：`activeDeadlineSeconds: 7200`，即 2 小时
- 运行时间：`2026-05-23T11:39:53Z` 到 `2026-05-23T13:40:02Z`，约 2 小时

本次未发现新的 PostgreSQL schema 漂移、dataset adapter 异常或 Python traceback。失败发生时两个有效 shard 都仍停留在 `normalize` 阶段，属于长耗时任务被平台 deadline 中止。

## 2. 影响范围

- `run-shard-2` 没有实际数据，正常 `SKIPPED`。
- `run-shard-0` 和 `run-shard-1` 被 deadline 终止，没有进入后续阶段。
- `merge-summary`、`build-ads`、`publish-event` 因依赖 shard 成功而被 Argo 标记为 `Omitted`。
- 对应 ODS / DWD / ADS 产物未完整生成。
- 监控侧如果只看数据库 `etl_shards`，被杀前可能短暂保留 `RUNNING` 状态；排查时已手工标记失败，避免误导后续监控。

## 3. 证据链

Workflow 节点状态：

```bash
kubectl get wf -n robot-dh robot-dh-scale30-etl-474k8 -o json \
  | jq -r '.status.nodes | to_entries[] | select(.value.phase != "Succeeded" and .value.phase != "Skipped") | [.key,.value.displayName,.value.type,.value.phase,(.value.message // ""),(.value.startedAt // ""),(.value.finishedAt // "")] | @tsv'
```

关键输出：

```text
robot-dh-scale30-etl-474k8-3644284294  run-shard-0  Pod  Failed  Pod was active on the node longer than the specified deadline  2026-05-23T11:40:13Z  2026-05-23T13:39:53Z
robot-dh-scale30-etl-474k8-3661061913  run-shard-1  Pod  Failed  Pod was active on the node longer than the specified deadline  2026-05-23T11:40:13Z  2026-05-23T13:39:54Z
```

Pod describe 关键信息：

```text
Status:  Failed
Reason:  DeadlineExceeded
Message: Pod was active on the node longer than the specified deadline
State:   Terminated
Reason:  Error
Exit Code: 143
```

`run-shard-0` 最后一段应用日志：

```text
etl_run START: job_id=plan-20260523T114003-513c7d73::shard-000::droid_lerobot_scale30-v1 dataset_uri=s3://robot-datasets/raw/droid_lerobot_scale30/v1/ -> droid_lerobot_scale30/v1
[normalize] s3://robot-datasets/raw/droid_lerobot_scale30/v1/ -> s3://robot-lake/ods/droid_lerobot_scale30/v1
normalize: job_id=plan-20260523T114003-513c7d73::shard-000::droid_lerobot_scale30-v1::normalize dataset_uri=s3://robot-datasets/raw/droid_lerobot_scale30/v1/ output_uri=s3://robot-lake/ods/droid_lerobot_scale30/v1
Error: exit status 143
```

`run-shard-1` 最后一段应用日志：

```text
etl_run START: job_id=plan-20260523T114003-513c7d73::shard-001::robomimic_scale30-v1 dataset_uri=s3://robot-datasets/raw/robomimic_scale30/v1/ -> robomimic_scale30/v1
[normalize] s3://robot-datasets/raw/robomimic_scale30/v1/ -> s3://robot-lake/ods/robomimic_scale30/v1
normalize: job_id=plan-20260523T114003-513c7d73::shard-001::robomimic_scale30-v1::normalize dataset_uri=s3://robot-datasets/raw/robomimic_scale30/v1/ output_uri=s3://robot-lake/ods/robomimic_scale30/v1
Error: exit status 143
```

## 4. 根因判断

直接根因是 `robot-dh-scale-etl` 的 `activeDeadlineSeconds` 原来只有 `7200` 秒。两个实际数据 shard 的 `normalize` 阶段在 2 小时内未结束，Kubernetes kubelet 按 Argo 注入的 deadline 停止了容器。

当前证据只能证明瓶颈发生在 `normalize` 阶段，尚不能精确归因到 CPU、内存、S3 下载、S3 上传或 Parquet/Arrow 转换。原因是本次日志只有阶段开始日志，没有 normalize 内部按数据块输出的进度、吞吐、行数、对象数和资源采样。

## 5. 已采取修复

- 将 `argo/templates/robot-dh-scale-etl-workflowtemplate.yaml` 的 `activeDeadlineSeconds` 调整为 `43200` 秒，即 12 小时。
- 将 `argo/scripts/argo_wait_workflow.sh` 默认 `TIMEOUT` 调整为 `43200` 秒，避免本地等待脚本先于 Workflow deadline 退出。
- README 补充 `tmux` 长任务运行、监控、日志、失败归因命令。

## 6. 后续优化方向

优先级一：补足 normalize 内部观测点。

- 在 normalize 读 raw、转换、写 ODS 的关键循环输出结构化进度日志，包括 dataset、文件数、已处理字节数、已处理行数、当前阶段耗时、平均 MB/s。
- 把 `EtlProfiler` 的下载、计算、上传计时细化到 normalize 子阶段，避免只看到一个总耗时。
- 给每个 dataset 写中间 heartbeat，定期更新 `etl_shards` 或 runtime event，避免长时间只有 `RUNNING` 而没有吞吐信息。

优先级二：降低单 shard 的大对象风险。

- 当前 `target_shard_size_gb: 5` 只是 planner 的目标值，真实 shard 仍可能被单个大 dataset 主导。需要确认 `droid_lerobot_scale30` 与 `robomimic_scale30` 是否各自超过目标分片粒度。
- 如果单 dataset 过大，考虑支持 dataset 内部分片，例如按 episode、文件前缀或 parquet group 切分，而不是只按 dataset 粒度切 shard。
- 将 `max_shards` 从提交样板里的 3 提高，并把实际有效 shard 数与数据大小绑定，减少两个大 shard 并行拖尾。

优先级三：验证资源瓶颈。

- 运行期间用 `kubectl top pod -n robot-dh` 观察 CPU / memory 是否长期打满。
- 如果 CPU 打满，提高 `resources.limits.cpu` 或降低 `max_workers` 避免 Python/Arrow 过度竞争。
- 如果内存接近 4Gi，优先改 streaming / batch size，再考虑提高 memory limit。
- 如果 CPU 与内存都不高，重点看 S3 带宽、对象数量、远端存储延迟和上传吞吐。

优先级四：减少重复计算和失败重跑成本。

- normalize 输出按 dataset / episode 幂等落盘，重跑时识别已完成 manifest，避免 deadline 后从头开始。
- shard summary 增加 partial progress，失败后可以只补跑未完成 dataset。
- 对长耗时 dataset 单独提供 `robot-dh etl run --dataset ... --phase normalize` 级别复现命令，便于单点 profiling。

## 7. 推荐复跑方式

长任务建议在 `tmux` 中运行，避免 IDE 会话中断影响观察：

```bash
tmux new -s robot-dh-scale-etl
cd /home/yunlong/workspace/robot-data-harness

make docker-build
make kind-load
make argo-apply-templates

wf=$(kubectl -n robot-dh create -f argo/workflows/submit-scale30-etl.yaml -o jsonpath='{.metadata.name}')
echo "workflow=${wf}"

TIMEOUT=43200 ./argo/scripts/argo_wait_workflow.sh "${wf}"
```

另开窗口持续看 Pod：

```bash
tmux new -s robot-dh-scale-watch
wf="robot-dh-scale30-etl-xxxxx"  # 替换为上一步输出的 workflow 名称
kubectl -n robot-dh get pods -l workflows.argoproj.io/workflow="${wf}" -w
```

查看失败节点：

```bash
kubectl get wf -n robot-dh "${wf}" -o json \
  | jq -r '.status.nodes | to_entries[] | select(.value.phase != "Succeeded" and .value.phase != "Skipped") | [.value.displayName,.value.type,.value.phase,(.value.message // ""),(.value.startedAt // ""),(.value.finishedAt // "")] | @tsv'
```

查看 shard 日志：

```bash
kubectl -n robot-dh logs -f <pod-name> -c main
```
