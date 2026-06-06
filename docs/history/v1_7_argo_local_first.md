# v1.7 Argo Local-First WorkflowTemplate

> 本文档面向「**用本地 kind 跑 Argo，3 GiB devscale 数据**」的场景。
> 远端 scale30 workflow（v1.6 `argo/templates/robot-dh-multisource-scale30-workflowtemplate.yaml`）保留，但**不再是默认入口**。

## 1. 为什么 v1.7 默认只跑 devscale

| 数据规模 | 单 family raw | 三 family 合计 | 本地 kind 表现                                  |
|---------|--------------|----------------|------------------------------------------------|
| scale30 | DROID 18 GiB / robomimic 6 GiB+ / Bridge 4 GiB | 30+ GiB     | Docker Desktop / WSL VHDX 一晚耗 8~12 小时；C 盘暴涨 |
| devscale| 1.2 GiB / 0.9 GiB / 0.4 GiB                    | <=3 GiB      | 全链路 30~60 分钟；不动 C 盘                       |

scale30 的痛点：

1. raw 数据每条都要从腾讯云 MinIO 经 WSL → Docker → kind 容器；
2. 视频文件多、单文件大，下载 IO 直接撞 VHDX；
3. 失败重试代价大：要么 retry 整 18 GiB，要么 partition 后还得重传 partition；
4. 调试 / 开发 / demo 完全不需要 30 GiB 数据。

devscale 把 raw 落在 Windows D 盘（绕开 C 盘 VHDX），通过 kind `extraMounts`
挂到容器，pod 直接读 `/mnt/local-data/robot-dh-local/raw/<dataset>/v1` —— 没有
公网传输、没有 VHDX 落地，整链路慢的只剩 CPU。

## 2. 为什么先同步到 D 盘

详细见 [`v1_7_windows_d_drive_kind_mount.md`](v1_7_windows_d_drive_kind_mount.md)，
要点：

- WSL ext4 VHDX 会无声膨胀到 100 GiB+，不归还磁盘空间；
- D 盘是 NTFS，Windows Explorer / 资源管理器可以直接看 / 删；
- D 盘 → kind 走 docker bind mount，**零拷贝、零下载**；
- pod 看到的依然是 POSIX 文件系统，pyarrow / h5py 完全无感。

## 3. 为什么 Argo 第一步必须 verify data

`local-runtime-doctor` 和 `verify-devscale-data` 是 **两个独立守门**：

1. `local-runtime-doctor`：检查 PVC 挂载是否成功、目录是否可写、`_manifest.json`
   是否存在、devscale 总大小是否 <= 3 GiB。
2. `verify-devscale-data`：和 `devscale_plan.json` 逐个对比文件大小（或 `_manifest.json`
   的 `files[]` 数组），缺一个 / 大小不对就 FAIL。

后续所有 step `depends` 这两步，**任意一步失败，下游全部 Skip**，pod 都不会启动。
这条规则一是保护数据完整性，二是避免 partial data 触发 `qc contract` 的
难以排查的 metric 偏差（参考 v1.6.7 ddbfb R1 bridge metric 静默回退）。

## 4. DAG 图

```text
local-runtime-doctor
   └── verify-devscale-data
         ├── adapter-probe-droid     ──> droid-qc     ──> droid-normalize     ──> droid-features
         ├── adapter-probe-robomimic ──> robomimic-qc ──> robomimic-normalize ──> robomimic-features
         └── adapter-probe-bridge    ──> bridge-qc    ──> bridge-normalize    ──> bridge-features

   (droid-features && robomimic-features && bridge-features)
      └── build-ads
            └── ml-ready-export
                  └── benchmark-regression
                        └── publish-lineage
                              └── argo-sync
                                    └── archive-logs-index
```

13 个节点：1 doctor + 1 verify + 3 probe + 3 qc + 3 normalize + 3 features +
1 ads + 1 ml-ready + 1 benchmark + 1 lineage + 1 argo-sync + 1 logs-index = **20 节点**。

## 5. Argo UI

```bash
kubectl -n argo port-forward svc/argo-server 2746:2746
```

默认浏览器打开 `https://localhost:2746`，左侧选 namespace `robot-dh`，能看到一棵
完整的 DAG 树，三 family 并行；点任意 step pod 看 `Logs`（如果是 PVC archive root
则在 `MAIN LOGS` 显示从 PVC 读到的归档；s3 archive root 显示 MinIO/S3 链接）。

## 6. 真·follow tail

```bash
WF=$(kubectl -n robot-dh get wf -l role=devscale-main -o jsonpath='{.items[-1:].metadata.name}')
./argo/v1_7_local/scripts/tail_live_workflow_logs.sh "$WF" --container main
```

脚本核心循环：

1. 每 3 秒 `kubectl get workflow $WF -o json` 一次；
2. 提取 `status.nodes[*]` 中 `type == "Pod"` 的所有节点；
3. 没 attach 过的 pod 立即 `kubectl logs -f --tail=-1 --container main <pod> &`；
4. 已 attach 的 pod 不重复 attach（用 sentinel 文件去重）；
5. 任意 pod `phase ∈ {Failed, Error}` 时立即 `kubectl describe pod` + `kubectl logs --previous`；
6. workflow 进入 `Succeeded / Failed / Error` 后回收所有后台 tail，echo 出每个 pod 的 archive log URI（按模板派生），干净退出。

## 7. archive logs 怎么看

devscale 默认 archive root：

```
file:///mnt/local-data/robot-dh-local/lake/argo-logs/<namespace>/<workflow>/<pod>/main.log
```

落在 PVC 内，所以从开发机 `ls /mnt/d/robot-dh-local/lake/argo-logs/` 直接可见。

`make argo-local-sync` 会调用 `robot-dh argo logs index` 把每个 step pod 的
archive_log_uri 写到 PG `workflow_steps.metrics` JSON 字段（PG schema 未升级时
warning 跳过，不抛）。

## 8. 怎么区分 devscale 和 scale30

| 维度                  | devscale (v1.7 default)        | scale30 (v1.6 manual stress test) |
|----------------------|--------------------------------|----------------------------------|
| WorkflowTemplate name | `robot-dh-local-devscale`      | `robot-dh-multisource-scale30`   |
| 提交脚本              | `make argo-local-submit`       | `make argo-submit-multisource-scale30` |
| 数据源 (raw)          | `file:///mnt/local-data/...`   | `s3://robot-datasets/raw/...`    |
| Lake (output)         | `file:///mnt/local-data/...`   | `s3://robot-lake`                |
| Volume                | PVC                            | emptyDir 32 GiB                  |
| activeDeadlineSeconds | 7200                           | 43200                            |
| 默认调用频率          | CronWorkflow 每天凌晨 2:30     | 手动，每月一次或回归压测时       |

判断 workflow 属于哪一类：

```bash
kubectl -n robot-dh get wf -L role,component
```

`role=devscale-main` 是 v1.7；`component=v1-6-argo` 是 v1.6 scale30。

## 9. 常见故障

### D 盘没挂进 kind

**现象**：debug pod ls `/mnt/local-data/robot-dh-local/raw` 看不到任何东西。

**根因**：`configs/kind-robot-dh-dev-local.yaml` 的 `extraMounts.hostPath` 没指向
`/mnt/d/robot-dh-local`，或者 Docker Desktop 没把 D 盘加入 Resources → File Sharing。

**修复**：

```bash
docker exec -it robot-dh-dev-control-plane ls -la /mnt/local-data/robot-dh-local
# 应该看到 raw/ lake/ cache/ manifests/ logs/
```

如果在 kind 节点里也是空，重建 kind：`make local-create-kind-dev --recreate`。

### PVC 为空

**现象**：`kubectl -n robot-dh exec robot-dh-local-debug -- ls /mnt/local-data/robot-dh-local/raw`
返回空目录，但宿主机 `ls /mnt/d/robot-dh-local/raw` 有数据。

**根因 1**：kind 节点和宿主机的 `extraMounts.containerPath` 不一致。

**根因 2**：PV 的 `hostPath.path` 写错了（必须是 kind 节点路径 `/mnt/local-data/robot-dh-local`，
**不是** WSL 路径 `/mnt/d/robot-dh-local`）。

**修复**：

```bash
kubectl get pv robot-dh-local-data-pv -o jsonpath='{.spec.hostPath.path}'
docker exec robot-dh-dev-control-plane ls /mnt/local-data/robot-dh-local/raw
```

两个路径必须一致。

### Pod Permission denied

**现象**：step pod 报 `mkdir -p failed for path /mnt/local-data/robot-dh-local/lake/...: Permission denied`。

**根因**：D 盘 raw 是从 WSL 写入的，owner 是当前 WSL 用户（uid=1000）；但 kind 节点
内的 uid 1000 不一定能写。

**修复**：在 WSL 上 `sudo chmod -R a+w /mnt/d/robot-dh-local/lake /mnt/d/robot-dh-local/cache`，
或者在 pod 里 `runAsUser` 改成宿主机 uid（不推荐，安全性会差）。

### benchmark 失败

**现象**：`benchmark-regression` step 报 `configs/benchmark_suite.yaml: not found` 或断言失败。

**根因**：benchmark suite 是 v1.5 留下的；如果 devscale 数据不包含相应 dataset，
对比会 FAIL。本步骤 `retryStrategy.limit=0`，重试也是失败。

**修复**：临时把 benchmark 删掉或 skip（修改 `templates/robot-dh-local-devscale-workflowtemplate.yaml`
的主 DAG，去掉 `benchmark-regression`），后续把 suite 改成 devscale 友好。

### droid normalize 缺 meta

**现象**：`droid-normalize` 报 `no meta/info.json found`。

**根因**：devscale plan 的 `include` 没把 `meta/info.json` 拉下来，或者文件被 `_manifest.json` 漏写。

**修复**：检查 `configs/devscale_datasets.yaml` 的 `droid_lerobot_dev1g.include` 必须含
`meta/**`，然后重跑 `make local-plan-devscale && make local-sync-devscale`。

### robomimic HDF5 结构不一致

**现象**：`robomimic-qc` 报 `episode_lens=[]` / `demo_count=0`。

**根因**：HDF5 文件里没有 `data/demo_X` group，可能是 raw 数据本身不规范，或
include glob 把 `low_dim*.hdf5` 之外的文件也拉进来了。

**修复**：`robot-dh adapter probe --dataset-uri file:///mnt/local-data/.../robomimic_dev1g/v1`
单独看哪些文件 readable，删掉异常文件后重跑。

### bridge parquet 读失败

**现象**：`bridge-qc` 报 `episode_count=0 → FAIL`。

**根因**：bridge raw 用 nested struct 存 episode_idx；老 probe 取不出来。本仓 v1.6.7
已经修过（`parquet_probe._fill_bridge_metrics`），但如果用户改过 parquet shape
就会回归。

**修复**：

```bash
robot-dh adapter probe --dataset-uri file:///mnt/local-data/robot-dh-local/raw/bridgedata_v2_dev/v1
```

看 `schema_summary.nested_columns` 是否含 `episode_idx` / `state.end_effector_pose`。

### C 盘空间上涨

**现象**：跑了一晚 workflow，C 盘空间下降几个 GiB。

**根因**：Docker Desktop 把镜像层和容器 overlayfs 落到 C 盘的 `docker-desktop` VHDX。
即使 raw 数据走 D 盘，镜像 / `/tmp` emptyDir 默认还是落 C 盘 VHDX。

**修复**：

1. Docker Desktop → Settings → Resources → Advanced → Disk image location
   改成 `D:\docker-data\DockerDesktop.vhdx`；重启 Docker。
2. emptyDir 不要给太大 `sizeLimit`（v1.7 templates 已经收敛到 256Mi ~ 2Gi）。
3. 定期 `docker system prune -a --volumes`。

## 10. 进一步阅读

- `argo/v1_7_local/README.md`：目录索引、提交 / tail / sync 命令速查。
- `docs/v1_7_local_data_runtime.md`：devscale / scale30 / full 三层数据策略。
- `docs/v1_7_windows_d_drive_kind_mount.md`：Windows D 盘 + kind extraMounts 详解。
- `docs/v1_6_argo_log_archive_handoff.md`：archiveLogs 怎么配 / 排查 0B archive log。
