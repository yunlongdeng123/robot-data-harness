# v1.7 Local-First Data Runtime

> 关键词：devscale / scale30 / full 三层数据策略；本地 Argo 默认只跑 devscale。

## 1. 背景

v1.6 多源 scale30 Argo workflow（DROID / robomimic / BridgeData V2）单次需要拉
~25 GiB 原始 parquet/hdf5/mp4 数据。在 Windows + WSL + Docker Desktop + kind 这条本地链路上：

- raw 数据从腾讯云 MinIO 经公网拉到 kind node 的 emptyDir；
- emptyDir 底层是 docker volume，对 Docker Desktop 来说会落到 **C 盘的 WSL VHDX**；
- 单次 DROID / LeRobot scale30 ~18 GiB，robomimic_scale30 ~6 GiB，叠加 normalize 中间产物
  会**几小时内把 C 盘 / WSL VHDX 吃光**；
- 跨公网拉取速度受限于运营商出口带宽，对 v1.6 `download_dir` 心跳与 `ContentLengthError`
  容错都是真实压测，但**不适合每天 dev 迭代**。

v1.7 的解法：

1. 把 dev 级数据（**总量 ≤ 3 GB**）一次性同步到 Windows D 盘的 `D:\robot-dh-local`；
2. kind 用 `extraMounts` 把 D 盘目录挂到 node 内 `/mnt/local-data/robot-dh-local`；
3. Argo workflow 默认 `dataset_uri=file:///mnt/local-data/robot-dh-local/raw/<dataset_id>/<version>`，
   走本地文件，不再跨公网；
4. scale30 远端 workflow 保留为**手动压测路径**，不进入默认。

## 2. 三层数据策略

| 层 | 体量 | 路径 | 触发方式 |
|---|---|---|---|
| **devscale** | ≤ 3 GB | `D:\robot-dh-local\raw\<dataset_id>\v1` | 本地 kind `robot-dh-dev`，`make local-*` 一条龙；默认入口 |
| **scale30** | ~25 GiB / 一次 | `s3://robot-datasets/raw/<dataset>_scale30/v1` | 手动 `make argo-submit-multisource-scale30`（跑 8–12h，建议远端 K8s 或夜间压测） |
| **full** | TB 级 | 同上 / 公司数据湖 | 不在本仓库 scope；下游研究脚本独立处理 |

> v1.7 **不**改变 scale30 的存储位置、不重写 v1.6 WorkflowTemplate，只是在
> Argo 入参层加一条 file:// 优先级。

## 3. devscale 数据清单

见 `configs/devscale_datasets.yaml`：

| dataset_id | family | source_uri | max_bytes | 主要 include |
|---|---|---|---:|---|
| `droid_lerobot_dev1g` | droid | `s3://robot-datasets/raw/droid_lerobot_scale30/v1` | 1.20 GB | `meta/**` + 单 parquet shard + 单一视图 mp4 |
| `robomimic_dev1g` | robomimic | `s3://robot-datasets/raw/robomimic_scale30/v1` | 0.90 GB | 前 3 个 `low_dim*.hdf5` |
| `bridgedata_v2_dev` | bridge | `s3://robot-datasets/raw/bridgedata_v2_scale30/v1` | 0.40 GB | 前 10 个 `data/*.parquet` + meta + README |

**total_max_bytes = 3 GB** 由 `local_plan_devscale_sync.sh` 严格执行：超过即拒绝下载，除非 `--allow-over-limit`。

## 4. 同步流程

```bash
source client/robot-dh-v1-6.env        # 复用 v1.6 平台 secret 里的 S3 AK/SK
make local-preflight                   # 检查 /mnt/d、df、工具链
make local-init-data                   # 在 D:\robot-dh-local 下创建目录
make local-mc-alias                    # mc alias set robotdh-remote ...
make local-plan-devscale               # 生成 devscale_plan.json
make local-sync-devscale               # mc cp 并发下载；自动调 verify
make local-verify-devscale             # 也可手动复跑
```

产物：

```
D:\robot-dh-local\
  raw\
    droid_lerobot_dev1g\v1\
      _manifest.json
      meta\info.json
      data\chunk-000\file-000.parquet
      videos\observation.images.exterior_1_left\chunk-000\file-000.mp4
    robomimic_dev1g\v1\
      _manifest.json
      <low_dim*.hdf5>
    bridgedata_v2_dev\v1\
      _manifest.json
      data\file-000.parquet ...
      meta\info.json
      README.md
  manifests\
    devscale_plan.json
    devscale_plan.md
    devscale_sync_report.json
    devscale_verify_report.json
    devscale_verify_report.md
```

> `_manifest.json` schema 由 `scripts/_local_devscale_lib.py::cmd_sync_post` 写入；
> 字段：`dataset_id / family / version / source_uri / local_uri /
> files[] / size_bytes / created_at / sync_tool / status`。

## 5. kind cluster 启动

```bash
make local-create-kind-dev             # 创建 robot-dh-dev（与默认 robot-dh 隔离）
kubectl config use-context kind-robot-dh-dev
make local-apply-data-pvc              # apply k8s/v1_7_local/*.yaml
make local-data-debug                  # exec 进 debug pod 看 /mnt/local-data
make local-devscale-summary            # 表格化展示三个 dataset 的本地状态
```

> 重建 cluster 必须 `--recreate` + 输入 `RECREATE_KIND` 二次确认；
> 删除 cluster 走 `make local-destroy-kind-dev` + `DELETE_DEV_KIND` 二次确认。

## 6. Argo workflow 走本地路径

v1.6 三个 WorkflowTemplate（`robot-dh-multisource-scale30` / `robot-dh-contract-qc` /
`robot-dh-ml-ready-export`）都接受 `dataset_uri` 入参。devscale 入参示例：

```bash
kubectl --context kind-robot-dh-dev -n robot-dh \
  create -f - <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: robot-dh-devscale-droid-
  namespace: robot-dh
spec:
  workflowTemplateRef:
    name: robot-dh-multisource-scale30
  arguments:
    parameters:
      - name: dataset_uri
        value: "file:///mnt/local-data/robot-dh-local/raw/droid_lerobot_dev1g/v1"
      - name: dataset_id
        value: "droid_lerobot_dev1g"
      - name: dataset_family
        value: "droid"
      - name: dataset_version
        value: "v1"
EOF
```

> 关键约束：
> - 该 Workflow 在 `robot-dh-dev` 上跑时，etl-phase 的 emptyDir 仍然存在，但
>   `download_dir` 命中 `file://` 直接走 Python `shutil.copytree`（已在 v1.6
>   `S3LakeStore` 之外通过 `LocalLakeStore` 实现），不再有 S3 IO。
> - 想让 v1.6 normalize input-cache 跨 pod-retry 复用，把
>   `ROBOT_DH_INPUT_CACHE_DIR` 指到 `/mnt/local-data/robot-dh-local/cache/input-cache`
>   即可（`local-runtime-configmap` 已经预置）。

## 7. 与 v1.6 scale30 workflow 的关系

| 视角 | devscale (v1.7) | scale30 (v1.6) |
|---|---|---|
| 数据位置 | `file:///mnt/local-data/...` | `s3://robot-datasets/raw/...` |
| WorkflowTemplate | 复用 v1.6 三个 | 同 v1.6 |
| 触发 | `make local-*` + 手动 submit | `make argo-submit-multisource-scale30` |
| 用途 | dev 迭代、跑通 DAG、看 metric | 压测、夜间跑回归、生产 |
| 是否默认 | 是（kind `robot-dh-dev`） | 否（远端 K8s 或 kind `robot-dh`） |
| 单次时长 | ~5–15 min | ~8–12 h |

## 8. 故障排查（速查）

参见 `docs/v1_7_windows_d_drive_kind_mount.md` 第 6 节。
