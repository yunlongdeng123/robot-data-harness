# v1.7 Windows D 盘 -> WSL -> kind node -> Pod 四层路径映射

> 适用环境：Windows 11 + Docker Desktop + WSL2 (Ubuntu 22.04+) + kind v0.22+。
> 适用 cluster：`robot-dh-dev`（与默认 `robot-dh` 隔离）。

## 1. 为什么不放 C 盘

- 大部分 dev 用户 C 盘只剩 30–80 GB；DROID + robomimic + bridge scale30 共 ~25 GiB，
  叠加 normalize 中间产物会**几小时塞满 C 盘**。
- Docker Desktop 的 WSL2 backend 默认把镜像、emptyDir 都落到 C 盘下的
  `%LOCALAPPDATA%\Docker\wsl\disk\docker_data.vhdx`，**VHDX 文件只增长不收缩**，
  即使删了容器空间也不会立刻还给 NTFS。
- Windows + WSL 的 antivirus（Defender 实时扫描）对 C 盘大对象 IO 有显著惩罚。

## 2. 为什么不放 WSL ext4 VHDX

- `\\wsl.localhost\Ubuntu-22.04\...` 这种 9P 路径走 Hyper-V VSOCK，**单线程**，
  大文件吞吐只有 50–150 MB/s；
- ext4 vhdx 与 docker_data.vhdx 是**两个**自动膨胀的 VHDX，互相竞争 C 盘空间；
- Docker Desktop 的 `bind mount` 在 `/home/<user>/...` 路径上经历多层 translation，
  Argo pod hostPath 命中 ext4 vhdx 会出现 `Permission denied` 等 9P 协议怪事。

> 结论：**默认走 D 盘**（或任意非 C 的 NTFS / exFAT 盘），通过 Docker Desktop
> "File Sharing" 显式共享。

## 3. 四层路径映射

| 层 | 路径 | 说明 |
|---|---|---|
| Windows | `D:\robot-dh-local` | 资源管理器可见；通过 Docker Desktop 共享给 WSL & kind |
| WSL | `/mnt/d/robot-dh-local` | WSL2 自动挂载所有 NTFS 盘到 `/mnt/<drive>` |
| kind node | `/mnt/local-data/robot-dh-local` | `configs/kind-robot-dh-dev-local.yaml` 的 `extraMounts.containerPath` |
| Pod | `/mnt/local-data/robot-dh-local` | `k8s/v1_7_local/local-data-pv-pvc.yaml` 的 hostPath，与 node 同 |

> 三层 K8s 路径（node / PV.hostPath / Pod mountPath）**必须完全一致**：
> kind 是把 host 目录 bind 到 node container 后再做一遍 hostPath，
> 中间任何错位都会让 Pod 看到空目录或 ENOENT。

### 3.1 一图流

```
Windows : D:\robot-dh-local
   |               (Docker Desktop file sharing)
   v
WSL     : /mnt/d/robot-dh-local
   |               (kind extraMounts: hostPath -> containerPath)
   v
kind node (container kind-control-plane): /mnt/local-data/robot-dh-local
   |               (PersistentVolume hostPath)
   v
Pod     : /mnt/local-data/robot-dh-local
```

## 4. 准备 Docker Desktop

1. 打开 Docker Desktop → Settings → Resources → **File Sharing**。
2. 确认 `D:\` 已加入共享列表；如果没有，点 `+`，浏览到 `D:\`，Apply & Restart。
3. Settings → Resources → **WSL Integration**：勾选 `Ubuntu-22.04`（或你用的发行版）。
4. WSL 重启 (`wsl --shutdown`) 一次让设置生效。
5. 在 WSL 中校验：

   ```bash
   ls -la /mnt/d/ | head
   # 若看到 D: 盘根目录内容，表示挂载 OK
   ```

## 5. 创建 kind cluster `robot-dh-dev`

```bash
# 1) 准备 D 盘目录
make local-init-data

# 2) 创建集群（用 configs/kind-robot-dh-dev-local.yaml）
make local-create-kind-dev

# 3) 切 context
kubectl config use-context kind-robot-dh-dev

# 4) 验证 node 内能看到 hostPath
docker exec robot-dh-dev-control-plane ls -la /mnt/local-data/robot-dh-local
# 期望看到 raw / lake / cache / manifests / logs 等子目录

# 5) apply v1.7 PV/PVC/ConfigMap/debug-pod
make local-apply-data-pvc

# 6) 进 debug pod 看 raw
kubectl -n robot-dh exec -it robot-dh-local-debug -- ls -lh /mnt/local-data/robot-dh-local/raw
```

## 6. 常见故障

### 6.1 `/mnt/d` 不存在

- 现象：`ls /mnt/d` → `No such file or directory`。
- 排查：
  ```bash
  cat /etc/wsl.conf
  cat /proc/mounts | grep drvfs
  ```
- 修复：
  1. 在 WSL `/etc/wsl.conf` 写入：
     ```ini
     [automount]
     enabled = true
     mountFsTab = false
     root = /mnt/
     options = "metadata,umask=22,fmask=11"
     ```
  2. PowerShell 里 `wsl --shutdown` 后重开终端。

### 6.2 Docker Desktop 没共享 D 盘

- 现象：kind cluster 创建成功，但 `docker exec robot-dh-dev-control-plane ls /mnt/local-data/robot-dh-local` 返回空，或者 `kind create cluster` 直接报 `failed to mount`。
- 修复：见上文第 4 节，把 D 盘加进 File Sharing 列表。

### 6.3 kind extraMounts 路径错误

- 现象：node 内能看到挂载，但 Pod 内 `mountPath` 是空目录。
- 根因：`local-data-pv-pvc.yaml.hostPath` 与 `configs/kind-robot-dh-dev-local.yaml.containerPath` 不一致。
- 修复：两者保持 `/mnt/local-data/robot-dh-local`，不要随意改名。

### 6.4 hostPath 在 Pod 中为空

- 现象：`kubectl exec ... ls /mnt/local-data/robot-dh-local/raw` → 空。
- 排查：
  ```bash
  # node 内有没有
  docker exec robot-dh-dev-control-plane ls -la /mnt/local-data/robot-dh-local/raw
  # 与 WSL 是否一致
  ls -la /mnt/d/robot-dh-local/raw
  ```
- 修复：通常是 D 盘里还没下载数据。运行 `make local-sync-devscale`。

### 6.5 mc alias 失败

- 现象：`./scripts/local_mc_alias_remote.sh` 报 `mc ls ... failed`。
- 排查：
  ```bash
  echo "$ROBOT_DH_S3_ENDPOINT_URL"
  mc alias list robotdh-remote
  mc ls robotdh-remote/robot-datasets --debug 2>&1 | head -n 40
  ```
- 常见原因：
  1. `client/robot-dh-v1-6.env` 未 source；
  2. `ROBOT_DH_S3_ENDPOINT_URL` 是 loopback (`127.0.0.1:9000`)：WSL tunnel
     不会在脚本里复用，必须用云端公网地址；
  3. AK/SK 失效。

### 6.6 计划总量超过 3 GB

- 现象：`./scripts/local_plan_devscale_sync.sh` 退出 1，提示 `over_limit=true`。
- 排查：查看 `D:\robot-dh-local\manifests\devscale_plan.md` 的每行 size。
- 修复：
  - 减少 `configs/devscale_datasets.yaml` 里的 `include`；
  - 或临时 `./scripts/local_plan_devscale_sync.sh --allow-over-limit`（不推荐，
    会侵占 D 盘 + 拖慢 sync）。

### 6.7 C 盘空间暴涨

- 现象：跑完一次 `make local-create-kind-dev` 后 `C:\` 少了几十 GB。
- 根因：通常是 **某个早先版本的 kind config 漏了 extraMounts**，emptyDir 还是写到 Docker
  data root。
- 排查：
  ```bash
  docker system df
  docker system prune -af   # 慎用，会清掉所有未使用的 image / 容器
  ```
- 修复：
  1. 在 Windows PowerShell 里执行：
     ```powershell
     wsl --shutdown
     Optimize-VHD -Path "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx" -Mode Full
     ```
     这一步把 VHDX 物理空间还给 NTFS。
  2. 确保 v1.7 kind config 一定有 `extraMounts`。

## 7. 跨用户复用提示

- 如果同一台机器有多个 dev，每个人各自维护一份 `D:\<user>\robot-dh-local`，
  把 `ROBOT_DH_LOCAL_DATA_ROOT` 指过去；kind config 也要同步改 `hostPath`。
- 多人共用同一份 devscale 数据：建议把数据放到 `D:\shared\robot-dh-local`，
  各自维护 kind cluster；不要共用 kind cluster（容易踩 context 切换 bug）。
