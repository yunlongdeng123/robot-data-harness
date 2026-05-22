# WSL 公网直连接入清单

这份清单用于本地 WSL 不走 SSH tunnel，直接通过公网 IP / DNS 访问云端 `robot-dh-infra`。

当前服务器公网 IP：`82.156.129.81`

## 使用前提

- 云端 `BIND_ADDR` 已改为 `0.0.0.0`
- 云安全组已只对你的 `TRUSTED_CIDR` 开放 5432 / 6379 / 9000 / 9001
- 服务器 UFW 已按白名单模式放行相同端口
- 服务器 `./scripts/04_up.sh` 已把 `.env` 中的 PostgreSQL 允许来源同步进受控 `pg_hba.conf` 区块
- 你接受数据库、Redis、MinIO 不再只监听本机回环地址

## 服务器侧准备

```bash
cd /opt/robot-dh-infra
./scripts/04_up.sh
./scripts/06_healthcheck.sh
./scripts/10_print_client_env.sh --mode public --host 82.156.129.81 --show-secrets
./scripts/12_firewall_plan.sh --public-host 82.156.129.81
```

如需实际应用 UFW：

```bash
cd /opt/robot-dh-infra
./scripts/12_firewall_plan.sh --apply --public-host 82.156.129.81
```

如果你已经把 `TRUSTED_CIDR` 和 `SSH_TRUSTED_CIDR` 写进云端 `.env`，这里不需要再手动带占位符参数。

## WSL 侧准备

把以下文件拷到 WSL 项目可访问的位置：

- `client/robot-dh-public.env`
- `client/wsl-export-public-env.sh`
- `client/wsl-remote-doctor.sh`

加载环境变量：

```bash
source ./wsl-export-public-env.sh
```

做一次体检：

```bash
./wsl-remote-doctor.sh
```

## WSL 直连验证

```bash
psql "$ROBOT_DH_DB_URI" -c 'select current_database(), current_user;'
redis-cli -u "$ROBOT_DH_REDIS_URL" ping
curl -fsS "$ROBOT_DH_S3_ENDPOINT_URL/minio/health/live"
```

## 明确不要做的事

- 不要再使用 `127.0.0.1:15432` / `19000` / `16379`
- 不要再依赖 SSH tunnel
- 不要在主项目里回退到 SQLite / PVC / 本地 artifacts 路径
- 不要把 5432 / 6379 / 9000 / 9001 开放给 `0.0.0.0/0`