#!/usr/bin/env bash
# 平台层 secret 创建/更新：从 client/robot-dh-platform.env 读取，apply 到 robot-dh namespace。
# 不打印 secret 内容；只显示来源文件、目标命名空间与 secret name。

set -euo pipefail

SECRET_NAME="${SECRET_NAME:-robot-dh-v1-6-secrets}"
NAMESPACE="${NAMESPACE:-robot-dh}"
ENV_FILE="${ENV_FILE:-${ENV_FILE_DEFAULT:-client/robot-dh-platform.env}}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[FAIL] env file not found: ${ENV_FILE}" >&2
  echo "      请先 scp 远端 robot-dh-platform.env 到 ${ENV_FILE} 并 chmod 600。" >&2
  exit 2
fi

mode=$(stat -c "%a" "${ENV_FILE}" || echo "unknown")
if [[ "${mode}" != "600" && "${mode}" != "400" ]]; then
  echo "[FAIL] env file mode is ${mode}; expected 600/400 (chmod 600 ${ENV_FILE})" >&2
  exit 2
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl not found on PATH" >&2
  exit 2
fi

ctx=$(kubectl config current-context 2>/dev/null || echo "")
if [[ -z "${ctx}" ]]; then
  echo "[FAIL] no current kubectl context" >&2
  exit 2
fi
echo "[INFO] context=${ctx} namespace=${NAMESPACE} secret=${SECRET_NAME}"

# 检查关键 endpoint 不是 127.0.0.1（容易跑到本机 SSH tunnel 上）
endpoint=$(grep -E '^ROBOT_DH_S3_ENDPOINT_URL=' "${ENV_FILE}" | cut -d= -f2- | tr -d '"' || true)
if [[ "${endpoint}" == http*://127.0.0.1* || "${endpoint}" == http*://localhost* ]]; then
  echo "[FAIL] ROBOT_DH_S3_ENDPOINT_URL points to 127.0.0.1/localhost (${endpoint})" >&2
  echo "      kind 集群里这一定不通；请改成集群可达的 endpoint。" >&2
  exit 2
fi

# 等待 namespace 存在
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 \
  || kubectl create namespace "${NAMESPACE}"

kubectl -n "${NAMESPACE}" delete secret "${SECRET_NAME}" --ignore-not-found

kubectl -n "${NAMESPACE}" create secret generic "${SECRET_NAME}" \
  --from-env-file="${ENV_FILE}"

echo "[OK] secret ${NAMESPACE}/${SECRET_NAME} 创建/更新成功（未打印内容）"
