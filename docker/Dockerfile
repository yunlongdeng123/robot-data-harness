FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MPLCONFIGDIR=/tmp/.matplotlib \
    XDG_CACHE_HOME=/tmp/.cache \
    XDG_CONFIG_HOME=/tmp/.config \
    FONTCONFIG_PATH=/etc/fonts \
    ROBOT_DH_EVENTS_DIR=/tmp/robot-dh/events

# 国内镜像源默认（阿里云），加速 PyPI + PyTorch CPU wheel 下载。
# 海外环境 / CI 可通过 --build-arg 覆盖回官方源，例如:
#   docker build \
#     --build-arg PIP_INDEX_URL=https://pypi.org/simple/ \
#     --build-arg TORCH_WHEEL_INDEX=https://download.pytorch.org/whl/cpu \
#     --build-arg TORCH_SPEC=torch \
#     -t robot-data-harness:local -f docker/Dockerfile .
ARG PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
ARG TORCH_WHEEL_INDEX=https://mirrors.aliyun.com/pytorch-wheels/cpu/
ARG TORCH_SPEC=torch==2.6.0+cpu

ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_DEFAULT_TIMEOUT=180 \
    PIP_RETRIES=5 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        libglib2.0-0 \
        libgl1 \
        libsm6 \
        libxext6 \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /tmp/.matplotlib /tmp/.cache /tmp/.config /tmp/.cache/fontconfig /tmp/robot-dh/events \
    && chmod -R 1777 /tmp/.matplotlib /tmp/.cache /tmp/.config /tmp/robot-dh

# v1.7 local Argo workflow 的 argo-sync / archive-logs-index step 在 pod 内调用
# `kubectl get workflow ... -o json` 拉 workflow JSON 写回 PG。固定到稳定 minor
# 版本（v1.30，含 client-side prune），通过 K8s 官方 mirror 拉对应架构的二进制。
ARG KUBECTL_VERSION=v1.30.4
RUN set -eux; \
    case "$(uname -m)" in \
        x86_64) arch="amd64" ;; \
        aarch64) arch="arm64" ;; \
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL -o /usr/local/bin/kubectl \
        "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}/kubectl"; \
    chmod 0755 /usr/local/bin/kubectl; \
    /usr/local/bin/kubectl version --client=true --output=yaml | head -10

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
        --find-links ${TORCH_WHEEL_INDEX} \
        "${TORCH_SPEC}"
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
RUN pip install --no-cache-dir --no-deps .

ENTRYPOINT ["robot-dh"]
CMD ["--help"]
