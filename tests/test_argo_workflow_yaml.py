"""平台 Argo yaml 静态校验：合法 YAML + envFrom secretRef 一致 + 不含真实凭据。"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


ARGO_DIR = Path(__file__).resolve().parent.parent / "argo"
PLATFORM_TEMPLATES = [
    ARGO_DIR / "templates" / "robot-dh-multisource-scale30-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-contract-qc-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-ml-ready-workflowtemplate.yaml",
]
PLATFORM_WORKFLOWS = [
    ARGO_DIR / "workflows" / "submit-multisource-scale30.yaml",
    ARGO_DIR / "workflows" / "submit-contract-qc.yaml",
    ARGO_DIR / "workflows" / "submit-ml-ready.yaml",
]
PLATFORM_CRON = [ARGO_DIR / "cron" / "multisource-scale30-cronworkflow.yaml"]

ALL_FILES = PLATFORM_TEMPLATES + PLATFORM_WORKFLOWS + PLATFORM_CRON


@pytest.mark.parametrize("path", ALL_FILES)
def test_yaml_valid(path: Path) -> None:
    assert path.is_file(), f"{path} not found"
    yaml.safe_load(path.read_text())


def _walk_strings(node):
    if isinstance(node, dict):
        for v in node.values():
            yield from _walk_strings(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_strings(item)
    elif isinstance(node, str):
        yield node


def test_platform_templates_use_platform_secret() -> None:
    """所有平台 WorkflowTemplate 必须 envFrom: robot-dh-v1-6-secrets。"""
    for path in PLATFORM_TEMPLATES:
        text = path.read_text()
        assert "robot-dh-v1-6-secrets" in text, f"{path} missing platform secret ref"
        # 不能引用旧 v1.5 secret name
        assert "robot-dh-v1-5-secrets" not in text, f"{path} should not reuse v1.5 secret"


def test_platform_templates_use_image_pull_if_not_present() -> None:
    for path in PLATFORM_TEMPLATES:
        text = path.read_text()
        assert "imagePullPolicy: IfNotPresent" in text, f"{path} missing IfNotPresent"


_REAL_SECRET_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),  # AWS access key
    re.compile(r"-----BEGIN [A-Z ]+PRIVATE KEY-----"),
]


@pytest.mark.parametrize("path", ALL_FILES)
def test_no_real_secrets(path: Path) -> None:
    text = path.read_text()
    for pat in _REAL_SECRET_PATTERNS:
        assert not pat.search(text), f"{path} contains potential secret"


# ---------- v1.6 Argo log archive（来自 robot-dh-infra 的需求）----------
# 详见 docs/history/v1_6_argo_log_archive_request.md / docs/history/v1_6_argo_log_archive_handoff.md。
# WorkflowTemplate 顶层 podGC.strategy 必须给 controller 留出 archiveLogs 的窗口，
# 不能是 OnPodCompletion / OnPodSuccess（这会让 step pod 终态立刻 GC，stdout 丢失）。
_LOG_ARCHIVE_TEMPLATES = [
    ARGO_DIR / "templates" / "robot-dh-multisource-scale30-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-contract-qc-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-ml-ready-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-scale-etl-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-benchmark-workflowtemplate.yaml",
    ARGO_DIR / "templates" / "robot-dh-build-ads-workflowtemplate.yaml",
]


@pytest.mark.parametrize("path", _LOG_ARCHIVE_TEMPLATES)
def test_pod_gc_keeps_archivelogs_window(path: Path) -> None:
    doc = yaml.safe_load(path.read_text())
    pod_gc = doc["spec"].get("podGC") or {}
    strategy = pod_gc.get("strategy")
    assert strategy in {"OnWorkflowCompletion", "OnWorkflowSuccess"}, (
        f"{path}: podGC.strategy={strategy!r} 会让 step pod 终态立刻 GC，"
        "controller 来不及把 stdout 归档到 robot-dh-artifacts/argo-logs/"
    )


def test_multisource_scale30_qc_contract_run_active_deadline_capped() -> None:
    """v1.6.8（fvx5z F1）：qc-contract-run 单 step 不允许跑 > 30min。

    现场：bridge-qc 单次 enrichment 在 fvx5z 跑了 1849s（30.8min）才放弃；
    根因是 s3fs 默认 ``adaptive`` retry 在 ContentLengthError 上累计指数退避。
    本 PR 用 ``get_s3fs_fast`` + ``activeDeadlineSeconds=1800`` 双层兜底，
    yaml 这里守门 deadline 不再回归到 7200s/2h。
    """
    path = ARGO_DIR / "templates" / "robot-dh-multisource-scale30-workflowtemplate.yaml"
    doc = yaml.safe_load(path.read_text())
    templates = {t["name"]: t for t in doc["spec"]["templates"]}
    qc = templates["qc-contract-run"]

    deadline = qc.get("activeDeadlineSeconds")
    assert deadline is not None, "qc-contract-run must declare activeDeadlineSeconds"
    assert deadline <= 1800, (
        f"qc-contract-run activeDeadlineSeconds={deadline} exceeds fvx5z F1 budget (1800s); "
        "bridge enrichment uses fast s3fs and robomimic uses fast boto client, "
        "26 hdf5 / 4 concurrency × 195s/file ≈ 1267s leaves 9 min margin"
    )


def test_multisource_scale30_etl_phase_uses_python_unbuffered_tee_pattern() -> None:
    """v1.6.8（fvx5z F3）：etl-phase 必须用 ``python -u`` + ``tee`` 双写，
    与 qc-contract-run 一致。droid-normalize 18 GiB download 静默 2h+ 的现场，
    archive log（pod 终态才写）完全没东西可看；events emptyDir + tee 让 pod
    还在跑就能 ``kubectl exec`` 进容器 ``cat /var/run/robot-dh/events/etl-stdout.log``
    实时查进度。
    """
    path = ARGO_DIR / "templates" / "robot-dh-multisource-scale30-workflowtemplate.yaml"
    doc = yaml.safe_load(path.read_text())
    templates = {t["name"]: t for t in doc["spec"]["templates"]}
    etl = templates["etl-phase"]

    cmd = etl["container"].get("command")
    args = etl["container"].get("args") or []
    args_str = "\n".join(args) if isinstance(args, list) else str(args)
    assert cmd and cmd[0] == "/bin/bash", (
        f"etl-phase command must be /bin/bash for tee pattern, got {cmd!r}"
    )
    assert "python -u -m robot_dh.cli" in args_str, (
        "etl-phase must run via `python -u -m robot_dh.cli` (unbuffered)"
    )
    assert "tee /var/run/robot-dh/events/etl-stdout.log" in args_str, (
        "etl-phase must tee stdout to /var/run/robot-dh/events/etl-stdout.log "
        "for in-flight log access while pod still running (fvx5z F3)"
    )
    assert "set -o pipefail" in args_str, (
        "etl-phase tee pipeline must set pipefail so python exit code propagates"
    )

    env_names = {e["name"] for e in etl["container"]["env"]}
    assert "ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC" in env_names, (
        "etl-phase must set ROBOT_DH_DOWNLOAD_PROGRESS_INTERVAL_SEC for download_dir "
        "wall-clock progress log (fvx5z F3); 30s avoids '50 files but each takes 5min' silent gap"
    )


def test_multisource_scale30_etl_phase_has_ephemeral_storage_limit() -> None:
    """v1.6.7（ddbfb R3）：etl-phase 必须有 ``ephemeral-storage`` limit + emptyDir.sizeLimit。

    droid_lerobot_scale30 跳过 videos/ 后单 partition raw ~14 GiB；emptyDir.sizeLimit
    与 limits.ephemeral-storage 双层兜底（仅 sizeLimit 不会拒绝调度）才能让 kubelet
    在 normalize 撑爆磁盘时显式触发 Evicted，方便排障。
    """
    path = ARGO_DIR / "templates" / "robot-dh-multisource-scale30-workflowtemplate.yaml"
    doc = yaml.safe_load(path.read_text())
    templates = {t["name"]: t for t in doc["spec"]["templates"]}
    etl = templates["etl-phase"]

    workdir_vol = next(v for v in etl["volumes"] if v["name"] == "workdir")
    size_limit = workdir_vol["emptyDir"]["sizeLimit"]
    assert size_limit.endswith("Gi"), f"workdir emptyDir.sizeLimit must be Gi, got {size_limit}"
    assert int(size_limit.removesuffix("Gi")) >= 16, (
        f"workdir emptyDir.sizeLimit={size_limit} too small for droid_lerobot_scale30 (~14 GiB)"
    )

    limits = etl["container"]["resources"]["limits"]
    assert "ephemeral-storage" in limits, (
        "etl-phase container.resources.limits must include ephemeral-storage; "
        "ddbfb R3 needs kubelet to trigger Evicted instead of silent OOM"
    )
    eph = limits["ephemeral-storage"]
    assert eph.endswith("Gi") and int(eph.removesuffix("Gi")) >= 16, (
        f"ephemeral-storage limit={eph} too small"
    )

    env_names = {e["name"] for e in etl["container"]["env"]}
    assert "PYTHONUNBUFFERED" in env_names, (
        "etl-phase env must set PYTHONUNBUFFERED=1 so SIGKILL doesn't drop stdout"
    )
    assert "ROBOT_DH_INPUT_CACHE_DIR" in env_names, (
        "etl-phase must set ROBOT_DH_INPUT_CACHE_DIR onto workdir emptyDir for resume cache"
    )


def test_workflow_controller_artifact_repository_template_shape() -> None:
    path = ARGO_DIR / "install" / "workflow-controller-artifact-repository.yaml"
    doc = yaml.safe_load(path.read_text())
    assert doc["kind"] == "ConfigMap"
    assert doc["metadata"]["name"] == "workflow-controller-configmap"
    assert doc["metadata"]["namespace"] == "argo"

    ar_yaml = doc["data"]["artifactRepository"]
    inner = yaml.safe_load(ar_yaml)
    assert inner["archiveLogs"] is True
    s3 = inner["s3"]
    assert s3["bucket"] == "robot-dh-artifacts"
    assert s3["keyFormat"] == (
        "argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log"
    )
    assert s3["accessKeySecret"] == {
        "name": "robot-dh-v1-6-secrets",
        "key": "ROBOT_DH_S3_ACCESS_KEY",
    }
    assert s3["secretKeySecret"] == {
        "name": "robot-dh-v1-6-secrets",
        "key": "ROBOT_DH_S3_SECRET_KEY",
    }
    # 占位符必须保留，由 argo_apply_log_archive.sh 渲染
    assert "__ROBOT_DH_S3_ENDPOINT_HOSTPORT__" in ar_yaml
    assert "__ROBOT_DH_S3_INSECURE__" in ar_yaml
