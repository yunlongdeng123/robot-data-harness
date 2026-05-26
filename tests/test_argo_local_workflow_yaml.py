"""v1.7 Argo Local 模板静态守门。

不连接 K8s，纯 YAML 结构断言。这些约束是用户 prompt 里写死的契约：
1. 默认 raw / lake 必须是 ``file:///mnt/local-data/robot-dh-local/...``，**不能** s3://。
2. 所有 step pod 必须 nonRoot / runAsUser=1000 / drop ALL caps。
3. 所有 step pod 必须 mount PVC ``robot-dh-local-data-pvc`` 到 ``/mnt/local-data/robot-dh-local``。
4. 每个 step 必须设 activeDeadlineSeconds；整 workflow 必须设 activeDeadlineSeconds=7200。
5. command 必须是 ``["/bin/bash", "-lc"]``，args 走 ``python -u -m robot_dh.cli ... | tee /tmp/...``。
6. 默认 DAG 第一步必须是 ``local-runtime-doctor``；``verify-devscale-data`` 必须 depends 它。
7. CronWorkflow 必须 concurrencyPolicy=Forbid。
8. 默认 archive_root 必须是 file:// 不是 s3://。
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
V17_DIR = REPO / "argo" / "v1_7_local"

DEVSCALE_TPL = V17_DIR / "templates" / "robot-dh-local-devscale-workflowtemplate.yaml"
QC_TPL = V17_DIR / "templates" / "robot-dh-local-qc-workflowtemplate.yaml"
ML_TPL = V17_DIR / "templates" / "robot-dh-local-ml-ready-workflowtemplate.yaml"
CRON = V17_DIR / "cron" / "local-devscale-cronworkflow.yaml"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _templates_by_name(wftpl: dict) -> dict[str, dict]:
    return {t["name"]: t for t in (wftpl.get("spec", {}).get("templates", []) or [])}


def test_devscale_template_loads_and_kind_is_workflowtemplate() -> None:
    doc = _load(DEVSCALE_TPL)
    assert doc["kind"] == "WorkflowTemplate"
    assert doc["metadata"]["name"] == "robot-dh-local-devscale"
    assert doc["spec"]["activeDeadlineSeconds"] == 7200


def test_devscale_parameters_default_to_local_file_uri() -> None:
    doc = _load(DEVSCALE_TPL)
    params = {p["name"]: p["value"] for p in doc["spec"]["arguments"]["parameters"]}
    assert params["raw_root"].startswith("file:///mnt/local-data/robot-dh-local/raw")
    assert params["lake_root"].startswith("file:///mnt/local-data/robot-dh-local/lake")
    assert params["archive_root"].startswith("file:///mnt/local-data/robot-dh-local/lake/argo-logs")
    # 一旦谁手贱把这些改成 s3://，本测试立即挂
    for k, v in params.items():
        assert "s3://" not in str(v), f"{k}={v}: must NOT default to s3://"


def test_devscale_main_dag_starts_with_doctor_then_verify() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    main = templates["main"]
    tasks = {t["name"]: t for t in main["dag"]["tasks"]}
    assert "local-runtime-doctor" in tasks
    assert tasks["local-runtime-doctor"].get("depends") in (None, "")  # 第一步无前置
    assert tasks["verify-devscale-data"]["depends"] == "local-runtime-doctor"
    # 三个 probe 都 depends verify
    for name in ("adapter-probe-droid", "adapter-probe-robomimic", "adapter-probe-bridge"):
        assert tasks[name]["depends"] == "verify-devscale-data"


def test_devscale_all_step_pods_run_nonroot_uid_1000() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    for name, tpl in templates.items():
        if name == "main":
            continue
        ctn = tpl.get("container")
        if not ctn:
            continue
        sc = ctn.get("securityContext") or {}
        assert sc.get("runAsNonRoot") is True, f"{name}: runAsNonRoot must be true"
        assert sc.get("runAsUser") == 1000, f"{name}: runAsUser must be 1000"
        assert sc.get("allowPrivilegeEscalation") is False, f"{name}"
        assert sc.get("capabilities", {}).get("drop") == ["ALL"], f"{name}"


def test_devscale_step_pods_have_active_deadline_and_command_shape() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    for name, tpl in templates.items():
        if name == "main":
            continue
        ctn = tpl.get("container")
        if not ctn:
            continue
        assert tpl.get("activeDeadlineSeconds"), f"{name}: missing activeDeadlineSeconds"
        # bash -lc + python -u 是 v1.6.8 的 lesson，禁止退回 command:["robot-dh"]
        assert ctn["command"] == ["/bin/bash", "-lc"], f"{name}: command must be bash -lc"
        args_text = "\n".join(ctn.get("args") or [])
        assert "python -u -m robot_dh.cli" in args_text, f"{name}: must use python -u -m robot_dh.cli"
        assert "tee /tmp/" in args_text, f"{name}: must tee stdout to /tmp/* for archive cross-witness"


def test_devscale_step_pods_mount_local_data_pvc() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    # argo-sync 是唯一一个不挂 PVC 的 step（它只调 kubectl PG sync），别的全部要挂
    pvc_required = set(templates) - {"main", "argo-sync"}
    for name in pvc_required:
        tpl = templates[name]
        vols = tpl.get("volumes") or []
        names = {v.get("name") for v in vols}
        assert "local-data" in names, f"{name}: missing local-data volume"
        pvc = next((v for v in vols if v.get("name") == "local-data"), {})
        claim = (pvc.get("persistentVolumeClaim") or {}).get("claimName")
        assert claim == "robot-dh-local-data-pvc", f"{name}: must use robot-dh-local-data-pvc"
        ctn = tpl.get("container") or {}
        mounts = {m.get("name"): m.get("mountPath") for m in (ctn.get("volumeMounts") or [])}
        assert mounts.get("local-data") == "/mnt/local-data/robot-dh-local", \
            f"{name}: mountPath must be /mnt/local-data/robot-dh-local"


def test_devscale_etl_phase_uses_resume_and_heartbeat() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    etl_args = "\n".join((templates["etl-phase"].get("container") or {}).get("args") or [])
    assert "--resume" in etl_args, "etl-phase must use --resume"
    assert "--heartbeat-interval-sec" in etl_args, "etl-phase must propagate heartbeat-interval-sec"


def test_qc_only_template_skips_normalize() -> None:
    doc = _load(QC_TPL)
    templates = _templates_by_name(doc)
    assert "etl-phase" not in templates
    assert "build-ads" not in templates
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    assert "verify-devscale-data" in tasks
    assert {"droid-qc", "robomimic-qc", "bridge-qc"}.issubset(tasks.keys())


def test_ml_ready_template_only_runs_ads_and_export() -> None:
    doc = _load(ML_TPL)
    templates = _templates_by_name(doc)
    assert set(templates).issuperset({"build-ads", "ml-ready-export"})
    main = templates["main"]
    tasks = [t["name"] for t in main["dag"]["tasks"]]
    assert tasks == ["build-ads", "ml-ready-export"]


def test_cron_concurrency_policy_is_forbid() -> None:
    doc = _load(CRON)
    assert doc["kind"] == "CronWorkflow"
    assert doc["spec"]["concurrencyPolicy"] == "Forbid"
    # 必须引用 v1.7 模板，不能误指向 v1.6 scale30
    assert doc["spec"]["workflowSpec"]["workflowTemplateRef"]["name"] == "robot-dh-local-devscale"


def test_submit_yamls_reference_local_templates_not_scale30() -> None:
    for sub in ("submit-local-devscale.yaml", "submit-local-qc.yaml", "submit-local-ml-ready.yaml"):
        doc = _load(V17_DIR / "workflows" / sub)
        ref = doc["spec"]["workflowTemplateRef"]["name"]
        assert ref.startswith("robot-dh-local-"), f"{sub} points to {ref}; must be a v1.7 local template"


def test_devscale_qc_step_passes_remote_lazy_disabled_for_bridge() -> None:
    """bridge devscale 全是本地 parquet，必须显式 --disable-remote-lazy，避免回退 S3。"""
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    bridge_qc = tasks["bridge-qc"]
    extras = next(p["value"] for p in bridge_qc["arguments"]["parameters"] if p["name"] == "extra_flags")
    assert "--disable-remote-lazy" in extras
    assert "--probe-timeout-sec" in extras


def test_devscale_qc_step_passes_max_workers_for_robomimic() -> None:
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    tasks = {t["name"]: t for t in templates["main"]["dag"]["tasks"]}
    rmm_qc = tasks["robomimic-qc"]
    extras = next(p["value"] for p in rmm_qc["arguments"]["parameters"] if p["name"] == "extra_flags")
    assert "--max-workers" in extras
    assert "--file-timeout-sec" in extras


def test_devscale_etl_resource_limits_are_within_kind_budget() -> None:
    """kind 默认单节点 16 GiB，超过就 evict。devscale 单 step 最高 4 GiB。"""
    doc = _load(DEVSCALE_TPL)
    templates = _templates_by_name(doc)
    for name in ("etl-phase", "build-ads", "ml-ready-export", "benchmark-regression"):
        ctn = templates[name]["container"]
        limits = (ctn.get("resources") or {}).get("limits") or {}
        mem = str(limits.get("memory", "0"))
        # 接受 Mi / Gi；不接受 > 4Gi
        if mem.endswith("Gi"):
            gi = float(mem.rstrip("Gi"))
            assert gi <= 4.0, f"{name}: memory limit {mem} > 4Gi (kind budget)"
