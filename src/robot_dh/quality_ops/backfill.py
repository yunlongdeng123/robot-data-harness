"""backfill plan / run / status。

v1.8 中 backfill 是"轻量调度元数据"：
    plan: 生成计划 + 写 backfill_plans + 写 backfill_tasks
    run:  默认只打印 recommended commands；--execute 时 subprocess 调用
    status: 查询 backfill_tasks 状态聚合
"""

from __future__ import annotations

import json
import logging
import shlex
import subprocess
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import delete, inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    BackfillPlanRow,
    BackfillTaskRow,
    FactEtlRunRow,
    FactWorkflowStepRow,
    SlaCheckRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics.dates import DateRange, parse_date_range, iter_dates

LOG = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"

DEFAULT_STATUS_FILTER = ("FAILED", "WARN", "ERROR", "FAIL")


@dataclass
class BackfillTask:
    task_id: str
    plan_id: str
    dataset_id: str | None
    version: str | None
    dataset_family: str | None
    dt: str | None
    phase: str | None
    input_uri: str | None
    output_uri: str | None
    recommended_command: str
    status: str = "PLANNED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BackfillPlanResult:
    plan_id: str
    from_date: str | None
    to_date: str | None
    dataset_id: str | None
    version: str | None
    phase: str | None
    reason: str | None
    status: str
    tasks: list[BackfillTask]
    dry_run: bool
    plan_path: Path | None = None
    plan_md_path: Path | None = None
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: v for k, v in asdict(self).items() if k not in ("tasks", "plan_path", "plan_md_path")}
        d["tasks"] = [t.to_dict() for t in self.tasks]
        d["plan_path"] = str(self.plan_path) if self.plan_path else None
        d["plan_md_path"] = str(self.plan_md_path) if self.plan_md_path else None
        d["task_count"] = len(self.tasks)
        return d


@dataclass
class BackfillRunResult:
    plan_id: str
    executed: int
    failed: int
    skipped: int
    execute: bool
    dry_run: bool
    tasks: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BackfillPlanner:
    def __init__(
        self,
        *,
        db_uri: str | None = None,
    ) -> None:
        self._db_uri = db_uri
        self._engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self._engine is None:
            resolved = resolve_db_uri(self._db_uri)
            engine = get_engine(resolved)
            if engine.dialect.name == "sqlite":
                init_db(resolved)
                ensure_lake_tables(engine)
            self._engine = engine
        return self._engine

    def plan(
        self,
        *,
        from_date: str | date,
        to_date: str | date,
        dataset_id: str | None = None,
        version: str | None = None,
        phase: str | None = None,
        reason: str | None = None,
        status_filter: Sequence[str] = DEFAULT_STATUS_FILTER,
        dry_run: bool = False,
        output_dir: Path | None = None,
        created_by: str | None = None,
    ) -> BackfillPlanResult:
        window = parse_date_range(from_date=from_date, to_date=to_date)
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())

        normalized_filter = tuple(s.upper() for s in (status_filter or DEFAULT_STATUS_FILTER))
        candidate_keys = self._collect_candidates(
            engine=engine,
            window=window,
            dataset_id=dataset_id,
            version=version,
            phase=phase,
            status_filter=normalized_filter,
            existing=existing,
        )

        plan_id = self._make_plan_id(dataset_id=dataset_id, phase=phase)
        tasks: list[BackfillTask] = []
        for key in sorted(candidate_keys):
            d, ds_id, ver, ph, family = key
            cmd = self._recommend_command(dataset_id=ds_id, version=ver, phase=ph)
            tasks.append(
                BackfillTask(
                    task_id=f"{plan_id}::{ds_id}::{ver}::{ph}::{d.isoformat()}",
                    plan_id=plan_id,
                    dataset_id=ds_id,
                    version=ver,
                    dataset_family=family,
                    dt=d.isoformat(),
                    phase=ph,
                    input_uri=None,
                    output_uri=None,
                    recommended_command=cmd,
                    status="PLANNED",
                )
            )

        result = BackfillPlanResult(
            plan_id=plan_id,
            from_date=window.start.isoformat(),
            to_date=window.end.isoformat(),
            dataset_id=dataset_id,
            version=version,
            phase=phase,
            reason=reason,
            status="PLANNED",
            tasks=tasks,
            dry_run=dry_run,
        )

        if not dry_run:
            self._persist_plan(
                result=result,
                dataset_id=dataset_id,
                version=version,
                phase=phase,
                reason=reason,
                created_by=created_by,
            )
            result.status = "PERSISTED"
        else:
            result.warnings.append("dry_run=True; backfill_plans / backfill_tasks 没有写库")

        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            plan_json_path = output_dir / "plan.json"
            plan_json_path.write_text(
                json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
            )
            result.plan_path = plan_json_path

            env = Environment(loader=FileSystemLoader(TEMPLATES_DIR.as_posix()), autoescape=False)
            md_path = output_dir / "plan.md"
            md_path.write_text(
                env.get_template("backfill_plan.md.j2").render(
                    plan=result,
                    tasks=[t.to_dict() for t in result.tasks],
                    generated_at=datetime.now(timezone.utc).isoformat(),
                ),
                encoding="utf-8",
            )
            result.plan_md_path = md_path

        return result

    def run(
        self,
        *,
        plan_id: str,
        max_parallel: int = 2,
        execute: bool = False,
        dry_run: bool = False,
    ) -> BackfillRunResult:
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        if "backfill_tasks" not in existing:
            raise RuntimeError(
                "backfill_tasks 表不存在；请先在 infra 项目执行 ./scripts/39_pg_apply_v1_8_schema.sh"
            )
        with Session(engine, expire_on_commit=False, future=True) as session:
            tasks = session.execute(
                select(BackfillTaskRow).where(BackfillTaskRow.plan_id == plan_id)
            ).scalars().all()
            if not tasks:
                return BackfillRunResult(plan_id=plan_id, executed=0, failed=0, skipped=0, execute=execute, dry_run=dry_run)

            executed = 0
            failed = 0
            skipped = 0
            outputs: list[dict[str, Any]] = []
            for t in tasks:
                if not execute:
                    LOG.info("[backfill] would run: %s", t.recommended_command)
                    outputs.append({"task_id": t.task_id, "status": t.status, "command": t.recommended_command, "executed": False})
                    skipped += 1
                    continue
                if dry_run:
                    full_cmd = t.recommended_command + " --dry-run"
                else:
                    full_cmd = t.recommended_command
                try:
                    LOG.info("[backfill] running: %s", full_cmd)
                    t.status = "RUNNING"
                    t.started_at = datetime.now(timezone.utc)
                    t.attempts = (t.attempts or 0) + 1
                    session.commit()
                    proc = subprocess.run(shlex.split(full_cmd), capture_output=True, text=True, timeout=3600)
                    if proc.returncode == 0:
                        t.status = "SUCCEEDED"
                        executed += 1
                    else:
                        t.status = "FAILED"
                        t.last_error = (proc.stderr or proc.stdout)[:2000]
                        failed += 1
                    t.finished_at = datetime.now(timezone.utc)
                    session.commit()
                    outputs.append({
                        "task_id": t.task_id,
                        "status": t.status,
                        "command": full_cmd,
                        "executed": True,
                        "exit_code": proc.returncode,
                    })
                except (subprocess.TimeoutExpired, OSError) as err:
                    t.status = "FAILED"
                    t.last_error = str(err)[:2000]
                    t.finished_at = datetime.now(timezone.utc)
                    session.commit()
                    outputs.append({"task_id": t.task_id, "status": "FAILED", "command": full_cmd, "executed": True, "error": str(err)})
                    failed += 1
            return BackfillRunResult(
                plan_id=plan_id, executed=executed, failed=failed, skipped=skipped,
                execute=execute, dry_run=dry_run, tasks=outputs,
            )

    def status(self, *, plan_id: str) -> dict[str, Any]:
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        if "backfill_plans" not in existing:
            return {"plan_id": plan_id, "missing_tables": ["backfill_plans"], "tasks": []}
        with Session(engine, expire_on_commit=False, future=True) as session:
            plan = session.get(BackfillPlanRow, plan_id)
            tasks = session.execute(
                select(BackfillTaskRow).where(BackfillTaskRow.plan_id == plan_id)
            ).scalars().all()
            buckets: dict[str, int] = {}
            for t in tasks:
                buckets[t.status or "UNKNOWN"] = buckets.get(t.status or "UNKNOWN", 0) + 1
            return {
                "plan_id": plan_id,
                "plan": _backfill_plan_to_dict(plan) if plan else None,
                "task_count": len(tasks),
                "status_buckets": buckets,
                "tasks": [_backfill_task_to_dict(t) for t in tasks],
            }

    # ---------------- internal ----------------

    def _collect_candidates(
        self,
        *,
        engine: Engine,
        window: DateRange,
        dataset_id: str | None,
        version: str | None,
        phase: str | None,
        status_filter: tuple[str, ...],
        existing: set[str],
    ) -> set[tuple[date, str, str, str, str | None]]:
        keys: set[tuple[date, str, str, str, str | None]] = set()
        with Session(engine, expire_on_commit=False, future=True) as session:
            if "fact_etl_run" in existing:
                q = select(FactEtlRunRow).where(
                    FactEtlRunRow.dt.is_not(None),
                    FactEtlRunRow.dt >= window.start,
                    FactEtlRunRow.dt <= window.end,
                )
                if dataset_id:
                    q = q.where(FactEtlRunRow.dataset_id == dataset_id)
                if version:
                    q = q.where(FactEtlRunRow.version == version)
                if phase:
                    q = q.where(FactEtlRunRow.phase == phase)
                for r in session.execute(q).scalars().all():
                    if (r.status or "").upper() in status_filter and r.dataset_id and r.version:
                        keys.add((r.dt, r.dataset_id, r.version, r.phase or "normalize", r.dataset_family))
            if "fact_workflow_step" in existing:
                q2 = select(FactWorkflowStepRow).where(
                    FactWorkflowStepRow.dt.is_not(None),
                    FactWorkflowStepRow.dt >= window.start,
                    FactWorkflowStepRow.dt <= window.end,
                )
                if dataset_id:
                    q2 = q2.where(FactWorkflowStepRow.dataset_id == dataset_id)
                if version:
                    q2 = q2.where(FactWorkflowStepRow.version == version)
                for r in session.execute(q2).scalars().all():
                    if (r.phase or "").upper() in status_filter and r.dataset_id and r.version:
                        keys.add((r.dt, r.dataset_id, r.version, phase or "workflow", r.dataset_family))
            if "ads_quality_dashboard" in existing:
                q3 = select(AdsQualityDashboardRow).where(
                    AdsQualityDashboardRow.dt >= window.start,
                    AdsQualityDashboardRow.dt <= window.end,
                )
                if dataset_id:
                    q3 = q3.where(AdsQualityDashboardRow.dataset_id == dataset_id)
                if version:
                    q3 = q3.where(AdsQualityDashboardRow.version == version)
                for r in session.execute(q3).scalars().all():
                    if (r.overall_status or "").upper() in status_filter and r.dataset_id and r.version:
                        keys.add((r.dt, r.dataset_id, r.version, phase or "build_features", r.dataset_family))
            if "sla_checks" in existing:
                q4 = select(SlaCheckRow).where(
                    SlaCheckRow.dt.is_not(None),
                    SlaCheckRow.dt >= window.start,
                    SlaCheckRow.dt <= window.end,
                )
                if dataset_id:
                    q4 = q4.where(SlaCheckRow.dataset_id == dataset_id)
                if version:
                    q4 = q4.where(SlaCheckRow.version == version)
                for r in session.execute(q4).scalars().all():
                    if (r.status or "").upper() in status_filter and r.dataset_id and r.version:
                        keys.add((r.dt, r.dataset_id, r.version, phase or "sla", None))

        # 显式 dataset_id+version 仍空 → 给 [start..end] 每天补一行（用于无运行历史时人工 backfill）
        if not keys and dataset_id and version:
            for d in iter_dates(window):
                keys.add((d, dataset_id, version, phase or "normalize", None))
        return keys

    @staticmethod
    def _make_plan_id(*, dataset_id: str | None, phase: str | None) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        suffix = uuid.uuid4().hex[:6]
        scope = "_".join(p for p in [dataset_id, phase] if p) or "all"
        return f"bf-{ts}-{scope}-{suffix}"

    @staticmethod
    def _recommend_command(*, dataset_id: str, version: str, phase: str) -> str:
        """根据 phase 推荐入口命令。"""
        ph = (phase or "normalize").lower()
        if ph in ("normalize", "etl", "build_features", "build_ads", "ml_ready", "qc", "workflow", "sla"):
            stage = ph if ph != "workflow" else "etl"
        else:
            stage = "normalize"
        if stage in ("qc",):
            return f"robot-dh qc contract run --dataset-id {shlex.quote(dataset_id)} --version {shlex.quote(version)} --resume"
        if stage in ("ml_ready",):
            return f"robot-dh ml-ready export --dataset-id {shlex.quote(dataset_id)} --version {shlex.quote(version)}"
        if stage in ("sla",):
            return f"robot-dh sla check --policy configs/sla_policies.yaml"
        return (
            f"robot-dh etl run --dataset-id {shlex.quote(dataset_id)} "
            f"--version {shlex.quote(version)} --phase {shlex.quote(stage)} --resume"
        )

    def _persist_plan(
        self,
        *,
        result: BackfillPlanResult,
        dataset_id: str | None,
        version: str | None,
        phase: str | None,
        reason: str | None,
        created_by: str | None,
    ) -> None:
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        if "backfill_plans" not in existing or "backfill_tasks" not in existing:
            result.warnings.append(
                "backfill_plans / backfill_tasks 缺失；请先在 infra 项目执行 ./scripts/39_pg_apply_v1_8_schema.sh"
            )
            return
        plan_json = {
            "dataset_id": dataset_id,
            "version": version,
            "phase": phase,
            "reason": reason,
            "task_count": len(result.tasks),
        }
        with Session(engine, expire_on_commit=False, future=True) as session:
            plan_row = session.get(BackfillPlanRow, result.plan_id)
            if plan_row is None:
                plan_row = BackfillPlanRow(
                    plan_id=result.plan_id,
                    from_date=date.fromisoformat(result.from_date) if result.from_date else None,
                    to_date=date.fromisoformat(result.to_date) if result.to_date else None,
                    dataset_id=dataset_id,
                    version=version,
                    phase=phase,
                    reason=reason,
                    status="PERSISTED",
                    task_count=len(result.tasks),
                    created_by=created_by,
                    plan_json=plan_json,
                )
                session.add(plan_row)
            else:
                plan_row.status = "PERSISTED"
                plan_row.task_count = len(result.tasks)
                plan_row.plan_json = plan_json
                plan_row.updated_at = datetime.now(timezone.utc)

            # 写 tasks（按 task_id UPSERT）
            for t in result.tasks:
                existing_task = session.get(BackfillTaskRow, t.task_id)
                payload = dict(
                    task_id=t.task_id, plan_id=t.plan_id,
                    dataset_id=t.dataset_id, version=t.version,
                    dataset_family=t.dataset_family,
                    dt=date.fromisoformat(t.dt) if t.dt else None,
                    phase=t.phase, input_uri=t.input_uri, output_uri=t.output_uri,
                    recommended_command=t.recommended_command,
                    status=t.status,
                )
                if existing_task is None:
                    session.add(BackfillTaskRow(**payload))
                else:
                    for k, v in payload.items():
                        setattr(existing_task, k, v)
                    existing_task.updated_at = datetime.now(timezone.utc)
            session.commit()


def generate_backfill_plan(
    *,
    from_date: str | date,
    to_date: str | date,
    dataset_id: str | None = None,
    version: str | None = None,
    phase: str | None = None,
    reason: str | None = None,
    status_filter: Iterable[str] = DEFAULT_STATUS_FILTER,
    dry_run: bool = False,
    output_dir: Path | None = None,
    db_uri: str | None = None,
    created_by: str | None = None,
) -> BackfillPlanResult:
    planner = BackfillPlanner(db_uri=db_uri)
    return planner.plan(
        from_date=from_date,
        to_date=to_date,
        dataset_id=dataset_id,
        version=version,
        phase=phase,
        reason=reason,
        status_filter=tuple(status_filter),
        dry_run=dry_run,
        output_dir=output_dir,
        created_by=created_by,
    )


def run_backfill_plan(
    *,
    plan_id: str,
    max_parallel: int = 2,
    execute: bool = False,
    dry_run: bool = False,
    db_uri: str | None = None,
) -> BackfillRunResult:
    return BackfillPlanner(db_uri=db_uri).run(
        plan_id=plan_id, max_parallel=max_parallel, execute=execute, dry_run=dry_run,
    )


def show_backfill_status(*, plan_id: str, db_uri: str | None = None) -> dict[str, Any]:
    return BackfillPlanner(db_uri=db_uri).status(plan_id=plan_id)


def _backfill_plan_to_dict(plan: BackfillPlanRow) -> dict[str, Any]:
    return {
        "plan_id": plan.plan_id,
        "from_date": plan.from_date.isoformat() if plan.from_date else None,
        "to_date": plan.to_date.isoformat() if plan.to_date else None,
        "dataset_id": plan.dataset_id,
        "version": plan.version,
        "phase": plan.phase,
        "reason": plan.reason,
        "status": plan.status,
        "task_count": plan.task_count,
        "created_by": plan.created_by,
        "created_at": plan.created_at.isoformat() if plan.created_at else None,
        "updated_at": plan.updated_at.isoformat() if plan.updated_at else None,
        "plan_json": plan.plan_json,
    }


def _backfill_task_to_dict(task: BackfillTaskRow) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "plan_id": task.plan_id,
        "dataset_id": task.dataset_id,
        "version": task.version,
        "dataset_family": task.dataset_family,
        "dt": task.dt.isoformat() if task.dt else None,
        "phase": task.phase,
        "input_uri": task.input_uri,
        "output_uri": task.output_uri,
        "recommended_command": task.recommended_command,
        "status": task.status,
        "attempts": task.attempts,
        "last_error": task.last_error,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "finished_at": task.finished_at.isoformat() if task.finished_at else None,
    }
