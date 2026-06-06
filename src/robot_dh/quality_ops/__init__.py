"""robot_dh v1.8 数据质量运营。

模块：
- summary       从 ads/dws 抽取一个日度 summary（数据集数 / pass 率 / TopN 失败 / p95 / archive log）
- report        渲染 HTML / JSON / CSV / parquet
- backfill      根据失败 fact / SLA / readiness 生成 backfill plan + tasks
- sla           读取 sla_policies.yaml，写 sla_checks
"""

from robot_dh.quality_ops.summary import (
    QualitySummary,
    build_quality_summary,
)
from robot_dh.quality_ops.report import (
    QualityReportArtifacts,
    QualityReportRenderer,
    render_quality_report,
)
from robot_dh.quality_ops.backfill import (
    BackfillPlanResult,
    BackfillPlanner,
    BackfillRunResult,
    generate_backfill_plan,
    run_backfill_plan,
    show_backfill_status,
)
from robot_dh.quality_ops.sla import (
    SlaCheckResult,
    SlaPolicy,
    SlaPolicyDoc,
    SlaReportArtifacts,
    load_sla_policies,
    perform_sla_checks,
    render_sla_report,
)

__all__ = [
    "QualitySummary",
    "build_quality_summary",
    "QualityReportArtifacts",
    "QualityReportRenderer",
    "render_quality_report",
    "BackfillPlanResult",
    "BackfillPlanner",
    "BackfillRunResult",
    "generate_backfill_plan",
    "run_backfill_plan",
    "show_backfill_status",
    "SlaCheckResult",
    "SlaPolicy",
    "SlaPolicyDoc",
    "SlaReportArtifacts",
    "load_sla_policies",
    "perform_sla_checks",
    "render_sla_report",
]
