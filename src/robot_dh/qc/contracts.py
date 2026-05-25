"""run_contract / write_report 编排入口。"""

from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any

from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import is_s3_uri, join_uri
from robot_dh.qc.base import (
    AssetProfile,
    ContractReport,
    Rule,
    RuleResult,
    aggregate_status,
    evaluate_rule,
    utcnow_iso,
)
from robot_dh.qc.profile import profile_dataset
from robot_dh.qc.registry import get_contract_runner

LOG = logging.getLogger(__name__)


def run_contract(
    *,
    dataset_uri: str,
    dataset_family: str,
    dataset_id: str,
    version: str,
    layer: str | None = None,
    rules_override: list[Rule] | None = None,
) -> tuple[ContractReport, AssetProfile]:
    """对 dataset 跑 contract；返回 ContractReport + 关联 AssetProfile。"""
    rules, metric_fn, contract_id = get_contract_runner(dataset_family)
    if rules_override:
        rules = list(rules_override)
    started = time.time()
    started_iso = utcnow_iso()

    profile = profile_dataset(
        dataset_uri=dataset_uri,
        dataset_id=dataset_id,
        version=version,
        dataset_family=dataset_family,
        layer=layer,
    )
    metrics = metric_fn(profile)
    results: list[RuleResult] = [evaluate_rule(rule, metrics.get(rule.metric)) for rule in rules]
    status = aggregate_status(results)
    failed = [r.to_dict() for r in results if r.status == "FAIL"]
    warning = [r.to_dict() for r in results if r.status == "WARN"]

    finished = time.time()
    report = ContractReport(
        contract_id=contract_id,
        dataset_family=dataset_family,
        dataset_id=dataset_id,
        version=version,
        dataset_uri=dataset_uri,
        status=status,
        started_at=started_iso,
        finished_at=utcnow_iso(),
        duration_sec=float(finished - started),
        metrics=metrics,
        rules=results,
        failed_rules=failed,
        warning_rules=warning,
        artifacts={},
        run_id=f"qc-{contract_id}-{uuid.uuid4().hex[:10]}",
    )
    return report, profile


_HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<title>QC contract report — {dataset_id} {version}</title>
<style>
body {{ font-family: -apple-system, system-ui, sans-serif; padding: 24px; color: #222; }}
h1 {{ margin-bottom: 0; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; font-size: 13px; }}
.status-PASS {{ color: #1f7a1f; font-weight: 600; }}
.status-WARN {{ color: #c08000; font-weight: 600; }}
.status-FAIL {{ color: #b00000; font-weight: 700; }}
.section {{ margin-top: 24px; }}
pre {{ background: #f6f6f6; padding: 12px; overflow: auto; }}
</style>
</head>
<body>
<h1>QC contract report</h1>
<p>contract_id={contract_id}, dataset_family={dataset_family}</p>
<p>dataset_id={dataset_id} version={version}</p>
<p>dataset_uri=<code>{dataset_uri}</code></p>
<p>status: <span class="status-{status}">{status}</span></p>
<p>started_at={started_at} finished_at={finished_at} duration_sec={duration_sec:.2f}</p>

<div class="section"><h2>Metrics</h2><pre>{metrics_json}</pre></div>
<div class="section"><h2>Rules</h2>
<table>
  <thead><tr><th>rule_id</th><th>metric</th><th>op</th><th>threshold</th><th>actual</th><th>severity</th><th>status</th></tr></thead>
  <tbody>{rule_rows}</tbody>
</table>
</div>
</body>
</html>
"""


def render_report_html(report: ContractReport) -> str:
    rule_rows = "\n".join(
        "<tr>"
        f"<td>{r.rule_id}</td>"
        f"<td>{r.metric}</td>"
        f"<td>{r.op}</td>"
        f"<td>{r.threshold}</td>"
        f"<td>{r.actual}</td>"
        f"<td>{r.severity}</td>"
        f"<td class='status-{r.status}'>{r.status}</td>"
        "</tr>"
        for r in report.rules
    )
    return _HTML_TEMPLATE.format(
        contract_id=report.contract_id,
        dataset_family=report.dataset_family,
        dataset_id=report.dataset_id,
        version=report.version,
        dataset_uri=report.dataset_uri,
        status=report.status,
        started_at=report.started_at,
        finished_at=report.finished_at,
        duration_sec=report.duration_sec,
        metrics_json=json.dumps(report.metrics, ensure_ascii=False, indent=2),
        rule_rows=rule_rows,
    )


def write_report(
    *,
    report: ContractReport,
    profile: AssetProfile,
    output_uri: str,
) -> dict[str, str]:
    """将 contract_report.json / .html / asset_profile.json 写到 output_uri。"""
    store = create_lake_store(output_uri)
    json_uri = join_uri(output_uri, "contract_report.json")
    html_uri = join_uri(output_uri, "contract_report.html")
    profile_uri = join_uri(output_uri, "asset_profile.json")

    store.write_json(json_uri, report.to_dict())
    store.write_text(html_uri, render_report_html(report))
    store.write_json(profile_uri, profile.to_dict())

    report.artifacts.update(
        {
            "report_uri": json_uri,
            "report_html_uri": html_uri,
            "profile_uri": profile_uri,
        }
    )
    # 重新写一次 json，让 artifacts 字段反映自身 uri
    store.write_json(json_uri, report.to_dict())
    return {
        "report_uri": json_uri,
        "report_html_uri": html_uri,
        "profile_uri": profile_uri,
    }
