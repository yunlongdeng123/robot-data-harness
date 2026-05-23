"""benchmark 报表：JSON / Markdown / HTML 渲染。"""

from __future__ import annotations

import html
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from robot_dh.benchmark.models import BenchmarkReport


LOG = logging.getLogger(__name__)


def write_benchmark_artifacts(report: "BenchmarkReport", output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "benchmark_report.json").write_text(
        json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
    )
    (output_dir / "benchmark_report.md").write_text(render_markdown_report(report))
    (output_dir / "benchmark_report.html").write_text(render_html_report(report))


def render_markdown_report(report: "BenchmarkReport") -> str:
    lines = [
        f"# Benchmark report `{report.benchmark_id}`",
        "",
        f"- Suite: **{report.suite_name}**",
        f"- Path: `{report.suite_path}`",
        f"- Status: **{report.status}**",
        f"- Total: {report.total}",
        f"- Passed: {report.passed}",
        f"- Failed: {report.failed}",
        f"- Duration: {report.duration_sec:.2f}s",
        f"- Started: {report.started_at}",
        f"- Finished: {report.finished_at}",
        "",
        "| Case | Mutation | Expected | Actual | Validators | Match | Duration |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for c in report.cases:
        validators = ", ".join(c.expected_failed_validators) or "-"
        actual_v = ", ".join(c.actual_failed_validators) or "-"
        lines.append(
            f"| {c.case_id} | {c.mutation or '-'} | {c.expected_status} | "
            f"{c.actual_status} | exp=`{validators}` got=`{actual_v}` | "
            f"{'YES' if c.match else 'NO'} | {c.duration_sec:.2f}s |"
        )
    return "\n".join(lines) + "\n"


def render_html_report(report: "BenchmarkReport") -> str:
    rows: list[str] = []
    for c in report.cases:
        row_color = "#e6ffe6" if c.match else "#ffe6e6"
        validators = ", ".join(c.expected_failed_validators) or "-"
        actual_v = ", ".join(c.actual_failed_validators) or "-"
        rows.append(
            "<tr style=\"background:" + row_color + "\">"
            f"<td>{html.escape(c.case_id)}</td>"
            f"<td>{html.escape(c.mutation or '-')}</td>"
            f"<td>{html.escape(c.expected_status)}</td>"
            f"<td>{html.escape(c.actual_status)}</td>"
            f"<td>exp=<code>{html.escape(validators)}</code><br>got=<code>{html.escape(actual_v)}</code></td>"
            f"<td>{'YES' if c.match else 'NO'}</td>"
            f"<td>{c.duration_sec:.2f}s</td>"
            "</tr>"
        )
    summary = (
        f"<h1>Benchmark report <code>{html.escape(report.benchmark_id)}</code></h1>"
        f"<p>Suite: <strong>{html.escape(report.suite_name)}</strong></p>"
        f"<p>Status: <strong>{html.escape(report.status)}</strong></p>"
        f"<p>Total: {report.total}, Passed: {report.passed}, Failed: {report.failed}</p>"
        f"<p>Duration: {report.duration_sec:.2f}s</p>"
        f"<p>Started: {html.escape(report.started_at)}</p>"
        f"<p>Finished: {html.escape(report.finished_at)}</p>"
    )
    return (
        "<!doctype html>\n<html><head><meta charset=\"utf-8\">"
        "<title>robot-dh benchmark</title>"
        "<style>body{font-family:Arial,Helvetica,sans-serif;}"
        "table{border-collapse:collapse;}td,th{border:1px solid #888;padding:6px 12px;}"
        "</style></head><body>" + summary
        + "<table><tr><th>Case</th><th>Mutation</th><th>Expected</th>"
        + "<th>Actual</th><th>Failed Validators</th><th>Match</th><th>Duration</th></tr>"
        + "\n".join(rows)
        + "</table></body></html>"
    )


def render_summary_from_dir(benchmark_dir: Path) -> str:
    """根据 benchmark_dir/benchmark_report.json 重新生成 Markdown 摘要。"""
    report_path = benchmark_dir / "benchmark_report.json"
    if not report_path.is_file():
        raise FileNotFoundError(f"benchmark_report.json not found under {benchmark_dir}")
    raw = json.loads(report_path.read_text())
    from robot_dh.benchmark.models import BenchmarkCaseResult, BenchmarkReport  # noqa: WPS433

    cases = [BenchmarkCaseResult(**c) for c in raw.get("cases", []) or []]
    report = BenchmarkReport(
        benchmark_id=str(raw.get("benchmark_id", "")),
        suite_name=str(raw.get("suite_name", "")),
        suite_path=str(raw.get("suite_path", "")),
        status=str(raw.get("status", "")),
        total=int(raw.get("total", 0)),
        passed=int(raw.get("passed", 0)),
        failed=int(raw.get("failed", 0)),
        mismatched=int(raw.get("mismatched", 0)),
        duration_sec=float(raw.get("duration_sec", 0.0)),
        started_at=str(raw.get("started_at", "")),
        finished_at=str(raw.get("finished_at", "")),
        cases=cases,
        output_dir=raw.get("output_dir"),
    )
    return render_markdown_report(report)
