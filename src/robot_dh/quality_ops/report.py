"""quality report 渲染：HTML / JSON / CSV / parquet。"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from robot_dh.quality_ops.summary import QualitySummary, build_quality_summary

LOG = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


@dataclass
class QualityReportArtifacts:
    summary_json: Path
    summary_html: Path
    rule_failure_top10: Path
    dataset_quality_daily: Path
    workflow_sla_summary: Path
    abnormal_partitions: Path
    archive_log_index: Path
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = {k: str(v) for k, v in asdict(self).items() if k != "warnings"}
        d["warnings"] = list(self.warnings)
        return d


class QualityReportRenderer:
    """quality report 渲染器。"""

    def __init__(self) -> None:
        self._env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR.as_posix()),
            autoescape=select_autoescape(["html", "j2"]),
        )
        self._env.globals.update(
            fmt_rate=self._fmt_rate,
            fmt_seconds=self._fmt_seconds,
            fmt_int=self._fmt_int,
            fmt_bytes=self._fmt_bytes,
            fmt_score=self._fmt_score,
        )

    def render(
        self,
        *,
        summary: QualitySummary,
        output_dir: Path,
        prefer_parquet: bool = True,
    ) -> QualityReportArtifacts:
        output_dir.mkdir(parents=True, exist_ok=True)
        warnings: list[str] = []
        summary_data = summary.to_dict()
        generated_at = datetime.now(timezone.utc).isoformat()

        # 1. JSON
        json_path = output_dir / "quality_summary.json"
        json_path.write_text(
            json.dumps(summary_data, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        # 2. HTML
        template = self._env.get_template("quality_summary.html.j2")
        html_path = output_dir / "quality_summary.html"
        html_path.write_text(
            template.render(summary=summary, generated_at=generated_at), encoding="utf-8"
        )

        # 3. CSV / parquet
        top10_path = output_dir / "rule_failure_top10.csv"
        _write_csv(top10_path, [r.to_dict() for r in summary.top_failed_rules])

        dq_rows = list(summary.dashboards)
        if prefer_parquet:
            dq_path, warn = _write_parquet_or_csv(output_dir / "dataset_quality_daily", dq_rows)
            if warn:
                warnings.append(warn)
        else:
            dq_path = output_dir / "dataset_quality_daily.csv"
            _write_csv(dq_path, dq_rows)

        wf_path = output_dir / "workflow_sla_summary.csv"
        _write_csv(wf_path, summary.workflow_ops)

        # abnormal_partitions：从 dashboards 里抽出 alert_level != OK 的行
        abnormal = [d for d in summary.dashboards if (d.get("alert_level") or "OK") != "OK"]
        abnormal_path = output_dir / "abnormal_partitions.csv"
        _write_csv(abnormal_path, abnormal)

        # archive_log_index：archive_log_uris 转 csv
        archive_rows = [{"archive_log_uri": uri} for uri in summary.archive_log_uris]
        archive_path = output_dir / "archive_log_index.csv"
        _write_csv(archive_path, archive_rows)

        return QualityReportArtifacts(
            summary_json=json_path,
            summary_html=html_path,
            rule_failure_top10=top10_path,
            dataset_quality_daily=dq_path,
            workflow_sla_summary=wf_path,
            abnormal_partitions=abnormal_path,
            archive_log_index=archive_path,
            warnings=warnings,
        )

    @staticmethod
    def _fmt_rate(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value * 100:.1f}%"

    @staticmethod
    def _fmt_score(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.1f}"

    @staticmethod
    def _fmt_seconds(value: float | None) -> str:
        if value is None:
            return "-"
        if value < 1:
            return f"{value * 1000:.0f} ms"
        if value < 60:
            return f"{value:.1f} s"
        return f"{value / 60:.1f} min"

    @staticmethod
    def _fmt_int(value: int | None) -> str:
        if value is None:
            return "-"
        return f"{int(value):,}"

    @staticmethod
    def _fmt_bytes(value: int | None) -> str:
        if value is None or value <= 0:
            return "-"
        unit = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
        v = float(value)
        i = 0
        while v >= 1024 and i < len(unit) - 1:
            v /= 1024
            i += 1
        return f"{v:.1f} {unit[i]}"


def render_quality_report(
    *,
    date_: str | None = None,
    output_dir: Path,
    db_uri: str | None = None,
) -> QualityReportArtifacts:
    summary = build_quality_summary(date_=date_, db_uri=db_uri)
    return QualityReportRenderer().render(summary=summary, output_dir=output_dir)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)
    if not keys:
        path.write_text("", encoding="utf-8")
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=keys)
    writer.writeheader()
    for r in rows:
        writer.writerow({k: _csv_value(r.get(k)) for k in keys})
    path.write_text(buf.getvalue(), encoding="utf-8")


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict, set)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _write_parquet_or_csv(stem: Path, rows: list[dict[str, Any]]) -> tuple[Path, str | None]:
    """优先 parquet，缺 pyarrow 时回退 csv。"""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        path = stem.with_suffix(".csv")
        _write_csv(path, rows)
        return path, "pyarrow not available; wrote csv instead of parquet"
    path = stem.with_suffix(".parquet")
    if not rows:
        pq.write_table(pa.table({}), path.as_posix())
        return path, None
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path.as_posix())
    return path, None
