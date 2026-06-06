"""v1.8 SQL 模板加载 + 参数渲染 + 执行。

设计要点：
    1. 不引入 jinja2 等模板引擎，只支持简单 ``{{ var }}`` 占位（与 promptB 第三节一致）。
    2. 参数渲染前会做"危险字符校验"：拒绝包含 ``;`` / 引号 / 注释符的值，避免 SQL injection。
    3. dry-run 模式：只渲染并返回字符串，不连 DB。
    4. transaction=True 时单文件包一个 BEGIN/COMMIT；DDL 文件含 ``CREATE`` 时也走事务（PostgreSQL 支持 DDL 事务）。
    5. 报错时把 SQL 文件名带在 Exception 里，方便回溯。
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

LOG = logging.getLogger(__name__)

# 占位符模式：{{ var }} 或 {{var}}；var 仅允许 [A-Za-z_][A-Za-z0-9_]*
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
# 危险字符：不允许出现在参数值里（避免 SQL injection）；schema / date 等本就限制字符集
_FORBIDDEN_IN_PARAM_RE = re.compile(r"[;'\"\\]|--|/\*|\*/")


class SqlExecutionError(RuntimeError):
    """SQL 执行失败；message 必带文件名。"""


@dataclass
class SqlExecution:
    """一次 SQL 执行的产物。"""

    sql_file: str
    duration_sec: float
    status: str
    affected_rows: int | str = "unknown"
    error: str | None = None
    rendered_sql_preview: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sql_file": self.sql_file,
            "duration_sec": round(self.duration_sec, 4),
            "status": self.status,
            "affected_rows": self.affected_rows,
            "error": self.error,
            "rendered_sql_preview": self.rendered_sql_preview,
            "extra": dict(self.extra),
        }


class SqlTemplateRunner:
    """SQL 模板加载 + 渲染 + 执行。

    Args:
        engine: SQLAlchemy Engine（PostgreSQL / SQLite 均可）。
        sql_root: warehouse/sql 根目录，找不到时由调用方早 fail。
        default_params: 全局默认参数（如 schema），会被 execute 时显式参数覆盖。
    """

    def __init__(
        self,
        *,
        engine: Engine,
        sql_root: Path | str,
        default_params: dict[str, str] | None = None,
    ) -> None:
        self._engine = engine
        self._sql_root = Path(sql_root)
        self._default_params: dict[str, str] = dict(default_params or {})
        if not self._sql_root.exists():
            raise SqlExecutionError(
                f"sql_root not found: {self._sql_root}; expected warehouse/sql layout"
            )

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def sql_root(self) -> Path:
        return self._sql_root

    def load_sql(self, sql_file: str | Path) -> str:
        """加载并校验 SQL 文件路径；不允许跨出 sql_root。

        允许的输入形式：
            - 绝对路径：必须落在 sql_root 内
            - "warehouse/sql/ddl/xxx.sql"：与 sql_root 的尾段对齐后剥掉前缀
            - "ddl/xxx.sql"：直接与 sql_root 拼接
        """
        candidate = Path(sql_file)
        if candidate.is_absolute():
            resolved = candidate.resolve()
        else:
            sql_root_resolved = self._sql_root.resolve()
            sql_root_parts = sql_root_resolved.parts
            cand_parts = candidate.parts
            joined: Path
            if len(cand_parts) >= 2 and tuple(cand_parts[:2]) == ("warehouse", "sql"):
                joined = sql_root_resolved.joinpath(*cand_parts[2:])
            else:
                joined = sql_root_resolved.joinpath(*cand_parts)
            resolved = joined.resolve()
        sql_root_resolved = self._sql_root.resolve()
        if not str(resolved).startswith(str(sql_root_resolved)):
            raise SqlExecutionError(f"refusing to load SQL outside warehouse tree: {sql_file}")
        if not resolved.exists():
            raise SqlExecutionError(f"SQL file not found: {sql_file} (resolved={resolved})")
        return resolved.read_text(encoding="utf-8")

    def render(self, sql: str, params: dict[str, str] | None = None) -> str:
        """渲染 {{ var }}；param 值会做危险字符校验。"""
        merged = dict(self._default_params)
        if params:
            merged.update({k: str(v) for k, v in params.items() if v is not None})

        for k, v in merged.items():
            if _FORBIDDEN_IN_PARAM_RE.search(str(v)):
                raise SqlExecutionError(
                    f"unsafe parameter value for '{k}': contains forbidden characters: {v!r}"
                )

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in merged:
                raise SqlExecutionError(
                    f"missing template parameter '{{{{ {name} }}}}'; provided keys={sorted(merged.keys())}"
                )
            return str(merged[name])

        return _PLACEHOLDER_RE.sub(replace, sql)

    def execute(
        self,
        sql_file: str | Path,
        *,
        params: dict[str, str] | None = None,
        dry_run: bool = False,
        transaction: bool = True,
    ) -> SqlExecution:
        """加载 + 渲染 + 执行。dry_run=True 时只返回渲染结果不落库。"""
        sql_file_str = str(sql_file)
        try:
            raw_sql = self.load_sql(sql_file)
        except SqlExecutionError as err:
            return SqlExecution(
                sql_file=sql_file_str,
                duration_sec=0.0,
                status="error",
                error=str(err),
                rendered_sql_preview="",
            )

        try:
            rendered = self.render(raw_sql, params=params)
        except SqlExecutionError as err:
            return SqlExecution(
                sql_file=sql_file_str,
                duration_sec=0.0,
                status="error",
                error=str(err),
                rendered_sql_preview=raw_sql[:200],
            )

        preview = rendered[:400]
        if dry_run:
            return SqlExecution(
                sql_file=sql_file_str,
                duration_sec=0.0,
                status="dry-run",
                affected_rows="unknown",
                rendered_sql_preview=preview,
            )

        start = time.monotonic()
        try:
            affected = self._execute_with_engine(rendered, transaction=transaction)
        except SQLAlchemyError as err:
            return SqlExecution(
                sql_file=sql_file_str,
                duration_sec=time.monotonic() - start,
                status="error",
                error=f"{type(err).__name__}: {err}",
                rendered_sql_preview=preview,
            )

        return SqlExecution(
            sql_file=sql_file_str,
            duration_sec=time.monotonic() - start,
            status="ok",
            affected_rows=affected,
            rendered_sql_preview=preview,
        )

    def _execute_with_engine(self, sql: str, *, transaction: bool) -> int | str:
        """实际执行；自动按"语句分号"拆分（PostgreSQL psycopg 支持多语句，但 SQLite 必须一条一条 execute）。"""
        statements = [s.strip() for s in self._split_statements(sql) if s.strip()]
        if not statements:
            return 0
        with self._engine.connect() as conn:
            if transaction:
                with conn.begin():
                    return self._run_statements(conn, statements)
            return self._run_statements(conn, statements)

    @staticmethod
    def _run_statements(conn: Connection, statements: list[str]) -> int | str:
        total: int = 0
        unknown = False
        for stmt in statements:
            result = conn.execute(text(stmt))
            rowcount = getattr(result, "rowcount", None)
            if rowcount is None or rowcount < 0:
                unknown = True
            else:
                total += rowcount
        return "unknown" if unknown and total == 0 else total

    @staticmethod
    def _split_statements(sql: str) -> list[str]:
        """按 ';' 拆分 SQL，跳过单引号字符串内的分号。"""
        out: list[str] = []
        cur: list[str] = []
        in_string = False
        i = 0
        while i < len(sql):
            ch = sql[i]
            if ch == "'":
                in_string = not in_string
                cur.append(ch)
            elif ch == ";" and not in_string:
                out.append("".join(cur))
                cur = []
            else:
                cur.append(ch)
            i += 1
        if cur:
            out.append("".join(cur))
        return out
