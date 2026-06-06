"""warehouse build window 工具。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator


@dataclass(frozen=True)
class DateRange:
    """[start, end] 闭区间，按天迭代。start <= end 保证。"""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.end < self.start:
            raise ValueError(f"DateRange: end={self.end} earlier than start={self.start}")

    def to_dict(self) -> dict[str, str]:
        return {"start_date": self.start.isoformat(), "end_date": self.end.isoformat()}

    def days(self) -> int:
        return (self.end - self.start).days + 1


def parse_date_range(
    *,
    date_: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> DateRange:
    """解析 (--date, --from-date, --to-date) 组合，统一返回 [start, end]。

    优先级：
        显式 from/to > date 单日 > 默认（昨天，UTC）
    """
    if from_date is not None or to_date is not None:
        start = _coerce_date(from_date) if from_date is not None else _coerce_date(to_date)
        end = _coerce_date(to_date) if to_date is not None else _coerce_date(from_date)
        return DateRange(start=start, end=end)
    if date_ is not None:
        single = _coerce_date(date_)
        return DateRange(start=single, end=single)
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    return DateRange(start=yesterday, end=yesterday)


def iter_dates(window: DateRange) -> Iterator[date]:
    """逐日迭代。"""
    cur = window.start
    while cur <= window.end:
        yield cur
        cur = cur + timedelta(days=1)


def _coerce_date(value: str | date | None) -> date:
    if value is None:
        raise ValueError("date value is None")
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as err:
        raise ValueError(f"Invalid date '{value}', expected YYYY-MM-DD") from err
