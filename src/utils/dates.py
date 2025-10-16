"""Date utilities for ingest and UI controls."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable

from zoneinfo import ZoneInfo

import pandas as pd


@dataclass(frozen=True)
class DateWindow:
    """Representation of a date window."""

    start: date
    end: date

    def as_strings(self) -> tuple[str, str]:
        return self.start.isoformat(), self.end.isoformat()


def end_of_today(tz_name: str) -> datetime:
    """Return the end of the current day in the provided timezone."""

    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    return datetime.combine(now.date(), time(23, 59, 59), tz)


def window_from_days_back(days: int, tz_name: str) -> DateWindow:
    """Produce a :class:`DateWindow` covering the trailing *days* in *tz_name*."""

    end_dt = end_of_today(tz_name).date()
    start_dt = end_dt - timedelta(days=days - 1)
    return DateWindow(start=start_dt, end=end_dt)


def to_datetime(value: str | datetime, tz_name: str) -> datetime:
    """Convert a value to a timezone-aware :class:`datetime`."""

    tz = ZoneInfo(tz_name)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=tz)
        return value.astimezone(tz)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def date_range(start: str | date, end: str | date) -> Iterable[date]:
    """Yield each date between *start* and *end* inclusive."""

    if isinstance(start, str):
        start_date = datetime.fromisoformat(start).date()
    else:
        start_date = start
    if isinstance(end, str):
        end_date = datetime.fromisoformat(end).date()
    else:
        end_date = end
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def coverage_ratio(dates: Iterable[pd.Timestamp], start: date, end: date) -> float:
    """Compute date coverage between *start* and *end* from an iterable of timestamps."""

    expected = set(date_range(start, end))
    observed = {d.date() if isinstance(d, pd.Timestamp) else d for d in dates}
    if not expected:
        return 0.0
    return len(expected & observed) / len(expected)
