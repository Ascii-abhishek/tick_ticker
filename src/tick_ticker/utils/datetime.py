"""Datetime helpers for market data ingestion."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Iterator


def parse_date(value: str | date | datetime) -> date:
    """Parse Breeze/D1 date values into a Python date."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    normalized = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            pass
    return datetime.fromisoformat(normalized.replace("Z", "+00:00")).date()


def parse_datetime(value: str | datetime) -> datetime:
    """Parse Breeze datetime values into a naive timestamp."""

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    normalized = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            pass
    return datetime.fromisoformat(normalized.replace("Z", "+00:00")).replace(tzinfo=None)


def breeze_datetime(value: date | datetime, *, end_of_day: bool = False) -> str:
    """Format a value for Breeze historical v2."""

    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.combine(value, time.max if end_of_day else time.min)
    return dt.replace(microsecond=0).isoformat() + ".000Z"


def utc_now() -> datetime:
    """Current UTC timestamp without timezone info for Parquet friendliness."""

    return datetime.now(tz=UTC).replace(tzinfo=None)


def iter_date_chunks(start: date, end: date, *, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Yield inclusive date chunks."""

    if chunk_days < 1:
        raise ValueError("chunk_days must be >= 1")
    current = start
    while current <= end:
        chunk_end = min(end, current + timedelta(days=chunk_days - 1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)
