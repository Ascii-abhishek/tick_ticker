"""Datetime parsing and range helpers."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone


def parse_datetime(value: str | datetime) -> datetime:
    """Parse API datetime values into naive UTC-compatible datetimes."""

    if isinstance(value, datetime):
        parsed = value
    else:
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%d-%b-%Y %H:%M:%S",
                "%d-%b-%Y",
            ):
                try:
                    parsed = datetime.strptime(value.strip(), fmt)
                    break
                except ValueError:
                    continue
            else:
                raise

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_date(value: str | date | datetime) -> date:
    """Parse date-like values."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return parse_datetime(value).date()


def breeze_datetime(value: datetime | date) -> str:
    """Format a value in the ISO form accepted by Breeze historical APIs."""

    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, time.min)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def chunk_datetime_range(start: datetime, end: datetime, days: int) -> Iterator[tuple[datetime, datetime]]:
    """Yield inclusive chunks over a datetime range."""

    if days < 1:
        raise ValueError("days must be >= 1")
    cursor = start
    step = timedelta(days=days)
    while cursor < end:
        chunk_end = min(cursor + step, end)
        yield cursor, chunk_end
        cursor = chunk_end


def seconds_until_next_minute(now: datetime | None = None) -> float:
    """Return sleep seconds until the next minute boundary."""

    now = now or datetime.now()
    next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1))
    return max((next_minute - now).total_seconds(), 0.0)

