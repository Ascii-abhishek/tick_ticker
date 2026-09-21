"""Local 1-minute cash coverage checks against the exchange trading calendar."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

# NIFTY trades on every exchange session, so its candles define the calendar.
CALENDAR_SYMBOL = "NIFTY"
# A day also counts as a session when this many symbols have candles, which
# catches days where NIFTY itself is the missing file.
CALENDAR_MIN_SYMBOLS = 20


@dataclass
class LocalCashInventory:
    """Non-empty local cash days per symbol, plus empty/placeholder files."""

    days: dict[str, set[date]] = field(default_factory=lambda: defaultdict(set))
    empty_days: dict[str, set[date]] = field(default_factory=lambda: defaultdict(set))

    def trading_calendar(self, *, min_symbols: int = CALENDAR_MIN_SYMBOLS) -> list[date]:
        counts: dict[date, int] = defaultdict(int)
        for symbol_days in self.days.values():
            for day in symbol_days:
                counts[day] += 1
        calendar = set(self.days.get(CALENDAR_SYMBOL, set()))
        calendar.update(day for day, count in counts.items() if count >= min_symbols)
        return sorted(calendar)


@dataclass(frozen=True)
class SymbolGap:
    """Trading days inside a symbol's active window without local candles."""

    nse_symbol: str
    first_day: date
    last_day: date
    missing_days: tuple[date, ...]


def scan_local_cash(data_dir: Path, *, from_date: date | None = None, to_date: date | None = None, symbols: set[str] | None = None) -> LocalCashInventory:
    """Read Parquet footers under data/cash/YYYY/MM/DD/SYMBOL.parquet."""

    inventory = LocalCashInventory()
    root = data_dir / "cash"
    if not root.exists():
        return inventory
    for year_dir in sorted(root.iterdir()):
        if not year_dir.name.isdigit():
            continue
        if from_date and int(year_dir.name) < from_date.year or to_date and int(year_dir.name) > to_date.year:
            continue
        for dirpath, _dirnames, filenames in os.walk(year_dir):
            day_dir = Path(dirpath)
            try:
                trade_date = date(int(day_dir.parent.parent.name), int(day_dir.parent.name), int(day_dir.name))
            except ValueError:
                continue
            if from_date and trade_date < from_date or to_date and trade_date > to_date:
                continue
            for filename in filenames:
                if not filename.endswith(".parquet"):
                    continue
                symbol = filename.removesuffix(".parquet")
                if symbols is not None and symbol not in symbols:
                    continue
                if _row_count(day_dir / filename):
                    inventory.days[symbol].add(trade_date)
                else:
                    inventory.empty_days[symbol].add(trade_date)
    return inventory


def _row_count(path: Path) -> int:
    """Footer row count; unreadable or truncated files count as empty."""

    try:
        return pq.ParquetFile(path).metadata.num_rows
    except (OSError, ValueError):
        return 0


def find_symbol_gaps(
    inventory: LocalCashInventory,
    calendar: list[date],
    *,
    symbols: list[str] | None = None,
    active_windows: dict[str, tuple[date, date | None]] | None = None,
) -> list[SymbolGap]:
    """Return calendar days missing between each symbol's first and last expected day.

    Without an explicit window the symbol is expected from its first local
    candle through the end of the calendar, so a symbol that stopped updating
    shows every later session as missing.
    """

    gaps: list[SymbolGap] = []
    for symbol in symbols or sorted(inventory.days):
        have = inventory.days.get(symbol, set())
        window = (active_windows or {}).get(symbol)
        if window is not None:
            start, end_exclusive = window
        elif have:
            start, end_exclusive = min(have), None
        else:
            continue
        expected = [day for day in calendar if day >= start and (end_exclusive is None or day < end_exclusive)]
        missing = tuple(day for day in expected if day not in have)
        if expected and missing:
            gaps.append(SymbolGap(nse_symbol=symbol, first_day=expected[0], last_day=expected[-1], missing_days=missing))
    return gaps


def group_day_ranges(days: list[date] | tuple[date, ...], calendar: list[date]) -> list[tuple[date, date]]:
    """Collapse days into ranges that are contiguous on the trading calendar."""

    position = {day: index for index, day in enumerate(calendar)}
    ranges: list[tuple[date, date]] = []
    for day in sorted(days):
        if ranges and day in position and ranges[-1][1] in position and position[day] == position[ranges[-1][1]] + 1:
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges
