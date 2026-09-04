"""1-second cash OHLCV schema, transformation, and local storage helpers."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict

from tick_ticker.services.cash_data import (
    CASH_ARROW_SCHEMA,
    CashOHLCV,
    CashSyncEvent,
    cash_local_path,
    read_cash_row_count,
    transform_cash_payload,
)
from tick_ticker.utils.datetime import utc_now

CASH_SECOND_STORAGE_NAME = "cash_1s"
CASH_SECOND_MARKET_TYPE = "cash_1s"


@dataclass(frozen=True)
class CashSecondWindow:
    """One Breeze-safe intraday 1-second request window."""

    trade_date: date
    start: datetime
    end: datetime

    @property
    def key(self) -> str:
        return f"{self.trade_date:%Y-%m-%d}/{self.start:%H%M%S}-{self.end:%H%M%S}"


class CashSecondSyncManifest(BaseModel):
    """Local resumability and audit state for one 1-second cash symbol."""

    model_config = ConfigDict(extra="forbid")

    nse_symbol: str
    breeze_code: str
    from_date: date
    to_date: date
    fetched_files: list[str] = []
    uploaded_files: list[str] = []
    fetched_windows: list[str] = []
    coverage_from_date: date | None = None
    coverage_to_date: date | None = None
    coverage_file_count: int = 0
    coverage_row_count: int = 0
    last_fetch: CashSyncEvent | None = None
    last_upload: CashSyncEvent | None = None

    @property
    def row_count(self) -> int:
        return sum(read_cash_row_count(Path(path)) for path in self.fetched_files if Path(path).exists())

    def begin_run(self, from_date: date, to_date: date, *, breeze_code: str | None = None) -> None:
        """Start a fresh resumable run while preserving durable audit fields."""

        if breeze_code is not None:
            self.breeze_code = breeze_code
        self.from_date = from_date
        self.to_date = to_date
        self.fetched_files = []
        self.uploaded_files = []
        self.fetched_windows = []

    def record_fetch_event(self, *, completed_at: datetime | None = None) -> None:
        """Persist the latest completed fetch range."""

        self.last_fetch = CashSyncEvent(
            from_date=self.from_date,
            to_date=self.to_date,
            file_count=len(self.fetched_files),
            row_count=self.row_count,
            completed_at=completed_at or utc_now(),
        )

    def record_upload_event(
        self,
        *,
        table: str,
        coverage_from_date: date,
        coverage_to_date: date,
        coverage_file_count: int,
        coverage_row_count: int,
        completed_at: datetime | None = None,
    ) -> None:
        """Persist the latest completed upload range and durable coverage."""

        completed = completed_at or utc_now()
        self.coverage_from_date = coverage_from_date
        self.coverage_to_date = coverage_to_date
        self.coverage_file_count = coverage_file_count
        self.coverage_row_count = coverage_row_count
        self.last_upload = CashSyncEvent(
            from_date=self.from_date,
            to_date=self.to_date,
            file_count=len(self.uploaded_files),
            row_count=self.row_count,
            completed_at=completed,
            table=table,
        )

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        tmp_path.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        tmp_path.replace(path)

    @classmethod
    def load(cls, path: Path) -> "CashSecondSyncManifest | None":
        if not path.exists():
            return None
        return cls.model_validate(json.loads(path.read_text(encoding="utf-8")))


def transform_cash_second_payload(
    payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    *,
    nse_symbol: str,
    exchange_code: str,
    product_type: str,
) -> list[CashOHLCV]:
    """Normalize Breeze cash 1-second historical rows into analytics rows."""

    return transform_cash_payload(
        payload,
        nse_symbol=nse_symbol,
        exchange_code=exchange_code,
        product_type=product_type,
    )


def cash_second_local_path(data_dir: Path, trade_date: date, nse_symbol: str) -> Path:
    """Return the final local symbol-first daily path for one 1-second file."""

    return (
        data_dir
        / CASH_SECOND_STORAGE_NAME
        / "by_symbol"
        / nse_symbol.upper()
        / f"{trade_date:%Y}"
        / f"{trade_date:%m}"
        / f"{trade_date:%d}.parquet"
    )


def cash_second_window_local_path(data_dir: Path, nse_symbol: str, window: CashSecondWindow) -> Path:
    """Return the temporary local path for one fetched intraday window."""

    return (
        cash_second_window_day_dir(data_dir, nse_symbol, window.trade_date)
        / f"{window.start:%H%M%S}-{window.end:%H%M%S}.parquet"
    )


def cash_second_window_day_dir(data_dir: Path, nse_symbol: str, trade_date: date) -> Path:
    """Return the temporary window directory for one symbol/day."""

    return (
        data_dir
        / CASH_SECOND_STORAGE_NAME
        / "_windows"
        / nse_symbol.upper()
        / f"{trade_date:%Y}"
        / f"{trade_date:%m}"
        / f"{trade_date:%d}"
    )


def cash_second_manifest_path(data_dir: Path, nse_symbol: str) -> Path:
    """Return the 1-second manifest path for one symbol."""

    return data_dir / "state" / CASH_SECOND_STORAGE_NAME / f"{nse_symbol.upper()}.json"


def iter_cash_second_windows(
    from_date: date,
    to_date: date,
    *,
    window_seconds: int,
    market_open_time: time,
    market_close_time: time,
    skip_weekends: bool,
) -> Iterable[CashSecondWindow]:
    """Yield inclusive intraday windows sized under Breeze's response cap."""

    if from_date > to_date:
        return
    if window_seconds < 1:
        raise ValueError("window_seconds must be >= 1")
    if market_close_time < market_open_time:
        raise ValueError("market_close_time must be after market_open_time")

    current_date = from_date
    while current_date <= to_date:
        if skip_weekends and current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        current = datetime.combine(current_date, market_open_time)
        session_end = datetime.combine(current_date, market_close_time)
        while current <= session_end:
            window_end = min(session_end, current + timedelta(seconds=window_seconds - 1))
            yield CashSecondWindow(trade_date=current_date, start=current, end=window_end)
            current = window_end + timedelta(seconds=1)

        current_date += timedelta(days=1)


def parse_market_time(value: str) -> time:
    """Parse HH:MM[:SS] market session settings."""

    return time.fromisoformat(value.strip())


def write_cash_second_parquet(rows: list[CashOHLCV], path: Path, *, row_group_size: int = 65_536) -> None:
    """Write sorted/deduped 1-second cash candles as Parquet."""

    write_cash_second_records([row.model_dump() for row in rows], path, row_group_size=row_group_size)


def write_cash_second_records(
    records: Iterable[Mapping[str, Any]],
    path: Path,
    *,
    row_group_size: int = 65_536,
) -> None:
    """Write sorted/deduped cash 1-second records as Parquet."""

    path.parent.mkdir(parents=True, exist_ok=True)
    deduped: dict[tuple[str, datetime], Mapping[str, Any]] = {}
    for record in records:
        key = (str(record["nse_symbol"]), record["datetime"])
        deduped[key] = record
    sorted_records = sorted(deduped.values(), key=lambda record: (record["nse_symbol"], record["trade_date"], record["datetime"]))
    table = pa.Table.from_pylist([dict(record) for record in sorted_records], schema=CASH_ARROW_SCHEMA)
    pq.write_table(table, path, compression="zstd", row_group_size=row_group_size)


def build_cash_second_daily_file(
    *,
    window_paths: Iterable[Path],
    daily_path: Path,
    row_group_size: int = 65_536,
) -> int:
    """Build one final symbol/day Parquet file from temporary window files."""

    records: list[Mapping[str, Any]] = []
    for path in window_paths:
        if not path.exists():
            raise FileNotFoundError(f"Missing 1-second window file: {path}")
        records.extend(pq.read_table(path, schema=CASH_ARROW_SCHEMA).to_pylist())
    write_cash_second_records(records, daily_path, row_group_size=row_group_size)
    return read_cash_row_count(daily_path)


def write_empty_cash_second_day(path: Path, *, row_group_size: int = 65_536) -> int:
    """Write an empty final 1-second file for a known non-trading day."""

    write_cash_second_records([], path, row_group_size=row_group_size)
    return 0


def is_known_empty_minute_day(data_dir: Path, trade_date: date, nse_symbol: str) -> bool:
    """Return whether the existing 1-minute cache proves this symbol/day is empty."""

    minute_path = cash_local_path(data_dir, trade_date, nse_symbol)
    return minute_path.exists() and read_cash_row_count(minute_path) == 0


def remove_cash_second_window_day(data_dir: Path, nse_symbol: str, trade_date: date) -> None:
    """Remove temporary intraday window files after a final daily file is built."""

    shutil.rmtree(cash_second_window_day_dir(data_dir, nse_symbol, trade_date), ignore_errors=True)


def clear_fetched_windows_for_date(manifest: CashSecondSyncManifest, trade_date: date) -> None:
    """Drop completed day windows from the manifest after finalization."""

    prefix = f"{trade_date:%Y-%m-%d}/"
    manifest.fetched_windows = [window for window in manifest.fetched_windows if not window.startswith(prefix)]
