"""Cash OHLCV schema, transformation, and local storage helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict

from tick_ticker.utils.datetime import parse_datetime, utc_now


class CashOHLCV(BaseModel):
    """Normalized 1-minute cash OHLCV candle."""

    model_config = ConfigDict(extra="forbid")

    datetime: datetime
    trade_date: date
    nse_symbol: str
    exchange_code: str
    product_type: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    count: int | None = None
    ingested_at: datetime


CASH_ARROW_SCHEMA = pa.schema(
    [
        pa.field("datetime", pa.timestamp("us")),
        pa.field("trade_date", pa.date32()),
        pa.field("nse_symbol", pa.string()),
        pa.field("exchange_code", pa.string()),
        pa.field("product_type", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("count", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us")),
    ]
)


def extract_success_records(payload: Mapping[str, Any] | Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Extract rows from Breeze's Success envelope or a raw iterable."""

    if isinstance(payload, Mapping):
        success = payload.get("Success") or payload.get("success") or payload.get("data") or []
        if isinstance(success, Mapping):
            return [success]
        if isinstance(success, list):
            return [record for record in success if isinstance(record, Mapping)]
        return []
    return [record for record in payload if isinstance(record, Mapping)]


def transform_cash_payload(
    payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
    *,
    nse_symbol: str,
    exchange_code: str,
    product_type: str,
) -> list[CashOHLCV]:
    """Normalize Breeze cash historical rows into NSE-symbol analytics rows."""

    rows: list[CashOHLCV] = []
    ingested_at = utc_now()
    for record in extract_success_records(payload):
        candle_at = parse_datetime(str(_first_present(record, "datetime", "time", "date")))
        rows.append(
            CashOHLCV(
                datetime=candle_at,
                trade_date=candle_at.date(),
                nse_symbol=nse_symbol,
                exchange_code=str(record.get("exchange_code") or exchange_code),
                product_type=str(record.get("product_type") or product_type),
                open=float(record.get("open") or record.get("open_price") or 0),
                high=float(record.get("high") or record.get("high_price") or 0),
                low=float(record.get("low") or record.get("low_price") or 0),
                close=float(record.get("close") or record.get("close_price") or 0),
                volume=max(0, int(float(record.get("volume") or 0))),
                count=_optional_int(record.get("count")),
                ingested_at=ingested_at,
            )
        )
    return rows


def cash_local_path(data_dir: Path, trade_date: date, nse_symbol: str) -> Path:
    """Return the local partitioned path for one symbol/date."""

    return data_dir / "cash" / f"{trade_date:%Y}" / f"{trade_date:%m}" / f"{trade_date:%d}" / f"{nse_symbol}.parquet"


def write_cash_parquet(rows: list[CashOHLCV], path: Path) -> None:
    """Write normalized cash candles as a Parquet file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    records = [row.model_dump() for row in rows]
    table = pa.Table.from_pylist(records, schema=CASH_ARROW_SCHEMA)
    pq.write_table(table, path, compression="zstd")


def read_cash_row_count(path: Path) -> int:
    """Read Parquet row count without loading all data."""

    return pq.ParquetFile(path).metadata.num_rows


class CashSyncManifest(BaseModel):
    """Local resumability state for one symbol."""

    nse_symbol: str
    breeze_code: str
    from_date: date
    to_date: date
    fetched_files: list[str] = []
    uploaded_files: list[str] = []

    @property
    def row_count(self) -> int:
        return sum(read_cash_row_count(Path(path)) for path in self.fetched_files if Path(path).exists())

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "CashSyncManifest | None":
        if not path.exists():
            return None
        return cls.model_validate(json.loads(path.read_text(encoding="utf-8")))


def cash_manifest_path(data_dir: Path, nse_symbol: str) -> Path:
    """Return the manifest path for one symbol."""

    return data_dir / "state" / "cash" / f"{nse_symbol}.json"


def _first_present(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    raise ValueError(f"missing datetime field; expected one of {keys}")


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))
