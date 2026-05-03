"""Bulk insert repositories with deduplication helpers."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime

from clickhouse_connect.driver.client import Client

from app.config.constants import (
    CONTRACT_COLUMNS,
    OHLCV_COLUMNS,
    SNAPSHOT_COLUMNS,
    UNDERLYING_COLUMNS,
)
from app.config.settings import Settings, get_settings
from app.db.models import OptionContract, OptionOHLCV, OptionsSnapshot, UnderlyingMapping
from app.utils.logger import get_logger

logger = get_logger(__name__)


class InsertRepository:
    """Write path for ClickHouse tables."""

    def __init__(self, client: Client, settings: Settings | None = None) -> None:
        self.client = client
        self.settings = settings or get_settings()

    def insert_options_ohlcv(
        self,
        rows: Sequence[OptionOHLCV],
        *,
        deduplicate: bool = True,
    ) -> int:
        """Insert OHLCV rows in batches and return inserted row count."""

        if not rows:
            return 0

        rows_to_insert = list(rows)
        if deduplicate:
            existing = self._existing_ohlcv_keys(rows_to_insert)
            rows_to_insert = [row for row in rows_to_insert if row.natural_key() not in existing]

        inserted = 0
        for batch in _batched(rows_to_insert, self.settings.batch_size):
            self.client.insert(
                "options_ohlcv",
                [row.insert_tuple() for row in batch],
                column_names=OHLCV_COLUMNS,
            )
            inserted += len(batch)
            logger.info("inserted_options_ohlcv_batch", extra={"rows": len(batch)})

        if rows and not rows_to_insert:
            logger.info("skipped_duplicate_options_ohlcv", extra={"rows": len(rows)})
        return inserted

    def insert_option_contracts(
        self,
        rows: Sequence[OptionContract],
        *,
        deduplicate: bool = True,
    ) -> int:
        """Insert contract metadata rows."""

        if not rows:
            return 0

        rows_to_insert = list(rows)
        if deduplicate:
            existing = self._existing_contract_keys(rows_to_insert)
            rows_to_insert = [row for row in rows_to_insert if row.natural_key() not in existing]

        inserted = 0
        for batch in _batched(rows_to_insert, self.settings.batch_size):
            self.client.insert(
                "option_contracts",
                [row.insert_tuple() for row in batch],
                column_names=CONTRACT_COLUMNS,
            )
            inserted += len(batch)
            logger.info("inserted_option_contracts_batch", extra={"rows": len(batch)})
        return inserted

    def insert_underlying_mappings(
        self,
        rows: Sequence[UnderlyingMapping],
        *,
        deduplicate: bool = True,
    ) -> int:
        """Insert underlying mappings."""

        if not rows:
            return 0

        rows_to_insert = list(rows)
        if deduplicate:
            existing_ids = self._existing_underlying_ids(rows_to_insert)
            rows_to_insert = [row for row in rows_to_insert if row.underlying_id not in existing_ids]

        inserted = 0
        for batch in _batched(rows_to_insert, self.settings.batch_size):
            self.client.insert(
                "underlying_mapping",
                [row.insert_tuple() for row in batch],
                column_names=UNDERLYING_COLUMNS,
            )
            inserted += len(batch)
            logger.info("inserted_underlying_mapping_batch", extra={"rows": len(batch)})
        return inserted

    def insert_options_snapshots(self, rows: Sequence[OptionsSnapshot]) -> int:
        """Insert option-chain snapshots."""

        if not rows:
            return 0

        inserted = 0
        for batch in _batched(rows, self.settings.batch_size):
            self.client.insert(
                "options_snapshot",
                [row.insert_tuple() for row in batch],
                column_names=SNAPSHOT_COLUMNS,
            )
            inserted += len(batch)
            logger.info("inserted_options_snapshot_batch", extra={"rows": len(batch)})
        return inserted

    def _existing_ohlcv_keys(self, rows: Sequence[OptionOHLCV]) -> set[tuple[str, date, float, str, datetime]]:
        start = min(row.datetime for row in rows)
        end = max(row.datetime for row in rows)
        underlyings = sorted({row.underlying for row in rows})
        expiries = sorted({row.expiry_date for row in rows})

        result = self.client.query(
            """
            SELECT
                underlying,
                expiry_date,
                toFloat32(strike_price),
                toString(option_type),
                datetime
            FROM options_ohlcv
            WHERE datetime BETWEEN {start:DateTime} AND {end:DateTime}
              AND underlying IN {underlyings:Array(String)}
              AND expiry_date IN {expiries:Array(Date)}
            """,
            parameters={
                "start": start,
                "end": end,
                "underlyings": underlyings,
                "expiries": expiries,
            },
        )
        return {
            (str(row[0]), row[1], float(row[2]), str(row[3]), row[4])
            for row in result.result_rows
        }

    def _existing_contract_keys(self, rows: Sequence[OptionContract]) -> set[tuple[str, date, float, str]]:
        underlyings = sorted({row.underlying for row in rows})
        expiries = sorted({row.expiry_date for row in rows})
        result = self.client.query(
            """
            SELECT
                underlying,
                expiry_date,
                toFloat32(strike_price),
                toString(option_type)
            FROM option_contracts
            WHERE underlying IN {underlyings:Array(String)}
              AND expiry_date IN {expiries:Array(Date)}
            """,
            parameters={"underlyings": underlyings, "expiries": expiries},
        )
        return {(str(row[0]), row[1], float(row[2]), str(row[3])) for row in result.result_rows}

    def _existing_underlying_ids(self, rows: Sequence[UnderlyingMapping]) -> set[int]:
        ids = sorted({row.underlying_id for row in rows})
        result = self.client.query(
            """
            SELECT underlying_id
            FROM underlying_mapping
            WHERE underlying_id IN {ids:Array(UInt32)}
            """,
            parameters={"ids": ids},
        )
        return {int(row[0]) for row in result.result_rows}


def _batched[T](values: Sequence[T], size: int) -> Iterable[Sequence[T]]:
    if size < 1:
        raise ValueError("batch size must be >= 1")
    for index in range(0, len(values), size):
        yield values[index : index + size]

