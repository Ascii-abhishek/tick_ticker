"""Backfill local cash Parquet files into the symbol-optimized Iceberg table."""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tick_ticker.config import Settings, get_settings
from tick_ticker.services.cash_data import CASH_ARROW_SCHEMA, read_cash_row_count
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class CashSymbolBackfillBatch:
    """One symbol/year batch of local files to append into Iceberg."""

    nse_symbol: str
    year: int
    batch_index: int
    paths: tuple[Path, ...]

    @property
    def source_paths(self) -> list[str]:
        return [str(path) for path in self.paths]


@dataclass(frozen=True)
class PreparedCashSymbolBatch:
    """A local batch after reading and sorting its Arrow rows."""

    batch: CashSymbolBackfillBatch
    table: pa.Table
    row_count: int
    trade_date_from: date
    trade_date_to: date


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = parse_args()
    workers = resolve_positive(args.workers, settings.cash_symbol_upload_workers, "workers")
    batch_size = resolve_positive(args.batch_size, settings.cash_symbol_upload_batch_size, "batch-size")

    result = backfill_cash_symbol_table(
        settings,
        nse_symbols={symbol.upper() for symbol in args.nse_symbol} if args.nse_symbol else None,
        years=set(args.year) if args.year else None,
        workers=workers,
        batch_size=batch_size,
        dry_run=args.dry_run,
        max_batches=args.max_batches,
    )
    logger.info(
        "cash_symbol_backfill_finished planned=%s committed=%s skipped_files=%s rows=%s",
        result["planned_batches"],
        result["committed_batches"],
        result["skipped_files"],
        result["committed_rows"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nse-symbol", action="append", help="Backfill only this NSE symbol. Can be passed multiple times.")
    parser.add_argument("--year", action="append", type=int, help="Backfill only this trade year. Can be passed multiple times.")
    parser.add_argument("--workers", type=int, help="Concurrent local batch preparation workers. Defaults to CASH_SYMBOL_UPLOAD_WORKERS.")
    parser.add_argument("--batch-size", type=int, help="Local Parquet files per Iceberg append. Defaults to CASH_SYMBOL_UPLOAD_BATCH_SIZE.")
    parser.add_argument("--max-batches", type=int, help="Stop after this many planned batches, useful for smoke tests.")
    parser.add_argument("--dry-run", action="store_true", help="Plan batches without writing to Iceberg.")
    return parser.parse_args()


def backfill_cash_symbol_table(
    settings: Settings,
    *,
    nse_symbols: set[str] | None = None,
    years: set[int] | None = None,
    workers: int,
    batch_size: int,
    dry_run: bool = False,
    max_batches: int | None = None,
) -> dict[str, int]:
    """Backfill local cash files into the symbol/year Iceberg table."""

    iceberg = IcebergMarketDataCatalog(settings)
    table_ids = iceberg.ensure_market_data_tables()
    table_id = ".".join(table_ids["cash"])
    table = iceberg.load_market_table("cash")
    committed_source_paths = iceberg.committed_source_paths("cash")
    batches = list(
        discover_cash_symbol_batches(
            settings.data_dir,
            committed_source_paths=committed_source_paths,
            nse_symbols=nse_symbols,
            years=years,
            batch_size=batch_size,
        )
    )
    if max_batches is not None:
        batches = batches[:max_batches]

    skipped_files = len(committed_source_paths)
    planned_files = sum(len(batch.paths) for batch in batches)
    logger.info(
        "cash_symbol_backfill_planned table=%s batches=%s files=%s skipped_committed_files=%s dry_run=%s",
        table_id,
        len(batches),
        planned_files,
        skipped_files,
        dry_run,
    )
    if dry_run:
        return {
            "planned_batches": len(batches),
            "committed_batches": 0,
            "skipped_files": skipped_files,
            "committed_rows": 0,
        }

    committed_batches = 0
    committed_rows = 0
    for prepared in prepare_batches_concurrently(batches, workers=workers):
        if prepared.row_count == 0:
            logger.info(
                "cash_symbol_backfill_empty_batch_skipped symbol=%s year=%s year_chunk=%s files=%s",
                prepared.batch.nse_symbol,
                prepared.batch.year,
                prepared.batch.batch_index,
                len(prepared.batch.paths),
            )
            continue
        table.append(prepared.table, snapshot_properties=snapshot_properties(prepared))
        committed_batches += 1
        committed_rows += prepared.row_count
        logger.info(
            "cash_symbol_backfill_batch_committed table=%s symbol=%s year=%s year_chunk=%s files=%s rows=%s",
            table_id,
            prepared.batch.nse_symbol,
            prepared.batch.year,
            prepared.batch.batch_index,
            len(prepared.batch.paths),
            prepared.row_count,
        )

    return {
        "planned_batches": len(batches),
        "committed_batches": committed_batches,
        "skipped_files": skipped_files,
        "committed_rows": committed_rows,
    }


def discover_cash_symbol_batches(
    data_dir: Path,
    *,
    committed_source_paths: set[str],
    nse_symbols: set[str] | None,
    years: set[int] | None,
    batch_size: int,
) -> Iterator[CashSymbolBackfillBatch]:
    """Yield stable symbol/year chunks from local cash Parquet files."""

    grouped_paths: dict[tuple[str, int], list[Path]] = defaultdict(list)
    cash_dir = data_dir / "cash"
    for path in sorted(cash_dir.glob("*/*/*/*.parquet")):
        trade_date = date_from_cash_file(path)
        nse_symbol = path.stem.upper()
        if nse_symbols and nse_symbol not in nse_symbols:
            continue
        if years and trade_date.year not in years:
            continue
        if str(path) in committed_source_paths:
            continue
        if read_cash_row_count(path) == 0:
            continue
        grouped_paths[(nse_symbol, trade_date.year)].append(path)

    for (nse_symbol, year), paths in sorted(grouped_paths.items()):
        for batch_index, chunk in enumerate(chunked(paths, batch_size), start=1):
            yield CashSymbolBackfillBatch(
                nse_symbol=nse_symbol,
                year=year,
                batch_index=batch_index,
                paths=tuple(chunk),
            )


def prepare_batches_concurrently(
    batches: Iterable[CashSymbolBackfillBatch],
    *,
    workers: int,
) -> Iterator[PreparedCashSymbolBatch]:
    """Read local Parquet batches concurrently while commits remain ordered."""

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-symbol-backfill") as executor:
        pending: set[Future[PreparedCashSymbolBatch]] = set()
        batch_iter = iter(batches)

        def submit_until_full() -> None:
            while len(pending) < workers:
                try:
                    batch = next(batch_iter)
                except StopIteration:
                    break
                pending.add(executor.submit(prepare_batch, batch))

        submit_until_full()
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()
            submit_until_full()


def prepare_batch(batch: CashSymbolBackfillBatch) -> PreparedCashSymbolBatch:
    """Read, concatenate, and sort one local symbol/year batch."""

    tables = [pq.read_table(path, schema=CASH_ARROW_SCHEMA) for path in batch.paths]
    table = pa.concat_tables(tables, promote_options="default")
    if table.num_rows:
        table = table.sort_by([("nse_symbol", "ascending"), ("trade_date", "ascending"), ("datetime", "ascending")])
    dates = [date_from_cash_file(path) for path in batch.paths]
    return PreparedCashSymbolBatch(
        batch=batch,
        table=table,
        row_count=table.num_rows,
        trade_date_from=min(dates),
        trade_date_to=max(dates),
    )


def snapshot_properties(prepared: PreparedCashSymbolBatch) -> dict[str, str]:
    """Return snapshot metadata for a backfill append."""

    return {
        "tick_ticker.nse_symbol": prepared.batch.nse_symbol,
        "tick_ticker.trade_year": str(prepared.batch.year),
        "tick_ticker.trade_date_from": prepared.trade_date_from.isoformat(),
        "tick_ticker.trade_date_to": prepared.trade_date_to.isoformat(),
        "tick_ticker.source_paths": json.dumps(prepared.batch.source_paths, separators=(",", ":")),
        "tick_ticker.source_path_count": str(len(prepared.batch.paths)),
        "tick_ticker.write_mode": "backfill_symbol_year",
        "tick_ticker.query_layout": "symbol_time_range",
    }


def date_from_cash_file(path: Path) -> date:
    """Parse data/cash/YYYY/MM/DD/SYMBOL.parquet into a date."""

    return date(int(path.parents[2].name), int(path.parents[1].name), int(path.parent.name))


def chunked(values: list[Path], size: int) -> Iterator[list[Path]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def resolve_positive(cli_value: int | None, settings_value: int, label: str) -> int:
    value = cli_value if cli_value is not None else settings_value
    if value < 1:
        raise ValueError(f"{label} must be >= 1")
    return value


if __name__ == "__main__":
    main()
