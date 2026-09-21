"""Audit and repair local/Iceberg 1-minute cash data against the trading calendar.

Without an action flag the command only reports. Actions:

- ``--repair-missing``: fetch trading days that have no local candles from Upstox,
  write only those days, and replace them in Iceberg.
- ``--refetch``: re-download the whole range from Upstox, replacing existing local
  files (old files are moved to ``data/_superseded/<run>/``), then republish.
- ``--republish``: replace the Iceberg rows of each symbol/year in the range with
  the deduplicated local files, without fetching.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.models import EquitySymbolReference
from tick_ticker.db.repositories import EquitySymbolReferenceRepository
from tick_ticker.scripts.sync_cash_data import date_from_cash_path
from tick_ticker.scripts.sync_cash_upstox_data import iter_calendar_month_chunks, resolve_upstox_instrument_key
from tick_ticker.services.cash_coverage import CALENDAR_SYMBOL, SymbolGap, find_symbol_gaps, group_day_ranges, scan_local_cash
from tick_ticker.services.cash_data import (
    CASH_ARROW_SCHEMA,
    CashOHLCV,
    CashSyncManifest,
    cash_local_path,
    cash_manifest_path,
    transform_upstox_cash_payload,
    write_cash_parquet,
)
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog
from tick_ticker.utils.datetime import last_completed_session_bound, parse_date, utc_now
from tick_ticker.utils.engines import create_d1_client, create_upstox_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class FetchPlan:
    """Month-bounded Upstox requests for one symbol and the days they may write."""

    symbol: EquitySymbolReference
    chunks: tuple[tuple[date, date], ...]
    target_days: frozenset[date] | None  # None: every returned day may replace local files


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    to_date = min(parse_date(args.to_date), last_completed_session_bound()) if args.to_date else last_completed_session_bound()
    if args.from_date:
        from_date = parse_date(args.from_date)
    elif args.since_sessions:
        # Enough calendar days to contain N sessions around long holiday breaks.
        from_date = to_date - timedelta(days=args.since_sessions * 2 + 14)
    else:
        from_date = date(2016, 1, 1)
    symbols = set(args.nse_symbols) if args.nse_symbols else None
    # The calendar needs every symbol, so scan broadly and filter afterwards.
    inventory = scan_local_cash(settings.data_dir, from_date=from_date, to_date=to_date)
    calendar = inventory.trading_calendar()
    if args.repair_missing and not args.dry_run and (symbols is None or CALENDAR_SYMBOL in symbols):
        # A session no symbol has yet (provider published late) is invisible to a
        # data-derived calendar. Probe the index for weekdays after the last known
        # session; if it has candles, the day joins the calendar for everyone.
        if probe_calendar_symbol(settings, calendar, to_date, run_id=utc_now().strftime("%Y%m%dT%H%M%S")):
            inventory = scan_local_cash(settings.data_dir, from_date=from_date, to_date=to_date)
            calendar = inventory.trading_calendar()
    if args.since_sessions:
        from_date = max(from_date, calendar[-args.since_sessions]) if len(calendar) >= args.since_sessions else from_date
    calendar = [day for day in calendar if from_date <= day <= to_date]

    gaps = find_symbol_gaps(inventory, calendar, symbols=sorted(symbols) if symbols else None)
    report_gaps(gaps, calendar)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(
                {
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "sessions": len(calendar),
                    "gaps": {
                        gap.nse_symbol: [f"{start}..{end}" for start, end in group_day_ranges(gap.missing_days, calendar)]
                        for gap in gaps
                    },
                },
                indent=2,
            )
        )

    if not (args.repair_missing or args.refetch or args.republish):
        return

    selected = sorted(symbols) if symbols else sorted({gap.nse_symbol for gap in gaps})
    if args.refetch or args.republish:
        if not symbols:
            raise SystemExit("--refetch/--republish need --nse-symbols or --symbols-file")

    run_id = utc_now().strftime("%Y%m%dT%H%M%S")
    written: dict[str, set[date]] = defaultdict(set)
    if args.repair_missing or args.refetch:
        reference_repo = EquitySymbolReferenceRepository(create_d1_client(settings))
        plans = build_fetch_plans(reference_repo, gaps, selected, from_date, to_date, refetch=args.refetch)
        request_count = sum(len(plan.chunks) for plan in plans)
        budget = settings.upstox_max_requests_per_run if args.upstox_max_requests is None else args.upstox_max_requests
        logger.info("cash_repair_fetch_planned symbols=%s upstox_requests=%s budget=%s dry_run=%s", len(plans), request_count, budget, args.dry_run)
        if budget and request_count > budget:
            raise SystemExit(f"Planned {request_count} Upstox requests exceeds budget {budget}; narrow the range or raise --upstox-max-requests")
        if args.dry_run:
            for plan in plans:
                logger.info("cash_repair_fetch_plan symbol=%s months=%s days=%s", plan.symbol.nse_symbol, len(plan.chunks), "all" if plan.target_days is None else len(plan.target_days))
            return
        for plan in plans:
            written[plan.symbol.nse_symbol] |= execute_fetch_plan(settings, plan, run_id=run_id)

    if args.dry_run:
        return
    iceberg = IcebergMarketDataCatalog(settings)
    if args.republish or args.refetch:
        for symbol in selected:
            republish_symbol_range(settings, iceberg, symbol, from_date, to_date, run_id=run_id)
            mark_range_uploaded(settings.data_dir, symbol, from_date, to_date)
    else:
        for symbol, days in sorted(written.items()):
            if days:
                republish_symbol_days(settings, iceberg, symbol, sorted(days), run_id=run_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nse-symbols", nargs="+", action="extend", help="Symbols to check or repair (spaces/commas).")
    parser.add_argument("--symbols-file", type=Path, help="Symbols separated by whitespace or commas; # comments supported.")
    parser.add_argument("--from-date", help="Inclusive start date. Default 2016-01-01.")
    parser.add_argument("--to-date", help="Inclusive end date. Default and maximum: yesterday (IST).")
    parser.add_argument("--since-sessions", type=int, help="Only check the latest N trading sessions (for the daily cron).")
    parser.add_argument("--report", type=Path, help="Write the gap report as JSON.")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--repair-missing", action="store_true", help="Fetch missing trading days from Upstox and replace them in Iceberg.")
    action.add_argument("--refetch", action="store_true", help="Re-download the full range from Upstox, replace local files, republish.")
    action.add_argument("--republish", action="store_true", help="Replace Iceberg rows per symbol/year from deduplicated local files.")
    parser.add_argument("--upstox-max-requests", type=int, help="Upstox request cap for this run. Defaults to UPSTOX_MAX_REQUESTS_PER_RUN; 0 disables.")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; no fetches or writes.")
    args = parser.parse_args()
    requested = list(args.nse_symbols or [])
    if args.symbols_file is not None:
        requested.extend(line.split("#", 1)[0] for line in args.symbols_file.read_text().splitlines())
    args.nse_symbols = list(dict.fromkeys(token.upper() for value in requested for token in re.split(r"[\s,]+", value.strip()) if token))
    return args


def report_gaps(gaps: list[SymbolGap], calendar: list[date]) -> None:
    if not calendar:
        logger.warning("cash_coverage_no_calendar")
        return
    logger.info("cash_coverage_checked sessions=%s from=%s to=%s symbols_with_gaps=%s", len(calendar), calendar[0], calendar[-1], len(gaps))
    for gap in gaps:
        ranges = group_day_ranges(gap.missing_days, calendar)
        rendered = ",".join(f"{start}..{end}" if start != end else str(start) for start, end in ranges[:12])
        logger.info("cash_coverage_gap symbol=%s missing_days=%s ranges=%s%s", gap.nse_symbol, len(gap.missing_days), rendered, ",..." if len(ranges) > 12 else "")


def build_fetch_plans(
    reference_repo: EquitySymbolReferenceRepository,
    gaps: list[SymbolGap],
    symbols: list[str],
    from_date: date,
    to_date: date,
    *,
    refetch: bool,
) -> list[FetchPlan]:
    gaps_by_symbol = {gap.nse_symbol: gap for gap in gaps}
    plans: list[FetchPlan] = []
    for name in symbols:
        reference = reference_repo.get_by_nse_symbol(name)
        if reference is None:
            logger.warning("cash_repair_symbol_not_in_reference symbol=%s", name)
            continue
        if refetch:
            plans.append(FetchPlan(symbol=reference, chunks=tuple(iter_calendar_month_chunks(from_date, to_date)), target_days=None))
            continue
        gap = gaps_by_symbol.get(name)
        if gap is None:
            continue
        months = sorted({(day.year, day.month) for day in gap.missing_days})
        chunks = tuple(
            (max(from_date, date(year, month, 1)), min(to_date, _month_end(year, month)))
            for year, month in months
        )
        plans.append(FetchPlan(symbol=reference, chunks=chunks, target_days=frozenset(gap.missing_days)))
    return plans


def probe_calendar_symbol(settings: Settings, calendar: list[date], to_date: date, *, run_id: str) -> bool:
    """Fetch the calendar symbol for weekdays after the last known session; return True if any day was added."""

    if not calendar:
        return False
    probe_days = [
        calendar[-1] + timedelta(days=offset)
        for offset in range(1, (to_date - calendar[-1]).days + 1)
        if (calendar[-1] + timedelta(days=offset)).weekday() < 5
    ]
    if not probe_days:
        return False
    reference = EquitySymbolReferenceRepository(create_d1_client(settings)).get_by_nse_symbol(CALENDAR_SYMBOL)
    if reference is None:
        return False
    months = sorted({(day.year, day.month) for day in probe_days})
    chunks = tuple((max(probe_days[0], date(year, month, 1)), min(to_date, _month_end(year, month))) for year, month in months)
    written = execute_fetch_plan(settings, FetchPlan(symbol=reference, chunks=chunks, target_days=frozenset(probe_days)), run_id=run_id)
    logger.info("cash_calendar_probe symbol=%s probed=%s sessions_found=%s", CALENDAR_SYMBOL, len(probe_days), len(written))
    if written:
        republish_symbol_days(settings, IcebergMarketDataCatalog(settings), CALENDAR_SYMBOL, sorted(written), run_id=run_id)
    return bool(written)


def execute_fetch_plan(settings: Settings, plan: FetchPlan, *, run_id: str) -> set[date]:
    """Fetch month chunks and write the planned days; return days written with candles."""

    upstox = create_upstox_client(settings)
    instrument_key = resolve_upstox_instrument_key(plan.symbol)
    written: set[date] = set()
    for chunk_start, chunk_end in plan.chunks:
        payload = upstox.get_historical_cash(instrument_key=instrument_key, from_date=chunk_start, to_date=chunk_end)
        rows = transform_upstox_cash_payload(
            payload,
            nse_symbol=plan.symbol.nse_symbol,
            exchange_code=settings.cash_exchange_code,
            product_type=settings.cash_product_type,
        )
        rows_by_date: dict[date, list[CashOHLCV]] = defaultdict(list)
        for row in rows:
            rows_by_date[row.trade_date].append(row)
        for trade_date, date_rows in sorted(rows_by_date.items()):
            if plan.target_days is not None and trade_date not in plan.target_days:
                continue
            replace_local_day(settings.data_dir, plan.symbol.nse_symbol, trade_date, date_rows, run_id=run_id)
            written.add(trade_date)
        if plan.target_days is not None:
            still_missing = sorted(day for day in plan.target_days if chunk_start <= day <= chunk_end and day not in rows_by_date)
            if still_missing:
                logger.warning("cash_repair_provider_has_no_candles symbol=%s days=%s", plan.symbol.nse_symbol, ",".join(map(str, still_missing)))
        logger.info("cash_repair_chunk_fetched symbol=%s from=%s to=%s days_written=%s", plan.symbol.nse_symbol, chunk_start, chunk_end, len(rows_by_date))
    record_manifest_files(settings.data_dir, plan.symbol, written)
    return written


def replace_local_day(data_dir: Path, nse_symbol: str, trade_date: date, rows: list[CashOHLCV], *, run_id: str) -> Path:
    """Atomically replace one local day, keeping any previous non-empty file under _superseded."""

    path = cash_local_path(data_dir, trade_date, nse_symbol)
    tmp_path = path.with_name(f".{path.name}.tmp")
    write_cash_parquet(sorted(rows, key=lambda row: row.datetime), tmp_path)
    if path.exists() and pq.ParquetFile(path).metadata.num_rows:
        backup = data_dir / "_superseded" / run_id / path.relative_to(data_dir)
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup)
    os.replace(tmp_path, path)
    return path


def record_manifest_files(data_dir: Path, symbol: EquitySymbolReference, days: set[date]) -> None:
    """Keep the sync manifest aware of repaired files so normal syncs treat them as done."""

    manifest_path = cash_manifest_path(data_dir, symbol.nse_symbol)
    manifest = CashSyncManifest.load(manifest_path)
    if manifest is None or not days:
        return
    paths = {str(cash_local_path(data_dir, day, symbol.nse_symbol)) for day in days}
    manifest.fetched_files = sorted(set(manifest.fetched_files) | paths)
    manifest.uploaded_files = sorted(set(manifest.uploaded_files) | paths)
    manifest.save(manifest_path)


def mark_range_uploaded(data_dir: Path, nse_symbol: str, from_date: date, to_date: date) -> None:
    """After a range republish, record the manifest's fetched files in that range as uploaded.

    A `--fetch-only` backfill leaves them pending, and a pending manifest blocks
    the next incremental sync from starting a new range.
    """

    manifest_path = cash_manifest_path(data_dir, nse_symbol)
    manifest = CashSyncManifest.load(manifest_path)
    if manifest is None:
        return
    in_range = [path for path in manifest.fetched_files if from_date <= date_from_cash_path(path) <= to_date]
    manifest.uploaded_files = sorted(set(manifest.uploaded_files) | set(in_range))
    manifest.save(manifest_path)


def read_local_days(data_dir: Path, nse_symbol: str, days: list[date]) -> pa.Table | None:
    """Read local days as one deduplicated table (last row per timestamp wins)."""

    tables = []
    for day in days:
        path = cash_local_path(data_dir, day, nse_symbol)
        if path.exists() and pq.ParquetFile(path).metadata.num_rows:
            tables.append(pq.read_table(path).cast(CASH_ARROW_SCHEMA))
    if not tables:
        return None
    table = pa.concat_tables(tables).sort_by("datetime")
    stamps = table.column("datetime").to_pylist()
    keep = [index for index, stamp in enumerate(stamps) if index + 1 == len(stamps) or stamps[index + 1] != stamp]
    return table.take(pa.array(keep)) if len(keep) != table.num_rows else table


def republish_symbol_range(settings: Settings, iceberg: IcebergMarketDataCatalog, nse_symbol: str, from_date: date, to_date: date, *, run_id: str) -> None:
    """Replace one symbol's Iceberg rows in the range with local files, in a single commit.

    Catalog commits dominate the cost, and a decade of one symbol's minute
    candles (~1M rows) fits comfortably in memory.
    """

    days = [from_date + timedelta(days=offset) for offset in range((to_date - from_date).days + 1)]
    table = read_local_days(settings.data_dir, nse_symbol, days)
    if table is None:
        logger.info("cash_republish_no_local_rows symbol=%s from=%s to=%s", nse_symbol, from_date, to_date)
        return
    iceberg.replace_symbol_range(
        "cash",
        table,
        nse_symbol=nse_symbol,
        from_date=from_date,
        to_date=to_date,
        snapshot_properties=_snapshot_properties(nse_symbol, from_date, to_date, run_id),
    )
    logger.info("cash_republished symbol=%s from=%s to=%s rows=%s days=%s", nse_symbol, from_date, to_date, table.num_rows, len(pc.unique(table.column("trade_date"))))


def republish_symbol_days(settings: Settings, iceberg: IcebergMarketDataCatalog, nse_symbol: str, days: list[date], *, run_id: str) -> None:
    """Replace specific symbol/days in Iceberg, one snapshot per calendar year."""

    by_year: dict[int, list[date]] = defaultdict(list)
    for day in days:
        by_year[day.year].append(day)
    for year_days in by_year.values():
        table = read_local_days(settings.data_dir, nse_symbol, year_days)
        if table is None:
            continue
        iceberg.replace_symbol_days(
            "cash",
            table,
            nse_symbol=nse_symbol,
            trade_dates=year_days,
            snapshot_properties=_snapshot_properties(nse_symbol, min(year_days), max(year_days), run_id),
        )
        logger.info("cash_days_republished symbol=%s days=%s rows=%s", nse_symbol, len(year_days), table.num_rows)


def _snapshot_properties(nse_symbol: str, start: date, end: date, run_id: str) -> dict[str, str]:
    return {
        "tick_ticker.nse_symbol": nse_symbol,
        "tick_ticker.trade_date_from": start.isoformat(),
        "tick_ticker.trade_date_to": end.isoformat(),
        "tick_ticker.write_mode": "replace",
        "tick_ticker.repair_run": run_id,
    }


def _month_end(year: int, month: int) -> date:
    return (date(year + (month == 12), month % 12 + 1, 1)) - timedelta(days=1)


if __name__ == "__main__":
    main()
