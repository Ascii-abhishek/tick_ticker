"""Generate synthetic index volume from constituent 1-minute candles.

Reads point-in-time membership (D1 by default), computes per-minute volume for
every local index day in the range, records per-day status in D1
``index_volume_state`` and, with ``--publish``, writes the volume into the index's
own Parquet files and replaces those days in Iceberg.

Only ``complete`` days (every trading member has candles) are published unless
``--allow-partial`` is passed. Unpublished days keep the provider volume (0).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.repositories.index_reference import IndexReferenceRepository
from tick_ticker.services.cash_data import CASH_ARROW_SCHEMA, cash_local_path
from tick_ticker.services.cash_history_provider import cash_provider_history_start_date
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog
from tick_ticker.services.index_volume import FORMULA_VERSION, DayVolumeResult, MemberInput, compute_day_volume, membership_fingerprint
from tick_ticker.services.reference_data import MembershipInterval, ReferenceData, load_reference_dir, membership_intervals
from tick_ticker.utils.datetime import last_completed_session_bound, parse_date, utc_now
from tick_ticker.utils.engines import create_d1_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)
PUBLISH_BATCH_DAYS = 60
UPSTOX_MINUTE_HISTORY_START = cash_provider_history_start_date("upstox")


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    repository = None if args.reference_source == "local" and args.dry_run else IndexReferenceRepository(create_d1_client(settings))
    data = reference_from(args, repository)
    definition = next((row for row in data.table("index_definition") if row["index_code"] == args.index), None)
    if definition is None:
        raise SystemExit(f"Unknown index {args.index}; load reference data first")
    index_symbol = str(definition["storage_symbol"])
    intervals = membership_intervals(data, args.index)

    to_date = min(parse_date(args.to_date), last_completed_session_bound()) if args.to_date else last_completed_session_bound()
    from_date = parse_date(args.from_date) if args.from_date else to_date
    days = local_index_days(settings.data_dir, index_symbol, from_date, to_date)
    if args.last_sessions:
        days = days[-args.last_sessions :]
    if not days:
        logger.info("index_volume_no_index_days index=%s from=%s to=%s", args.index, from_date, to_date)
        return

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        computed = list(executor.map(lambda day: compute_for_day(settings.data_dir, index_symbol, intervals, day), days))

    summarize(args.index, computed)
    if args.report:
        write_report(args.report, computed)
    if args.backfill_plan:
        write_backfill_plan(args.backfill_plan, computed, data)
    if args.dry_run:
        return

    publishable = [result for result, _membership in computed if result.status == "complete" or (args.allow_partial and result.status == "partial")]
    if args.publish and publishable:
        publish(settings, index_symbol, publishable, run_id=utc_now().strftime("%Y%m%dT%H%M%S"))
    published_days = {result.trade_date for result in publishable} if args.publish else set()
    assert repository is not None
    repository.upsert_volume_states(args.index, [item for item in computed if item[0].status != "no_index_data"], published_days=published_days)
    logger.info("index_volume_state_recorded index=%s days=%s published=%s", args.index, len(computed), len(published_days))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--index", default="NIFTY50", help="index_definition.index_code. Default NIFTY50.")
    parser.add_argument("--from-date", help="Inclusive start date. Default: --to-date.")
    parser.add_argument("--to-date", help="Inclusive end date. Default and maximum: yesterday (IST).")
    parser.add_argument("--last-sessions", type=int, help="Only the latest N index sessions in the range (daily cron).")
    parser.add_argument("--reference-source", choices=("d1", "local"), default="d1", help="Read membership from D1 (default) or reference/*.csv.")
    parser.add_argument("--reference-dir", type=Path, default=Path("reference"))
    parser.add_argument("--publish", action="store_true", help="Write volume into local index Parquet and replace those days in Iceberg.")
    parser.add_argument("--allow-partial", action="store_true", help="Also publish days with missing member data (not recommended).")
    parser.add_argument("--dry-run", action="store_true", help="Compute and report only; no D1, local, or Iceberg writes.")
    parser.add_argument("--report", type=Path, help="Write a per-day JSON report.")
    parser.add_argument("--workers", type=int, default=8, help="Days computed in parallel (local file reads).")
    parser.add_argument("--backfill-plan", type=Path, help="Write missing member ranges (CSV) with the provider to fetch them from.")
    args = parser.parse_args()
    if args.dry_run and args.publish:
        parser.error("--dry-run cannot be combined with --publish")
    return args


def local_index_days(data_dir: Path, index_symbol: str, from_date: date, to_date: date) -> list[date]:
    days = []
    for path in (data_dir / "cash").glob(f"*/*/*/{index_symbol}.parquet"):
        trade_date = date(int(path.parent.parent.parent.name), int(path.parent.parent.name), int(path.parent.name))
        if from_date <= trade_date <= to_date and pq.ParquetFile(path).metadata.num_rows:
            days.append(trade_date)
    return sorted(days)


def compute_for_day(data_dir: Path, index_symbol: str, intervals: list[MembershipInterval], day: date) -> tuple[DayVolumeResult, str]:
    index_frame = pl.read_parquet(cash_local_path(data_dir, day, index_symbol))
    members = [
        MemberInput(
            security_id=interval.security_id,
            storage_symbol=interval.storage_symbol,
            frame=read_member(data_dir, day, interval.storage_symbol),
            expects_data=interval.trades_on(day),
        )
        for interval in intervals
        if interval.active_on(day)
    ]
    return compute_day_volume(day, index_frame, members), membership_fingerprint(members)


def read_member(data_dir: Path, day: date, storage_symbol: str) -> pl.DataFrame | None:
    path = cash_local_path(data_dir, day, storage_symbol)
    if not path.exists():
        return None
    frame = pl.read_parquet(path, columns=["datetime", "open", "high", "low", "close", "volume"])
    return frame if frame.height else None


def summarize(index_code: str, computed: list[tuple[DayVolumeResult, str]]) -> None:
    statuses = Counter(result.status for result, _membership in computed)
    missing = Counter(symbol for result, _membership in computed for symbol in result.missing_members)
    logger.info(
        "index_volume_computed index=%s formula=%s days=%s from=%s to=%s statuses=%s",
        index_code,
        FORMULA_VERSION,
        len(computed),
        computed[0][0].trade_date,
        computed[-1][0].trade_date,
        dict(statuses),
    )
    if missing:
        logger.info("index_volume_missing_members days_by_symbol=%s", dict(missing.most_common()))


def write_report(path: Path, computed: list[tuple[DayVolumeResult, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            [
                {
                    "trade_date": result.trade_date.isoformat(),
                    "status": result.status,
                    "expected_members": result.expected_members,
                    "observed_members": result.observed_members,
                    "missing_members": result.missing_members,
                    "not_trading_members": result.not_trading_members,
                    "session_minutes": result.session_minutes,
                    "minutes_with_turnover": result.minutes_with_turnover,
                    "total_turnover": round(result.total_turnover, 2),
                    "total_volume": result.total_volume,
                    "dropped_member_rows": result.dropped_member_rows,
                    "membership_fingerprint": membership,
                    "input_fingerprint": result.input_fingerprint,
                }
                for result, membership in computed
            ],
            indent=1,
        )
    )


def write_backfill_plan(path: Path, computed: list[tuple[DayVolumeResult, str]], data: ReferenceData) -> None:
    """Group missing member-days into index-session ranges and pick a provider for each.

    Upstox serves 2022 onwards unless the mapping is marked unavailable (delisted
    ISINs); everything else comes from Breeze.
    """

    sessions = [result.trade_date for result, _membership in computed]
    position = {day: index for index, day in enumerate(sessions)}
    upstox_unavailable = {
        row["security_id"] for row in data.table("security_provider_mapping") if row["provider"] == "upstox" and row["availability"] == "unavailable"
    }
    storage_to_security = {row["storage_symbol"]: row["security_id"] for row in data.table("security_master")}
    missing: dict[tuple[str, str], list[date]] = defaultdict(list)
    for result, _membership in computed:
        for symbol in result.missing_members:
            upstox = result.trade_date >= UPSTOX_MINUTE_HISTORY_START and storage_to_security.get(symbol) not in upstox_unavailable
            missing[(symbol, "upstox" if upstox else "breeze")].append(result.trade_date)
    rows = []
    for (symbol, provider), days in sorted(missing.items()):
        start = previous = days[0]
        for day in days[1:] + [None]:
            if day is not None and position[day] == position[previous] + 1:
                previous = day
                continue
            count = position[previous] - position[start] + 1
            rows.append({"storage_symbol": symbol, "provider": provider, "from_date": start.isoformat(), "to_date": previous.isoformat(), "sessions": count})
            if day is not None:
                start = previous = day
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["storage_symbol", "provider", "from_date", "to_date", "sessions"])
        writer.writeheader()
        writer.writerows(rows)
    logger.info("index_volume_backfill_plan_written path=%s ranges=%s sessions=%s", path, len(rows), sum(row["sessions"] for row in rows))


def publish(settings: Settings, index_symbol: str, results: list[DayVolumeResult], *, run_id: str) -> None:
    """Write local index files atomically, then replace the same days in Iceberg."""

    tables: dict[int, list[tuple[date, pa.Table]]] = defaultdict(list)
    for result in results:
        table = result.index_frame.to_arrow().cast(CASH_ARROW_SCHEMA)
        path = cash_local_path(settings.data_dir, result.trade_date, index_symbol)
        tmp_path = path.with_name(f".{path.name}.tmp")
        pq.write_table(table, tmp_path, compression="zstd")
        os.replace(tmp_path, path)
        tables[result.trade_date.year].append((result.trade_date, table))

    iceberg = IcebergMarketDataCatalog(settings)
    for year, items in sorted(tables.items()):
        for start in range(0, len(items), PUBLISH_BATCH_DAYS):
            batch = items[start : start + PUBLISH_BATCH_DAYS]
            batch_days = [day for day, _table in batch]
            iceberg.replace_symbol_days(
                "cash",
                pa.concat_tables([table for _day, table in batch]),
                nse_symbol=index_symbol,
                trade_dates=batch_days,
                snapshot_properties={
                    "tick_ticker.nse_symbol": index_symbol,
                    "tick_ticker.trade_date_from": min(batch_days).isoformat(),
                    "tick_ticker.trade_date_to": max(batch_days).isoformat(),
                    "tick_ticker.volume_formula": FORMULA_VERSION,
                    "tick_ticker.volume_run": run_id,
                },
            )
            logger.info("index_volume_published symbol=%s year=%s days=%s", index_symbol, year, len(batch_days))


def reference_from(args: argparse.Namespace, repository: IndexReferenceRepository | None) -> ReferenceData:
    if args.reference_source == "local":
        return load_reference_dir(args.reference_dir)
    assert repository is not None
    return repository.fetch_reference()


if __name__ == "__main__":
    main()
