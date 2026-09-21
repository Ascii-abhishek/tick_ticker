"""Sync pending cash symbols from Breeze into local Parquet and Iceberg."""

from __future__ import annotations

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncCompletion, MarketDataSyncState
from tick_ticker.db.repositories import EquitySymbolReferenceRepository, MarketDataSyncStateRepository
from tick_ticker.services.cash_data import (
    CashSyncManifest,
    cash_local_path,
    cash_manifest_path,
    read_cash_row_count,
    transform_cash_payload,
    write_cash_parquet,
)
from tick_ticker.services.cash_coverage import scan_local_cash
from tick_ticker.services.cash_history_provider import cash_provider_history_start_date
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog
from tick_ticker.utils.datetime import breeze_datetime, iter_date_chunks, last_completed_session_bound, parse_date, utc_now
from tick_ticker.utils.engines import create_breeze_client, create_d1_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)
_CASH_ICEBERG_APPEND_LOCK = threading.Lock()
# Breeze historical v2 returns at most 1000 candles; a 1-minute session with
# pre-open and post-close candles has up to 378.
BREEZE_MAX_CANDLES_PER_REQUEST = 1000
BREEZE_MAX_CANDLES_PER_SESSION = 378
# Pre-2022 local data has few symbols, so a low threshold still rejects noise.
SESSION_CALENDAR_MIN_SYMBOLS = 3


@dataclass(frozen=True)
class CashFetchResult:
    """Outcome from fetching one date chunk."""

    local_files: tuple[str, ...]
    row_count: int
    existed: bool


@dataclass(frozen=True)
class CashUploadTask:
    """One local cash parquet file ready for Iceberg."""

    local_file: str
    local_path: Path
    trade_date: date
    row_count: int


@dataclass(frozen=True)
class CashUploadResult:
    """Outcome from uploading one local cash parquet file."""

    tasks: tuple[CashUploadTask, ...]
    committed: bool


@dataclass(frozen=True)
class CashLocalCoverage:
    """Local cash file coverage for one symbol."""

    from_date: date | None
    to_date: date | None
    file_count: int
    row_count: int


class DateRangeTooLargeError(ValueError):
    """Raised when a requested sync range exceeds the configured safety limit."""


class BreezeRequestBudgetExceededError(RuntimeError):
    """Raised when a run would exceed the configured Breeze request budget."""


class BreezeRequestBudget:
    """Thread-safe counter for Breeze historical requests reserved by one run."""

    def __init__(self, max_requests: int) -> None:
        self.max_requests = max_requests
        self._reserved_requests = 0
        self._lock = threading.Lock()

    @property
    def reserved_requests(self) -> int:
        return self._reserved_requests

    def reserve(self, *, symbol: str, requests: int) -> None:
        if requests <= 0 or self.max_requests <= 0:
            return
        with self._lock:
            next_total = self._reserved_requests + requests
            if next_total > self.max_requests:
                raise BreezeRequestBudgetExceededError(
                    f"Refusing to reserve {requests} Breeze requests for {symbol}; "
                    f"run budget would become {next_total}/{self.max_requests}. "
                    "Raise BREEZE_MAX_REQUESTS_PER_RUN or reduce the symbol/date range."
                )
            self._reserved_requests = next_total


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    d1_client = create_d1_client(settings)
    symbol_repo = EquitySymbolReferenceRepository(d1_client)
    sync_repo = MarketDataSyncStateRepository(d1_client)
    if args.ensure_sync_table:
        sync_repo.ensure_table()

    to_date = resolve_to_date(settings, args)
    download_workers = resolve_worker_count(args.download_workers, settings.cash_download_workers, "download-workers")
    upload_workers = resolve_worker_count(args.upload_workers, settings.cash_upload_workers, "upload-workers")
    upload_batch_size = resolve_worker_count(None, settings.cash_upload_batch_size, "upload-batch-size")
    symbol_workers = resolve_worker_count(args.symbol_workers, settings.cash_symbol_workers, "symbol-workers")
    breeze_request_budget = BreezeRequestBudget(
        resolve_non_negative_count(args.breeze_max_requests, settings.breeze_max_requests_per_run, "breeze-max-requests")
    )

    symbols = resolve_symbols(symbol_repo, sync_repo, args, to_date)
    if not symbols:
        logger.info("no_pending_cash_symbols")
        return

    synced_count, skipped_count = sync_resolved_symbols(
        settings=settings,
        sync_repo=sync_repo,
        symbols=symbols,
        args=args,
        to_date=to_date,
        download_workers=download_workers,
        upload_workers=upload_workers,
        upload_batch_size=upload_batch_size,
        symbol_workers=symbol_workers,
        breeze_request_budget=breeze_request_budget,
    )

    if args.all_symbols:
        logger.info(
            "cash_sync_all_completed symbols=%s synced=%s skipped=%s breeze_requests_reserved=%s breeze_request_budget=%s",
            len(symbols),
            synced_count,
            skipped_count,
            breeze_request_budget.reserved_requests,
            breeze_request_budget.max_requests,
        )


def sync_resolved_symbols(
    *,
    settings: Settings,
    sync_repo: MarketDataSyncStateRepository,
    symbols: list[EquitySymbolReference],
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    symbol_workers: int,
    breeze_request_budget: BreezeRequestBudget | None,
) -> tuple[int, int]:
    """Sync resolved symbols and skip oversized ranges during all-symbol runs."""

    synced_count = 0
    skipped_count = 0

    if symbol_workers == 1 or len(symbols) == 1:
        for symbol in symbols:
            synced, skipped = sync_resolved_symbol(
                settings=settings,
                sync_repo=sync_repo,
                symbol=symbol,
                args=args,
                to_date=to_date,
                download_workers=download_workers,
                upload_workers=upload_workers,
                upload_batch_size=upload_batch_size,
                breeze_request_budget=breeze_request_budget,
            )
            if synced:
                synced_count += 1
            if skipped:
                skipped_count += 1
        return synced_count, skipped_count

    errors: list[BaseException] = []
    worker_count = min(symbol_workers, len(symbols))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="cash-symbol") as executor:
        futures = [
            executor.submit(
                sync_resolved_symbol,
                settings=settings,
                sync_repo=sync_repo,
                symbol=symbol,
                args=args,
                to_date=to_date,
                download_workers=download_workers,
                upload_workers=upload_workers,
                upload_batch_size=upload_batch_size,
                breeze_request_budget=breeze_request_budget,
            )
            for symbol in symbols
        ]
        for future in as_completed(futures):
            try:
                synced, skipped = future.result()
            except BaseException as exc:
                errors.append(exc)
                continue
            if synced:
                synced_count += 1
            if skipped:
                skipped_count += 1
    if errors:
        raise errors[0]
    return synced_count, skipped_count


def sync_resolved_symbol(
    *,
    settings: Settings,
    sync_repo: MarketDataSyncStateRepository,
    symbol: EquitySymbolReference,
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    breeze_request_budget: BreezeRequestBudget | None,
) -> tuple[bool, bool]:
    """Sync one symbol and return (synced, skipped)."""

    try:
        synced = sync_cash_symbol(
            settings=settings,
            sync_repo=sync_repo,
            symbol=symbol,
            args=args,
            to_date=to_date,
            download_workers=download_workers,
            upload_workers=upload_workers,
            upload_batch_size=upload_batch_size,
            breeze_request_budget=breeze_request_budget,
        )
    except (DateRangeTooLargeError, BreezeRequestBudgetExceededError) as exc:
        if not args.all_symbols:
            raise
        logger.warning("cash_symbol_skipped symbol=%s error=%s", symbol.nse_symbol, exc)
        return False, True
    return synced, False


def sync_cash_symbol(
    *,
    settings: Settings,
    sync_repo: MarketDataSyncStateRepository,
    symbol: EquitySymbolReference,
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    breeze_request_budget: BreezeRequestBudget | None = None,
) -> bool:
    """Sync one resolved cash symbol."""

    manifest_path = cash_manifest_path(settings.data_dir, symbol.nse_symbol)
    existing_manifest = CashSyncManifest.load(manifest_path) if args.upload_only else None
    sync_state = sync_repo.get_state(market_type="cash", nse_symbol=symbol.nse_symbol)
    from_date = resolve_from_date(symbol, sync_state, settings, args)
    if existing_manifest is not None and not args.from_date and settings.cash_sync_from_date is None:
        from_date = existing_manifest.from_date
    if existing_manifest is not None and not args.to_date and settings.cash_sync_to_date is None:
        to_date = existing_manifest.to_date
    if from_date > to_date:
        logger.info("cash_symbol_already_synced symbol=%s synced_to=%s", symbol.nse_symbol, sync_state.to_date if sync_state else None)
        return False
    validate_date_range(from_date, to_date, settings, args.allow_large_range)

    local_only = args.fetch_only or args.local_only
    manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        allow_range_reset=local_only,
    )

    if not args.upload_only and breeze_request_budget is not None:
        request_count = count_missing_fetch_requests(settings, symbol, from_date, to_date, manifest)
        breeze_request_budget.reserve(symbol=symbol.nse_symbol, requests=request_count)
        if request_count:
            logger.info(
                "cash_breeze_requests_reserved symbol=%s requests=%s reserved=%s budget=%s",
                symbol.nse_symbol,
                request_count,
                breeze_request_budget.reserved_requests,
                breeze_request_budget.max_requests,
            )

    if not local_only:
        sync_repo.mark_started(
            market_type="cash",
            nse_symbol=symbol.nse_symbol,
            from_date=from_date.isoformat(),
            to_date=to_date.isoformat(),
        )
    try:
        if not args.upload_only:
            fetch_to_local_parquet(settings, symbol, from_date, to_date, manifest, manifest_path, workers=download_workers)
            manifest.record_fetch_event(completed_at=utc_now())
            manifest.save(manifest_path)
        if not local_only:
            cash_table_id = upload_to_iceberg(settings, manifest, manifest_path, workers=upload_workers, batch_size=upload_batch_size)
            local_coverage = cash_local_coverage(settings.data_dir, symbol.nse_symbol)
            coverage_start = local_coverage.from_date or coverage_from_date(sync_state, from_date)
            coverage_end = local_coverage.to_date if local_coverage.to_date and local_coverage.to_date > to_date else to_date
            completed_at = utc_now()
            manifest.record_upload_event(
                table=cash_table_id,
                coverage_from_date=coverage_start,
                coverage_to_date=coverage_end,
                coverage_file_count=local_coverage.file_count,
                coverage_row_count=local_coverage.row_count,
                completed_at=completed_at,
            )
            manifest.save(manifest_path)
            sync_repo.mark_completed(
                MarketDataSyncCompletion(
                    market_type="cash",
                    nse_symbol=symbol.nse_symbol,
                    from_date=coverage_start,
                    to_date=coverage_end,
                    row_count=local_coverage.row_count,
                    local_path=str(settings.data_dir / "cash"),
                    r2_prefix=cash_table_id,
                    completed_at=completed_at,
                )
            )
            logger.info("cash_sync_completed symbol=%s rows=%s", symbol.nse_symbol, local_coverage.row_count)
        else:
            logger.info("cash_local_download_completed symbol=%s rows=%s", symbol.nse_symbol, manifest.row_count)
    except Exception as exc:
        if not local_only:
            sync_repo.mark_failed(market_type="cash", nse_symbol=symbol.nse_symbol, error=str(exc))
        raise
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nse-symbol", help="Sync one NSE symbol only. Example: RELIANCE.")
    parser.add_argument(
        "--all",
        dest="all_symbols",
        action="store_true",
        help="Sync every due cash symbol instead of stopping after the first one.",
    )
    parser.add_argument(
        "--from-date",
        help="Inclusive start date, YYYY-MM-DD. Defaults to CASH_SYNC_FROM_DATE or the provider-supported listing date.",
    )
    parser.add_argument("--to-date", help="Inclusive end date, YYYY-MM-DD. Defaults to CASH_SYNC_TO_DATE or yesterday (IST); later dates are clamped to yesterday.")
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch Breeze data into local Parquet.")
    parser.add_argument("--local-only", action="store_true", help="Only download local Parquet; do not upload to Iceberg or update sync state.")
    parser.add_argument("--upload-only", action="store_true", help="Only upload existing local Parquet files to Iceberg and mark D1.")
    parser.add_argument(
        "--allow-large-range",
        action="store_true",
        help="Allow ranges larger than CASH_SYNC_MAX_DAYS_PER_RUN.",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        help="Concurrent Breeze download workers. Defaults to CASH_DOWNLOAD_WORKERS.",
    )
    parser.add_argument(
        "--symbol-workers",
        type=int,
        help="Concurrent cash symbols to process for --all. Defaults to CASH_SYMBOL_WORKERS.",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        help="Concurrent Iceberg upload workers. Defaults to CASH_UPLOAD_WORKERS.",
    )
    parser.add_argument(
        "--breeze-max-requests",
        type=int,
        help="Maximum Breeze historical requests to reserve in this run. Defaults to BREEZE_MAX_REQUESTS_PER_RUN; 0 disables.",
    )
    parser.add_argument(
        "--ensure-sync-table",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run idempotent CREATE TABLE statements for sync state before syncing.",
    )
    args = parser.parse_args()
    if args.local_only and args.upload_only:
        parser.error("--local-only cannot be used with --upload-only")
    if args.fetch_only and args.upload_only:
        parser.error("--fetch-only cannot be used with --upload-only")
    if args.all_symbols and args.nse_symbol:
        parser.error("--all cannot be used with --nse-symbol")
    return args


def resolve_worker_count(cli_value: int | None, settings_value: int, label: str) -> int:
    """Resolve and validate a bounded worker count."""

    value = cli_value if cli_value is not None else settings_value
    if value < 1:
        raise ValueError(f"{label} must be >= 1")
    return value


def resolve_non_negative_count(cli_value: int | None, settings_value: int, label: str) -> int:
    """Resolve and validate a non-negative numeric limit."""

    value = cli_value if cli_value is not None else settings_value
    if value < 0:
        raise ValueError(f"{label} must be >= 0")
    return value


def resolve_symbol(
    symbol_repo: EquitySymbolReferenceRepository,
    sync_repo: MarketDataSyncStateRepository,
    args: argparse.Namespace,
    to_date: date,
) -> EquitySymbolReference | None:
    """Resolve either an explicit NSE symbol or the next due cash symbol."""

    if args.nse_symbol:
        symbol = symbol_repo.get_by_nse_symbol(args.nse_symbol)
        if symbol is None:
            raise ValueError(f"NSE symbol not found in equity_symbol_reference: {args.nse_symbol}")
        return symbol
    return sync_repo.next_due_cash_symbol(target_to_date=to_date)


def resolve_symbols(
    symbol_repo: EquitySymbolReferenceRepository,
    sync_repo: MarketDataSyncStateRepository,
    args: argparse.Namespace,
    to_date: date,
) -> list[EquitySymbolReference]:
    """Resolve one explicit/next symbol, or all due cash symbols."""

    if args.all_symbols:
        return sync_repo.due_cash_symbols(target_to_date=to_date, synced_only=getattr(args, "synced_only", False))

    symbol = resolve_symbol(symbol_repo, sync_repo, args, to_date)
    return [symbol] if symbol is not None else []


def resolve_to_date(settings: Settings, args: argparse.Namespace) -> date:
    """Resolve the inclusive end date, never later than the last completed session."""

    requested = parse_date(args.to_date) if args.to_date else settings.cash_sync_to_date
    bound = last_completed_session_bound()
    if requested is None:
        return bound
    if requested > bound:
        logger.warning("cash_to_date_clamped requested_to_date=%s last_completed_session_bound=%s", requested, bound)
        return bound
    return requested


def resolve_from_date(
    symbol: EquitySymbolReference,
    sync_state: MarketDataSyncState | None,
    settings: Settings,
    args: argparse.Namespace,
) -> date:
    """Resolve the inclusive start date from args, env, sync state, or default policy."""

    if args.from_date:
        return parse_date(args.from_date)
    if settings.cash_sync_from_date:
        return settings.cash_sync_from_date

    if sync_state and sync_state.status == "completed" and sync_state.to_date:
        return sync_state.to_date + timedelta(days=1)
    if sync_state and sync_state.status in {"in_progress", "failed"} and sync_state.from_date:
        return sync_state.from_date

    return resolve_default_from_date(symbol, settings)


def resolve_default_from_date(symbol: EquitySymbolReference, settings: Settings) -> date:
    """Resolve the default start date supported by the current cash data provider."""

    listing_date = symbol.listing_date
    if listing_date is None:
        raise ValueError(
            f"No start date for {symbol.nse_symbol}; pass --from-date or set CASH_SYNC_FROM_DATE because listing_date is empty."
        )

    provider_start_date = cash_provider_history_start_date(settings.cash_history_provider)
    if provider_start_date is None:
        return listing_date
    return max(listing_date, provider_start_date)


def coverage_from_date(sync_state: MarketDataSyncState | None, run_from_date: date) -> date:
    """Keep completed state as total known coverage where possible."""

    if sync_state and sync_state.status == "completed" and sync_state.from_date:
        return min(sync_state.from_date, run_from_date)
    return run_from_date


def load_or_create_manifest(
    *,
    manifest_path: Path,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    allow_range_reset: bool,
) -> CashSyncManifest:
    """Load a reusable manifest or safely start a new range."""

    manifest = CashSyncManifest.load(manifest_path)
    if manifest is None:
        manifest = CashSyncManifest(
            nse_symbol=symbol.nse_symbol,
            breeze_code=symbol.breeze_code,
            from_date=from_date,
            to_date=to_date,
        )
        manifest.save(manifest_path)
        return manifest

    if manifest.from_date == from_date and manifest.to_date == to_date:
        return manifest

    fully_uploaded = bool(manifest.fetched_files) and set(manifest.fetched_files).issubset(set(manifest.uploaded_files))
    if fully_uploaded or allow_range_reset:
        manifest.begin_run(from_date, to_date, breeze_code=symbol.breeze_code)
        manifest.save(manifest_path)
        return manifest

    if ranges_touch_or_overlap(manifest.from_date, manifest.to_date, from_date, to_date):
        manifest.from_date = min(manifest.from_date, from_date)
        manifest.to_date = max(manifest.to_date, to_date)
        manifest.save(manifest_path)
        return manifest

    raise RuntimeError(
        f"Existing manifest range is {manifest.from_date}..{manifest.to_date}, "
        f"but requested {from_date}..{to_date}. Finish or remove {manifest_path} before changing ranges."
    )


def ranges_touch_or_overlap(left_from: date, left_to: date, right_from: date, right_to: date) -> bool:
    """Return true when two inclusive ranges can share one resumable manifest."""

    return left_from <= right_to + timedelta(days=1) and right_from <= left_to + timedelta(days=1)


def validate_date_range(from_date: date, to_date: date, settings: Settings, allow_large_range: bool) -> None:
    if from_date > to_date:
        raise ValueError(f"from-date {from_date} is after to-date {to_date}")

    days = (to_date - from_date).days + 1
    if not allow_large_range and days > settings.cash_sync_max_days_per_run:
        raise DateRangeTooLargeError(
            f"Refusing to sync {days} days in one run. Set --allow-large-range or reduce the range; "
            f"CASH_SYNC_MAX_DAYS_PER_RUN={settings.cash_sync_max_days_per_run}."
        )


def count_missing_fetch_requests(
    settings: Settings,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    manifest: CashSyncManifest,
) -> int:
    """Count Breeze requests needed after local manifest/file resumability checks."""

    fetched = set(manifest.fetched_files)
    requests = 0
    for chunk_start, _chunk_end in plan_fetch_chunks(settings, from_date, to_date):
        local_path = cash_local_path(settings.data_dir, chunk_start, symbol.nse_symbol)
        if str(local_path) in fetched or local_path.exists():
            continue
        requests += 1
    return requests


def plan_fetch_chunks(settings: Settings, from_date: date, to_date: date) -> list[tuple[date, date]]:
    """Breeze request windows for an inclusive range.

    Inside the span of known exchange sessions (days with local candles for
    several symbols), weekends and holidays are skipped and sessions are grouped
    CASH_HISTORY_SESSIONS_PER_REQUEST at a time. Outside that span, where a new
    session could not be known yet, plain CASH_HISTORY_CHUNK_DAYS windows are used.
    """

    per_request = settings.cash_history_sessions_per_request
    if per_request < 1 or per_request * BREEZE_MAX_CANDLES_PER_SESSION > BREEZE_MAX_CANDLES_PER_REQUEST:
        raise ValueError(f"CASH_HISTORY_SESSIONS_PER_REQUEST must be 1..{BREEZE_MAX_CANDLES_PER_REQUEST // BREEZE_MAX_CANDLES_PER_SESSION}")
    sessions = known_trading_sessions(settings.data_dir, from_date.year, to_date.year) if settings.cash_history_use_session_calendar else ()
    if not sessions:
        return list(iter_date_chunks(from_date, to_date, chunk_days=settings.cash_history_chunk_days))

    first_known, last_known = sessions[0], sessions[-1]
    chunks: list[tuple[date, date]] = []
    if from_date < first_known:
        chunks += iter_date_chunks(from_date, min(to_date, first_known - timedelta(days=1)), chunk_days=settings.cash_history_chunk_days)
    known = [day for day in sessions if from_date <= day <= to_date]
    chunks += [(known[index], known[min(index + per_request, len(known)) - 1]) for index in range(0, len(known), per_request)]
    if to_date > last_known:
        chunks += iter_date_chunks(max(from_date, last_known + timedelta(days=1)), to_date, chunk_days=settings.cash_history_chunk_days)
    return chunks


@lru_cache(maxsize=8)
def known_trading_sessions(data_dir: Path, from_year: int, to_year: int) -> tuple[date, ...]:
    """Exchange sessions evidenced by local candles of at least a few symbols."""

    inventory = scan_local_cash(data_dir, from_date=date(from_year, 1, 1), to_date=date(to_year, 12, 31))
    return tuple(inventory.trading_calendar(min_symbols=SESSION_CALENDAR_MIN_SYMBOLS))


def fetch_to_local_parquet(
    settings: Settings,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    manifest: CashSyncManifest,
    manifest_path: Path,
    *,
    workers: int,
) -> None:
    breeze = create_breeze_client(settings)
    fetched = set(manifest.fetched_files)
    chunks = plan_fetch_chunks(settings, from_date, to_date)

    if workers == 1:
        for chunk_start, chunk_end in chunks:
            result = fetch_cash_chunk(settings, breeze, symbol, chunk_start, chunk_end, fetched)
            record_fetch_result(manifest, manifest_path, symbol, result)
        return

    errors: list[BaseException] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-download") as executor:
        futures = [
            executor.submit(fetch_cash_chunk, settings, breeze, symbol, chunk_start, chunk_end, fetched)
            for chunk_start, chunk_end in chunks
        ]
        for future in as_completed(futures):
            try:
                result = future.result()
            except BaseException as exc:
                errors.append(exc)
                continue
            record_fetch_result(manifest, manifest_path, symbol, result)
    if errors:
        raise errors[0]


def fetch_cash_chunk(
    settings: Settings,
    breeze: Any,
    symbol: EquitySymbolReference,
    chunk_start: date,
    chunk_end: date,
    fetched: set[str],
) -> CashFetchResult:
    """Fetch one cash date chunk into local parquet."""

    local_path = cash_local_path(settings.data_dir, chunk_start, symbol.nse_symbol)
    if str(local_path) in fetched or local_path.exists():
        return CashFetchResult(local_files=(str(local_path),), row_count=read_cash_row_count(local_path) if local_path.exists() else 0, existed=True)

    payload = breeze.get_historical_cash(
        stock_code=symbol.breeze_code,
        from_date=breeze_datetime(chunk_start),
        to_date=breeze_datetime(chunk_end, end_of_day=True),
        interval=settings.default_interval,
        exchange_code=settings.cash_exchange_code,
        product_type=settings.cash_product_type,
    )
    rows = transform_cash_payload(
        payload,
        nse_symbol=symbol.nse_symbol,
        exchange_code=settings.cash_exchange_code,
        product_type=settings.cash_product_type,
    )
    local_files = write_cash_chunk_files(settings, symbol, chunk_start, rows)
    return CashFetchResult(local_files=tuple(str(path) for path in local_files), row_count=len(rows), existed=False)


def record_fetch_result(
    manifest: CashSyncManifest,
    manifest_path: Path,
    symbol: EquitySymbolReference,
    result: CashFetchResult,
) -> None:
    """Persist fetched paths from one chunk result."""

    for local_file in result.local_files:
        add_manifest_file(manifest.fetched_files, local_file)
    manifest.save(manifest_path)
    log_name = "cash_local_file_exists" if result.existed else "cash_local_file_written"
    for local_file in result.local_files:
        log = logger.debug if result.existed else logger.info
        log("%s symbol=%s path=%s rows=%s", log_name, symbol.nse_symbol, local_file, result.row_count)


def write_cash_chunk_files(
    settings: Settings,
    symbol: EquitySymbolReference,
    chunk_start: date,
    rows: list[Any],
) -> list[Path]:
    """Write one file per trade date, plus an empty chunk-start marker when that day had no candles.

    The chunk-start file is the resumability marker, so rows are always filed by
    their own trade_date and never under the chunk-start name.
    """

    rows_by_date: dict[date, list] = {}
    for row in rows:
        rows_by_date.setdefault(row.trade_date, []).append(row)

    local_files: list[Path] = []
    for trade_date, date_rows in sorted(rows_by_date.items()):
        path = cash_local_path(settings.data_dir, trade_date, symbol.nse_symbol)
        write_cash_parquet(date_rows, path)
        local_files.append(path)
    if chunk_start not in rows_by_date:
        marker_path = cash_local_path(settings.data_dir, chunk_start, symbol.nse_symbol)
        write_cash_parquet([], marker_path)
        local_files.append(marker_path)
    return sorted(local_files)


def upload_to_iceberg(settings: Settings, manifest: CashSyncManifest, manifest_path: Path, *, workers: int, batch_size: int) -> str:
    iceberg = IcebergMarketDataCatalog(settings)
    table_ids = iceberg.ensure_market_data_tables()
    uploaded = set(manifest.uploaded_files)
    cash_table_id = ".".join(table_ids["cash"])
    upload_tasks: list[CashUploadTask] = []
    committed_source_paths = iceberg.committed_source_paths("cash")

    for local_file in manifest.fetched_files:
        local_path = Path(local_file)
        if not local_path.exists():
            raise FileNotFoundError(f"Manifest references missing file: {local_path}")
        trade_date = date_from_cash_path(local_path)
        if local_file in uploaded:
            logger.debug("cash_iceberg_file_already_uploaded symbol=%s table=%s path=%s", manifest.nse_symbol, cash_table_id, local_path)
            continue

        row_count = read_cash_row_count(local_path)
        if row_count == 0:
            logger.debug("cash_empty_file_skipped_for_iceberg symbol=%s table=%s path=%s", manifest.nse_symbol, cash_table_id, local_path)
            add_manifest_file(manifest.uploaded_files, local_file)
            manifest.save(manifest_path)
            continue

        if local_file in committed_source_paths:
            record_upload_result(
                manifest,
                manifest_path,
                cash_table_id,
                CashUploadResult(tasks=(CashUploadTask(local_file=local_file, local_path=local_path, trade_date=trade_date, row_count=row_count),), committed=False),
            )
            continue

        upload_tasks.append(CashUploadTask(local_file=local_file, local_path=local_path, trade_date=trade_date, row_count=row_count))

    upload_batches = list(chunk_upload_tasks(upload_tasks, batch_size))
    if workers == 1:
        for tasks in upload_batches:
            results = upload_cash_task_batch(settings, iceberg, "cash", manifest.nse_symbol, tasks)
            for result in results:
                record_upload_result(manifest, manifest_path, cash_table_id, result)
        return cash_table_id

    errors: list[BaseException] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-upload") as executor:
        futures = [
            executor.submit(upload_cash_task_batch, settings, iceberg, "cash", manifest.nse_symbol, tasks)
            for tasks in upload_batches
        ]
        for future in as_completed(futures):
            try:
                results = future.result()
            except BaseException as exc:
                errors.append(exc)
                continue
            for result in results:
                record_upload_result(manifest, manifest_path, cash_table_id, result)
    if errors:
        raise errors[0]
    return cash_table_id


def upload_cash_task_batch(
    settings: Settings,
    iceberg: IcebergMarketDataCatalog,
    market_type: str,
    nse_symbol: str,
    tasks: tuple[CashUploadTask, ...],
) -> tuple[CashUploadResult, ...]:
    """Append a batch, splitting it if R2/catalog writes keep failing."""

    try:
        return (upload_cash_tasks(settings, iceberg, market_type, nse_symbol, tasks),)
    except Exception as exc:
        if len(tasks) == 1:
            raise
        midpoint = len(tasks) // 2
        logger.warning(
            "cash_iceberg_batch_split_after_failure symbol=%s files=%s left=%s right=%s error=%s",
            nse_symbol,
            len(tasks),
            midpoint,
            len(tasks) - midpoint,
            exc,
        )
        left_results = upload_cash_task_batch(settings, iceberg, market_type, nse_symbol, tasks[:midpoint])
        right_results = upload_cash_task_batch(settings, iceberg, market_type, nse_symbol, tasks[midpoint:])
        return left_results + right_results


def upload_cash_tasks(
    settings: Settings,
    iceberg: IcebergMarketDataCatalog,
    market_type: str,
    nse_symbol: str,
    tasks: tuple[CashUploadTask, ...],
) -> CashUploadResult:
    """Replace cash symbol/days in one snapshot, retrying transient catalog conflicts.

    Replace (delete + insert) instead of append keeps retries and re-uploads from
    duplicating candles when snapshot source-path metadata is missing or stale.
    """

    for attempt in range(1, settings.cash_upload_retry_attempts + 1):
        try:
            with _CASH_ICEBERG_APPEND_LOCK:
                iceberg.replace_parquet_files(
                    market_type,
                    [task.local_path for task in tasks],
                    nse_symbol=nse_symbol,
                    trade_dates=[task.trade_date for task in tasks],
                    snapshot_properties={
                        "tick_ticker.nse_symbol": nse_symbol,
                        "tick_ticker.trade_date_from": min(task.trade_date for task in tasks).isoformat(),
                        "tick_ticker.trade_date_to": max(task.trade_date for task in tasks).isoformat(),
                        "tick_ticker.source_paths": json.dumps([task.local_file for task in tasks], separators=(",", ":")),
                        "tick_ticker.write_mode": "replace",
                    },
                )
            return CashUploadResult(tasks=tasks, committed=True)
        except Exception as exc:
            if attempt >= settings.cash_upload_retry_attempts:
                raise
            logger.warning(
                "cash_iceberg_batch_append_retrying symbol=%s files=%s attempt=%s/%s error=%s",
                nse_symbol,
                len(tasks),
                attempt,
                settings.cash_upload_retry_attempts,
                exc,
            )
            time.sleep(settings.cash_upload_retry_base_delay_seconds * attempt)
    return CashUploadResult(tasks=tasks, committed=False)


def record_upload_result(
    manifest: CashSyncManifest,
    manifest_path: Path,
    cash_table_id: str,
    result: CashUploadResult,
) -> None:
    """Persist a successful Iceberg upload in the manifest."""

    for task in result.tasks:
        add_manifest_file(manifest.uploaded_files, task.local_file)
    manifest.save(manifest_path)
    log_name = "cash_iceberg_batch_appended" if result.committed else "cash_iceberg_file_already_committed"
    log = logger.info if result.committed else logger.debug
    row_count = sum(task.row_count for task in result.tasks)
    log(
        "%s symbol=%s table=%s files=%s rows=%s",
        log_name,
        manifest.nse_symbol,
        cash_table_id,
        len(result.tasks),
        row_count,
    )


def chunk_upload_tasks(tasks: list[CashUploadTask], batch_size: int) -> list[tuple[CashUploadTask, ...]]:
    """Split upload tasks into stable batches."""

    return [tuple(tasks[index : index + batch_size]) for index in range(0, len(tasks), batch_size)]


def add_manifest_file(files: list[str], path: str) -> None:
    """Add one manifest path in stable order."""

    if path not in files:
        files.append(path)
        files.sort()


def date_from_cash_path(path: str | Path) -> date:
    """Parse data/cash/YYYY/MM/DD/SYMBOL.parquet into a date."""

    path = Path(path)
    day = int(path.parent.name)
    month = int(path.parent.parent.name)
    year = int(path.parent.parent.parent.name)
    return date(year, month, day)


def cash_local_coverage(data_dir: Path, nse_symbol: str) -> CashLocalCoverage:
    """Return local file coverage and row count for one cash symbol."""

    paths = sorted((data_dir / "cash").glob(f"*/*/*/{nse_symbol}.parquet"))
    if not paths:
        return CashLocalCoverage(from_date=None, to_date=None, file_count=0, row_count=0)
    return CashLocalCoverage(
        from_date=date_from_cash_path(paths[0]),
        to_date=date_from_cash_path(paths[-1]),
        file_count=len(paths),
        row_count=sum(read_cash_row_count(path) for path in paths),
    )


if __name__ == "__main__":
    main()
