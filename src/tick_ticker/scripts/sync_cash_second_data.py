"""Sync cash 1-second candles from Breeze into local Parquet and Iceberg."""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.models import EquitySymbolReference
from tick_ticker.db.repositories import EquitySymbolReferenceRepository
from tick_ticker.services.cash_data import CashSyncManifest, cash_manifest_path, read_cash_row_count
from tick_ticker.services.cash_history_provider import cash_provider_history_start_date
from tick_ticker.services.cash_second_data import (
    CASH_SECOND_MARKET_TYPE,
    CashSecondSyncManifest,
    CashSecondWindow,
    build_cash_second_daily_file,
    cash_second_local_path,
    cash_second_manifest_path,
    cash_second_window_local_path,
    clear_fetched_windows_for_date,
    is_known_empty_minute_day,
    iter_cash_second_windows,
    parse_market_time,
    remove_cash_second_window_day,
    transform_cash_second_payload,
    write_cash_second_parquet,
    write_empty_cash_second_day,
)
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog
from tick_ticker.utils.datetime import breeze_datetime, parse_date, utc_now
from tick_ticker.utils.engines import create_breeze_client, create_d1_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)
_CASH_SECOND_ICEBERG_APPEND_LOCK = threading.Lock()
_CASH_SECOND_IDENTITY_BREEZE_CODES = {"NIFTY"}


@dataclass(frozen=True)
class CashSecondFetchResult:
    """Outcome from fetching one intraday 1-second window."""

    window: CashSecondWindow
    local_file: str
    row_count: int
    existed: bool


@dataclass(frozen=True)
class CashSecondUploadTask:
    """One local 1-second cash parquet file ready for Iceberg."""

    local_file: str
    local_path: Path
    trade_date: date
    row_count: int


@dataclass(frozen=True)
class CashSecondUploadResult:
    """Outcome from uploading one or more local 1-second files."""

    tasks: tuple[CashSecondUploadTask, ...]
    committed: bool


@dataclass(frozen=True)
class CashSecondLocalCoverage:
    """Local 1-second cash file coverage for one symbol."""

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
    validate_cash_second_settings(settings)

    if args.ensure_table_only:
        IcebergMarketDataCatalog(settings).load_market_table(CASH_SECOND_MARKET_TYPE)
        table_id = cash_second_table_id(settings)
        logger.info("cash_second_iceberg_table_ensured table=%s", table_id)
        return

    to_date = resolve_to_date(settings, args)
    download_workers = resolve_worker_count(
        args.download_workers, settings.cash_second_download_workers, "download-workers"
    )
    upload_workers = resolve_worker_count(args.upload_workers, settings.cash_second_upload_workers, "upload-workers")
    upload_batch_size = resolve_worker_count(None, settings.cash_second_upload_batch_size, "upload-batch-size")
    symbol_workers = resolve_worker_count(args.symbol_workers, settings.cash_second_symbol_workers, "symbol-workers")
    breeze_request_budget = BreezeRequestBudget(
        resolve_non_negative_count(args.breeze_max_requests, settings.breeze_max_requests_per_run, "breeze-max-requests")
    )

    explicit_codes = parse_symbol_codes(args.symbol_code)
    symbols = resolve_cash_second_symbols(
        settings,
        requested_symbols=dedupe_symbols([*(args.nse_symbol or []), *explicit_codes.keys()]),
        explicit_codes=explicit_codes,
        d1_fallback=args.d1_fallback,
    )
    if not symbols:
        raise ValueError("Pass at least one --nse-symbol or --symbol-code SYMBOL:BREEZE_CODE")

    synced_count, skipped_count = sync_resolved_symbols(
        settings=settings,
        symbols=symbols,
        args=args,
        to_date=to_date,
        download_workers=download_workers,
        upload_workers=upload_workers,
        upload_batch_size=upload_batch_size,
        symbol_workers=symbol_workers,
        breeze_request_budget=breeze_request_budget,
    )
    logger.info(
        "cash_second_sync_completed symbols=%s synced=%s skipped=%s breeze_requests_reserved=%s breeze_request_budget=%s",
        len(symbols),
        synced_count,
        skipped_count,
        breeze_request_budget.reserved_requests,
        breeze_request_budget.max_requests,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nse-symbol", action="append", help="Sync this NSE symbol. Can be passed multiple times.")
    parser.add_argument(
        "--symbol-code",
        action="append",
        help="Sync SYMBOL using an explicit Breeze stock code. Example: HDFCBANK:HDFBAN.",
    )
    parser.add_argument("--from-date", help="Inclusive start date, YYYY-MM-DD. Defaults to CASH_SECOND_SYNC_FROM_DATE.")
    parser.add_argument("--to-date", help="Inclusive end date, YYYY-MM-DD. Defaults to CASH_SECOND_SYNC_TO_DATE or today.")
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch Breeze data into local Parquet.")
    parser.add_argument("--local-only", action="store_true", help="Only download local Parquet; do not upload to Iceberg.")
    parser.add_argument("--upload-only", action="store_true", help="Only upload existing local Parquet files to Iceberg.")
    parser.add_argument(
        "--allow-large-range",
        action="store_true",
        help="Allow ranges larger than CASH_SECOND_SYNC_MAX_DAYS_PER_RUN.",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        help="Concurrent Breeze window workers. Defaults to CASH_SECOND_DOWNLOAD_WORKERS.",
    )
    parser.add_argument(
        "--symbol-workers",
        type=int,
        help="Concurrent symbols to process. Defaults to CASH_SECOND_SYMBOL_WORKERS.",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        help="Concurrent Iceberg upload workers. Defaults to CASH_SECOND_UPLOAD_WORKERS.",
    )
    parser.add_argument(
        "--breeze-max-requests",
        type=int,
        help="Maximum Breeze historical requests to reserve in this run. Defaults to BREEZE_MAX_REQUESTS_PER_RUN; 0 disables.",
    )
    parser.add_argument(
        "--d1-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use D1 equity_symbol_reference only when a local minute manifest or explicit code is unavailable.",
    )
    parser.add_argument("--ensure-table-only", action="store_true", help="Only create/load the 1-second Iceberg table.")
    args = parser.parse_args()
    if args.local_only and args.upload_only:
        parser.error("--local-only cannot be used with --upload-only")
    if args.fetch_only and args.upload_only:
        parser.error("--fetch-only cannot be used with --upload-only")
    return args


def sync_resolved_symbols(
    *,
    settings: Settings,
    symbols: list[EquitySymbolReference],
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    symbol_workers: int,
    breeze_request_budget: BreezeRequestBudget | None,
) -> tuple[int, int]:
    """Sync resolved symbols and skip oversized ranges during multi-symbol runs."""

    synced_count = 0
    skipped_count = 0

    if symbol_workers == 1 or len(symbols) == 1:
        for symbol in symbols:
            synced, skipped = sync_resolved_symbol(
                settings=settings,
                symbol=symbol,
                args=args,
                to_date=to_date,
                download_workers=download_workers,
                upload_workers=upload_workers,
                upload_batch_size=upload_batch_size,
                breeze_request_budget=breeze_request_budget,
                skip_large_range=len(symbols) > 1,
            )
            synced_count += int(synced)
            skipped_count += int(skipped)
        return synced_count, skipped_count

    errors: list[BaseException] = []
    worker_count = min(symbol_workers, len(symbols))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="cash-second-symbol") as executor:
        futures = [
            executor.submit(
                sync_resolved_symbol,
                settings=settings,
                symbol=symbol,
                args=args,
                to_date=to_date,
                download_workers=download_workers,
                upload_workers=upload_workers,
                upload_batch_size=upload_batch_size,
                breeze_request_budget=breeze_request_budget,
                skip_large_range=True,
            )
            for symbol in symbols
        ]
        for future in as_completed(futures):
            try:
                synced, skipped = future.result()
            except BaseException as exc:
                errors.append(exc)
                continue
            synced_count += int(synced)
            skipped_count += int(skipped)
    if errors:
        raise errors[0]
    return synced_count, skipped_count


def sync_resolved_symbol(
    *,
    settings: Settings,
    symbol: EquitySymbolReference,
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    breeze_request_budget: BreezeRequestBudget | None,
    skip_large_range: bool,
) -> tuple[bool, bool]:
    """Sync one symbol and return (synced, skipped)."""

    try:
        synced = sync_cash_second_symbol(
            settings=settings,
            symbol=symbol,
            args=args,
            to_date=to_date,
            download_workers=download_workers,
            upload_workers=upload_workers,
            upload_batch_size=upload_batch_size,
            breeze_request_budget=breeze_request_budget,
        )
    except (DateRangeTooLargeError, BreezeRequestBudgetExceededError) as exc:
        if not skip_large_range:
            raise
        logger.warning("cash_second_symbol_skipped symbol=%s error=%s", symbol.nse_symbol, exc)
        return False, True
    return synced, False


def sync_cash_second_symbol(
    *,
    settings: Settings,
    symbol: EquitySymbolReference,
    args: argparse.Namespace,
    to_date: date,
    download_workers: int,
    upload_workers: int,
    upload_batch_size: int,
    breeze_request_budget: BreezeRequestBudget | None = None,
) -> bool:
    """Sync one resolved cash 1-second symbol."""

    manifest_path = cash_second_manifest_path(settings.data_dir, symbol.nse_symbol)
    existing_manifest = CashSecondSyncManifest.load(manifest_path)
    from_date = resolve_from_date(settings, args, symbol, existing_manifest)
    if args.upload_only and existing_manifest is not None:
        if not args.from_date:
            from_date = existing_manifest.from_date
        if not args.to_date:
            to_date = existing_manifest.to_date

    if from_date > to_date:
        logger.info("cash_second_symbol_already_synced symbol=%s from=%s to=%s", symbol.nse_symbol, from_date, to_date)
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
                "cash_second_breeze_requests_reserved symbol=%s requests=%s reserved=%s budget=%s",
                symbol.nse_symbol,
                request_count,
                breeze_request_budget.reserved_requests,
                breeze_request_budget.max_requests,
            )

    if not args.upload_only:
        fetch_to_local_parquet(settings, symbol, from_date, to_date, manifest, manifest_path, workers=download_workers)
        manifest.record_fetch_event(completed_at=utc_now())
        manifest.save(manifest_path)
    if not local_only:
        table_id = upload_to_iceberg(settings, manifest, manifest_path, workers=upload_workers, batch_size=upload_batch_size)
        local_coverage = cash_second_local_coverage(settings.data_dir, symbol.nse_symbol)
        completed_at = utc_now()
        manifest.record_upload_event(
            table=table_id,
            coverage_from_date=local_coverage.from_date or from_date,
            coverage_to_date=local_coverage.to_date or to_date,
            coverage_file_count=local_coverage.file_count,
            coverage_row_count=local_coverage.row_count,
            completed_at=completed_at,
        )
        manifest.save(manifest_path)
        logger.info("cash_second_sync_uploaded symbol=%s table=%s rows=%s", symbol.nse_symbol, table_id, local_coverage.row_count)
    else:
        logger.info("cash_second_local_download_completed symbol=%s rows=%s", symbol.nse_symbol, manifest.row_count)
    return True


def resolve_cash_second_symbols(
    settings: Settings,
    *,
    requested_symbols: list[str],
    explicit_codes: dict[str, str],
    d1_fallback: bool,
) -> list[EquitySymbolReference]:
    """Resolve requested symbols from explicit codes, local minute manifests, then optional D1 fallback."""

    if not requested_symbols:
        return []

    symbol_repo: EquitySymbolReferenceRepository | None = None
    symbols: list[EquitySymbolReference] = []
    for raw_symbol in requested_symbols:
        nse_symbol = raw_symbol.upper()
        if nse_symbol in explicit_codes:
            symbols.append(EquitySymbolReference(nse_symbol=nse_symbol, breeze_code=explicit_codes[nse_symbol]))
            continue

        minute_manifest = CashSyncManifest.load(cash_manifest_path(settings.data_dir, nse_symbol))
        if minute_manifest is not None:
            symbols.append(
                EquitySymbolReference(
                    nse_symbol=nse_symbol,
                    breeze_code=minute_manifest.breeze_code,
                    listing_date=minute_manifest.coverage_from_date or minute_manifest.from_date,
                )
            )
            continue

        if d1_fallback:
            if symbol_repo is None:
                symbol_repo = EquitySymbolReferenceRepository(create_d1_client(settings))
            d1_symbol = symbol_repo.get_by_nse_symbol(nse_symbol)
            if d1_symbol is not None:
                symbols.append(d1_symbol)
                continue

        if nse_symbol in _CASH_SECOND_IDENTITY_BREEZE_CODES:
            symbols.append(EquitySymbolReference(nse_symbol=nse_symbol, breeze_code=nse_symbol))
            continue

        raise ValueError(
            f"No Breeze code found for {nse_symbol}. Pass --symbol-code {nse_symbol}:CODE "
            "or fetch/create the 1-minute cash manifest first."
        )
    return symbols


def resolve_to_date(settings: Settings, args: argparse.Namespace) -> date:
    """Resolve the inclusive end date."""

    return parse_date(args.to_date) if args.to_date else settings.cash_second_sync_to_date or date.today()


def resolve_from_date(
    settings: Settings,
    args: argparse.Namespace,
    symbol: EquitySymbolReference,
    existing_manifest: CashSecondSyncManifest | None,
) -> date:
    """Resolve the inclusive start date for 1-second history."""

    if args.from_date:
        from_date = parse_date(args.from_date)
    elif settings.cash_second_sync_from_date:
        from_date = settings.cash_second_sync_from_date
    elif existing_manifest and existing_manifest.coverage_to_date:
        from_date = existing_manifest.coverage_to_date + timedelta(days=1)
    else:
        from_date = date(2026, 1, 1)

    if symbol.listing_date:
        from_date = max(from_date, symbol.listing_date)
    provider_start_date = cash_provider_history_start_date(settings.cash_history_provider)
    if provider_start_date is not None:
        from_date = max(from_date, provider_start_date)
    return from_date


def load_or_create_manifest(
    *,
    manifest_path: Path,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    allow_range_reset: bool,
) -> CashSecondSyncManifest:
    """Load a reusable 1-second manifest or safely start a new range."""

    manifest = CashSecondSyncManifest.load(manifest_path)
    if manifest is None:
        manifest = CashSecondSyncManifest(
            nse_symbol=symbol.nse_symbol,
            breeze_code=symbol.breeze_code,
            from_date=from_date,
            to_date=to_date,
        )
        manifest.save(manifest_path)
        return manifest

    if manifest.from_date == from_date and manifest.to_date == to_date:
        manifest.breeze_code = symbol.breeze_code
        manifest.save(manifest_path)
        return manifest

    fully_uploaded = bool(manifest.fetched_files) and set(manifest.fetched_files).issubset(set(manifest.uploaded_files))
    if fully_uploaded or allow_range_reset:
        manifest.begin_run(from_date, to_date, breeze_code=symbol.breeze_code)
        manifest.save(manifest_path)
        return manifest

    if ranges_touch_or_overlap(manifest.from_date, manifest.to_date, from_date, to_date):
        manifest.from_date = min(manifest.from_date, from_date)
        manifest.to_date = max(manifest.to_date, to_date)
        manifest.breeze_code = symbol.breeze_code
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
    """Validate the requested 1-second sync range."""

    if from_date > to_date:
        raise ValueError(f"from-date {from_date} is after to-date {to_date}")

    days = (to_date - from_date).days + 1
    if not allow_large_range and days > settings.cash_second_sync_max_days_per_run:
        raise DateRangeTooLargeError(
            f"Refusing to sync {days} days in one run. Set --allow-large-range or reduce the range; "
            f"CASH_SECOND_SYNC_MAX_DAYS_PER_RUN={settings.cash_second_sync_max_days_per_run}."
        )


def validate_cash_second_settings(settings: Settings) -> None:
    """Validate 1-second fetch settings before making Breeze calls."""

    if settings.cash_second_history_window_seconds < 1:
        raise ValueError("CASH_SECOND_HISTORY_WINDOW_SECONDS must be >= 1")
    if settings.cash_second_max_candles_per_request < 1:
        raise ValueError("CASH_SECOND_MAX_CANDLES_PER_REQUEST must be >= 1")
    if settings.cash_second_history_window_seconds > settings.cash_second_max_candles_per_request:
        raise ValueError(
            "CASH_SECOND_HISTORY_WINDOW_SECONDS must be <= CASH_SECOND_MAX_CANDLES_PER_REQUEST "
            "to avoid truncated Breeze historical responses."
        )
    parse_market_time(settings.cash_second_market_open_time)
    parse_market_time(settings.cash_second_market_close_time)


def count_missing_fetch_requests(
    settings: Settings,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    manifest: CashSecondSyncManifest,
) -> int:
    """Count Breeze 1-second requests needed after local/window resumability checks."""

    fetched_files = set(manifest.fetched_files)
    fetched_windows = set(manifest.fetched_windows)
    requests = 0
    for window in configured_cash_second_windows(settings, from_date, to_date):
        daily_path = cash_second_local_path(settings.data_dir, window.trade_date, symbol.nse_symbol)
        if str(daily_path) in fetched_files or daily_path.exists():
            continue
        if settings.cash_second_use_minute_empty_days and is_known_empty_minute_day(
            settings.data_dir, window.trade_date, symbol.nse_symbol
        ):
            continue
        window_path = cash_second_window_local_path(settings.data_dir, symbol.nse_symbol, window)
        if str(window_path) in fetched_windows and window_path.exists():
            continue
        if window_path.exists():
            continue
        requests += 1
    return requests


def fetch_to_local_parquet(
    settings: Settings,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    *,
    workers: int,
) -> None:
    """Fetch missing 1-second windows and build final daily Parquet files."""

    breeze = create_breeze_client(settings)
    windows = list(configured_cash_second_windows(settings, from_date, to_date))
    windows_by_day = group_windows_by_day(windows)
    scheduled_windows = prepare_existing_and_missing_windows(settings, symbol, manifest, manifest_path, windows)

    if workers == 1:
        for window in scheduled_windows:
            result = fetch_cash_second_window(settings, breeze, symbol, window)
            record_fetch_result(manifest, manifest_path, symbol, result)
    else:
        errors: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-second-download") as executor:
            futures = [executor.submit(fetch_cash_second_window, settings, breeze, symbol, window) for window in scheduled_windows]
            for future in as_completed(futures):
                try:
                    result = future.result()
                except BaseException as exc:
                    errors.append(exc)
                    continue
                record_fetch_result(manifest, manifest_path, symbol, result)
        if errors:
            raise errors[0]

    finalize_cash_second_days(settings, symbol, manifest, manifest_path, windows_by_day)


def configured_cash_second_windows(settings: Settings, from_date: date, to_date: date) -> list[CashSecondWindow]:
    """Return configured Breeze-safe 1-second windows for a date range."""

    return list(
        iter_cash_second_windows(
            from_date,
            to_date,
            window_seconds=settings.cash_second_history_window_seconds,
            market_open_time=parse_market_time(settings.cash_second_market_open_time),
            market_close_time=parse_market_time(settings.cash_second_market_close_time),
            skip_weekends=settings.cash_second_skip_weekends,
        )
    )


def group_windows_by_day(windows: list[CashSecondWindow]) -> dict[date, list[CashSecondWindow]]:
    grouped: dict[date, list[CashSecondWindow]] = defaultdict(list)
    for window in windows:
        grouped[window.trade_date].append(window)
    return dict(grouped)


def prepare_existing_and_missing_windows(
    settings: Settings,
    symbol: EquitySymbolReference,
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    windows: list[CashSecondWindow],
) -> list[CashSecondWindow]:
    """Register reusable local files/windows and return only missing windows to fetch."""

    scheduled_windows: list[CashSecondWindow] = []
    for window in windows:
        daily_path = cash_second_local_path(settings.data_dir, window.trade_date, symbol.nse_symbol)
        if str(daily_path) in manifest.fetched_files or daily_path.exists():
            add_manifest_file(manifest.fetched_files, str(daily_path))
            continue

        if settings.cash_second_use_minute_empty_days and is_known_empty_minute_day(
            settings.data_dir, window.trade_date, symbol.nse_symbol
        ):
            write_empty_cash_second_day(daily_path)
            add_manifest_file(manifest.fetched_files, str(daily_path))
            clear_fetched_windows_for_date(manifest, window.trade_date)
            manifest.save(manifest_path)
            logger.info("cash_second_empty_day_from_minute_cache symbol=%s path=%s", symbol.nse_symbol, daily_path)
            continue

        window_path = cash_second_window_local_path(settings.data_dir, symbol.nse_symbol, window)
        if window_path.exists():
            add_manifest_file(manifest.fetched_windows, window.key)
            continue
        scheduled_windows.append(window)
    manifest.save(manifest_path)
    return scheduled_windows


def fetch_cash_second_window(
    settings: Settings,
    breeze: Any,
    symbol: EquitySymbolReference,
    window: CashSecondWindow,
) -> CashSecondFetchResult:
    """Fetch one 1-second intraday window into temporary local Parquet."""

    local_path = cash_second_window_local_path(settings.data_dir, symbol.nse_symbol, window)
    if local_path.exists():
        return CashSecondFetchResult(
            window=window,
            local_file=str(local_path),
            row_count=read_cash_row_count(local_path),
            existed=True,
        )

    payload = breeze.get_historical_cash(
        stock_code=symbol.breeze_code,
        from_date=breeze_datetime(window.start),
        to_date=breeze_datetime(window.end),
        interval=settings.cash_second_interval,
        exchange_code=settings.cash_exchange_code,
        product_type=settings.cash_product_type,
    )
    rows = [
        row
        for row in transform_cash_second_payload(
            payload,
            nse_symbol=symbol.nse_symbol,
            exchange_code=settings.cash_exchange_code,
            product_type=settings.cash_product_type,
        )
        if window.start <= row.datetime <= window.end and row.trade_date == window.trade_date
    ]
    if len(rows) > settings.cash_second_max_candles_per_request:
        raise RuntimeError(
            f"Breeze returned {len(rows)} rows for {symbol.nse_symbol} {window.key}, above "
            f"CASH_SECOND_MAX_CANDLES_PER_REQUEST={settings.cash_second_max_candles_per_request}."
        )
    write_cash_second_parquet(rows, local_path)
    return CashSecondFetchResult(window=window, local_file=str(local_path), row_count=len(rows), existed=False)


def record_fetch_result(
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    symbol: EquitySymbolReference,
    result: CashSecondFetchResult,
) -> None:
    """Persist fetched temporary window paths in the manifest."""

    add_manifest_file(manifest.fetched_windows, result.window.key)
    manifest.save(manifest_path)
    log_name = "cash_second_window_exists" if result.existed else "cash_second_window_written"
    log = logger.debug if result.existed else logger.info
    log("%s symbol=%s window=%s path=%s rows=%s", log_name, symbol.nse_symbol, result.window.key, result.local_file, result.row_count)


def finalize_cash_second_days(
    settings: Settings,
    symbol: EquitySymbolReference,
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    windows_by_day: dict[date, list[CashSecondWindow]],
) -> None:
    """Build final daily files for days whose windows are all locally available."""

    for trade_date, windows in sorted(windows_by_day.items()):
        daily_path = cash_second_local_path(settings.data_dir, trade_date, symbol.nse_symbol)
        if str(daily_path) in manifest.fetched_files and daily_path.exists():
            continue

        if settings.cash_second_use_minute_empty_days and is_known_empty_minute_day(settings.data_dir, trade_date, symbol.nse_symbol):
            if not daily_path.exists():
                write_empty_cash_second_day(daily_path)
            add_manifest_file(manifest.fetched_files, str(daily_path))
            clear_fetched_windows_for_date(manifest, trade_date)
            manifest.save(manifest_path)
            continue

        window_paths = [cash_second_window_local_path(settings.data_dir, symbol.nse_symbol, window) for window in windows]
        if not all(path.exists() for path in window_paths):
            logger.info("cash_second_day_waiting_for_windows symbol=%s trade_date=%s", symbol.nse_symbol, trade_date)
            continue

        row_count = build_cash_second_daily_file(window_paths=window_paths, daily_path=daily_path)
        add_manifest_file(manifest.fetched_files, str(daily_path))
        clear_fetched_windows_for_date(manifest, trade_date)
        manifest.save(manifest_path)
        remove_cash_second_window_day(settings.data_dir, symbol.nse_symbol, trade_date)
        logger.info("cash_second_local_file_written symbol=%s path=%s rows=%s", symbol.nse_symbol, daily_path, row_count)


def upload_to_iceberg(
    settings: Settings,
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    *,
    workers: int,
    batch_size: int,
) -> str:
    """Upload fetched final 1-second daily files to the 1-second Iceberg table."""

    iceberg = IcebergMarketDataCatalog(settings)
    iceberg.load_market_table(CASH_SECOND_MARKET_TYPE)
    table_id = cash_second_table_id(settings)
    uploaded = set(manifest.uploaded_files)
    upload_tasks: list[CashSecondUploadTask] = []
    committed_source_paths = iceberg.committed_source_paths(CASH_SECOND_MARKET_TYPE)

    for local_file in manifest.fetched_files:
        local_path = Path(local_file)
        if not local_path.exists():
            raise FileNotFoundError(f"Manifest references missing file: {local_path}")
        trade_date = date_from_cash_second_path(local_path)
        if local_file in uploaded:
            logger.debug("cash_second_iceberg_file_already_uploaded symbol=%s table=%s path=%s", manifest.nse_symbol, table_id, local_path)
            continue

        row_count = read_cash_row_count(local_path)
        if row_count == 0:
            logger.debug("cash_second_empty_file_skipped_for_iceberg symbol=%s table=%s path=%s", manifest.nse_symbol, table_id, local_path)
            add_manifest_file(manifest.uploaded_files, local_file)
            manifest.save(manifest_path)
            continue

        task = CashSecondUploadTask(local_file=local_file, local_path=local_path, trade_date=trade_date, row_count=row_count)
        if local_file in committed_source_paths:
            record_upload_result(
                manifest,
                manifest_path,
                table_id,
                CashSecondUploadResult(tasks=(task,), committed=False),
            )
            continue
        upload_tasks.append(task)

    upload_batches = list(chunk_upload_tasks(upload_tasks, batch_size))
    if workers == 1:
        for tasks in upload_batches:
            results = upload_cash_second_task_batch(settings, iceberg, manifest.nse_symbol, tasks)
            for result in results:
                record_upload_result(manifest, manifest_path, table_id, result)
        return table_id

    errors: list[BaseException] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-second-upload") as executor:
        futures = [executor.submit(upload_cash_second_task_batch, settings, iceberg, manifest.nse_symbol, tasks) for tasks in upload_batches]
        for future in as_completed(futures):
            try:
                results = future.result()
            except BaseException as exc:
                errors.append(exc)
                continue
            for result in results:
                record_upload_result(manifest, manifest_path, table_id, result)
    if errors:
        raise errors[0]
    return table_id


def upload_cash_second_task_batch(
    settings: Settings,
    iceberg: IcebergMarketDataCatalog,
    nse_symbol: str,
    tasks: tuple[CashSecondUploadTask, ...],
) -> tuple[CashSecondUploadResult, ...]:
    """Append a batch, splitting it if R2/catalog writes keep failing."""

    try:
        return (upload_cash_second_tasks(settings, iceberg, nse_symbol, tasks),)
    except Exception as exc:
        if len(tasks) == 1:
            raise
        midpoint = len(tasks) // 2
        logger.warning(
            "cash_second_iceberg_batch_split_after_failure symbol=%s files=%s left=%s right=%s error=%s",
            nse_symbol,
            len(tasks),
            midpoint,
            len(tasks) - midpoint,
            exc,
        )
        left_results = upload_cash_second_task_batch(settings, iceberg, nse_symbol, tasks[:midpoint])
        right_results = upload_cash_second_task_batch(settings, iceberg, nse_symbol, tasks[midpoint:])
        return left_results + right_results


def upload_cash_second_tasks(
    settings: Settings,
    iceberg: IcebergMarketDataCatalog,
    nse_symbol: str,
    tasks: tuple[CashSecondUploadTask, ...],
) -> CashSecondUploadResult:
    """Append 1-second cash files in one snapshot, retrying transient catalog conflicts."""

    for attempt in range(1, settings.cash_upload_retry_attempts + 1):
        try:
            with _CASH_SECOND_ICEBERG_APPEND_LOCK:
                iceberg.append_parquet_files(
                    CASH_SECOND_MARKET_TYPE,
                    [task.local_path for task in tasks],
                    snapshot_properties={
                        "tick_ticker.nse_symbol": nse_symbol,
                        "tick_ticker.time_grain": "1second",
                        "tick_ticker.trade_date_from": min(task.trade_date for task in tasks).isoformat(),
                        "tick_ticker.trade_date_to": max(task.trade_date for task in tasks).isoformat(),
                        "tick_ticker.source_paths": json.dumps([task.local_file for task in tasks], separators=(",", ":")),
                        "tick_ticker.source_path_count": str(len(tasks)),
                        "tick_ticker.write_mode": "append",
                        "tick_ticker.query_layout": "symbol_intraday_time_range",
                    },
                )
            return CashSecondUploadResult(tasks=tasks, committed=True)
        except Exception as exc:
            if attempt >= settings.cash_upload_retry_attempts:
                raise
            logger.warning(
                "cash_second_iceberg_batch_append_retrying symbol=%s files=%s attempt=%s/%s error=%s",
                nse_symbol,
                len(tasks),
                attempt,
                settings.cash_upload_retry_attempts,
                exc,
            )
            time.sleep(settings.cash_upload_retry_base_delay_seconds * attempt)
    return CashSecondUploadResult(tasks=tasks, committed=False)


def record_upload_result(
    manifest: CashSecondSyncManifest,
    manifest_path: Path,
    table_id: str,
    result: CashSecondUploadResult,
) -> None:
    """Persist a successful 1-second Iceberg upload in the manifest."""

    for task in result.tasks:
        add_manifest_file(manifest.uploaded_files, task.local_file)
    manifest.save(manifest_path)
    log_name = "cash_second_iceberg_batch_appended" if result.committed else "cash_second_iceberg_file_already_committed"
    log = logger.info if result.committed else logger.debug
    row_count = sum(task.row_count for task in result.tasks)
    log("%s symbol=%s table=%s files=%s rows=%s", log_name, manifest.nse_symbol, table_id, len(result.tasks), row_count)


def chunk_upload_tasks(tasks: list[CashSecondUploadTask], batch_size: int) -> list[tuple[CashSecondUploadTask, ...]]:
    """Split upload tasks into stable batches."""

    return [tuple(tasks[index : index + batch_size]) for index in range(0, len(tasks), batch_size)]


def add_manifest_file(files: list[str], path: str) -> None:
    """Add one manifest path/key in stable order."""

    if path not in files:
        files.append(path)
        files.sort()


def date_from_cash_second_path(path: str | Path) -> date:
    """Parse data/cash_1s/by_symbol/SYMBOL/YYYY/MM/DD.parquet into a date."""

    path = Path(path)
    day = int(path.stem)
    month = int(path.parent.name)
    year = int(path.parent.parent.name)
    return date(year, month, day)


def cash_second_local_coverage(data_dir: Path, nse_symbol: str) -> CashSecondLocalCoverage:
    """Return local 1-second file coverage and row count for one cash symbol."""

    paths = sorted((data_dir / "cash_1s" / "by_symbol" / nse_symbol.upper()).glob("*/*/*.parquet"))
    if not paths:
        return CashSecondLocalCoverage(from_date=None, to_date=None, file_count=0, row_count=0)
    return CashSecondLocalCoverage(
        from_date=date_from_cash_second_path(paths[0]),
        to_date=date_from_cash_second_path(paths[-1]),
        file_count=len(paths),
        row_count=sum(read_cash_row_count(path) for path in paths),
    )


def cash_second_table_id(settings: Settings) -> str:
    """Return the configured 1-second Iceberg table id."""

    return f"{settings.iceberg_cash_second_namespace}.{settings.iceberg_cash_second_table}"


def parse_symbol_codes(values: list[str] | None) -> dict[str, str]:
    """Parse repeated --symbol-code SYMBOL:BREEZE values."""

    parsed: dict[str, str] = {}
    for value in values or []:
        if ":" not in value:
            raise ValueError(f"Invalid --symbol-code {value!r}; expected SYMBOL:BREEZE_CODE")
        symbol, breeze_code = value.split(":", 1)
        symbol = symbol.strip().upper()
        breeze_code = breeze_code.strip().upper()
        if not symbol or not breeze_code:
            raise ValueError(f"Invalid --symbol-code {value!r}; expected SYMBOL:BREEZE_CODE")
        parsed[symbol] = breeze_code
    return parsed


def dedupe_symbols(values: list[str]) -> list[str]:
    """Return uppercase symbols in first-seen order."""

    seen: set[str] = set()
    symbols: list[str] = []
    for value in values:
        symbol = value.strip().upper()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


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


if __name__ == "__main__":
    main()
