"""Sync cash 1-minute candles from Upstox into local Parquet and Iceberg."""

from __future__ import annotations

import argparse
import calendar
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncCompletion, MarketDataSyncState
from tick_ticker.db.repositories import EquitySymbolReferenceRepository, MarketDataSyncStateRepository
from tick_ticker.scripts.sync_cash_data import (
    CashFetchResult,
    add_manifest_file,
    cash_local_coverage,
    coverage_from_date,
    load_or_create_manifest,
    resolve_non_negative_count,
    resolve_symbols,
    resolve_to_date,
    resolve_worker_count,
    upload_to_iceberg,
)
from tick_ticker.services.cash_data import (
    CashOHLCV,
    CashSyncManifest,
    cash_local_path,
    cash_manifest_path,
    read_cash_row_count,
    transform_upstox_cash_payload,
    write_cash_parquet,
)
from tick_ticker.services.cash_history_provider import cash_provider_history_start_date
from tick_ticker.utils.datetime import last_completed_session_bound, parse_date, utc_now
from tick_ticker.utils.engines import UpstoxClient, create_d1_client, create_upstox_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


class UpstoxRequestBudgetExceededError(RuntimeError):
    """Raised when a run would exceed the configured Upstox request budget."""


class UpstoxInstrumentKeyError(ValueError):
    """Raised when a D1 reference row cannot be converted to an Upstox instrument key."""


class UpstoxRequestBudget:
    """Thread-safe counter for Upstox historical requests reserved by one run."""

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
                raise UpstoxRequestBudgetExceededError(
                    f"Refusing to reserve {requests} Upstox requests for {symbol}; "
                    f"run budget would become {next_total}/{self.max_requests}. "
                    "Raise UPSTOX_MAX_REQUESTS_PER_RUN or reduce the symbol/date range."
                )
            self._reserved_requests = next_total


@dataclass(frozen=True)
class UpstoxFetchChunk:
    """One Upstox-compatible historical request window."""

    from_date: date
    to_date: date
    marker_path: Path

    def already_fetched(self, fetched: set[str], *, bound: date | None = None) -> bool:
        """Whether this chunk can be skipped.

        A chunk is only settled once its last day has elapsed. Upstox publishes a
        session during the night after it, so a chunk covering recent days is
        re-requested; existing non-empty daily files are kept either way.
        """

        if self.to_date >= (bound or last_completed_session_bound()):
            return False
        return str(self.marker_path) in fetched or self.marker_path.exists()


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
    upstox_request_budget = UpstoxRequestBudget(
        resolve_non_negative_count(args.upstox_max_requests, settings.upstox_max_requests_per_run, "upstox-max-requests")
    )

    symbols = resolve_requested_symbols(symbol_repo, sync_repo, args, to_date)
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
        upstox_request_budget=upstox_request_budget,
    )

    if args.all_symbols or args.nse_symbols:
        logger.info(
            "cash_upstox_sync_batch_completed symbols=%s synced=%s skipped=%s upstox_requests_reserved=%s upstox_request_budget=%s",
            len(symbols),
            synced_count,
            skipped_count,
            upstox_request_budget.reserved_requests,
            upstox_request_budget.max_requests,
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
    upstox_request_budget: UpstoxRequestBudget | None,
) -> tuple[int, int]:
    """Sync resolved symbols and skip symbols that exceed the Upstox run budget."""

    synced_count = 0
    skipped_count = 0

    if symbol_workers == 1 or len(symbols) == 1:
        for index, symbol in enumerate(symbols):
            try:
                synced, skipped = sync_resolved_symbol(
                    settings=settings,
                    sync_repo=sync_repo,
                    symbol=symbol,
                    args=args,
                    to_date=to_date,
                    download_workers=download_workers,
                    upload_workers=upload_workers,
                    upload_batch_size=upload_batch_size,
                    upstox_request_budget=upstox_request_budget,
                )
            except UpstoxRequestBudgetExceededError as exc:
                if not args.all_symbols:
                    raise
                logger.warning(
                    "cash_upstox_request_budget_exhausted synced=%s skipped=%s remaining_symbols=%s error=%s",
                    synced_count,
                    skipped_count,
                    len(symbols) - index,
                    exc,
                )
                break
            if synced:
                synced_count += 1
            if skipped:
                skipped_count += 1
        return synced_count, skipped_count

    errors: list[BaseException] = []
    worker_count = min(symbol_workers, len(symbols))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="cash-upstox-symbol") as executor:
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
                upstox_request_budget=upstox_request_budget,
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
    upstox_request_budget: UpstoxRequestBudget | None,
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
            upstox_request_budget=upstox_request_budget,
        )
    except UpstoxInstrumentKeyError as exc:
        if not args.all_symbols:
            raise
        logger.warning("cash_upstox_symbol_skipped symbol=%s error=%s", symbol.nse_symbol, exc)
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
    upstox_request_budget: UpstoxRequestBudget | None = None,
) -> bool:
    """Sync one resolved cash symbol through Upstox."""

    manifest_path = cash_manifest_path(settings.data_dir, symbol.nse_symbol)
    existing_manifest = CashSyncManifest.load(manifest_path) if args.upload_only else None
    sync_state = sync_repo.get_state(market_type="cash", nse_symbol=symbol.nse_symbol)
    from_date = resolve_upstox_from_date(symbol, sync_state, settings, args)
    if existing_manifest is not None and not args.from_date and settings.cash_sync_from_date is None:
        from_date = existing_manifest.from_date
    if existing_manifest is not None and not args.to_date and settings.cash_sync_to_date is None:
        to_date = existing_manifest.to_date
    if from_date > to_date:
        logger.info("cash_symbol_already_synced symbol=%s synced_to=%s", symbol.nse_symbol, sync_state.to_date if sync_state else None)
        return False
    validate_upstox_date_range(from_date, to_date)
    resolve_upstox_instrument_key(symbol)

    local_only = args.fetch_only or args.local_only
    manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        allow_range_reset=local_only,
    )

    if not args.upload_only and upstox_request_budget is not None:
        request_count = count_missing_upstox_fetch_requests(settings, symbol, from_date, to_date, manifest)
        upstox_request_budget.reserve(symbol=symbol.nse_symbol, requests=request_count)
        if request_count:
            logger.info(
                "cash_upstox_requests_reserved symbol=%s requests=%s reserved=%s budget=%s",
                symbol.nse_symbol,
                request_count,
                upstox_request_budget.reserved_requests,
                upstox_request_budget.max_requests,
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
            # Never record a to_date beyond the last day actually stored: the
            # provider may publish a session late, and a to_date ahead of the
            # data makes the next run start after the gap.
            coverage_end = local_coverage.to_date or to_date
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
            logger.info("cash_upstox_sync_completed symbol=%s rows=%s", symbol.nse_symbol, local_coverage.row_count)
        else:
            logger.info("cash_upstox_local_download_completed symbol=%s rows=%s", symbol.nse_symbol, manifest.row_count)
    except Exception as exc:
        if not local_only:
            sync_repo.mark_failed(market_type="cash", nse_symbol=symbol.nse_symbol, error=str(exc))
        raise
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nse-symbol", help="Sync one NSE symbol only. Example: RELIANCE.")
    parser.add_argument("--nse-symbols", nargs="+", action="extend", help="Ordered NSE symbols, separated by spaces or commas; may be repeated.")
    parser.add_argument("--symbols-file", type=Path, help="Ordered symbols separated by whitespace or commas; # comments supported.")
    parser.add_argument("--skip-missing-symbols", action="store_true", help="Log and skip requested symbols absent from D1 instead of failing before fetching.")
    parser.add_argument(
        "--all",
        dest="all_symbols",
        action="store_true",
        help="Sync every due cash symbol. Default when no symbol selection is passed.",
    )
    parser.add_argument(
        "--from-date",
        help="Inclusive start date, YYYY-MM-DD. Defaults to CASH_SYNC_FROM_DATE, sync state, or the Upstox start date.",
    )
    parser.add_argument("--to-date", help="Inclusive end date, YYYY-MM-DD. Defaults to CASH_SYNC_TO_DATE or yesterday (IST); later dates are clamped to yesterday.")
    parser.add_argument(
        "--synced-only",
        action="store_true",
        help="With --all: only symbols that already have sync state (daily updates; skips never-synced backfills).",
    )
    parser.add_argument(
        "--ignore-listing-date",
        action="store_true",
        help="Clamp only to the Upstox history floor, not listing_date. For ISIN changes where NSE resets the listing date (NESTLEIND).",
    )
    parser.add_argument("--fetch-only", action="store_true", help="Only fetch Upstox data into local Parquet.")
    parser.add_argument("--local-only", action="store_true", help="Only download local Parquet; do not upload to Iceberg or update sync state.")
    parser.add_argument("--upload-only", action="store_true", help="Only upload existing local Parquet files to Iceberg and mark D1.")
    parser.add_argument(
        "--allow-large-range",
        action="store_true",
        help="Accepted for CLI parity with sync-cash-data; Upstox runs are bounded by request budget.",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        help="Concurrent Upstox download workers. Defaults to CASH_DOWNLOAD_WORKERS.",
    )
    parser.add_argument(
        "--symbol-workers",
        type=int,
        help="Concurrent cash symbols. Use 1 to finish each symbol in list order. Defaults to CASH_SYMBOL_WORKERS.",
    )
    parser.add_argument(
        "--upload-workers",
        type=int,
        help="Concurrent Iceberg upload workers. Defaults to CASH_UPLOAD_WORKERS.",
    )
    parser.add_argument(
        "--upstox-max-requests",
        type=int,
        help="Maximum Upstox historical requests to reserve in this run. Defaults to UPSTOX_MAX_REQUESTS_PER_RUN; 0 disables.",
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
    explicit_selection = args.nse_symbol or args.nse_symbols is not None or args.symbols_file is not None
    if args.all_symbols and explicit_selection:
        parser.error("--all cannot be used with an explicit symbol selection")
    if args.nse_symbol and (args.nse_symbols is not None or args.symbols_file is not None):
        parser.error("--nse-symbol cannot be combined with --nse-symbols or --symbols-file")
    requested = list(args.nse_symbols or [])
    if args.symbols_file is not None:
        try:
            requested.extend(line.split("#", 1)[0] for line in args.symbols_file.read_text().splitlines())
        except OSError as exc:
            parser.error(str(exc))
    args.nse_symbols = list(dict.fromkeys(token.upper() for value in requested for token in re.split(r"[\s,]+", value.strip()) if token))
    if explicit_selection and not args.nse_symbol and not args.nse_symbols:
        parser.error("symbol selection must contain at least one symbol")
    if not explicit_selection:
        args.all_symbols = True
    return args


def resolve_requested_symbols(
    symbol_repo: EquitySymbolReferenceRepository,
    sync_repo: MarketDataSyncStateRepository,
    args: argparse.Namespace,
    to_date: date,
) -> list[EquitySymbolReference]:
    """Resolve an explicit list in input order before any downloads start."""

    if not args.nse_symbols:
        return resolve_symbols(symbol_repo, sync_repo, args, to_date)
    symbols = []
    missing = []
    for name in args.nse_symbols:
        symbol = symbol_repo.get_by_nse_symbol(name)
        if symbol is None:
            missing.append(name)
        else:
            symbols.append(symbol)
    if missing:
        message = f"NSE symbols not found in equity_symbol_reference: {', '.join(missing)}"
        if not args.skip_missing_symbols:
            raise ValueError(message)
        logger.warning("cash_upstox_missing_symbols_skipped symbols=%s", ",".join(missing))
    logger.info("cash_upstox_symbols_selected requested=%s resolved=%s missing=%s", len(args.nse_symbols), len(symbols), len(missing))
    return symbols


def resolve_upstox_from_date(
    symbol: EquitySymbolReference,
    sync_state: MarketDataSyncState | None,
    settings: Settings,
    args: argparse.Namespace,
) -> date:
    """Resolve the inclusive start date while respecting Upstox's history floor."""

    provider_start_date = cash_provider_history_start_date("upstox")
    if provider_start_date is None:
        raise ValueError("Upstox history start date is not configured.")

    if args.from_date:
        resolved = parse_date(args.from_date)
    elif settings.cash_sync_from_date:
        resolved = settings.cash_sync_from_date
    elif sync_state and sync_state.status == "completed" and sync_state.to_date:
        resolved = sync_state.to_date + timedelta(days=1)
    elif sync_state and sync_state.status in {"in_progress", "failed"} and sync_state.from_date:
        resolved = sync_state.from_date
    elif symbol.listing_date:
        resolved = max(symbol.listing_date, provider_start_date)
    else:
        resolved = provider_start_date

    # NSE's "date of listing" moves when an ISIN changes (NESTLEIND shows
    # 2023-08-01 after its split) while Upstox serves the older candles under
    # the current ISIN; --ignore-listing-date lets such symbols go below it.
    ignore_listing_date = getattr(args, "ignore_listing_date", False)
    earliest_date = provider_start_date if ignore_listing_date else max(provider_start_date, symbol.listing_date or provider_start_date)
    if resolved < earliest_date:
        logger.info(
            "cash_upstox_from_date_clamped symbol=%s requested_from_date=%s earliest_date=%s",
            symbol.nse_symbol,
            resolved,
            earliest_date,
        )
        return earliest_date
    return resolved


def validate_upstox_date_range(from_date: date, to_date: date) -> None:
    if from_date > to_date:
        raise ValueError(f"from-date {from_date} is after to-date {to_date}")


def count_missing_upstox_fetch_requests(
    settings: Settings,
    symbol: EquitySymbolReference,
    from_date: date,
    to_date: date,
    manifest: CashSyncManifest,
) -> int:
    """Count Upstox monthly requests needed after local manifest/file resumability checks."""

    fetched = set(manifest.fetched_files)
    requests = 0
    for chunk in iter_upstox_fetch_chunks(settings.data_dir, symbol.nse_symbol, from_date, to_date):
        if chunk.already_fetched(fetched):
            continue
        requests += 1
    return requests


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
    upstox = create_upstox_client(settings)
    fetched = set(manifest.fetched_files)
    chunks = list(iter_upstox_fetch_chunks(settings.data_dir, symbol.nse_symbol, from_date, to_date))

    if workers == 1:
        for chunk in chunks:
            result = fetch_cash_chunk(settings, upstox, symbol, chunk, fetched)
            record_fetch_result(manifest, manifest_path, symbol, result)
        return

    errors: list[BaseException] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="cash-upstox-download") as executor:
        futures = [executor.submit(fetch_cash_chunk, settings, upstox, symbol, chunk, fetched) for chunk in chunks]
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
    upstox: UpstoxClient,
    symbol: EquitySymbolReference,
    chunk: UpstoxFetchChunk,
    fetched: set[str],
) -> CashFetchResult:
    """Fetch one Upstox monthly chunk into daily local parquet files."""

    if chunk.already_fetched(fetched):
        # A chunk marker represents all of its daily files. Preserve those paths
        # when rebuilding a manifest for a new range, so later uploads see them.
        local_files = []
        current = chunk.from_date
        while current <= chunk.to_date:
            path = cash_local_path(settings.data_dir, current, symbol.nse_symbol)
            if path.exists():
                local_files.append(str(path))
            current += timedelta(days=1)
        return CashFetchResult(
            local_files=tuple(local_files),
            row_count=sum(read_cash_row_count(Path(path)) for path in local_files),
            existed=True,
        )

    payload = upstox.get_historical_cash(
        instrument_key=resolve_upstox_instrument_key(symbol),
        from_date=chunk.from_date,
        to_date=chunk.to_date,
    )
    rows = transform_upstox_cash_payload(
        payload,
        nse_symbol=symbol.nse_symbol,
        exchange_code=settings.cash_exchange_code,
        product_type=settings.cash_product_type,
    )
    local_files = write_upstox_cash_chunk_files(settings.data_dir, symbol, chunk.from_date, rows)
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
    log_name = "cash_upstox_local_file_exists" if result.existed else "cash_upstox_local_file_written"
    for local_file in result.local_files:
        log = logger.debug if result.existed else logger.info
        local_path = Path(local_file)
        row_count = read_cash_row_count(local_path) if local_path.exists() else result.row_count
        log("%s symbol=%s path=%s rows=%s", log_name, symbol.nse_symbol, local_file, row_count)


def write_upstox_cash_chunk_files(
    data_dir: Path,
    symbol: EquitySymbolReference,
    chunk_start: date,
    rows: list[CashOHLCV],
) -> list[Path]:
    """Write one Upstox monthly response as daily files plus a chunk-start marker."""

    local_files: list[Path] = []
    rows_by_date: dict[date, list[CashOHLCV]] = {}
    for row in rows:
        rows_by_date.setdefault(row.trade_date, []).append(row)

    for trade_date, date_rows in sorted(rows_by_date.items()):
        path = cash_local_path(data_dir, trade_date, symbol.nse_symbol)
        if not path.exists() or read_cash_row_count(path) == 0:
            write_cash_parquet(date_rows, path)
        local_files.append(path)

    marker_path = cash_local_path(data_dir, chunk_start, symbol.nse_symbol)
    if marker_path not in local_files:
        if not marker_path.exists():
            write_cash_parquet([], marker_path)
        local_files.append(marker_path)

    return sorted(local_files)


def iter_upstox_fetch_chunks(data_dir: Path, nse_symbol: str, from_date: date, to_date: date) -> Iterator[UpstoxFetchChunk]:
    """Yield month-bounded Upstox requests with local marker paths."""

    for chunk_start, chunk_end in iter_calendar_month_chunks(from_date, to_date):
        yield UpstoxFetchChunk(
            from_date=chunk_start,
            to_date=chunk_end,
            marker_path=cash_local_path(data_dir, chunk_start, nse_symbol),
        )


def iter_calendar_month_chunks(from_date: date, to_date: date) -> Iterator[tuple[date, date]]:
    """Yield inclusive chunks that never cross a calendar month."""

    current = from_date
    while current <= to_date:
        last_day = calendar.monthrange(current.year, current.month)[1]
        chunk_end = min(to_date, date(current.year, current.month, last_day))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def resolve_upstox_instrument_key(symbol: EquitySymbolReference) -> str:
    """Convert a D1 reference row to the instrument key expected by Upstox."""

    isin = (symbol.isin or "").strip()
    if "|" in isin:
        return isin
    if isin:
        return f"NSE_EQ|{isin}"
    if symbol.nse_symbol.upper() == "NIFTY":
        return "NSE_INDEX|Nifty 50"
    raise UpstoxInstrumentKeyError(f"No ISIN or Upstox instrument key available for {symbol.nse_symbol}")


if __name__ == "__main__":
    main()
