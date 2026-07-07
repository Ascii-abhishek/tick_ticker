"""Reconcile local cash manifests and D1 state from local Parquet coverage."""

from __future__ import annotations

import argparse
from pathlib import Path

from tick_ticker.config import Settings, get_settings
from tick_ticker.db.models import MarketDataSyncCompletion
from tick_ticker.db.repositories import MarketDataSyncStateRepository
from tick_ticker.services.cash_data import CashSyncManifest, cash_manifest_path
from tick_ticker.scripts.sync_cash_data import cash_local_coverage, date_from_cash_path
from tick_ticker.utils.datetime import utc_now
from tick_ticker.utils.engines import create_d1_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = parse_args()
    nse_symbols = {symbol.upper() for symbol in args.nse_symbol} if args.nse_symbol else None
    result = reconcile_cash_sync_state(settings, update_d1=not args.local_only, nse_symbols=nse_symbols)
    logger.info(
        "cash_sync_state_reconciled manifests=%s d1_rows=%s symbols=%s",
        result["manifests"],
        result["d1_rows"],
        result["symbols"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nse-symbol", action="append", help="Reconcile only this NSE symbol. Can be passed multiple times.")
    parser.add_argument("--local-only", action="store_true", help="Update local JSON manifests only; do not update D1.")
    return parser.parse_args()


def reconcile_cash_sync_state(settings: Settings, *, update_d1: bool, nse_symbols: set[str] | None = None) -> dict[str, int]:
    """Rebuild local cash manifests and optionally D1 state from local files."""

    symbols = sorted(nse_symbols or cash_symbols_with_local_files(settings.data_dir))
    sync_repo = MarketDataSyncStateRepository(create_d1_client(settings)) if update_d1 else None
    if sync_repo is not None:
        sync_repo.ensure_table()

    table_id = cash_table_id(settings)
    completed_at = utc_now()
    manifest_count = 0
    d1_count = 0
    for nse_symbol in symbols:
        local_paths = cash_local_paths(settings.data_dir, nse_symbol)
        if not local_paths:
            continue
        coverage = cash_local_coverage(settings.data_dir, nse_symbol)
        if coverage.from_date is None or coverage.to_date is None:
            continue

        manifest_path = cash_manifest_path(settings.data_dir, nse_symbol)
        existing_manifest = CashSyncManifest.load(manifest_path)
        manifest = CashSyncManifest(
            nse_symbol=nse_symbol,
            breeze_code=existing_manifest.breeze_code if existing_manifest else nse_symbol,
            from_date=coverage.from_date,
            to_date=coverage.to_date,
            fetched_files=[str(path) for path in local_paths],
            uploaded_files=[str(path) for path in local_paths],
        )
        manifest.record_fetch_event(completed_at=completed_at)
        manifest.record_upload_event(
            table=table_id,
            coverage_from_date=coverage.from_date,
            coverage_to_date=coverage.to_date,
            coverage_file_count=coverage.file_count,
            coverage_row_count=coverage.row_count,
            completed_at=completed_at,
        )
        manifest.save(manifest_path)
        manifest_count += 1

        if sync_repo is not None:
            sync_repo.mark_completed(
                MarketDataSyncCompletion(
                    market_type="cash",
                    nse_symbol=nse_symbol,
                    from_date=coverage.from_date,
                    to_date=coverage.to_date,
                    row_count=coverage.row_count,
                    local_path=str(settings.data_dir / "cash"),
                    r2_prefix=table_id,
                    completed_at=completed_at,
                )
            )
            d1_count += 1
        logger.info(
            "cash_sync_state_reconciled_symbol symbol=%s from=%s to=%s files=%s rows=%s table=%s",
            nse_symbol,
            coverage.from_date,
            coverage.to_date,
            coverage.file_count,
            coverage.row_count,
            table_id,
        )

    return {"manifests": manifest_count, "d1_rows": d1_count, "symbols": len(symbols)}


def cash_symbols_with_local_files(data_dir: Path) -> set[str]:
    return {path.stem.upper() for path in (data_dir / "cash").glob("*/*/*/*.parquet")}


def cash_local_paths(data_dir: Path, nse_symbol: str) -> list[Path]:
    return sorted((data_dir / "cash").glob(f"*/*/*/{nse_symbol}.parquet"), key=date_from_cash_path)


def cash_table_id(settings: Settings) -> str:
    return f"{settings.iceberg_cash_namespace}.{settings.iceberg_cash_table}"


if __name__ == "__main__":
    main()
