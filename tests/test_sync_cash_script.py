from datetime import date
from pathlib import Path

import pytest

from tick_ticker.config import Settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncState
from tick_ticker.scripts.sync_cash_data import (
    coverage_from_date,
    date_from_cash_path,
    load_or_create_manifest,
    resolve_from_date,
    resolve_worker_count,
    upload_to_iceberg,
    validate_date_range,
)
from tick_ticker.services.cash_data import CashSyncManifest, transform_cash_payload, write_cash_parquet


class Args:
    from_date: str | None = None


def test_validate_date_range_rejects_large_range_without_override() -> None:
    settings = Settings(cash_sync_max_days_per_run=2)

    with pytest.raises(ValueError, match="Refusing to sync"):
        validate_date_range(date(2026, 1, 1), date(2026, 1, 3), settings, allow_large_range=False)


def test_validate_date_range_allows_large_range_with_override() -> None:
    settings = Settings(cash_sync_max_days_per_run=2)

    validate_date_range(date(2026, 1, 1), date(2026, 1, 3), settings, allow_large_range=True)


def test_resolve_worker_count_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="download-workers"):
        resolve_worker_count(0, 1, "download-workers")


def test_date_from_cash_path() -> None:
    assert date_from_cash_path("data/cash/2026/01/02/RELIANCE.parquet") == date(2026, 1, 2)


def test_resolve_from_date_uses_listing_date_when_no_state() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))

    assert resolve_from_date(symbol, None, Settings(_env_file=None), Args()) == date(2020, 1, 1)


def test_resolve_from_date_uses_provider_start_when_listing_is_older() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(1995, 1, 1))

    assert resolve_from_date(symbol, None, Settings(_env_file=None), Args()) == date(2016, 1, 1)


def test_resolve_from_date_uses_listing_date_when_after_provider_start() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2018, 1, 1))

    assert resolve_from_date(symbol, None, Settings(_env_file=None), Args()) == date(2018, 1, 1)


def test_resolve_from_date_continues_after_completed_to_date() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    state = MarketDataSyncState(
        market_type="cash",
        nse_symbol="RELIANCE",
        status="completed",
        from_date=date(2020, 1, 1),
        to_date=date(2026, 1, 10),
    )

    assert resolve_from_date(symbol, state, Settings(_env_file=None), Args()) == date(2026, 1, 11)


def test_resolve_from_date_retries_failed_range() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    state = MarketDataSyncState(
        market_type="cash",
        nse_symbol="RELIANCE",
        status="failed",
        from_date=date(2026, 1, 10),
        to_date=date(2026, 1, 20),
    )

    assert resolve_from_date(symbol, state, Settings(_env_file=None), Args()) == date(2026, 1, 10)


def test_coverage_from_date_keeps_original_completed_start() -> None:
    state = MarketDataSyncState(
        market_type="cash",
        nse_symbol="RELIANCE",
        status="completed",
        from_date=date(2020, 1, 1),
        to_date=date(2026, 1, 10),
    )

    assert coverage_from_date(state, date(2026, 1, 11)) == date(2020, 1, 1)


def test_manifest_resets_for_new_range_after_upload(tmp_path: Path) -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    manifest_path = tmp_path / "RELIANCE.json"
    manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 1),
        allow_range_reset=False,
    )
    manifest.fetched_files = ["data/cash/2026/01/01/RELIANCE.parquet"]
    manifest.uploaded_files = ["data/cash/2026/01/01/RELIANCE.parquet"]
    manifest.save(manifest_path)

    next_manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
        allow_range_reset=False,
    )

    assert next_manifest.from_date == date(2026, 1, 2)
    assert next_manifest.to_date == date(2026, 1, 2)
    assert next_manifest.fetched_files == []


def test_manifest_extends_partial_range_for_incremental_retry(tmp_path: Path) -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    manifest_path = tmp_path / "RELIANCE.json"
    manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
        allow_range_reset=False,
    )
    manifest.fetched_files = [
        "data/cash/2026/01/01/RELIANCE.parquet",
        "data/cash/2026/01/02/RELIANCE.parquet",
    ]
    manifest.uploaded_files = ["data/cash/2026/01/01/RELIANCE.parquet"]
    manifest.save(manifest_path)

    next_manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 5),
        allow_range_reset=False,
    )

    assert next_manifest.from_date == date(2026, 1, 1)
    assert next_manifest.to_date == date(2026, 1, 5)
    assert next_manifest.fetched_files == [
        "data/cash/2026/01/01/RELIANCE.parquet",
        "data/cash/2026/01/02/RELIANCE.parquet",
    ]
    assert next_manifest.uploaded_files == ["data/cash/2026/01/01/RELIANCE.parquet"]


def test_manifest_extends_adjacent_incremental_range(tmp_path: Path) -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    manifest_path = tmp_path / "RELIANCE.json"
    manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
        allow_range_reset=False,
    )
    manifest.fetched_files = ["data/cash/2026/01/02/RELIANCE.parquet"]
    manifest.save(manifest_path)

    next_manifest = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=symbol,
        from_date=date(2026, 1, 3),
        to_date=date(2026, 1, 5),
        allow_range_reset=False,
    )

    assert next_manifest.from_date == date(2026, 1, 1)
    assert next_manifest.to_date == date(2026, 1, 5)
    assert next_manifest.fetched_files == ["data/cash/2026/01/02/RELIANCE.parquet"]


def test_upload_to_iceberg_appends_uncommitted_source_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    parquet_path = tmp_path / "data" / "cash" / "2026" / "01" / "02" / "RELIANCE.parquet"
    rows = transform_cash_payload(
        {
            "Success": [
                {
                    "datetime": "2026-01-02 09:15:00",
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100.5",
                    "volume": "10",
                }
            ]
        },
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )
    write_cash_parquet(rows, parquet_path)
    manifest_path = tmp_path / "RELIANCE.json"
    manifest = CashSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
        fetched_files=[str(parquet_path)],
    )
    manifest.save(manifest_path)
    calls = []

    class FakeCatalog:
        def __init__(self, settings: Settings) -> None:
            self.settings = settings

        def ensure_market_data_tables(self) -> dict[str, tuple[str, str]]:
            return {"cash": ("cash", "ohlcv")}

        def committed_source_paths(self, market_type: str) -> set[str]:
            return set()

        def append_parquet_files(self, market_type: str, paths: list[Path], *, snapshot_properties: dict[str, str]) -> None:
            calls.append((market_type, paths, snapshot_properties))

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.IcebergMarketDataCatalog", FakeCatalog)

    upload_to_iceberg(Settings(_env_file=None), manifest, manifest_path, workers=1, batch_size=25)

    assert len(calls) == 1
    assert calls[0][0] == "cash"
    assert calls[0][1] == [parquet_path]
    assert calls[0][2]["tick_ticker.nse_symbol"] == "RELIANCE"
    assert calls[0][2]["tick_ticker.trade_date_from"] == "2026-01-02"
    assert calls[0][2]["tick_ticker.trade_date_to"] == "2026-01-02"
    assert calls[0][2]["tick_ticker.source_paths"] == f'["{parquet_path}"]'
    assert calls[0][2]["tick_ticker.write_mode"] == "append"
    assert CashSyncManifest.load(manifest_path).uploaded_files == [str(parquet_path)]


def test_upload_to_iceberg_marks_already_committed_source_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    parquet_path = tmp_path / "data" / "cash" / "2026" / "01" / "02" / "RELIANCE.parquet"
    rows = transform_cash_payload(
        {
            "Success": [
                {
                    "datetime": "2026-01-02 09:15:00",
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100.5",
                    "volume": "10",
                }
            ]
        },
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )
    write_cash_parquet(rows, parquet_path)
    manifest_path = tmp_path / "RELIANCE.json"
    manifest = CashSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
        fetched_files=[str(parquet_path)],
    )
    manifest.save(manifest_path)
    calls = []

    class FakeCatalog:
        def __init__(self, settings: Settings) -> None:
            self.settings = settings

        def ensure_market_data_tables(self) -> dict[str, tuple[str, str]]:
            return {"cash": ("cash", "ohlcv")}

        def committed_source_paths(self, market_type: str) -> set[str]:
            return {str(parquet_path)}

        def append_parquet_files(self, market_type: str, paths: list[Path], *, snapshot_properties: dict[str, str]) -> None:
            calls.append((market_type, paths, snapshot_properties))

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.IcebergMarketDataCatalog", FakeCatalog)

    upload_to_iceberg(Settings(_env_file=None), manifest, manifest_path, workers=1, batch_size=25)

    assert calls == []
    assert CashSyncManifest.load(manifest_path).uploaded_files == [str(parquet_path)]
