import sqlite3
import threading
import time
from datetime import date, datetime
from pathlib import Path

import pytest

from tick_ticker.config import Settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncState
from tick_ticker.db.repositories import MarketDataSyncStateRepository
from tick_ticker.scripts.sync_cash_data import (
    BreezeRequestBudget,
    BreezeRequestBudgetExceededError,
    CashUploadTask,
    DateRangeTooLargeError,
    count_missing_fetch_requests,
    coverage_from_date,
    date_from_cash_path,
    load_or_create_manifest,
    resolve_from_date,
    resolve_non_negative_count,
    resolve_worker_count,
    sync_resolved_symbols,
    upload_cash_tasks,
    upload_to_iceberg,
    validate_date_range,
)
from tick_ticker.services.cash_data import CashOHLCV, CashSyncManifest, cash_local_path, read_cash_row_count, transform_cash_payload, write_cash_parquet


class Args:
    from_date: str | None = None


def test_validate_date_range_rejects_large_range_without_override() -> None:
    settings = Settings(cash_sync_max_days_per_run=2)

    with pytest.raises(DateRangeTooLargeError, match="Refusing to sync"):
        validate_date_range(date(2026, 1, 1), date(2026, 1, 3), settings, allow_large_range=False)


def test_validate_date_range_allows_large_range_with_override() -> None:
    settings = Settings(cash_sync_max_days_per_run=2)

    validate_date_range(date(2026, 1, 1), date(2026, 1, 3), settings, allow_large_range=True)


def test_resolve_worker_count_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="download-workers"):
        resolve_worker_count(0, 1, "download-workers")


def test_resolve_non_negative_count_rejects_negative_values() -> None:
    with pytest.raises(ValueError, match="breeze-max-requests"):
        resolve_non_negative_count(-1, 1, "breeze-max-requests")


def test_breeze_request_budget_rejects_when_run_limit_would_be_exceeded() -> None:
    budget = BreezeRequestBudget(max_requests=2)

    budget.reserve(symbol="AAA", requests=2)

    with pytest.raises(BreezeRequestBudgetExceededError, match="BBB"):
        budget.reserve(symbol="BBB", requests=1)


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


def test_due_cash_symbols_include_unsynced_and_stale_completed_symbols() -> None:
    client = SqliteD1Client()
    client.execute(
        """
        CREATE TABLE equity_symbol_reference (
            nse_symbol TEXT PRIMARY KEY,
            breeze_code TEXT NOT NULL,
            nse_company_name TEXT,
            listing_date TEXT,
            isin TEXT
        )
        """
    )
    MarketDataSyncStateRepository(client).ensure_table()
    client.execute(
        """
        INSERT INTO equity_symbol_reference (nse_symbol, breeze_code, listing_date)
        VALUES
            ('AAA', 'AAA', '2020-01-01'),
            ('BBB', 'BBB', '2020-01-01'),
            ('CCC', 'CCC', '2020-01-01')
        """
    )
    client.execute(
        """
        INSERT INTO market_data_sync_state (market_type, nse_symbol, status, from_date, to_date, updated_at)
        VALUES
            ('cash', 'BBB', 'completed', '2020-01-01', '2026-01-09', '2026-01-09T00:00:00+00:00'),
            ('cash', 'CCC', 'completed', '2020-01-01', '2026-01-10', '2026-01-10T00:00:00+00:00')
        """
    )

    symbols = MarketDataSyncStateRepository(client).due_cash_symbols(target_to_date=date(2026, 1, 10))
    synced = MarketDataSyncStateRepository(client).due_cash_symbols(target_to_date=date(2026, 1, 10), synced_only=True)

    assert [symbol.nse_symbol for symbol in symbols] == ["AAA", "BBB"]
    assert [symbol.nse_symbol for symbol in synced] == ["BBB"]


def test_sync_resolved_symbols_skips_large_ranges_during_all_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    symbols = [
        EquitySymbolReference(nse_symbol="AAA", breeze_code="AAA", listing_date=date(2020, 1, 1)),
        EquitySymbolReference(nse_symbol="BBB", breeze_code="BBB", listing_date=date(2020, 1, 1)),
    ]
    args = Args()
    args.all_symbols = True
    calls = []

    def fake_sync_cash_symbol(**kwargs: object) -> bool:
        symbol = kwargs["symbol"]
        assert isinstance(symbol, EquitySymbolReference)
        calls.append(symbol.nse_symbol)
        if symbol.nse_symbol == "AAA":
            raise DateRangeTooLargeError("too large")
        return True

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.sync_cash_symbol", fake_sync_cash_symbol)

    synced_count, skipped_count = sync_resolved_symbols(
        settings=Settings(_env_file=None),
        sync_repo=object(),
        symbols=symbols,
        args=args,
        to_date=date(2026, 1, 10),
        download_workers=1,
        upload_workers=1,
        upload_batch_size=25,
        symbol_workers=1,
        breeze_request_budget=None,
    )

    assert calls == ["AAA", "BBB"]
    assert synced_count == 1
    assert skipped_count == 1


def test_sync_resolved_symbols_uses_symbol_worker_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    symbols = [
        EquitySymbolReference(nse_symbol="AAA", breeze_code="AAA", listing_date=date(2020, 1, 1)),
        EquitySymbolReference(nse_symbol="BBB", breeze_code="BBB", listing_date=date(2020, 1, 1)),
        EquitySymbolReference(nse_symbol="CCC", breeze_code="CCC", listing_date=date(2020, 1, 1)),
    ]
    args = Args()
    args.all_symbols = True
    calls = []
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_sync_cash_symbol(**kwargs: object) -> bool:
        nonlocal active, max_active
        symbol = kwargs["symbol"]
        assert isinstance(symbol, EquitySymbolReference)
        with lock:
            calls.append(symbol.nse_symbol)
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return True

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.sync_cash_symbol", fake_sync_cash_symbol)

    synced_count, skipped_count = sync_resolved_symbols(
        settings=Settings(_env_file=None),
        sync_repo=object(),
        symbols=symbols,
        args=args,
        to_date=date(2026, 1, 10),
        download_workers=1,
        upload_workers=1,
        upload_batch_size=25,
        symbol_workers=2,
        breeze_request_budget=None,
    )

    assert sorted(calls) == ["AAA", "BBB", "CCC"]
    assert synced_count == 3
    assert skipped_count == 0
    assert max_active == 2


def test_count_missing_fetch_requests_uses_manifest_and_existing_files(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(2020, 1, 1))
    fetched_path = cash_local_path(tmp_path, date(2026, 1, 1), "RELIANCE")
    existing_path = cash_local_path(tmp_path, date(2026, 1, 2), "RELIANCE")
    existing_path.parent.mkdir(parents=True)
    existing_path.touch()
    manifest = CashSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 3),
        fetched_files=[str(fetched_path)],
    )

    request_count = count_missing_fetch_requests(settings, symbol, date(2026, 1, 1), date(2026, 1, 3), manifest)

    assert request_count == 1


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
    manifest.record_upload_event(
        table="cash.ohlcv_by_symbol",
        coverage_from_date=date(2026, 1, 1),
        coverage_to_date=date(2026, 1, 1),
        coverage_file_count=1,
        coverage_row_count=100,
    )
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
    assert next_manifest.uploaded_files == []
    assert next_manifest.coverage_from_date == date(2026, 1, 1)
    assert next_manifest.coverage_to_date == date(2026, 1, 1)
    assert next_manifest.coverage_file_count == 1
    assert next_manifest.coverage_row_count == 100
    assert next_manifest.last_upload is not None
    assert next_manifest.last_upload.table == "cash.ohlcv_by_symbol"


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
            return {"cash": ("cash", "ohlcv_by_symbol")}

        def committed_source_paths(self, market_type: str) -> set[str]:
            return set()

        def replace_parquet_files(self, market_type: str, paths: list[Path], *, nse_symbol: str, trade_dates: list, snapshot_properties: dict[str, str]) -> None:
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
    assert calls[0][2]["tick_ticker.write_mode"] == "replace"
    assert CashSyncManifest.load(manifest_path).uploaded_files == [str(parquet_path)]


def test_upload_to_iceberg_splits_failed_batch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    parquet_paths = []
    for day in range(1, 5):
        parquet_path = tmp_path / "data" / "cash" / "2026" / "01" / f"0{day}" / "RELIANCE.parquet"
        rows = transform_cash_payload(
            {
                "Success": [
                    {
                        "datetime": f"2026-01-0{day} 09:15:00",
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
        parquet_paths.append(parquet_path)

    manifest_path = tmp_path / "RELIANCE.json"
    manifest = CashSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 4),
        fetched_files=[str(path) for path in parquet_paths],
    )
    manifest.save(manifest_path)
    calls = []

    class FakeCatalog:
        def __init__(self, settings: Settings) -> None:
            self.settings = settings

        def ensure_market_data_tables(self) -> dict[str, tuple[str, str]]:
            return {"cash": ("cash", "ohlcv_by_symbol")}

        def committed_source_paths(self, market_type: str) -> set[str]:
            return set()

        def replace_parquet_files(self, market_type: str, paths: list[Path], *, nse_symbol: str, trade_dates: list, snapshot_properties: dict[str, str]) -> None:
            calls.append(paths)
            if len(paths) > 1:
                raise OSError("NO_SUCH_UPLOAD")

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.IcebergMarketDataCatalog", FakeCatalog)

    upload_to_iceberg(Settings(_env_file=None, cash_upload_retry_attempts=1), manifest, manifest_path, workers=1, batch_size=4)

    assert [len(paths) for paths in calls] == [4, 2, 1, 1, 2, 1, 1]
    assert CashSyncManifest.load(manifest_path).uploaded_files == [str(path) for path in parquet_paths]


def test_upload_cash_tasks_serializes_cash_iceberg_commits(tmp_path: Path) -> None:
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
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    write_cash_parquet(rows, first_path)
    write_cash_parquet(rows, second_path)
    active = 0
    max_active = 0
    lock = threading.Lock()

    class FakeCatalog:
        def replace_parquet_files(self, market_type: str, paths: list[Path], *, nse_symbol: str, trade_dates: list, snapshot_properties: dict[str, str]) -> None:
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1

    def task(path: Path) -> tuple[CashUploadTask, ...]:
        return (
            CashUploadTask(
                local_file=str(path),
                local_path=path,
                trade_date=date(2026, 1, 2),
                row_count=1,
            ),
        )

    errors = []

    def run_upload(symbol: str, path: Path) -> None:
        try:
            upload_cash_tasks(Settings(_env_file=None), FakeCatalog(), "cash", symbol, task(path))
        except BaseException as exc:
            errors.append(exc)

    first_thread = threading.Thread(target=run_upload, args=("AAA", first_path))
    second_thread = threading.Thread(target=run_upload, args=("BBB", second_path))

    first_thread.start()
    second_thread.start()
    first_thread.join()
    second_thread.join()

    assert errors == []
    assert max_active == 1


class SqliteD1Client:
    def __init__(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row

    def query(self, sql: str, params: list[object] | None = None) -> list[dict[str, object]]:
        cursor = self.connection.execute(sql, params or [])
        self.connection.commit()
        return [dict(row) for row in cursor.fetchall()]

    def execute(self, sql: str, params: list[object] | None = None) -> None:
        self.query(sql, params)


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
            return {"cash": ("cash", "ohlcv_by_symbol")}

        def committed_source_paths(self, market_type: str) -> set[str]:
            return {str(parquet_path)}

        def replace_parquet_files(self, market_type: str, paths: list[Path], *, nse_symbol: str, trade_dates: list, snapshot_properties: dict[str, str]) -> None:
            calls.append((market_type, paths, snapshot_properties))

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_data.IcebergMarketDataCatalog", FakeCatalog)

    upload_to_iceberg(Settings(_env_file=None), manifest, manifest_path, workers=1, batch_size=25)

    assert calls == []
    assert CashSyncManifest.load(manifest_path).uploaded_files == [str(parquet_path)]


def test_plan_fetch_chunks_groups_known_sessions_and_falls_back_outside(tmp_path: Path) -> None:
    from tick_ticker.scripts.sync_cash_data import known_trading_sessions, plan_fetch_chunks

    sessions = [date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6), date(2020, 1, 7), date(2020, 1, 8)]
    for day in sessions:
        for symbol in ("AAA", "BBB", "CCC"):
            write_cash_parquet(
                [
                    CashOHLCV(
                        datetime=datetime(day.year, day.month, day.day, 9, 15),
                        trade_date=day,
                        nse_symbol=symbol,
                        exchange_code="NSE",
                        product_type="cash",
                        open=1,
                        high=1,
                        low=1,
                        close=1,
                        volume=1,
                        ingested_at=datetime(2026, 1, 1),
                    )
                ],
                cash_local_path(tmp_path, day, symbol),
            )
    known_trading_sessions.cache_clear()
    settings = Settings(data_dir=tmp_path, _env_file=None)

    chunks = plan_fetch_chunks(settings, date(2020, 1, 1), date(2020, 1, 10))

    assert chunks == [
        (date(2020, 1, 1), date(2020, 1, 1)),
        (date(2020, 1, 2), date(2020, 1, 3)),
        (date(2020, 1, 6), date(2020, 1, 7)),
        (date(2020, 1, 8), date(2020, 1, 8)),
        (date(2020, 1, 9), date(2020, 1, 9)),
        (date(2020, 1, 10), date(2020, 1, 10)),
    ]


def test_write_cash_chunk_files_files_rows_by_trade_date_and_marks_empty_start(tmp_path: Path) -> None:
    from tick_ticker.scripts.sync_cash_data import write_cash_chunk_files

    monday = date(2020, 1, 6)
    row = CashOHLCV(
        datetime=datetime(2020, 1, 6, 9, 15),
        trade_date=monday,
        nse_symbol="AAA",
        exchange_code="NSE",
        product_type="cash",
        open=1,
        high=1,
        low=1,
        close=1,
        volume=1,
        ingested_at=datetime(2026, 1, 1),
    )
    symbol = EquitySymbolReference(nse_symbol="AAA", breeze_code="AAA")

    paths = write_cash_chunk_files(Settings(data_dir=tmp_path, _env_file=None), symbol, date(2020, 1, 5), [row])

    assert paths == [cash_local_path(tmp_path, date(2020, 1, 5), "AAA"), cash_local_path(tmp_path, monday, "AAA")]
    assert read_cash_row_count(paths[0]) == 0
    assert read_cash_row_count(paths[1]) == 1
