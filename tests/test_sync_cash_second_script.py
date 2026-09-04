from datetime import date

from tick_ticker.config import Settings
from tick_ticker.db.models import EquitySymbolReference
from tick_ticker.scripts.sync_cash_second_data import (
    configured_cash_second_windows,
    count_missing_fetch_requests,
    date_from_cash_second_path,
    resolve_cash_second_symbols,
)
from tick_ticker.services.cash_data import CashSyncManifest, cash_local_path, write_cash_parquet
from tick_ticker.services.cash_second_data import (
    CashSecondSyncManifest,
    cash_second_local_path,
    cash_second_window_local_path,
    transform_cash_second_payload,
    write_cash_second_parquet,
)


def test_count_missing_fetch_requests_counts_intraday_windows(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path,
        cash_second_history_window_seconds=3,
        cash_second_market_open_time="09:15:00",
        cash_second_market_close_time="09:15:05",
        cash_second_skip_weekends=False,
        cash_second_use_minute_empty_days=False,
    )
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND")
    manifest = CashSecondSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
    )

    request_count = count_missing_fetch_requests(settings, symbol, date(2026, 1, 2), date(2026, 1, 2), manifest)

    assert request_count == 2


def test_count_missing_fetch_requests_uses_existing_window_file(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path,
        cash_second_history_window_seconds=3,
        cash_second_market_open_time="09:15:00",
        cash_second_market_close_time="09:15:05",
        cash_second_skip_weekends=False,
        cash_second_use_minute_empty_days=False,
    )
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND")
    manifest = CashSecondSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
    )
    first_window = configured_cash_second_windows(settings, date(2026, 1, 2), date(2026, 1, 2))[0]
    rows = transform_cash_second_payload(
        {"Success": [{"datetime": "2026-01-02 09:15:00", "open": "1", "high": "1", "low": "1", "close": "1"}]},
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )
    write_cash_second_parquet(rows, cash_second_window_local_path(tmp_path, "RELIANCE", first_window))

    request_count = count_missing_fetch_requests(settings, symbol, date(2026, 1, 2), date(2026, 1, 2), manifest)

    assert request_count == 1


def test_count_missing_fetch_requests_skips_existing_daily_file(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path,
        cash_second_history_window_seconds=3,
        cash_second_market_open_time="09:15:00",
        cash_second_market_close_time="09:15:05",
        cash_second_skip_weekends=False,
        cash_second_use_minute_empty_days=False,
    )
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND")
    rows = transform_cash_second_payload(
        {"Success": [{"datetime": "2026-01-02 09:15:00", "open": "1", "high": "1", "low": "1", "close": "1"}]},
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )
    write_cash_second_parquet(rows, cash_second_local_path(tmp_path, date(2026, 1, 2), "RELIANCE"))
    manifest = CashSecondSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
    )

    request_count = count_missing_fetch_requests(settings, symbol, date(2026, 1, 2), date(2026, 1, 2), manifest)

    assert request_count == 0


def test_count_missing_fetch_requests_skips_known_empty_minute_day(tmp_path) -> None:
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path,
        cash_second_history_window_seconds=3,
        cash_second_market_open_time="09:15:00",
        cash_second_market_close_time="09:15:05",
        cash_second_skip_weekends=False,
        cash_second_use_minute_empty_days=True,
    )
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND")
    write_cash_parquet([], cash_local_path(tmp_path, date(2026, 1, 2), "RELIANCE"))
    manifest = CashSecondSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2026, 1, 2),
        to_date=date(2026, 1, 2),
    )

    request_count = count_missing_fetch_requests(settings, symbol, date(2026, 1, 2), date(2026, 1, 2), manifest)

    assert request_count == 0


def test_resolve_cash_second_symbols_uses_local_minute_manifest_without_d1(tmp_path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    manifest_path = tmp_path / "state" / "cash" / "HDFCBANK.json"
    CashSyncManifest(
        nse_symbol="HDFCBANK",
        breeze_code="HDFBAN",
        from_date=date(2026, 1, 1),
        to_date=date(2026, 1, 2),
        coverage_from_date=date(2026, 1, 1),
    ).save(manifest_path)

    symbols = resolve_cash_second_symbols(
        settings,
        requested_symbols=["HDFCBANK"],
        explicit_codes={},
        d1_fallback=False,
    )

    assert symbols[0].nse_symbol == "HDFCBANK"
    assert symbols[0].breeze_code == "HDFBAN"
    assert symbols[0].listing_date == date(2026, 1, 1)


def test_resolve_cash_second_symbols_uses_identity_code_for_nifty_without_d1(tmp_path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)

    symbols = resolve_cash_second_symbols(
        settings,
        requested_symbols=["NIFTY"],
        explicit_codes={},
        d1_fallback=False,
    )

    assert symbols[0].nse_symbol == "NIFTY"
    assert symbols[0].breeze_code == "NIFTY"
    assert symbols[0].listing_date is None


def test_date_from_cash_second_path() -> None:
    assert date_from_cash_second_path("data/cash_1s/by_symbol/RELIANCE/2026/01/02.parquet") == date(2026, 1, 2)
