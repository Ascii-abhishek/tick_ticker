from datetime import date
from pathlib import Path

from tick_ticker.config import Settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncState
from tick_ticker.scripts.sync_cash_upstox_data import (
    UpstoxInstrumentKeyError,
    UpstoxRequestBudget,
    UpstoxRequestBudgetExceededError,
    count_missing_upstox_fetch_requests,
    iter_calendar_month_chunks,
    parse_args,
    resolve_upstox_from_date,
    resolve_upstox_instrument_key,
    sync_resolved_symbols,
    write_upstox_cash_chunk_files,
)
from tick_ticker.services.cash_data import CashSyncManifest, cash_local_path, read_cash_row_count, transform_upstox_cash_payload


class Args:
    from_date: str | None = None


def test_parse_args_defaults_to_all_symbols(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sync-cash-upstox-data"])

    args = parse_args()

    assert args.all_symbols is True
    assert args.nse_symbol is None


def test_parse_args_keeps_single_symbol_mode(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sync-cash-upstox-data", "--nse-symbol", "RELIANCE"])

    args = parse_args()

    assert args.all_symbols is False
    assert args.nse_symbol == "RELIANCE"


def test_resolve_upstox_instrument_key_wraps_equity_isin() -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", isin="INE002A01018")

    assert resolve_upstox_instrument_key(symbol) == "NSE_EQ|INE002A01018"


def test_resolve_upstox_instrument_key_uses_full_reference_key() -> None:
    symbol = EquitySymbolReference(nse_symbol="NIFTY", breeze_code="NIFTY", isin="NSE_INDEX|Nifty 50")

    assert resolve_upstox_instrument_key(symbol) == "NSE_INDEX|Nifty 50"


def test_resolve_upstox_instrument_key_rejects_missing_isin() -> None:
    symbol = EquitySymbolReference(nse_symbol="AAA", breeze_code="AAA")

    try:
        resolve_upstox_instrument_key(symbol)
    except UpstoxInstrumentKeyError as exc:
        assert "AAA" in str(exc)
    else:
        raise AssertionError("expected UpstoxInstrumentKeyError")


def test_iter_calendar_month_chunks_stays_inside_months() -> None:
    chunks = list(iter_calendar_month_chunks(date(2022, 1, 15), date(2022, 3, 5)))

    assert chunks == [
        (date(2022, 1, 15), date(2022, 1, 31)),
        (date(2022, 2, 1), date(2022, 2, 28)),
        (date(2022, 3, 1), date(2022, 3, 5)),
    ]


def test_resolve_upstox_from_date_clamps_to_history_start() -> None:
    args = Args()
    args.from_date = "2020-01-01"
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(1995, 1, 1))

    from_date = resolve_upstox_from_date(symbol, None, Settings(_env_file=None), args)

    assert from_date == date(2022, 1, 1)


def test_resolve_upstox_from_date_continues_after_completed_state() -> None:
    args = Args()
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", listing_date=date(1995, 1, 1))
    state = MarketDataSyncState(
        market_type="cash",
        nse_symbol="RELIANCE",
        status="completed",
        from_date=date(2022, 1, 1),
        to_date=date(2026, 1, 10),
    )

    from_date = resolve_upstox_from_date(symbol, state, Settings(_env_file=None), args)

    assert from_date == date(2026, 1, 11)


def test_upstox_request_budget_rejects_when_run_limit_would_be_exceeded() -> None:
    budget = UpstoxRequestBudget(max_requests=2)

    budget.reserve(symbol="AAA", requests=2)

    try:
        budget.reserve(symbol="BBB", requests=1)
    except UpstoxRequestBudgetExceededError as exc:
        assert "BBB" in str(exc)
    else:
        raise AssertionError("expected UpstoxRequestBudgetExceededError")


def test_sync_resolved_symbols_stops_when_request_budget_is_exhausted(monkeypatch) -> None:
    symbols = [
        EquitySymbolReference(nse_symbol="AAA", breeze_code="AAA", isin="INE000A00001"),
        EquitySymbolReference(nse_symbol="BBB", breeze_code="BBB", isin="INE000A00002"),
        EquitySymbolReference(nse_symbol="CCC", breeze_code="CCC", isin="INE000A00003"),
    ]
    args = Args()
    args.all_symbols = True
    calls = []

    def fake_sync_resolved_symbol(**kwargs: object) -> tuple[bool, bool]:
        symbol = kwargs["symbol"]
        assert isinstance(symbol, EquitySymbolReference)
        calls.append(symbol.nse_symbol)
        if symbol.nse_symbol == "BBB":
            raise UpstoxRequestBudgetExceededError("budget exhausted")
        return True, False

    monkeypatch.setattr("tick_ticker.scripts.sync_cash_upstox_data.sync_resolved_symbol", fake_sync_resolved_symbol)

    synced_count, skipped_count = sync_resolved_symbols(
        settings=Settings(_env_file=None),
        sync_repo=object(),
        symbols=symbols,
        args=args,
        to_date=date(2026, 9, 4),
        download_workers=1,
        upload_workers=1,
        upload_batch_size=25,
        symbol_workers=1,
        upstox_request_budget=UpstoxRequestBudget(1),
    )

    assert calls == ["AAA", "BBB"]
    assert synced_count == 1
    assert skipped_count == 0


def test_count_missing_upstox_fetch_requests_uses_month_marker(tmp_path: Path) -> None:
    settings = Settings(_env_file=None, data_dir=tmp_path)
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", isin="INE002A01018")
    marker_path = cash_local_path(tmp_path, date(2022, 1, 1), "RELIANCE")
    marker_path.parent.mkdir(parents=True)
    marker_path.touch()
    manifest = CashSyncManifest(
        nse_symbol="RELIANCE",
        breeze_code="RELIND",
        from_date=date(2022, 1, 1),
        to_date=date(2022, 3, 31),
        fetched_files=[str(marker_path)],
    )

    request_count = count_missing_upstox_fetch_requests(settings, symbol, date(2022, 1, 1), date(2022, 3, 31), manifest)

    assert request_count == 2


def test_write_upstox_cash_chunk_files_splits_daily_and_writes_marker(tmp_path: Path) -> None:
    symbol = EquitySymbolReference(nse_symbol="RELIANCE", breeze_code="RELIND", isin="INE002A01018")
    rows = transform_upstox_cash_payload(
        {
            "data": {
                "candles": [
                    ["2022-01-04T09:15:00+05:30", 100, 101, 99, 100.5, 10, 0],
                    ["2022-01-03T09:15:00+05:30", 99, 100, 98, 99.5, 20, 0],
                ]
            }
        },
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )

    paths = write_upstox_cash_chunk_files(tmp_path, symbol, date(2022, 1, 1), rows)

    assert paths == [
        cash_local_path(tmp_path, date(2022, 1, 1), "RELIANCE"),
        cash_local_path(tmp_path, date(2022, 1, 3), "RELIANCE"),
        cash_local_path(tmp_path, date(2022, 1, 4), "RELIANCE"),
    ]
    assert read_cash_row_count(paths[0]) == 0
    assert read_cash_row_count(paths[1]) == 1
    assert read_cash_row_count(paths[2]) == 1
