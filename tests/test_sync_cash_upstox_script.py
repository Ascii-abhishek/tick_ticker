from datetime import date
from pathlib import Path

import pytest

from tick_ticker.config import Settings
from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncState
from tick_ticker.scripts.sync_cash_upstox_data import (
    UpstoxInstrumentKeyError,
    UpstoxRequestBudget,
    UpstoxRequestBudgetExceededError,
    UpstoxFetchChunk,
    count_missing_upstox_fetch_requests,
    fetch_cash_chunk,
    iter_calendar_month_chunks,
    parse_args,
    resolve_requested_symbols,
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


def test_parse_ordered_symbols_from_arguments_and_file(monkeypatch, tmp_path) -> None:
    source = tmp_path / "symbols.txt"
    source.write_text("# priority\nM&M, ACC\nBAJAJ-AUTO # comment\n")
    monkeypatch.setattr("sys.argv", ["sync-cash-upstox-data", "--nse-symbols", "acc,RELIANCE", "--nse-symbols", "ACC", "--symbols-file", str(source)])
    args = parse_args()
    assert args.nse_symbols == ["ACC", "RELIANCE", "M&M", "BAJAJ-AUTO"]
    assert not args.all_symbols


@pytest.mark.parametrize("selection", [
    ["--all", "--nse-symbols", "ACC"],
    ["--nse-symbol", "ACC", "--nse-symbols", "RELIANCE"],
    ["--nse-symbols", ","],
])
def test_parse_rejects_invalid_selection(monkeypatch, selection) -> None:
    monkeypatch.setattr("sys.argv", ["sync-cash-upstox-data", *selection])
    with pytest.raises(SystemExit):
        parse_args()


def test_resolve_list_reports_missing_and_preserves_order(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["sync-cash-upstox-data", "--nse-symbols", "BBB", "MISSING", "AAA"])
    args = parse_args()

    class Repository:
        def get_by_nse_symbol(self, name):
            return None if name == "MISSING" else EquitySymbolReference(nse_symbol=name, breeze_code=name)

    with pytest.raises(ValueError, match="MISSING"):
        resolve_requested_symbols(Repository(), object(), args, date(2026, 9, 5))
    args.skip_missing_symbols = True
    result = resolve_requested_symbols(Repository(), object(), args, date(2026, 9, 5))
    assert [symbol.nse_symbol for symbol in result] == ["BBB", "AAA"]


def test_explicit_start_respects_listing_date() -> None:
    args = Args()
    args.from_date = "2016-01-01"
    symbol = EquitySymbolReference(nse_symbol="JIOFIN", breeze_code="JIOFIN", listing_date=date(2023, 8, 21))
    assert resolve_upstox_from_date(symbol, None, Settings(_env_file=None), args) == date(2023, 8, 21)


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

    # Resuming a monthly response must keep every daily file in the manifest,
    # otherwise a later upload only sees the (often empty) marker.
    resumed = fetch_cash_chunk(
        Settings(_env_file=None, data_dir=tmp_path),
        object(),  # No API call should be made for an existing chunk.
        symbol,
        UpstoxFetchChunk(date(2022, 1, 1), date(2022, 1, 31), paths[0]),
        set(),
    )
    assert resumed.existed
    assert resumed.local_files == tuple(str(path) for path in paths)
    assert resumed.row_count == 2


def test_ignore_listing_date_clamps_only_to_provider_floor() -> None:
    args = Args()
    args.from_date = "2016-01-01"
    args.ignore_listing_date = True
    symbol = EquitySymbolReference(nse_symbol="NESTLEIND", breeze_code="NESIND", listing_date=date(2023, 8, 1))

    assert resolve_upstox_from_date(symbol, None, Settings(_env_file=None), args) == date(2022, 1, 1)


def test_chunk_is_not_settled_until_its_last_day_has_elapsed(tmp_path) -> None:
    from tick_ticker.scripts.sync_cash_upstox_data import UpstoxFetchChunk

    marker = cash_local_path(tmp_path, date(2026, 9, 1), "ACC")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    current = UpstoxFetchChunk(from_date=date(2026, 9, 1), to_date=date(2026, 9, 30), marker_path=marker)
    past = UpstoxFetchChunk(from_date=date(2026, 8, 1), to_date=date(2026, 8, 31), marker_path=marker)

    # bound = last completed session: the September chunk still has days to come.
    assert current.already_fetched(set(), bound=date(2026, 9, 21)) is False
    assert past.already_fetched(set(), bound=date(2026, 9, 21)) is True
