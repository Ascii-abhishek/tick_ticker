from datetime import date, datetime, time
from pathlib import Path

import pyarrow.parquet as pq

from tick_ticker.services.cash_second_data import (
    build_cash_second_daily_file,
    cash_second_local_path,
    cash_second_window_local_path,
    iter_cash_second_windows,
    transform_cash_second_payload,
    write_cash_second_parquet,
)


def test_cash_second_partition_paths_are_symbol_first() -> None:
    trade_date = date(2026, 1, 2)

    assert cash_second_local_path(Path("data"), trade_date, "RELIANCE") == Path(
        "data/cash_1s/by_symbol/RELIANCE/2026/01/02.parquet"
    )


def test_cash_second_windows_split_intraday_ranges_and_skip_weekends() -> None:
    windows = list(
        iter_cash_second_windows(
            date(2026, 1, 2),
            date(2026, 1, 4),
            window_seconds=3,
            market_open_time=time(9, 15),
            market_close_time=time(9, 15, 5),
            skip_weekends=True,
        )
    )

    assert [window.key for window in windows] == [
        "2026-01-02/091500-091502",
        "2026-01-02/091503-091505",
    ]


def test_write_cash_second_parquet_sorts_and_dedupes(tmp_path: Path) -> None:
    rows = transform_cash_second_payload(
        {
            "Success": [
                {"datetime": "2026-01-02 09:15:02", "open": "102", "high": "102", "low": "102", "close": "102"},
                {"datetime": "2026-01-02 09:15:01", "open": "101", "high": "101", "low": "101", "close": "101"},
                {"datetime": "2026-01-02 09:15:01", "open": "103", "high": "103", "low": "103", "close": "103"},
            ]
        },
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )
    path = tmp_path / "cash_1s.parquet"

    write_cash_second_parquet(rows, path)

    table = pq.read_table(path)
    assert table.num_rows == 2
    assert table["datetime"].to_pylist() == [
        datetime(2026, 1, 2, 9, 15, 1),
        datetime(2026, 1, 2, 9, 15, 2),
    ]
    assert table["close"].to_pylist() == [103.0, 102.0]


def test_build_cash_second_daily_file_from_window_files(tmp_path: Path) -> None:
    windows = list(
        iter_cash_second_windows(
            date(2026, 1, 2),
            date(2026, 1, 2),
            window_seconds=2,
            market_open_time=time(9, 15),
            market_close_time=time(9, 15, 3),
            skip_weekends=False,
        )
    )
    for index, window in enumerate(windows, start=1):
        rows = transform_cash_second_payload(
            {
                "Success": [
                    {
                        "datetime": f"2026-01-02 09:15:0{index}",
                        "open": str(index),
                        "high": str(index),
                        "low": str(index),
                        "close": str(index),
                    }
                ]
            },
            nse_symbol="RELIANCE",
            exchange_code="NSE",
            product_type="cash",
        )
        write_cash_second_parquet(rows, cash_second_window_local_path(tmp_path, "RELIANCE", window))

    daily_path = cash_second_local_path(tmp_path, date(2026, 1, 2), "RELIANCE")

    row_count = build_cash_second_daily_file(
        window_paths=[cash_second_window_local_path(tmp_path, "RELIANCE", window) for window in windows],
        daily_path=daily_path,
    )

    assert row_count == 2
    assert pq.read_table(daily_path).column_names == [
        "datetime",
        "trade_date",
        "nse_symbol",
        "exchange_code",
        "product_type",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "count",
        "ingested_at",
    ]
