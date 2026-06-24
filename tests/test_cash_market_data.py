from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq

from tick_ticker.services.cash_data import cash_local_path, transform_cash_payload, write_cash_parquet


def test_transform_cash_payload_normalizes_breeze_cash_rows() -> None:
    payload = {
        "Success": [
            {
                "datetime": "2026-01-02 09:15:00",
                "stock_code": "RELIND",
                "exchange_code": "NSE",
                "open": "100.5",
                "high": "101.0",
                "low": "99.75",
                "close": "100.25",
                "volume": "1200",
                "count": 3,
            }
        ],
        "Status": 200,
        "Error": None,
    }

    rows = transform_cash_payload(
        payload,
        nse_symbol="RELIANCE",
        exchange_code="NSE",
        product_type="cash",
    )

    assert len(rows) == 1
    assert rows[0].datetime == datetime(2026, 1, 2, 9, 15)
    assert rows[0].trade_date == date(2026, 1, 2)
    assert rows[0].nse_symbol == "RELIANCE"
    assert rows[0].close == 100.25
    assert rows[0].volume == 1200
    assert rows[0].count == 3


def test_cash_partition_paths_are_date_first() -> None:
    trade_date = date(2026, 1, 2)

    assert cash_local_path(Path("data"), trade_date, "RELIANCE") == Path("data/cash/2026/01/02/RELIANCE.parquet")


def test_write_cash_parquet_uses_expected_schema(tmp_path: Path) -> None:
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
    path = tmp_path / "cash.parquet"

    write_cash_parquet(rows, path)

    table = pq.read_table(path)
    assert table.num_rows == 1
    assert table.column_names == [
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
