from tick_ticker.config import Settings
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog


def test_iceberg_table_specs_default_to_market_namespaces() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    specs = catalog._table_specs()

    assert [spec.identifier for spec in specs] == [
        ("cash", "ohlcv_by_symbol"),
        ("cash", "ohlcv_1s_by_symbol"),
        ("options", "ohlcv"),
        ("future", "ohlcv"),
    ]


def test_cash_table_spec_uses_symbol_year_partitioning_and_sort_order() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )
    spec = catalog._table_spec("cash")

    partition_spec = catalog._partition_spec(spec)
    sort_order = catalog._sort_order(spec)

    assert [(field.name, str(field.transform)) for field in partition_spec.fields] == [
        ("nse_symbol", "identity"),
        ("year_trade_date", "year"),
    ]
    assert [field.source_id for field in sort_order.fields] == [3, 2, 1]


def test_cash_second_table_spec_uses_symbol_month_partitioning_and_sort_order() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )
    spec = catalog._table_spec("cash_1s")

    partition_spec = catalog._partition_spec(spec)
    sort_order = catalog._sort_order(spec)

    assert [(field.name, str(field.transform)) for field in partition_spec.fields] == [
        ("nse_symbol", "identity"),
        ("month_trade_date", "month"),
    ]
    assert [field.source_id for field in sort_order.fields] == [3, 2, 1]


def test_iceberg_table_properties_include_retention_and_description() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    properties = catalog._table_properties("cash")

    assert properties["write.metadata.delete-after-commit.enabled"] == "false"
    assert properties["write.metadata.previous-versions-max"] == "20"
    assert properties["history.expire.min-snapshots-to-keep"] == "10"
    assert properties["write.target-file-size-bytes"] == str(128 * 1024 * 1024)


def test_cash_table_properties_describe_query_layout() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    properties = catalog._table_properties("cash")

    assert properties["tick_ticker.query_layout"] == "symbol_time_range"
    assert properties["tick_ticker.partitioning"] == "nse_symbol,year(trade_date)"


def test_cash_second_table_properties_describe_query_layout() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    properties = catalog._table_properties("cash_1s")

    assert properties["tick_ticker.query_layout"] == "symbol_intraday_time_range"
    assert properties["tick_ticker.partitioning"] == "nse_symbol,month(trade_date)"
    assert properties["tick_ticker.time_grain"] == "1second"
    assert properties["write.target-file-size-bytes"] == str(256 * 1024 * 1024)


def test_iceberg_catalog_defaults_match_cloudflare_r2_data_catalog() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    assert catalog._warehouse() == "account_market-data"
    assert catalog._catalog_uri() == "https://catalog.cloudflarestorage.com/account/market-data"


def _local_catalog(tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )
    catalog.catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")
    return catalog


def _cash_rows(symbol: str, trade_date, volume: int):
    from datetime import datetime, time

    import pyarrow as pa

    from tick_ticker.services.cash_data import CASH_ARROW_SCHEMA

    stamps = [datetime.combine(trade_date, time(9, 15 + minute)) for minute in range(3)]
    return pa.Table.from_pylist(
        [
            {
                "datetime": stamp,
                "trade_date": trade_date,
                "nse_symbol": symbol,
                "exchange_code": "NSE",
                "product_type": "cash",
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": volume,
                "count": None,
                "ingested_at": stamp,
            }
            for stamp in stamps
        ],
        schema=CASH_ARROW_SCHEMA,
    )


def test_replace_symbol_days_is_idempotent_and_leaves_other_rows(tmp_path) -> None:
    from datetime import date

    import pyarrow as pa

    catalog = _local_catalog(tmp_path)
    day_one, day_two = date(2026, 1, 1), date(2026, 1, 2)
    catalog.append_arrow_table(
        "cash",
        pa.concat_tables([_cash_rows("NIFTY", day_one, 0), _cash_rows("NIFTY", day_two, 0), _cash_rows("ACC", day_one, 7)]),
    )

    for _ in range(2):
        catalog.replace_symbol_days("cash", _cash_rows("NIFTY", day_one, 5), nse_symbol="NIFTY", trade_dates=[day_one])

    rows = catalog.load_market_table("cash").scan().to_arrow().to_pylist()
    nifty_day_one = [row["volume"] for row in rows if row["nse_symbol"] == "NIFTY" and row["trade_date"] == day_one]
    assert nifty_day_one == [5, 5, 5]
    assert len([row for row in rows if row["nse_symbol"] == "NIFTY" and row["trade_date"] == day_two]) == 3
    assert [row["volume"] for row in rows if row["nse_symbol"] == "ACC"] == [7, 7, 7]


def test_replace_symbol_range_removes_duplicates(tmp_path) -> None:
    from datetime import date

    catalog = _local_catalog(tmp_path)
    day = date(2026, 1, 1)
    catalog.append_arrow_table("cash", _cash_rows("RELIANCE", day, 1))
    catalog.append_arrow_table("cash", _cash_rows("RELIANCE", day, 1))

    catalog.replace_symbol_range("cash", _cash_rows("RELIANCE", day, 2), nse_symbol="RELIANCE", from_date=date(2026, 1, 1), to_date=date(2026, 12, 31))

    rows = catalog.load_market_table("cash").scan().to_arrow().to_pylist()
    assert [row["volume"] for row in rows] == [2, 2, 2]
