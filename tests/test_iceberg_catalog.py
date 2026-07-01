from tick_ticker.config import Settings
from tick_ticker.services.iceberg_catalog import IcebergMarketDataCatalog


def test_iceberg_table_specs_default_to_three_market_namespaces() -> None:
    catalog = IcebergMarketDataCatalog.__new__(IcebergMarketDataCatalog)
    catalog.settings = Settings(
        cloudflare_account_id="account",
        cloudflare_api_token="token",
        r2_bucket_name="market-data",
        _env_file=None,
    )

    specs = catalog._table_specs()

    assert [spec.identifier for spec in specs] == [("cash", "ohlcv"), ("options", "ohlcv"), ("future", "ohlcv")]


def test_cash_table_spec_uses_date_symbol_partitioning_and_sort_order() -> None:
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
        ("trade_date", "identity"),
        ("nse_symbol", "identity"),
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
