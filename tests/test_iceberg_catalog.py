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
