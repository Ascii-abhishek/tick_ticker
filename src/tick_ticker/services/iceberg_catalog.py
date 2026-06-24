"""Cloudflare R2 Data Catalog / Iceberg helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType, TimestampType

from tick_ticker.config import Settings, get_settings

MarketType = Literal["cash", "options", "future"]
Identifier = tuple[str, str]


@dataclass(frozen=True)
class IcebergTableSpec:
    """One managed market-data Iceberg table."""

    market_type: MarketType
    namespace: str
    table_name: str
    schema: Schema

    @property
    def identifier(self) -> Identifier:
        return (self.namespace, self.table_name)


CASH_OHLCV_SCHEMA = Schema(
    NestedField(1, "datetime", TimestampType()),
    NestedField(2, "trade_date", DateType()),
    NestedField(3, "nse_symbol", StringType()),
    NestedField(4, "exchange_code", StringType()),
    NestedField(5, "product_type", StringType()),
    NestedField(6, "open", DoubleType()),
    NestedField(7, "high", DoubleType()),
    NestedField(8, "low", DoubleType()),
    NestedField(9, "close", DoubleType()),
    NestedField(10, "volume", LongType()),
    NestedField(11, "count", LongType()),
    NestedField(12, "ingested_at", TimestampType()),
)

OPTIONS_OHLCV_SCHEMA = Schema(
    NestedField(1, "datetime", TimestampType()),
    NestedField(2, "trade_date", DateType()),
    NestedField(3, "underlying", StringType()),
    NestedField(4, "exchange", StringType()),
    NestedField(5, "expiry_date", DateType()),
    NestedField(6, "strike_price", DoubleType()),
    NestedField(7, "option_type", StringType()),
    NestedField(8, "open", DoubleType()),
    NestedField(9, "high", DoubleType()),
    NestedField(10, "low", DoubleType()),
    NestedField(11, "close", DoubleType()),
    NestedField(12, "volume", LongType()),
    NestedField(13, "open_interest", LongType()),
    NestedField(14, "ingested_at", TimestampType()),
)

FUTURE_OHLCV_SCHEMA = Schema(
    NestedField(1, "datetime", TimestampType()),
    NestedField(2, "trade_date", DateType()),
    NestedField(3, "underlying", StringType()),
    NestedField(4, "exchange", StringType()),
    NestedField(5, "expiry_date", DateType()),
    NestedField(6, "open", DoubleType()),
    NestedField(7, "high", DoubleType()),
    NestedField(8, "low", DoubleType()),
    NestedField(9, "close", DoubleType()),
    NestedField(10, "volume", LongType()),
    NestedField(11, "open_interest", LongType()),
    NestedField(12, "ingested_at", TimestampType()),
)


class IcebergMarketDataCatalog:
    """Create and append to market-data Iceberg tables in R2 Data Catalog."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        if not all([self.settings.cloudflare_account_id, self.settings.cloudflare_api_token, self.settings.r2_bucket_name]):
            raise ValueError("Set CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, and R2_BUCKET_NAME")
        self.catalog = RestCatalog(
            name=self.settings.r2_data_catalog_name,
            warehouse=self._warehouse(),
            uri=self._catalog_uri(),
            token=self.settings.cloudflare_api_token,
        )

    def ensure_market_data_tables(self) -> dict[MarketType, Identifier]:
        """Ensure all canonical market namespaces and tables exist."""

        table_ids: dict[MarketType, Identifier] = {}
        for spec in self._table_specs():
            self.ensure_table(spec)
            table_ids[spec.market_type] = spec.identifier
        return table_ids

    def ensure_table(self, spec: IcebergTableSpec) -> Table:
        """Create the namespace/table if needed and return the loaded table."""

        self.catalog.create_namespace_if_not_exists(spec.namespace)
        if not self.catalog.table_exists(spec.identifier):
            return self.catalog.create_table(
                spec.identifier,
                schema=spec.schema,
                properties=self._table_properties(spec.market_type),
            )
        return self.catalog.load_table(spec.identifier)

    def append_parquet_file(
        self,
        market_type: MarketType,
        path: str | Path,
        *,
        snapshot_properties: dict[str, str] | None = None,
    ) -> Identifier:
        """Append one local Parquet file into its managed Iceberg table."""

        spec = self._table_spec(market_type)
        table = self.ensure_table(spec)
        arrow_table = pq.read_table(path)
        table.append(arrow_table, snapshot_properties=snapshot_properties or {})
        return spec.identifier

    def _table_spec(self, market_type: MarketType) -> IcebergTableSpec:
        for spec in self._table_specs():
            if spec.market_type == market_type:
                return spec
        raise ValueError(f"Unsupported market type: {market_type}")

    def _table_specs(self) -> tuple[IcebergTableSpec, ...]:
        return (
            IcebergTableSpec(
                market_type="cash",
                namespace=self.settings.iceberg_cash_namespace,
                table_name=self.settings.iceberg_cash_table,
                schema=CASH_OHLCV_SCHEMA,
            ),
            IcebergTableSpec(
                market_type="options",
                namespace=self.settings.iceberg_options_namespace,
                table_name=self.settings.iceberg_options_table,
                schema=OPTIONS_OHLCV_SCHEMA,
            ),
            IcebergTableSpec(
                market_type="future",
                namespace=self.settings.iceberg_future_namespace,
                table_name=self.settings.iceberg_future_table,
                schema=FUTURE_OHLCV_SCHEMA,
            ),
        )

    def _table_properties(self, market_type: MarketType) -> dict[str, str]:
        return {
            "format-version": self.settings.iceberg_table_format_version,
            "write.parquet.compression-codec": self.settings.iceberg_parquet_compression,
            "tick_ticker.market_type": market_type,
        }

    def _catalog_uri(self) -> str:
        if self.settings.r2_data_catalog_uri:
            return self.settings.r2_data_catalog_uri
        return f"https://catalog.cloudflarestorage.com/{self.settings.cloudflare_account_id}/{self.settings.r2_bucket_name}"

    def _warehouse(self) -> str:
        if self.settings.r2_data_catalog_warehouse:
            return self.settings.r2_data_catalog_warehouse
        return f"{self.settings.cloudflare_account_id}_{self.settings.r2_bucket_name}"
