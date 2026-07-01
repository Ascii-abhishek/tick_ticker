"""Cloudflare R2 Data Catalog / Iceberg helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
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
    partition_fields: tuple[tuple[str, str], ...]
    sort_fields: tuple[str, ...]
    description: str

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
                partition_spec=self._partition_spec(spec),
                sort_order=self._sort_order(spec),
                properties=self._table_properties(spec.market_type),
            )
        table = self.catalog.load_table(spec.identifier)
        self._ensure_table_metadata(table, spec)
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

    def append_parquet_files(
        self,
        market_type: MarketType,
        paths: list[str | Path],
        *,
        snapshot_properties: dict[str, str] | None = None,
    ) -> Identifier:
        """Append local Parquet files into one managed Iceberg snapshot."""

        if not paths:
            raise ValueError("paths must not be empty")
        spec = self._table_spec(market_type)
        table = self.ensure_table(spec)
        arrow_table = pa.concat_tables([pq.read_table(path) for path in paths], promote_options="default")
        table.append(arrow_table, snapshot_properties=snapshot_properties or {})
        return spec.identifier

    def source_path_uploaded(self, market_type: MarketType, source_path: str | Path) -> bool:
        """Return whether a source path appears in committed snapshot metadata."""

        return str(source_path) in self.committed_source_paths(market_type)

    def committed_source_paths(self, market_type: MarketType) -> set[str]:
        """Return source paths recorded in committed snapshot metadata."""

        spec = self._table_spec(market_type)
        table = self.ensure_table(spec)
        source_paths: set[str] = set()
        for snapshot in table.snapshots():
            if source_path := snapshot.summary.get("tick_ticker.source_path"):
                source_paths.add(source_path)
            if source_paths_json := snapshot.summary.get("tick_ticker.source_paths"):
                source_paths.update(json.loads(source_paths_json))
        return source_paths

    def overwrite_cash_file(
        self,
        path: str | Path,
        *,
        nse_symbol: str,
        trade_date: date,
        snapshot_properties: dict[str, str] | None = None,
    ) -> Identifier:
        """Replace one cash symbol/day with a local Parquet file."""

        spec = self._table_spec("cash")
        table = self.ensure_table(spec)
        arrow_table = pq.read_table(path)
        overwrite_filter = And(EqualTo("nse_symbol", nse_symbol), EqualTo("trade_date", trade_date))
        table.overwrite(
            arrow_table,
            overwrite_filter=overwrite_filter,
            snapshot_properties=snapshot_properties or {},
        )
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
                partition_fields=(("trade_date", "identity"), ("nse_symbol", "identity")),
                sort_fields=("nse_symbol", "trade_date", "datetime"),
                description="NSE cash OHLCV candles by symbol and trade date.",
            ),
            IcebergTableSpec(
                market_type="options",
                namespace=self.settings.iceberg_options_namespace,
                table_name=self.settings.iceberg_options_table,
                schema=OPTIONS_OHLCV_SCHEMA,
                partition_fields=(("trade_date", "identity"), ("underlying", "identity"), ("expiry_date", "identity")),
                sort_fields=("underlying", "expiry_date", "strike_price", "option_type", "datetime"),
                description="NSE options OHLCV candles by underlying, expiry, strike, option type, and trade date.",
            ),
            IcebergTableSpec(
                market_type="future",
                namespace=self.settings.iceberg_future_namespace,
                table_name=self.settings.iceberg_future_table,
                schema=FUTURE_OHLCV_SCHEMA,
                partition_fields=(("trade_date", "identity"), ("underlying", "identity"), ("expiry_date", "identity")),
                sort_fields=("underlying", "expiry_date", "datetime"),
                description="NSE futures OHLCV candles by underlying, expiry, and trade date.",
            ),
        )

    def _table_properties(self, market_type: MarketType) -> dict[str, str]:
        return {
            "format-version": self.settings.iceberg_table_format_version,
            "write.parquet.compression-codec": self.settings.iceberg_parquet_compression,
            "write.metadata.delete-after-commit.enabled": "false",
            "write.metadata.previous-versions-max": "20",
            "history.expire.min-snapshots-to-keep": "10",
            "history.expire.max-snapshot-age-ms": str(30 * 24 * 60 * 60 * 1000),
            "commit.manifest-merge.enabled": "true",
            "tick_ticker.market_type": market_type,
        }

    def _ensure_table_metadata(self, table: Table, spec: IcebergTableSpec) -> None:
        """Evolve table metadata to the current Tick Ticker layout."""

        self._ensure_table_properties(table, spec)
        self._ensure_partition_fields(table, spec)
        self._ensure_sort_order(table, spec)

    def _ensure_table_properties(self, table: Table, spec: IcebergTableSpec) -> None:
        desired_properties = self._table_properties(spec.market_type).copy()
        desired_properties.pop("format-version", None)
        desired_properties |= {
            "comment": spec.description,
            "tick_ticker.description": spec.description,
        }
        updates = {key: value for key, value in desired_properties.items() if str(table.properties.get(key)) != value}
        if not updates:
            return
        table.transaction().set_properties(updates).commit_transaction()

    def _ensure_partition_fields(self, table: Table, spec: IcebergTableSpec) -> None:
        existing_names = {field.name for field in table.spec().fields}
        missing_fields = [field for field in spec.partition_fields if self._partition_field_name(*field) not in existing_names]
        if not missing_fields:
            return
        update = table.update_spec()
        for source_column, transform_name in missing_fields:
            update.add_field(source_column, self._partition_transform(transform_name), self._partition_field_name(source_column, transform_name))
        update.commit()

    def _ensure_sort_order(self, table: Table, spec: IcebergTableSpec) -> None:
        if table.sort_order().fields:
            return
        update = table.update_sort_order()
        for source_column in spec.sort_fields:
            update.asc(source_column, IdentityTransform(), NullOrder.NULLS_LAST)
        update.commit()

    def _partition_spec(self, spec: IcebergTableSpec) -> PartitionSpec:
        return PartitionSpec(
            *(
                PartitionField(
                    source_id=spec.schema.find_field(source_column).field_id,
                    field_id=1000 + index,
                    transform=self._partition_transform(transform_name),
                    name=self._partition_field_name(source_column, transform_name),
                )
                for index, (source_column, transform_name) in enumerate(spec.partition_fields)
            )
        )

    def _sort_order(self, spec: IcebergTableSpec) -> SortOrder:
        return SortOrder(
            *(
                SortField(
                    source_id=spec.schema.find_field(source_column).field_id,
                    transform=IdentityTransform(),
                    direction=SortDirection.ASC,
                    null_order=NullOrder.NULLS_LAST,
                )
                for source_column in spec.sort_fields
            ),
            order_id=1,
        )

    @staticmethod
    def _partition_field_name(source_column: str, transform_name: str) -> str:
        return source_column if transform_name == "identity" else f"{transform_name}_{source_column}"

    @staticmethod
    def _partition_transform(transform_name: str) -> IdentityTransform:
        if transform_name != "identity":
            raise ValueError(f"Unsupported partition transform: {transform_name}")
        return IdentityTransform()

    def _catalog_uri(self) -> str:
        if self.settings.r2_data_catalog_uri:
            return self.settings.r2_data_catalog_uri
        return f"https://catalog.cloudflarestorage.com/{self.settings.cloudflare_account_id}/{self.settings.r2_bucket_name}"

    def _warehouse(self) -> str:
        if self.settings.r2_data_catalog_warehouse:
            return self.settings.r2_data_catalog_warehouse
        return f"{self.settings.cloudflare_account_id}_{self.settings.r2_bucket_name}"
