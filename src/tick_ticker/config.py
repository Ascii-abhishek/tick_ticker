"""Application settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for sync scripts.

    Values are loaded from `.env` and process environment. Environment variables
    take precedence over `.env` values.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Literal["dev", "prod", "test"] = "dev"
    log_level: str = "INFO"

    breeze_api_key: str = Field(default="")
    breeze_api_secret: str = Field(default="")
    breeze_session_token: str = Field(default="")
    breeze_min_request_interval_seconds: float = 0.65
    breeze_request_retry_attempts: int = 3
    breeze_request_retry_base_delay_seconds: float = 1.0

    cloudflare_account_id: str = Field(default="")
    cloudflare_api_token: str = Field(default="")
    d1_database_id: str = Field(default="")

    r2_access_key_id: str = Field(default="")
    r2_secret_access_key: str = Field(default="")
    r2_s3_endpoint: str = Field(default="")
    r2_bucket_name: str = "market-data"
    r2_data_catalog_name: str = "cloudflare_r2"
    r2_data_catalog_uri: str = ""
    r2_data_catalog_warehouse: str = ""
    iceberg_cash_namespace: str = "cash"
    iceberg_cash_table: str = "ohlcv"
    iceberg_options_namespace: str = "options"
    iceberg_options_table: str = "ohlcv"
    iceberg_future_namespace: str = "future"
    iceberg_future_table: str = "ohlcv"
    iceberg_table_format_version: str = "2"
    iceberg_parquet_compression: str = "zstd"

    data_dir: Path = Path("data")
    default_interval: str = "1minute"
    cash_history_provider: Literal["breeze"] = "breeze"
    cash_exchange_code: str = "NSE"
    cash_product_type: str = "cash"
    cash_history_chunk_days: int = 1
    cash_download_workers: int = 1
    cash_upload_workers: int = 1
    cash_upload_batch_size: int = 25
    cash_upload_retry_attempts: int = 3
    cash_upload_retry_base_delay_seconds: float = 1.0
    cash_sync_max_days_per_run: int = 30
    cash_sync_from_date: date | None = None
    cash_sync_to_date: date | None = None

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper()

    @field_validator("data_dir", mode="before")
    @classmethod
    def normalize_data_dir(cls, value: str | Path) -> Path:
        return Path(value)

    @field_validator("cash_sync_from_date", "cash_sync_to_date", mode="before")
    @classmethod
    def blank_dates_are_none(cls, value: object) -> object:
        if value == "":
            return None
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()
