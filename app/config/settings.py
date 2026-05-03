"""Application settings loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration.

    Values are loaded from `.env` and the process environment. Environment
    variables take precedence over `.env` values.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Literal["dev", "prod", "test"] = "dev"
    log_level: str = "INFO"

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
    clickhouse_secure: bool = False
    clickhouse_connect_timeout_seconds: int = 10
    clickhouse_send_receive_timeout_seconds: int = 300

    breeze_api_key: str = Field(default="")
    breeze_api_secret: str = Field(default="")
    breeze_session_token: str = Field(default="")

    default_underlying: str = "NIFTY"
    default_exchange: str = "NFO"
    default_interval: str = "1minute"
    default_lot_size: int = 50
    default_tick_size: float = 0.05

    batch_size: int = 1_000
    request_retry_attempts: int = 3
    request_retry_base_delay_seconds: float = 1.0
    breeze_min_request_interval_seconds: float = 0.35
    historical_chunk_days: int = 5

    strike_step: int = 50
    strike_window: int = 10

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        return value.upper()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""

    return Settings()

