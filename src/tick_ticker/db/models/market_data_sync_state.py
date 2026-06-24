"""Models for market data sync state."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

from tick_ticker.utils.datetime import parse_date, parse_datetime

SyncStatus = Literal["in_progress", "completed", "failed"]


class MarketDataSyncState(BaseModel):
    """One sync-state row keyed by market_type and NSE symbol."""

    model_config = ConfigDict(extra="allow")

    market_type: Literal["cash", "futures", "options"]
    nse_symbol: str
    status: SyncStatus
    from_date: date | None = None
    to_date: date | None = None
    row_count: int | None = None
    local_path: str | None = None
    r2_prefix: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("from_date", "to_date", mode="before")
    @classmethod
    def parse_optional_date(cls, value: object) -> date | None:
        if value in (None, ""):
            return None
        return parse_date(value)  # type: ignore[arg-type]

    @field_validator("started_at", "completed_at", "updated_at", mode="before")
    @classmethod
    def parse_optional_datetime(cls, value: object) -> datetime | None:
        if value in (None, ""):
            return None
        return parse_datetime(value)  # type: ignore[arg-type]


class MarketDataSyncCompletion(BaseModel):
    """Metadata written after one market data sync completes."""

    market_type: Literal["cash", "futures", "options"]
    nse_symbol: str
    from_date: date
    to_date: date
    row_count: int
    local_path: str
    r2_prefix: str
    completed_at: datetime
