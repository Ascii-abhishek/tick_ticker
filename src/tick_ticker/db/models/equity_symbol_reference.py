"""Models for symbol reference data."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, field_validator

from tick_ticker.utils.datetime import parse_date


class EquitySymbolReference(BaseModel):
    """One row from equity_symbol_reference.

    This table should stay focused on symbol identity and reference fields.
    Sync progress belongs in market_data_sync_state.
    """

    model_config = ConfigDict(extra="allow")

    nse_symbol: str
    breeze_code: str
    nse_company_name: str | None = None
    listing_date: date | None = None
    isin: str | None = None

    @field_validator("listing_date", mode="before")
    @classmethod
    def parse_listing_date(cls, value: object) -> date | None:
        if value in (None, ""):
            return None
        return parse_date(value)  # type: ignore[arg-type]
