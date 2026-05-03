"""Shared constants for the options data platform."""

from __future__ import annotations

from typing import Final

DEFAULT_EXCHANGE: Final[str] = "NFO"
DEFAULT_UNDERLYING: Final[str] = "NIFTY"
DEFAULT_PRODUCT_TYPE: Final[str] = "options"
DEFAULT_INTERVAL: Final[str] = "1minute"

OPTION_TYPES: Final[tuple[str, str]] = ("CE", "PE")
RIGHT_TO_OPTION_TYPE: Final[dict[str, str]] = {
    "call": "CE",
    "calls": "CE",
    "ce": "CE",
    "c": "CE",
    "put": "PE",
    "puts": "PE",
    "pe": "PE",
    "p": "PE",
}
OPTION_TYPE_TO_RIGHT: Final[dict[str, str]] = {
    "CE": "call",
    "PE": "put",
}

OHLCV_COLUMNS: Final[tuple[str, ...]] = (
    "datetime",
    "underlying",
    "exchange",
    "expiry_date",
    "strike_price",
    "option_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
)

CONTRACT_COLUMNS: Final[tuple[str, ...]] = (
    "contract_id",
    "underlying",
    "expiry_date",
    "strike_price",
    "option_type",
    "lot_size",
    "is_weekly",
)

UNDERLYING_COLUMNS: Final[tuple[str, ...]] = (
    "underlying_id",
    "breeze_symbol",
    "nse_symbol",
    "display_name",
    "exchange",
    "lot_size",
    "tick_size",
)

SNAPSHOT_COLUMNS: Final[tuple[str, ...]] = (
    "datetime",
    "underlying",
    "expiry_date",
    "atm_strike",
    "total_call_oi",
    "total_put_oi",
    "pcr",
)

