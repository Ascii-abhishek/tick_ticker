"""Model for the options_ohlcv table."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config.constants import RIGHT_TO_OPTION_TYPE
from app.utils.datetime_utils import parse_date, parse_datetime


class OptionOHLCV(BaseModel):
    """A single 1-minute option OHLCV candle."""

    model_config = ConfigDict(frozen=True)

    datetime: datetime
    underlying: str
    exchange: str
    expiry_date: date
    strike_price: float
    option_type: Literal["CE", "PE"]
    open: float
    high: float
    low: float
    close: float
    volume: int = Field(ge=0)
    open_interest: int = Field(ge=0)

    @field_validator("datetime", mode="before")
    @classmethod
    def validate_datetime(cls, value: str | datetime) -> datetime:
        return parse_datetime(value)

    @field_validator("expiry_date", mode="before")
    @classmethod
    def validate_expiry_date(cls, value: str | date | datetime) -> date:
        return parse_date(value)

    @field_validator("option_type", mode="before")
    @classmethod
    def validate_option_type(cls, value: str) -> str:
        normalized = str(value).strip().lower()
        option_type = RIGHT_TO_OPTION_TYPE.get(normalized, str(value).strip().upper())
        if option_type not in {"CE", "PE"}:
            raise ValueError(f"unsupported option_type: {value}")
        return option_type

    def insert_tuple(self) -> tuple[object, ...]:
        return (
            self.datetime,
            self.underlying,
            self.exchange,
            self.expiry_date,
            self.strike_price,
            self.option_type,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.open_interest,
        )

    def natural_key(self) -> tuple[str, date, float, str, datetime]:
        return (
            self.underlying,
            self.expiry_date,
            float(self.strike_price),
            self.option_type,
            self.datetime,
        )

