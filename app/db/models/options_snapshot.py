"""Model for the options_snapshot table."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.utils.datetime_utils import parse_date, parse_datetime


class OptionsSnapshot(BaseModel):
    """Aggregated option-chain snapshot for BI and dashboard queries."""

    model_config = ConfigDict(frozen=True)

    datetime: datetime
    underlying: str
    expiry_date: date
    atm_strike: float
    total_call_oi: int = Field(ge=0)
    total_put_oi: int = Field(ge=0)
    pcr: float = Field(ge=0)

    @field_validator("datetime", mode="before")
    @classmethod
    def validate_datetime(cls, value: str | datetime) -> datetime:
        return parse_datetime(value)

    @field_validator("expiry_date", mode="before")
    @classmethod
    def validate_expiry_date(cls, value: str | date | datetime) -> date:
        return parse_date(value)

    def insert_tuple(self) -> tuple[object, ...]:
        return (
            self.datetime,
            self.underlying,
            self.expiry_date,
            self.atm_strike,
            self.total_call_oi,
            self.total_put_oi,
            self.pcr,
        )

