"""Model for the option_contracts table."""

from __future__ import annotations

from datetime import date, datetime
from hashlib import blake2b
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config.constants import RIGHT_TO_OPTION_TYPE
from app.utils.datetime_utils import parse_date


class OptionContract(BaseModel):
    """Static metadata for an option contract."""

    model_config = ConfigDict(frozen=True)

    contract_id: int | None = None
    underlying: str
    expiry_date: date
    strike_price: float
    option_type: Literal["CE", "PE"]
    lot_size: int = Field(ge=1, le=65535)
    is_weekly: int = Field(default=1, ge=0, le=1)

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

    @property
    def resolved_contract_id(self) -> int:
        if self.contract_id is not None:
            return self.contract_id
        raw_key = f"{self.underlying}|{self.expiry_date}|{self.strike_price:.2f}|{self.option_type}"
        digest = blake2b(raw_key.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(digest, byteorder="big", signed=False)

    def insert_tuple(self) -> tuple[object, ...]:
        return (
            self.resolved_contract_id,
            self.underlying,
            self.expiry_date,
            self.strike_price,
            self.option_type,
            self.lot_size,
            self.is_weekly,
        )

    def natural_key(self) -> tuple[str, date, float, str]:
        return (
            self.underlying,
            self.expiry_date,
            float(self.strike_price),
            self.option_type,
        )

