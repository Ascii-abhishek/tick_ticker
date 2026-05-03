"""Model for the underlying_mapping table."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class UnderlyingMapping(BaseModel):
    """Mapping between internal and broker symbols."""

    model_config = ConfigDict(frozen=True)

    underlying_id: int = Field(ge=1)
    breeze_symbol: str
    nse_symbol: str
    display_name: str
    exchange: str
    lot_size: int = Field(ge=1, le=65535)
    tick_size: float = Field(gt=0)

    def insert_tuple(self) -> tuple[object, ...]:
        return (
            self.underlying_id,
            self.breeze_symbol,
            self.nse_symbol,
            self.display_name,
            self.exchange,
            self.lot_size,
            self.tick_size,
        )

