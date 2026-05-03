"""YAML-backed underlying configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from app.db.models import UnderlyingMapping

DEFAULT_UNDERLYING_CONFIG_PATH = Path(__file__).with_name("underlyings.yml")


class UnderlyingConfig(BaseModel):
    """Config row for one underlying.

    The fields shared with `underlying_mapping` can be seeded into ClickHouse.
    Extra strategy/runtime fields, such as `strike_step`, stay in YAML.
    """

    underlying_id: int = Field(ge=1)
    breeze_symbol: str
    nse_symbol: str
    display_name: str
    exchange: str
    lot_size: int = Field(ge=1, le=65535)
    tick_size: float = Field(gt=0)
    strike_step: int = Field(default=50, ge=1)

    def to_mapping(self) -> UnderlyingMapping:
        return UnderlyingMapping(
            underlying_id=self.underlying_id,
            breeze_symbol=self.breeze_symbol,
            nse_symbol=self.nse_symbol,
            display_name=self.display_name,
            exchange=self.exchange,
            lot_size=self.lot_size,
            tick_size=self.tick_size,
        )


class UnderlyingConfigFile(BaseModel):
    underlyings: list[UnderlyingConfig]


def load_underlying_config(path: str | Path = DEFAULT_UNDERLYING_CONFIG_PATH) -> UnderlyingConfigFile:
    """Load underlyings from YAML."""

    config_path = Path(path)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"invalid underlying config shape in {config_path}")
    return UnderlyingConfigFile.model_validate(raw)


def find_underlying_config(
    underlying: str,
    *,
    path: str | Path = DEFAULT_UNDERLYING_CONFIG_PATH,
) -> UnderlyingConfig | None:
    """Find an underlying config by NSE, Breeze, or display symbol."""

    normalized = underlying.upper()
    for config in load_underlying_config(path).underlyings:
        aliases = {
            config.nse_symbol.upper(),
            config.breeze_symbol.upper(),
            config.display_name.upper(),
        }
        if normalized in aliases:
            return config
    return None


def configs_to_mappings(configs: list[UnderlyingConfig]) -> list[UnderlyingMapping]:
    return [config.to_mapping() for config in configs]


def underlying_config_as_dict(config: UnderlyingConfig) -> dict[str, Any]:
    return config.model_dump()

