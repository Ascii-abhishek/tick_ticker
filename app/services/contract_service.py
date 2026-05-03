"""Contract and strike selection helpers."""

from __future__ import annotations

from datetime import date
from math import floor

from app.config.settings import Settings, get_settings
from app.db.models import OptionContract


class ContractService:
    """Build option contract metadata and config-driven strike ranges."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def strikes_around_atm(
        self,
        *,
        spot_price: float,
        step: int | None = None,
        window: int | None = None,
    ) -> tuple[float, ...]:
        """Return strikes centered around the nearest ATM strike."""

        strike_step = step or self.settings.strike_step
        strike_window = window if window is not None else self.settings.strike_window
        atm = round_to_nearest_step(spot_price, strike_step)
        return tuple(float(atm + offset * strike_step) for offset in range(-strike_window, strike_window + 1))

    def build_contracts(
        self,
        *,
        underlying: str,
        expiry_date: date,
        strikes: tuple[float, ...],
        lot_size: int | None = None,
        is_weekly: int = 1,
    ) -> list[OptionContract]:
        """Build CE and PE contracts for the given strikes."""

        return [
            OptionContract(
                underlying=underlying,
                expiry_date=expiry_date,
                strike_price=strike,
                option_type=option_type,
                lot_size=lot_size or self.settings.default_lot_size,
                is_weekly=is_weekly,
            )
            for strike in strikes
            for option_type in ("CE", "PE")
        ]


def round_to_nearest_step(value: float, step: int) -> int:
    if step <= 0:
        raise ValueError("step must be positive")
    return int(floor((value + step / 2) / step) * step)

