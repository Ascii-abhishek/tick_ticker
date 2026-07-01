"""Provider-specific cash history availability rules."""

from __future__ import annotations

from datetime import date


CASH_PROVIDER_HISTORY_START_DATES = {
    "breeze": date(2016, 1, 1),
}


def cash_provider_history_start_date(provider: str) -> date | None:
    """Return the earliest date supported by a cash history provider."""

    return CASH_PROVIDER_HISTORY_START_DATES.get(provider)
