"""Repository for equity_symbol_reference."""

from __future__ import annotations

from tick_ticker.db.models import EquitySymbolReference
from tick_ticker.utils.engines import D1Client


class EquitySymbolReferenceRepository:
    """D1 access for symbol reference rows only."""

    def __init__(self, client: D1Client) -> None:
        self.client = client

    def get_by_nse_symbol(self, nse_symbol: str) -> EquitySymbolReference | None:
        """Return one reference row by NSE symbol."""

        rows = self.client.query(
            """
            SELECT
                nse_symbol,
                breeze_code,
                nse_company_name,
                listing_date,
                isin
            FROM equity_symbol_reference
            WHERE UPPER(nse_symbol) = UPPER(?)
            LIMIT 1
            """,
            [nse_symbol],
        )
        if not rows:
            return None
        return EquitySymbolReference.model_validate(rows[0])
