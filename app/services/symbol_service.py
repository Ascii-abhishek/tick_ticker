"""Underlying symbol mapping service."""

from __future__ import annotations

from app.config.settings import Settings, get_settings
from app.db.models import UnderlyingMapping
from app.db.repository.query import QueryRepository


class SymbolService:
    """Resolve symbols across Breeze, NSE, and display naming."""

    def __init__(
        self,
        *,
        query_repository: QueryRepository | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.query_repository = query_repository
        self.settings = settings or get_settings()

    def default_nifty_mapping(self) -> UnderlyingMapping:
        """Return a sane default NIFTY mapping for first-time setup."""

        return UnderlyingMapping(
            underlying_id=1,
            breeze_symbol="NIFTY",
            nse_symbol="NIFTY",
            display_name="Nifty 50",
            exchange=self.settings.default_exchange,
            lot_size=self.settings.default_lot_size,
            tick_size=self.settings.default_tick_size,
        )

    def load_mappings(self) -> list[UnderlyingMapping]:
        """Load mappings from ClickHouse when a repository is available."""

        if self.query_repository is None:
            return [self.default_nifty_mapping()]
        rows = self.query_repository.get_underlying_mappings()
        if not rows:
            return [self.default_nifty_mapping()]
        return [UnderlyingMapping(**row) for row in rows]

    def resolve_breeze_symbol(self, underlying: str) -> str:
        """Resolve an internal underlying to its Breeze symbol."""

        normalized = underlying.upper()
        for mapping in self.load_mappings():
            if mapping.nse_symbol.upper() == normalized or mapping.display_name.upper() == normalized:
                return mapping.breeze_symbol
        return underlying

