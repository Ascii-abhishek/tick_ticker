"""Underlying symbol mapping service."""

from __future__ import annotations

from app.config.settings import Settings, get_settings
from app.config.underlyings import load_underlying_config
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

        for config in load_underlying_config().underlyings:
            if config.nse_symbol.upper() == "NIFTY":
                return config.to_mapping()
        raise ValueError("NIFTY is not configured in app/config/underlyings.yml")

    def load_mappings(self) -> list[UnderlyingMapping]:
        """Load mappings from ClickHouse when a repository is available."""

        yaml_mappings = [config.to_mapping() for config in load_underlying_config().underlyings]
        if self.query_repository is None:
            return yaml_mappings

        rows = self.query_repository.get_underlying_mappings()
        db_mappings = [UnderlyingMapping(**row) for row in rows]
        merged = {mapping.nse_symbol.upper(): mapping for mapping in yaml_mappings}
        merged.update({mapping.nse_symbol.upper(): mapping for mapping in db_mappings})
        return list(merged.values())

    def resolve_mapping(self, underlying: str) -> UnderlyingMapping:
        """Resolve an underlying to its configured mapping."""

        normalized = underlying.upper()
        for mapping in self.load_mappings():
            aliases = {
                mapping.nse_symbol.upper(),
                mapping.breeze_symbol.upper(),
                mapping.display_name.upper(),
            }
            if normalized in aliases:
                return mapping
        raise ValueError(f"underlying {underlying!r} is not configured in underlying_mapping or YAML")

    def resolve_breeze_symbol(self, underlying: str) -> str:
        """Resolve an internal underlying to its Breeze symbol."""

        return self.resolve_mapping(underlying).breeze_symbol

    def resolve_canonical_symbol(self, underlying: str) -> str:
        """Resolve a user/broker symbol to the canonical stored symbol."""

        return self.resolve_mapping(underlying).nse_symbol
