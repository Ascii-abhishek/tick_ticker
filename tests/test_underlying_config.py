from pathlib import Path

from app.config.underlyings import load_underlying_config
from app.services.symbol_service import SymbolService


def test_underlying_config_loads_yaml(tmp_path: Path) -> None:
    config_file = tmp_path / "underlyings.yml"
    config_file.write_text(
        """
underlyings:
  - underlying_id: 7
    breeze_symbol: BROKER_NIFTY
    nse_symbol: NIFTY
    display_name: Nifty 50
    exchange: NFO
    lot_size: 50
    tick_size: 0.05
    strike_step: 50
""",
        encoding="utf-8",
    )

    config = load_underlying_config(config_file)

    assert config.underlyings[0].breeze_symbol == "BROKER_NIFTY"
    assert config.underlyings[0].to_mapping().nse_symbol == "NIFTY"


def test_symbol_service_resolves_canonical_and_breeze_symbol() -> None:
    service = SymbolService()

    assert service.resolve_canonical_symbol("NIFTY") == "NIFTY"
    assert service.resolve_breeze_symbol("NIFTY") == "NIFTY"

