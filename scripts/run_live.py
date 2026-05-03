#!/usr/bin/env python
"""Run live minute polling ingestion."""

from __future__ import annotations

import argparse
from datetime import date

from app.config.settings import get_settings
from app.config.underlyings import find_underlying_config
from app.db.client import clickhouse_client
from app.db.repository.query import QueryRepository
from app.ingestion.breeze_client import BreezeClient
from app.ingestion.live_ingestion import LiveIngestionRequest, LiveIngestionRunner
from app.services.contract_service import ContractService
from app.services.symbol_service import SymbolService
from app.utils.logger import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run live options ingestion")
    parser.add_argument("--underlying", default=None, help="Underlying symbol, e.g. NIFTY")
    parser.add_argument("--exchange", default=None, help="Exchange code, e.g. NFO")
    parser.add_argument("--expiry", required=True, help="YYYY-MM-DD expiry")
    parser.add_argument("--strikes", default=None, help="Comma-separated strikes")
    parser.add_argument("--spot-price", type=float, default=None, help="Spot price for ATM-based strike selection")
    parser.add_argument("--strike-window", type=int, default=None, help="Number of strikes on each side of ATM")
    parser.add_argument("--option-types", default="CE,PE", help="Comma-separated option types")
    parser.add_argument("--lookback-minutes", type=int, default=3)
    parser.add_argument("--run-once", action="store_true")
    return parser


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = build_parser().parse_args()

    with clickhouse_client(settings) as client:
        mapping = SymbolService(query_repository=QueryRepository(client), settings=settings).resolve_mapping(
            args.underlying or settings.default_underlying
        )
        underlying_config = find_underlying_config(mapping.nse_symbol)
        strikes = _resolve_strikes(
            args.strikes,
            args.spot_price,
            args.strike_window,
            strike_step=underlying_config.strike_step if underlying_config else settings.strike_step,
        )
        request = LiveIngestionRequest(
            underlying=mapping.nse_symbol,
            breeze_symbol=mapping.breeze_symbol,
            exchange=args.exchange or mapping.exchange,
            expiry_date=date.fromisoformat(args.expiry),
            strikes=strikes,
            option_types=tuple(value.strip().upper() for value in args.option_types.split(",")),
            lookback_minutes=args.lookback_minutes,
            run_once=args.run_once,
        )
        runner = LiveIngestionRunner(
            breeze_client=BreezeClient(settings),
            clickhouse_client=client,
            settings=settings,
        )
        runner.run(request)


def _resolve_strikes(
    strikes: str | None,
    spot_price: float | None,
    strike_window: int | None,
    *,
    strike_step: int,
) -> tuple[float, ...]:
    if strikes:
        return tuple(float(value.strip()) for value in strikes.split(","))
    if spot_price is None:
        raise ValueError("provide either --strikes or --spot-price")
    return ContractService().strikes_around_atm(spot_price=spot_price, step=strike_step, window=strike_window)


if __name__ == "__main__":
    main()
