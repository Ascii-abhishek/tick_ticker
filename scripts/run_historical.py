#!/usr/bin/env python
"""Run historical options ingestion."""

from __future__ import annotations

import argparse
from datetime import date, datetime, time

from app.config.settings import get_settings
from app.config.underlyings import find_underlying_config
from app.db.client import clickhouse_client
from app.db.repository.query import QueryRepository
from app.ingestion.breeze_client import BreezeClient
from app.ingestion.historical_loader import HistoricalLoadRequest, HistoricalLoader
from app.services.contract_service import ContractService
from app.services.symbol_service import SymbolService
from app.utils.logger import configure_logging, get_logger

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run historical options backfill")
    parser.add_argument("--underlying", default=None, help="Underlying symbol, e.g. NIFTY")
    parser.add_argument("--exchange", default=None, help="Exchange code, e.g. NFO")
    parser.add_argument("--expiries", required=True, help="Comma-separated YYYY-MM-DD expiries")
    parser.add_argument("--strikes", default=None, help="Comma-separated strikes")
    parser.add_argument("--spot-price", type=float, default=None, help="Spot price for ATM-based strike selection")
    parser.add_argument("--strike-window", type=int, default=None, help="Number of strikes on each side of ATM")
    parser.add_argument("--from-date", required=True, help="Start datetime/date, e.g. 2026-04-01")
    parser.add_argument("--to-date", required=True, help="End datetime/date, e.g. 2026-04-05")
    parser.add_argument("--option-types", default="CE,PE", help="Comma-separated option types")
    parser.add_argument("--interval", default=None, help="Breeze candle interval")
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
        request = HistoricalLoadRequest(
            underlying=mapping.nse_symbol,
            breeze_symbol=mapping.breeze_symbol,
            exchange=args.exchange or mapping.exchange,
            expiries=tuple(date.fromisoformat(value.strip()) for value in args.expiries.split(",")),
            strikes=strikes,
            start=_parse_cli_datetime(args.from_date),
            end=_parse_cli_datetime(args.to_date, end_of_day=True),
            option_types=tuple(value.strip().upper() for value in args.option_types.split(",")),
            interval=args.interval or settings.default_interval,
            lot_size=mapping.lot_size,
        )
        loader = HistoricalLoader(
            breeze_client=BreezeClient(settings),
            clickhouse_client=client,
            settings=settings,
        )
        inserted = loader.load(request)
        logger.info("historical_ingestion_finished", extra={"inserted_rows": inserted})


def _parse_cli_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    if "T" not in value and " " not in value:
        parsed_date = date.fromisoformat(value)
        return datetime.combine(parsed_date, time.max if end_of_day else time.min)
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        parsed_date = date.fromisoformat(value)
        return datetime.combine(parsed_date, time.max if end_of_day else time.min)


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
