#!/usr/bin/env python
"""Run live minute polling ingestion."""

from __future__ import annotations

import argparse
from datetime import date

from app.config.settings import get_settings
from app.db.client import clickhouse_client
from app.ingestion.breeze_client import BreezeClient
from app.ingestion.live_ingestion import LiveIngestionRequest, LiveIngestionRunner
from app.utils.logger import configure_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run live options ingestion")
    parser.add_argument("--underlying", default=None, help="Underlying symbol, e.g. NIFTY")
    parser.add_argument("--exchange", default=None, help="Exchange code, e.g. NFO")
    parser.add_argument("--expiry", required=True, help="YYYY-MM-DD expiry")
    parser.add_argument("--strikes", required=True, help="Comma-separated strikes")
    parser.add_argument("--option-types", default="CE,PE", help="Comma-separated option types")
    parser.add_argument("--lookback-minutes", type=int, default=3)
    parser.add_argument("--run-once", action="store_true")
    return parser


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = build_parser().parse_args()

    request = LiveIngestionRequest(
        underlying=args.underlying or settings.default_underlying,
        exchange=args.exchange or settings.default_exchange,
        expiry_date=date.fromisoformat(args.expiry),
        strikes=tuple(float(value.strip()) for value in args.strikes.split(",")),
        option_types=tuple(value.strip().upper() for value in args.option_types.split(",")),
        lookback_minutes=args.lookback_minutes,
        run_once=args.run_once,
    )

    with clickhouse_client(settings) as client:
        runner = LiveIngestionRunner(
            breeze_client=BreezeClient(settings),
            clickhouse_client=client,
            settings=settings,
        )
        runner.run(request)


if __name__ == "__main__":
    main()
