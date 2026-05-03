"""Application CLI entry point."""

from __future__ import annotations

import argparse

from app.config.settings import get_settings
from app.config.underlyings import DEFAULT_UNDERLYING_CONFIG_PATH, load_underlying_config
from app.db.client import clickhouse_client, ping
from app.db.optimizations import apply_clickhouse_optimizations
from app.db.repository.insert import InsertRepository
from app.services.symbol_service import SymbolService
from app.utils.logger import configure_logging, get_logger

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Options data platform")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("healthcheck", help="Check ClickHouse connectivity")
    subparsers.add_parser("seed-nifty", help="Insert the default NIFTY underlying mapping")

    seed_underlyings = subparsers.add_parser("seed-underlyings", help="Insert underlyings from YAML config")
    seed_underlyings.add_argument("--config", default=str(DEFAULT_UNDERLYING_CONFIG_PATH), help="Path to YAML config")

    apply_optimizations = subparsers.add_parser(
        "apply-optimizations",
        help="Apply ClickHouse data-skipping indexes for faster analytics",
    )
    apply_optimizations.add_argument(
        "--materialize",
        action="store_true",
        help="Materialize indexes for existing parts. Can be expensive on large tables.",
    )
    return parser


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    args = build_parser().parse_args()

    try:
        if args.command == "healthcheck":
            with clickhouse_client(settings) as client:
                is_ok = ping(client)
                logger.info("clickhouse_healthcheck", extra={"ok": is_ok})
                if not is_ok:
                    raise SystemExit(1)

        if args.command == "seed-nifty":
            with clickhouse_client(settings) as client:
                mapping = SymbolService(settings=settings).default_nifty_mapping()
                inserted = InsertRepository(client, settings).insert_underlying_mappings([mapping])
                logger.info("seed_nifty_finished", extra={"inserted_rows": inserted})

        if args.command == "seed-underlyings":
            config = load_underlying_config(args.config)
            mappings = [underlying.to_mapping() for underlying in config.underlyings]
            with clickhouse_client(settings) as client:
                inserted = InsertRepository(client, settings).insert_underlying_mappings(mappings)
                logger.info(
                    "seed_underlyings_finished",
                    extra={"config": args.config, "configured_rows": len(mappings), "inserted_rows": inserted},
                )

        if args.command == "apply-optimizations":
            with clickhouse_client(settings) as client:
                applied = apply_clickhouse_optimizations(client, materialize=args.materialize)
                logger.info("apply_optimizations_finished", extra={"statements": applied})
    except Exception as exc:
        logger.exception("command_failed", extra={"command": args.command, "error": str(exc)})
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
