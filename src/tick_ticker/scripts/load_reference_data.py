"""Validate reference/*.csv and upsert it into the D1 reference tables."""

from __future__ import annotations

import argparse
from pathlib import Path

from tick_ticker.config import get_settings
from tick_ticker.db.repositories.index_reference import IndexReferenceRepository
from tick_ticker.services.reference_data import load_reference_dir, validate_reference_data
from tick_ticker.utils.engines import create_d1_client
from tick_ticker.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


def main() -> None:
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    data = load_reference_dir(args.reference_dir)
    errors, warnings = validate_reference_data(data)
    for warning in warnings:
        logger.warning("reference_data_warning %s", warning)
    for error in errors:
        logger.error("reference_data_error %s", error)
    if errors:
        raise SystemExit(f"{len(errors)} reference data error(s); nothing loaded")
    logger.info("reference_data_valid tables=%s", {table: len(rows) for table, rows in data.rows.items()})
    if args.validate_only:
        return

    repository = IndexReferenceRepository(create_d1_client(settings))
    if args.apply_migration:
        logger.info("reference_migration_applied statements=%s", repository.apply_migration())
    counts = repository.upsert_reference(data, prune=args.prune)
    logger.info("reference_data_loaded prune=%s counts=%s", args.prune, counts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path, default=Path("reference"), help="Directory with the reference CSV files.")
    parser.add_argument("--validate-only", action="store_true", help="Validate the CSV files without touching D1.")
    parser.add_argument("--apply-migration", action="store_true", help="Run migrations/d1/002 (idempotent) before loading.")
    parser.add_argument("--prune", action="store_true", help="Delete D1 reference rows whose keys are no longer in the CSV files.")
    return parser.parse_args()


if __name__ == "__main__":
    main()
