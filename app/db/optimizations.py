"""ClickHouse optimizations for the existing table schemas."""

from __future__ import annotations

from clickhouse_connect.driver.client import Client

from app.utils.logger import get_logger

logger = get_logger(__name__)


OPTIMIZATION_STATEMENTS: tuple[str, ...] = (
    """
    ALTER TABLE options_ohlcv
    ADD INDEX IF NOT EXISTS idx_ohlcv_datetime datetime TYPE minmax GRANULARITY 1
    """,
    """
    ALTER TABLE options_ohlcv
    ADD INDEX IF NOT EXISTS idx_ohlcv_volume volume TYPE minmax GRANULARITY 4
    """,
    """
    ALTER TABLE options_ohlcv
    ADD INDEX IF NOT EXISTS idx_ohlcv_open_interest open_interest TYPE minmax GRANULARITY 4
    """,
    """
    ALTER TABLE option_contracts
    ADD INDEX IF NOT EXISTS idx_contract_id contract_id TYPE minmax GRANULARITY 1
    """,
    """
    ALTER TABLE options_snapshot
    ADD INDEX IF NOT EXISTS idx_snapshot_datetime datetime TYPE minmax GRANULARITY 1
    """,
)

MATERIALIZE_INDEX_STATEMENTS: tuple[str, ...] = (
    "ALTER TABLE options_ohlcv MATERIALIZE INDEX idx_ohlcv_datetime",
    "ALTER TABLE options_ohlcv MATERIALIZE INDEX idx_ohlcv_volume",
    "ALTER TABLE options_ohlcv MATERIALIZE INDEX idx_ohlcv_open_interest",
    "ALTER TABLE option_contracts MATERIALIZE INDEX idx_contract_id",
    "ALTER TABLE options_snapshot MATERIALIZE INDEX idx_snapshot_datetime",
)


def apply_clickhouse_optimizations(client: Client, *, materialize: bool = False) -> int:
    """Apply secondary data-skipping indexes without changing table schemas.

    The existing ORDER BY keys remain the main performance feature. These
    indexes help ad hoc scans over datetime, volume, OI, and contract id.
    Materialization is optional because it can be expensive on large tables.
    """

    applied = 0
    for statement in OPTIMIZATION_STATEMENTS:
        client.command(_compact_sql(statement))
        applied += 1
        logger.info("clickhouse_optimization_applied", extra={"statement": _compact_sql(statement)})

    if materialize:
        for statement in MATERIALIZE_INDEX_STATEMENTS:
            client.command(statement)
            applied += 1
            logger.info("clickhouse_index_materialized", extra={"statement": statement})

    return applied


def _compact_sql(statement: str) -> str:
    return " ".join(statement.split())
