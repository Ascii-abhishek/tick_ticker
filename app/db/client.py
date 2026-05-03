"""Reusable ClickHouse client factory."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from app.config.settings import Settings, get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


def create_clickhouse_client(settings: Settings | None = None) -> Client:
    """Create a ClickHouse HTTP client."""

    settings = settings or get_settings()
    logger.info(
        "creating_clickhouse_client",
        extra={
            "host": settings.clickhouse_host,
            "port": settings.clickhouse_port,
            "database": settings.clickhouse_database,
            "secure": settings.clickhouse_secure,
        },
    )
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
        connect_timeout=settings.clickhouse_connect_timeout_seconds,
        send_receive_timeout=settings.clickhouse_send_receive_timeout_seconds,
    )


@contextmanager
def clickhouse_client(settings: Settings | None = None) -> Iterator[Client]:
    """Context manager for ClickHouse client lifecycle."""

    client = create_clickhouse_client(settings)
    try:
        yield client
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def ping(client: Client) -> bool:
    """Return True when ClickHouse is reachable."""

    result: Any = client.query("SELECT 1").result_rows
    return bool(result and result[0][0] == 1)

