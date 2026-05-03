"""Read repositories for analytics and services."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from clickhouse_connect.driver.client import Client


class QueryRepository:
    """Read path for ClickHouse tables."""

    def __init__(self, client: Client) -> None:
        self.client = client

    def latest_candle_time(
        self,
        *,
        underlying: str,
        expiry_date: date,
        strike_price: float,
        option_type: str,
    ) -> datetime | None:
        result = self.client.query(
            """
            SELECT max(datetime)
            FROM options_ohlcv
            WHERE underlying = {underlying:String}
              AND expiry_date = {expiry_date:Date}
              AND strike_price = {strike_price:Float32}
              AND option_type = {option_type:String}
            """,
            parameters={
                "underlying": underlying,
                "expiry_date": expiry_date,
                "strike_price": strike_price,
                "option_type": option_type,
            },
        )
        value = result.result_rows[0][0] if result.result_rows else None
        return value if isinstance(value, datetime) else None

    def get_underlying_mappings(self) -> list[dict[str, Any]]:
        result = self.client.query(
            """
            SELECT
                underlying_id,
                breeze_symbol,
                nse_symbol,
                display_name,
                exchange,
                lot_size,
                tick_size
            FROM underlying_mapping
            ORDER BY underlying_id
            """
        )
        columns = [
            "underlying_id",
            "breeze_symbol",
            "nse_symbol",
            "display_name",
            "exchange",
            "lot_size",
            "tick_size",
        ]
        return [dict(zip(columns, row, strict=True)) for row in result.result_rows]

    def get_recent_options_ohlcv(
        self,
        *,
        underlying: str,
        expiry_date: date,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        result = self.client.query(
            """
            SELECT
                datetime,
                underlying,
                exchange,
                expiry_date,
                strike_price,
                toString(option_type) AS option_type,
                open,
                high,
                low,
                close,
                volume,
                open_interest
            FROM options_ohlcv
            WHERE underlying = {underlying:String}
              AND expiry_date = {expiry_date:Date}
            ORDER BY datetime DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "underlying": underlying,
                "expiry_date": expiry_date,
                "limit": limit,
            },
        )
        columns = [
            "datetime",
            "underlying",
            "exchange",
            "expiry_date",
            "strike_price",
            "option_type",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
        ]
        return [dict(zip(columns, row, strict=True)) for row in result.result_rows]

