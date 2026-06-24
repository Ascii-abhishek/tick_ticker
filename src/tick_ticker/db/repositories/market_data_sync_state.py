"""Repository for market_data_sync_state."""

from __future__ import annotations

from datetime import UTC, date, datetime

from tick_ticker.db.models import EquitySymbolReference, MarketDataSyncCompletion, MarketDataSyncState
from tick_ticker.utils.engines import D1Client


class MarketDataSyncStateRepository:
    """D1 access for reusable market data sync state."""

    def __init__(self, client: D1Client) -> None:
        self.client = client

    def ensure_table(self) -> None:
        """Create the sync state table when it does not exist."""

        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS market_data_sync_state (
                market_type TEXT NOT NULL,
                nse_symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                from_date TEXT,
                to_date TEXT,
                row_count INTEGER,
                local_path TEXT,
                r2_prefix TEXT,
                error TEXT,
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (market_type, nse_symbol)
            )
            """
        )
        self.client.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_data_sync_state_status
            ON market_data_sync_state (market_type, status, nse_symbol)
            """
        )

    def get_state(self, *, market_type: str, nse_symbol: str) -> MarketDataSyncState | None:
        """Return one sync-state row."""

        rows = self.client.query(
            """
            SELECT
                market_type,
                nse_symbol,
                status,
                from_date,
                to_date,
                row_count,
                local_path,
                r2_prefix,
                error,
                started_at,
                completed_at,
                updated_at
            FROM market_data_sync_state
            WHERE market_type = ? AND UPPER(nse_symbol) = UPPER(?)
            LIMIT 1
            """,
            [market_type, nse_symbol],
        )
        if not rows:
            return None
        return MarketDataSyncState.model_validate(rows[0])

    def next_due_cash_symbol(self, *, target_to_date: date) -> EquitySymbolReference | None:
        """Return the next cash symbol needing work through target_to_date."""

        rows = self.client.query(
            """
            SELECT
                refs.nse_symbol,
                refs.breeze_code,
                refs.nse_company_name,
                refs.listing_date,
                refs.isin
            FROM equity_symbol_reference refs
            LEFT JOIN market_data_sync_state state
                ON state.market_type = 'cash'
                AND state.nse_symbol = refs.nse_symbol
            WHERE
                state.nse_symbol IS NULL
                OR state.status != 'completed'
                OR state.to_date IS NULL
                OR state.to_date < ?
            ORDER BY refs.nse_symbol
            LIMIT 1
            """,
            [target_to_date.isoformat()],
        )
        if not rows:
            return None
        return EquitySymbolReference.model_validate(rows[0])

    def mark_started(self, *, market_type: str, nse_symbol: str, from_date: str, to_date: str) -> None:
        """Upsert in-progress state."""

        now = _now()
        self.client.execute(
            """
            INSERT INTO market_data_sync_state (
                market_type,
                nse_symbol,
                status,
                from_date,
                to_date,
                error,
                started_at,
                updated_at
            )
            VALUES (?, ?, 'in_progress', ?, ?, NULL, ?, ?)
            ON CONFLICT(market_type, nse_symbol) DO UPDATE SET
                status = 'in_progress',
                from_date = excluded.from_date,
                to_date = excluded.to_date,
                error = NULL,
                started_at = excluded.started_at,
                updated_at = excluded.updated_at
            """,
            [market_type, nse_symbol, from_date, to_date, now, now],
        )

    def mark_failed(self, *, market_type: str, nse_symbol: str, error: str) -> None:
        """Store the latest error while keeping the symbol eligible for retry."""

        self.client.execute(
            """
            UPDATE market_data_sync_state
            SET status = 'failed', error = ?, updated_at = ?
            WHERE market_type = ? AND nse_symbol = ?
            """,
            [error[:2000], _now(), market_type, nse_symbol],
        )

    def mark_completed(self, completion: MarketDataSyncCompletion) -> None:
        """Mark a sync as completed."""

        completed_at = completion.completed_at.replace(microsecond=0).isoformat()
        self.client.execute(
            """
            INSERT INTO market_data_sync_state (
                market_type,
                nse_symbol,
                status,
                from_date,
                to_date,
                row_count,
                local_path,
                r2_prefix,
                error,
                completed_at,
                updated_at
            )
            VALUES (?, ?, 'completed', ?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(market_type, nse_symbol) DO UPDATE SET
                status = 'completed',
                from_date = excluded.from_date,
                to_date = excluded.to_date,
                row_count = excluded.row_count,
                local_path = excluded.local_path,
                r2_prefix = excluded.r2_prefix,
                error = NULL,
                completed_at = excluded.completed_at,
                updated_at = excluded.updated_at
            """,
            [
                completion.market_type,
                completion.nse_symbol,
                completion.from_date.isoformat(),
                completion.to_date.isoformat(),
                completion.row_count,
                completion.local_path,
                completion.r2_prefix,
                completed_at,
                completed_at,
            ],
        )


def _now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()
