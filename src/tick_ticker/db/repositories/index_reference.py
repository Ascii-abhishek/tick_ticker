"""D1 access for security/index reference tables and index volume state."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path

from tick_ticker.services.index_volume import FORMULA_VERSION, DayVolumeResult
from tick_ticker.services.reference_data import REFERENCE_TABLES, ReferenceData
from tick_ticker.utils.engines import D1Client

# D1 allows at most 100 bound parameters per statement.
D1_MAX_PARAMS = 100
MIGRATION_PATH = Path(__file__).resolve().parents[4] / "migrations" / "d1" / "002_security_and_index_reference.sql"


class IndexReferenceRepository:
    def __init__(self, client: D1Client) -> None:
        self.client = client

    def apply_migration(self, sql_path: Path = MIGRATION_PATH) -> int:
        """Run each statement of an idempotent migration file; return the statement count."""

        statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))
        for statement in statements:
            self.client.execute(statement)
        return len(statements)

    def upsert_reference(self, data: ReferenceData, *, prune: bool) -> dict[str, int]:
        """Upsert every reference table; optionally delete D1 rows absent from the seed."""

        now = _now()
        counts: dict[str, int] = {}
        for spec in REFERENCE_TABLES:
            rows = [dict(row, updated_at=now) for row in data.table(spec.table)]
            if not rows:
                counts[spec.table] = 0
                continue
            columns = list(rows[0])
            self._upsert(spec.table, columns, spec.key_columns, rows)
            if prune:
                self._prune(spec.table, spec.key_columns, rows)
            counts[spec.table] = len(rows)
        return counts

    def _upsert(self, table: str, columns: list[str], key_columns: tuple[str, ...], rows: list[dict[str, str | None]]) -> None:
        per_statement = max(1, D1_MAX_PARAMS // len(columns))
        updates = ", ".join(f"{column} = excluded.{column}" for column in columns if column not in key_columns)
        for start in range(0, len(rows), per_statement):
            batch = rows[start : start + per_statement]
            placeholders = ", ".join("(" + ", ".join("?" for _ in columns) + ")" for _ in batch)
            self.client.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES {placeholders} "
                f"ON CONFLICT({', '.join(key_columns)}) DO UPDATE SET {updates}",
                [row.get(column) for row in batch for column in columns],
            )

    def _prune(self, table: str, key_columns: tuple[str, ...], rows: list[dict[str, str | None]]) -> None:
        keep = {tuple(row[column] for column in key_columns) for row in rows}
        existing = self.client.query(f"SELECT {', '.join(key_columns)} FROM {table}")
        for row in existing:
            key = tuple(row[column] for column in key_columns)
            if key not in keep:
                where = " AND ".join(f"{column} = ?" for column in key_columns)
                self.client.execute(f"DELETE FROM {table} WHERE {where}", list(key))

    def fetch_reference(self) -> ReferenceData:
        """Read the reference tables back from D1 in the CSV row shape."""

        rows = {}
        for spec in REFERENCE_TABLES:
            fetched = self.client.query(f"SELECT * FROM {spec.table} ORDER BY {', '.join(spec.key_columns)}")
            rows[spec.table] = [{key: (None if value is None else str(value)) for key, value in row.items() if key != "updated_at"} for row in fetched]
        return ReferenceData(rows)

    def volume_states(self, index_code: str, from_date: date, to_date: date) -> dict[date, dict[str, object]]:
        rows = self.client.query(
            "SELECT * FROM index_volume_state WHERE index_code = ? AND trade_date BETWEEN ? AND ?",
            [index_code, from_date.isoformat(), to_date.isoformat()],
        )
        return {date.fromisoformat(str(row["trade_date"])): row for row in rows}

    def upsert_volume_states(
        self,
        index_code: str,
        results: list[tuple[DayVolumeResult, str]],
        *,
        published_days: set[date],
    ) -> None:
        """Record per-day generation results; published_at is set only for published days."""

        now = _now()
        columns = [
            "index_code", "trade_date", "status", "formula_version", "membership_fingerprint", "input_fingerprint",
            "expected_members", "observed_members", "missing_members", "not_trading_members", "session_minutes",
            "minutes_with_turnover", "total_turnover", "total_volume", "published_at", "error", "updated_at",
        ]
        rows = [
            {
                "index_code": index_code,
                "trade_date": result.trade_date.isoformat(),
                "status": result.status,
                "formula_version": FORMULA_VERSION,
                "membership_fingerprint": membership_hash,
                "input_fingerprint": result.input_fingerprint,
                "expected_members": result.expected_members,
                "observed_members": result.observed_members,
                "missing_members": json.dumps(result.missing_members),
                "not_trading_members": json.dumps(result.not_trading_members),
                "session_minutes": result.session_minutes,
                "minutes_with_turnover": result.minutes_with_turnover,
                "total_turnover": round(result.total_turnover, 2),
                "total_volume": result.total_volume,
                "published_at": now if result.trade_date in published_days else None,
                "error": None,
                "updated_at": now,
            }
            for result, membership_hash in results
        ]
        # Keep an earlier published_at when this run only recomputes without publishing.
        per_statement = max(1, D1_MAX_PARAMS // len(columns))
        updates = ", ".join(
            f"{column} = excluded.{column}"
            if column != "published_at"
            else "published_at = COALESCE(excluded.published_at, CASE WHEN index_volume_state.input_fingerprint = excluded.input_fingerprint "
            "AND index_volume_state.membership_fingerprint = excluded.membership_fingerprint THEN index_volume_state.published_at END)"
            for column in columns
            if column not in ("index_code", "trade_date")
        )
        for start in range(0, len(rows), per_statement):
            batch = rows[start : start + per_statement]
            placeholders = ", ".join("(" + ", ".join("?" for _ in columns) + ")" for _ in batch)
            self.client.execute(
                f"INSERT INTO index_volume_state ({', '.join(columns)}) VALUES {placeholders} "
                f"ON CONFLICT(index_code, trade_date) DO UPDATE SET {updates}",
                [row[column] for row in batch for column in columns],
            )


def split_sql_statements(sql: str) -> list[str]:
    """Split a migration on semicolons that end a line, dropping comment-only chunks."""

    without_comments = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [statement.strip() for statement in re.split(r";\s*(?:\n|$)", without_comments) if statement.strip()]


def _now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()
