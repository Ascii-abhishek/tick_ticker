"""Reviewed security/index reference data (reference/*.csv) and its validation.

The CSV files are the version-controlled source; `load-reference-data` validates
them and upserts them into D1, which scripts read at runtime.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

OPEN_START = "1900-01-01"


@dataclass(frozen=True)
class ReferenceTable:
    """One CSV file and the D1 table it loads into."""

    file_name: str
    table: str
    key_columns: tuple[str, ...]
    date_columns: tuple[str, ...] = ()


REFERENCE_TABLES: tuple[ReferenceTable, ...] = (
    ReferenceTable("sources.csv", "reference_source", ("source_id",)),
    ReferenceTable("securities.csv", "security_master", ("security_id",), ("first_trade_date", "last_trade_date")),
    ReferenceTable("security_identifiers.csv", "security_identifier_history", ("identifier_type", "identifier_value", "valid_from"), ("valid_from", "valid_to")),
    ReferenceTable("provider_mappings.csv", "security_provider_mapping", ("security_id", "provider", "valid_from"), ("valid_from", "valid_to", "verified_on")),
    ReferenceTable("corporate_actions.csv", "corporate_action", ("action_id",), ("ex_date", "effective_date")),
    ReferenceTable("index_definitions.csv", "index_definition", ("index_code",)),
    ReferenceTable("index_membership.csv", "index_membership", ("index_code", "security_id", "valid_from"), ("valid_from", "valid_to")),
)


@dataclass(frozen=True)
class ReferenceData:
    rows: dict[str, list[dict[str, str | None]]]

    def table(self, name: str) -> list[dict[str, str | None]]:
        return self.rows[name]


@dataclass(frozen=True)
class MembershipInterval:
    """A security's membership interval with the fields volume generation needs."""

    index_code: str
    security_id: str
    storage_symbol: str
    valid_from: date
    valid_to: date | None
    membership_type: str
    first_trade_date: date | None
    last_trade_date: date | None

    def active_on(self, day: date) -> bool:
        return self.valid_from <= day and (self.valid_to is None or day < self.valid_to)

    def trades_on(self, day: date) -> bool:
        """Whether candles can exist: placeholders and delisted names do not trade."""

        if self.first_trade_date and day < self.first_trade_date:
            return False
        return not (self.last_trade_date and day > self.last_trade_date)


def load_reference_dir(directory: Path) -> ReferenceData:
    rows: dict[str, list[dict[str, str | None]]] = {}
    for spec in REFERENCE_TABLES:
        with (directory / spec.file_name).open(newline="", encoding="utf-8") as handle:
            rows[spec.table] = [{key: (value.strip() or None) if isinstance(value, str) else value for key, value in row.items()} for row in csv.DictReader(handle)]
    return ReferenceData(rows)


def validate_reference_data(data: ReferenceData) -> tuple[list[str], list[str]]:
    """Return (errors, warnings). Errors block loading."""

    errors: list[str] = []
    warnings: list[str] = []
    for spec in REFERENCE_TABLES:
        seen: set[tuple[str | None, ...]] = set()
        for index, row in enumerate(data.table(spec.table), start=2):
            key = tuple(row.get(column) for column in spec.key_columns)
            if any(value is None for value in key):
                errors.append(f"{spec.file_name}:{index} missing key {spec.key_columns}")
            if key in seen:
                errors.append(f"{spec.file_name}:{index} duplicate key {key}")
            seen.add(key)
            for column in spec.date_columns:
                value = row.get(column)
                if value is not None:
                    try:
                        date.fromisoformat(value)
                    except ValueError:
                        errors.append(f"{spec.file_name}:{index} {column}={value!r} is not YYYY-MM-DD")
            if row.get("valid_to") and row.get("valid_from") and row["valid_to"] <= row["valid_from"]:
                errors.append(f"{spec.file_name}:{index} valid_to must be after valid_from")

    securities = {row["security_id"]: row for row in data.table("security_master")}
    for table, columns in (
        ("security_identifier_history", ("security_id",)),
        ("security_provider_mapping", ("security_id",)),
        ("corporate_action", ("security_id", "related_security_id")),
        ("index_membership", ("security_id",)),
        ("security_master", ("successor_security_id",)),
    ):
        for row in data.table(table):
            for column in columns:
                if row.get(column) and row[column] not in securities:
                    errors.append(f"{table}: unknown {column} {row[column]}")
    storage = [row["storage_symbol"] for row in securities.values()]
    if len(storage) != len(set(storage)):
        errors.append("security_master: storage_symbol must be unique")

    errors += _overlaps(data.table("index_membership"), ("index_code", "security_id"), "index_membership")
    errors += _overlaps(data.table("security_identifier_history"), ("security_id", "identifier_type"), "security_identifier_history")
    errors += _overlaps(data.table("security_identifier_history"), ("identifier_type", "identifier_value"), "security_identifier_history (same identifier on two securities)")
    errors += _overlaps(data.table("security_provider_mapping"), ("security_id", "provider"), "security_provider_mapping")

    definitions = {row["index_code"]: row for row in data.table("index_definition")}
    for row in data.table("index_membership"):
        if row["index_code"] not in definitions:
            errors.append(f"index_membership: unknown index_code {row['index_code']}")
    for index_code, definition in definitions.items():
        target = int(definition.get("target_constituents") or 0)
        if target:
            warnings += _basket_size_warnings(data, index_code, target)
    return errors, warnings


def membership_intervals(data: ReferenceData, index_code: str) -> list[MembershipInterval]:
    securities = {row["security_id"]: row for row in data.table("security_master")}
    return [
        MembershipInterval(
            index_code=index_code,
            security_id=row["security_id"],
            storage_symbol=securities[row["security_id"]]["storage_symbol"],
            valid_from=date.fromisoformat(row["valid_from"]),
            valid_to=date.fromisoformat(row["valid_to"]) if row.get("valid_to") else None,
            membership_type=row["membership_type"],
            first_trade_date=_optional_date(securities[row["security_id"]].get("first_trade_date")),
            last_trade_date=_optional_date(securities[row["security_id"]].get("last_trade_date")),
        )
        for row in data.table("index_membership")
        if row["index_code"] == index_code
    ]


def _overlaps(rows: list[dict[str, str | None]], group_columns: tuple[str, ...], label: str) -> list[str]:
    groups: dict[tuple[str | None, ...], list[tuple[str, str]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(column) for column in group_columns)].append((row.get("valid_from") or OPEN_START, row.get("valid_to") or "9999-12-31"))
    errors = []
    for key, intervals in groups.items():
        intervals.sort()
        for (start, end), (next_start, _next_end) in zip(intervals, intervals[1:]):
            if next_start < end:
                errors.append(f"{label}: overlapping intervals for {key}: [{start}, {end}) and from {next_start}")
    return errors


def _basket_size_warnings(data: ReferenceData, index_code: str, target: int) -> list[str]:
    rows = [row for row in data.table("index_membership") if row["index_code"] == index_code]
    securities = {row["security_id"]: row for row in data.table("security_master")}
    change_dates = sorted({row["valid_from"] for row in rows} | {row["valid_to"] for row in rows if row.get("valid_to")})
    warnings = []
    for day in change_dates:
        active = [row for row in rows if row["valid_from"] <= day and (not row.get("valid_to") or day < row["valid_to"])]
        extra = sum(
            1
            for row in active
            if row["membership_type"] == "demerger_placeholder" or securities.get(row["security_id"], {}).get("security_type") == "equity_dvr"
        )
        if len(active) - extra != target:
            warnings.append(f"{index_code} on {day}: {len(active)} securities ({extra} DVR/placeholder), expected {target} companies")
    return warnings


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None
