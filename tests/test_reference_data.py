from datetime import date
from pathlib import Path

from tick_ticker.db.repositories.index_reference import split_sql_statements
from tick_ticker.services.reference_data import (
    ReferenceData,
    load_reference_dir,
    membership_intervals,
    validate_reference_data,
)

REFERENCE_DIR = Path(__file__).resolve().parents[1] / "reference"


def _minimal(membership, identifiers=()):
    return ReferenceData(
        {
            "reference_source": [],
            "security_master": [
                {"security_id": "A", "storage_symbol": "A", "security_type": "equity", "first_trade_date": None, "last_trade_date": None, "successor_security_id": None},
                {"security_id": "B", "storage_symbol": "B", "security_type": "equity", "first_trade_date": "2020-01-10", "last_trade_date": None, "successor_security_id": None},
            ],
            "security_identifier_history": list(identifiers),
            "security_provider_mapping": [],
            "corporate_action": [],
            "index_definition": [{"index_code": "IDX", "storage_symbol": "IDX", "target_constituents": "1"}],
            "index_membership": membership,
        }
    )


def _member(security_id, valid_from, valid_to=None, membership_type="regular"):
    return {"index_code": "IDX", "security_id": security_id, "valid_from": valid_from, "valid_to": valid_to, "membership_type": membership_type}


def test_repository_reference_files_are_valid() -> None:
    errors, warnings = validate_reference_data(load_reference_dir(REFERENCE_DIR))

    assert errors == []
    assert warnings == []


def test_nifty_membership_has_fifty_companies_on_known_dates() -> None:
    intervals = membership_intervals(load_reference_dir(REFERENCE_DIR), "NIFTY50")

    def active(day):
        return {interval.storage_symbol for interval in intervals if interval.active_on(day)}

    assert len(active(date(2023, 7, 12))) == 50 and "HDFC" in active(date(2023, 7, 12))
    assert "HDFC" not in active(date(2023, 7, 13)) and "LTM" in active(date(2023, 7, 13))
    assert len(active(date(2023, 8, 25))) == 51 and "JIOFIN" in active(date(2023, 8, 25))
    assert "LUPIN" in active(date(2018, 6, 1))
    assert "TMCV" in active(date(2025, 11, 14)) and "TMCV" not in active(date(2025, 11, 17))
    assert "BSE" not in active(date(2026, 9, 29)) and "BSE" in active(date(2026, 9, 30))


def test_placeholder_does_not_trade_before_listing() -> None:
    intervals = membership_intervals(load_reference_dir(REFERENCE_DIR), "NIFTY50")
    tmcv = next(interval for interval in intervals if interval.security_id == "TMCV")

    assert tmcv.active_on(date(2025, 10, 20)) and not tmcv.trades_on(date(2025, 10, 20))
    assert tmcv.trades_on(date(2025, 11, 12))


def test_overlapping_membership_is_an_error() -> None:
    errors, _warnings = validate_reference_data(_minimal([_member("A", "2020-01-01", "2021-01-01"), _member("A", "2020-06-01")]))

    assert any("overlapping" in error for error in errors)


def test_same_identifier_on_two_securities_is_an_error() -> None:
    identifiers = [
        {"security_id": "A", "identifier_type": "nse_symbol", "identifier_value": "X", "valid_from": "2020-01-01", "valid_to": None},
        {"security_id": "B", "identifier_type": "nse_symbol", "identifier_value": "X", "valid_from": "2020-06-01", "valid_to": None},
    ]
    errors, _warnings = validate_reference_data(_minimal([_member("A", "2020-01-01")], identifiers))

    assert any("same identifier" in error for error in errors)


def test_unknown_security_and_basket_size_are_reported() -> None:
    errors, warnings = validate_reference_data(_minimal([_member("A", "2020-01-01"), _member("B", "2020-01-01"), _member("Z", "2020-01-01")]))

    assert any("unknown security_id Z" in error for error in errors)
    assert any("expected 1 companies" in warning for warning in warnings)


def test_split_sql_statements_keeps_view_body_whole() -> None:
    sql = "-- comment\nCREATE TABLE a (x TEXT);\nCREATE VIEW v AS\nSELECT x FROM a\nWHERE x = 'a;b';\n"

    assert split_sql_statements(sql) == ["CREATE TABLE a (x TEXT)", "CREATE VIEW v AS\nSELECT x FROM a\nWHERE x = 'a;b'"]
