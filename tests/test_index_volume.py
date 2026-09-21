from datetime import date, datetime

import polars as pl
import pytest

from tick_ticker.services.index_volume import MemberInput, compute_day_volume, membership_fingerprint

DAY = date(2026, 9, 2)


def _frame(rows):
    return pl.DataFrame(
        rows,
        schema={"datetime": pl.Datetime("us"), "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64, "volume": pl.Int64},
        orient="row",
    )


def _index(*minutes, close=100.0):
    return _frame([(datetime(2026, 9, 2, hour, minute), close, close, close, close, 0) for hour, minute in minutes])


def test_turnover_over_index_close() -> None:
    index = _index((9, 15), (9, 16))
    acc = _frame([(datetime(2026, 9, 2, 9, 15), 10.0, 12.0, 8.0, 10.0, 100), (datetime(2026, 9, 2, 9, 16), 20.0, 20.0, 20.0, 20.0, 50)])
    infy = _frame([(datetime(2026, 9, 2, 9, 15), 50.0, 50.0, 50.0, 50.0, 10)])

    result = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", acc), MemberInput("INFY", "INFY", infy)])

    # 9:15 -> (10*100 + 50*10) / 100 = 15 ; 9:16 -> 20*50/100 = 10 (INFY had no trade)
    assert result.index_frame.get_column("volume").to_list() == [15, 10]
    assert result.status == "complete"
    assert result.observed_members == 2
    assert result.total_turnover == pytest.approx(2500.0)


def test_split_invariance_when_price_and_volume_adjust_together() -> None:
    index = _index((9, 15))
    before = _frame([(datetime(2026, 9, 2, 9, 15), 1000.0, 1000.0, 1000.0, 1000.0, 10)])
    after = _frame([(datetime(2026, 9, 2, 9, 15), 100.0, 100.0, 100.0, 100.0, 100)])

    first = compute_day_volume(DAY, index, [MemberInput("X", "X", before)])
    second = compute_day_volume(DAY, index, [MemberInput("X", "X", after)])

    assert first.index_frame.get_column("volume").to_list() == second.index_frame.get_column("volume").to_list() == [100]


def test_missing_member_marks_day_partial_but_placeholder_does_not() -> None:
    index = _index((9, 15))
    acc = _frame([(datetime(2026, 9, 2, 9, 15), 10.0, 10.0, 10.0, 10.0, 10)])

    partial = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", acc), MemberInput("HDFC", "HDFC", None)])
    placeholder = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", acc), MemberInput("TMCV", "TMCV", None, expects_data=False)])

    assert partial.status == "partial"
    assert partial.missing_members == ["HDFC"]
    assert placeholder.status == "complete"
    assert placeholder.not_trading_members == ["TMCV"]
    assert placeholder.expected_members == 1


def test_muhurat_evening_session_is_counted() -> None:
    index = _index((18, 15))
    member = _frame([(datetime(2026, 9, 2, 18, 15), 10.0, 10.0, 10.0, 10.0, 100)])

    result = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", member)])

    assert result.index_frame.get_column("volume").to_list() == [10]


def test_minutes_outside_regular_session_get_zero_volume() -> None:
    index = _index((9, 7), (9, 15), (15, 30))
    member = _frame([(datetime(2026, 9, 2, hour, minute), 10.0, 10.0, 10.0, 10.0, 100) for hour, minute in [(9, 7), (9, 15), (15, 30)]])

    result = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", member)])

    assert result.index_frame.get_column("volume").to_list() == [0, 10, 0]
    assert result.session_minutes == 1


def test_invalid_and_duplicate_member_rows_are_dropped() -> None:
    index = _index((9, 15), (9, 16))
    member = _frame(
        [
            (datetime(2026, 9, 2, 9, 15), 10.0, 10.0, 10.0, 10.0, 100),
            (datetime(2026, 9, 2, 9, 15), 10.0, 10.0, 10.0, 10.0, 200),
            (datetime(2026, 9, 2, 9, 16), 0.0, 10.0, 10.0, 10.0, 100),
        ]
    )

    result = compute_day_volume(DAY, index, [MemberInput("ACC", "ACC", member)])

    assert result.index_frame.get_column("volume").to_list() == [20, 0]
    assert result.dropped_member_rows == 2


def test_rejects_non_positive_index_close() -> None:
    with pytest.raises(ValueError):
        compute_day_volume(DAY, _index((9, 15), close=0.0), [])


def test_membership_fingerprint_changes_with_storage_symbol() -> None:
    assert membership_fingerprint([MemberInput("TATAMOTORS", "TMPV", None)]) != membership_fingerprint([MemberInput("TATAMOTORS", "TATAMOTORS", None)])
