"""Synthetic index volume from constituent 1-minute candles.

For each regular-session minute t of an index:

    turnover_t = sum over members s of ((open + high + low + close) / 4) * volume
    volume_t   = round(turnover_t / index_close_t)

The result is constituent traded value expressed in index units. Price x volume
is invariant to splits/bonuses as long as a provider adjusts price and volume
together (Upstox and Breeze both do), so members fetched on different
adjustment bases can be summed. It is a traded-value weighting, not the index's
free-float market-cap weighting.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import date, time
from typing import Literal

import polars as pl

FORMULA_VERSION = "ohlc4_turnover_over_close_v1"
# Auction windows excluded from the sum (candles are labelled by start minute):
# the 09:00-09:15 pre-open and the 15:30-16:00 post-close session. Breeze has
# candles there and Upstox does not. Excluding windows rather than keeping only
# 09:15-15:29 keeps special sessions such as evening Muhurat trading.
EXCLUDED_WINDOWS = ((time(9, 0), time(9, 14)), (time(15, 30), time(15, 59)))

DayStatus = Literal["complete", "partial", "no_index_data"]


@dataclass(frozen=True)
class MemberInput:
    """One index member's candles for a single trade date."""

    security_id: str
    storage_symbol: str
    frame: pl.DataFrame | None
    # False when the member cannot trade that day (placeholder before listing,
    # suspension); its absence is then expected rather than missing data.
    expects_data: bool = True


@dataclass
class DayVolumeResult:
    """Synthetic volume for one index trade date plus its quality metadata."""

    trade_date: date
    index_frame: pl.DataFrame
    status: DayStatus
    expected_members: int
    observed_members: int
    missing_members: list[str] = field(default_factory=list)
    not_trading_members: list[str] = field(default_factory=list)
    session_minutes: int = 0
    minutes_with_turnover: int = 0
    total_turnover: float = 0.0
    total_volume: int = 0
    dropped_member_rows: int = 0
    input_fingerprint: str = ""


def compute_day_volume(trade_date: date, index_frame: pl.DataFrame, members: list[MemberInput]) -> DayVolumeResult:
    """Compute synthetic volume for one day; the index frame keeps all its rows.

    Minutes in the pre-open and post-close windows (Breeze-only candles) get
    volume 0. A member minute without a candle is treated as no trades.
    """

    if index_frame.is_empty():
        return DayVolumeResult(trade_date, index_frame, "no_index_data", 0, 0)
    close = index_frame.get_column("close")
    if close.is_null().any() or not bool((close > 0).all()) or not all(math.isfinite(value) for value in close.to_list()):
        raise ValueError(f"index close must be finite and positive on {trade_date}")
    if index_frame.get_column("datetime").is_duplicated().any():
        raise ValueError(f"index has duplicate timestamps on {trade_date}")

    turnover_parts: list[pl.DataFrame] = []
    missing: list[str] = []
    not_trading: list[str] = []
    observed = 0
    dropped = 0
    fingerprint_parts: list[tuple[str, int, int, float]] = []
    for member in sorted(members, key=lambda item: item.storage_symbol):
        frame = member.frame
        if frame is None or frame.is_empty():
            (missing if member.expects_data else not_trading).append(member.storage_symbol)
            fingerprint_parts.append((member.storage_symbol, 0, 0, 0.0))
            continue
        observed += 1
        clean = _clean_member_frame(frame)
        dropped += frame.height - clean.height
        fingerprint_parts.append(
            (member.storage_symbol, clean.height, int(clean.get_column("volume").sum() or 0), round(float(clean.get_column("close").sum() or 0.0), 4))
        )
        turnover_parts.append(
            clean.select(
                "datetime",
                (((pl.col("open") + pl.col("high") + pl.col("low") + pl.col("close")) / 4) * pl.col("volume")).alias("turnover"),
            )
        )

    clock = pl.col("datetime").dt.time()
    in_session = ~pl.any_horizontal([clock.is_between(start, end) for start, end in EXCLUDED_WINDOWS])
    if turnover_parts:
        turnover = pl.concat(turnover_parts).group_by("datetime").agg(pl.col("turnover").sum())
    else:
        turnover = pl.DataFrame(schema={"datetime": index_frame.schema["datetime"], "turnover": pl.Float64})
    joined = index_frame.join(turnover, on="datetime", how="left").with_columns(
        pl.when(in_session).then(pl.col("turnover").fill_null(0.0)).otherwise(0.0).alias("turnover")
    )
    result_frame = joined.with_columns((pl.col("turnover") / pl.col("close")).round(0).cast(pl.Int64).alias("volume")).drop("turnover")
    session_rows = joined.filter(in_session)

    expected = observed + len(missing)
    return DayVolumeResult(
        trade_date=trade_date,
        index_frame=result_frame.select(index_frame.columns),
        status="complete" if not missing else "partial",
        expected_members=expected,
        observed_members=observed,
        missing_members=missing,
        not_trading_members=not_trading,
        session_minutes=session_rows.height,
        minutes_with_turnover=session_rows.filter(pl.col("turnover") > 0).height,
        total_turnover=float(session_rows.get_column("turnover").sum() or 0.0),
        total_volume=int(result_frame.get_column("volume").sum() or 0),
        dropped_member_rows=dropped,
        input_fingerprint=_fingerprint(fingerprint_parts),
    )


def membership_fingerprint(members: list[MemberInput]) -> str:
    """Stable hash of which securities (and storage symbols) made up the basket."""

    return _fingerprint(sorted((member.security_id, member.storage_symbol, member.expects_data) for member in members))


def _clean_member_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Keep one valid candle per minute (last wins) with finite, positive prices."""

    prices = ["open", "high", "low", "close"]
    valid = pl.all_horizontal([pl.col(name).is_finite() & (pl.col(name) > 0) for name in prices]) & (pl.col("volume") >= 0)
    valid = valid & (pl.col("high") >= pl.max_horizontal("open", "close", "low")) & (pl.col("low") <= pl.min_horizontal("open", "close", "high"))
    return (
        frame.select("datetime", *prices, "volume")
        .with_columns(pl.col(prices).cast(pl.Float64), pl.col("volume").cast(pl.Int64))
        .filter(valid)
        .unique(subset="datetime", keep="last", maintain_order=True)
    )


def _fingerprint(parts: object) -> str:
    return hashlib.sha256(json.dumps(parts, default=str, separators=(",", ":")).encode()).hexdigest()[:16]
