"""Transform Breeze API responses into database row models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

from app.config.constants import DEFAULT_EXCHANGE, RIGHT_TO_OPTION_TYPE
from app.db.models import OptionOHLCV
from app.utils.datetime_utils import parse_date, parse_datetime
from app.utils.logger import get_logger

logger = get_logger(__name__)


class BreezeTransformer:
    """Normalize Breeze records to the options_ohlcv schema."""

    def to_ohlcv_rows(
        self,
        payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        underlying: str,
        exchange: str = DEFAULT_EXCHANGE,
        expiry_date: date | str | None = None,
        strike_price: float | None = None,
        option_type: str | None = None,
    ) -> list[OptionOHLCV]:
        records = extract_success_records(payload)
        rows: list[OptionOHLCV] = []

        for record in records:
            try:
                rows.append(
                    OptionOHLCV(
                        datetime=_first_present(record, "datetime", "time", "date"),
                        underlying=underlying,
                        exchange=str(record.get("exchange_code") or record.get("exchange") or exchange),
                        expiry_date=record.get("expiry_date") or expiry_date,
                        strike_price=float(record.get("strike_price") or strike_price or 0),
                        option_type=_resolve_option_type(record, option_type),
                        open=float(record.get("open") or record.get("open_price") or 0),
                        high=float(record.get("high") or record.get("high_price") or 0),
                        low=float(record.get("low") or record.get("low_price") or 0),
                        close=float(record.get("close") or record.get("close_price") or 0),
                        volume=int(float(record.get("volume") or 0)),
                        open_interest=int(float(record.get("open_interest") or record.get("oi") or 0)),
                    )
                )
            except Exception as exc:
                logger.exception(
                    "failed_to_transform_breeze_record",
                    extra={"record": dict(record), "error": str(exc)},
                )

        return rows


def extract_success_records(
    payload: Mapping[str, Any] | Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Extract rows from Breeze's Success envelope or from a raw list."""

    if isinstance(payload, Mapping):
        success = payload.get("Success") or payload.get("success") or payload.get("data") or []
        if isinstance(success, Mapping):
            return [success]
        if isinstance(success, list):
            return [record for record in success if isinstance(record, Mapping)]
        return []
    return [record for record in payload if isinstance(record, Mapping)]


def _resolve_option_type(record: Mapping[str, Any], fallback: str | None) -> str:
    raw_value = record.get("right") or record.get("option_type") or fallback
    if raw_value is None:
        raise ValueError("option type/right missing from Breeze record")
    normalized = str(raw_value).strip().lower()
    return RIGHT_TO_OPTION_TYPE.get(normalized, str(raw_value).strip().upper())


def _first_present(record: Mapping[str, Any], *keys: str) -> datetime:
    for key in keys:
        value = record.get(key)
        if value:
            return parse_datetime(value)
    raise ValueError(f"missing datetime field; expected one of {keys}")


def normalize_expiry(value: str | date | datetime) -> date:
    return parse_date(value)

