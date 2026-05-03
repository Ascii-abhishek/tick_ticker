"""Historical backfill orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

from clickhouse_connect.driver.client import Client

from app.config.constants import DEFAULT_EXCHANGE, DEFAULT_INTERVAL, OPTION_TYPES
from app.config.settings import Settings, get_settings
from app.db.models import OptionContract
from app.db.repository.insert import InsertRepository
from app.ingestion.breeze_client import BreezeClient
from app.ingestion.transformer import BreezeTransformer
from app.utils.datetime_utils import chunk_datetime_range
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class HistoricalLoadRequest:
    underlying: str
    expiries: tuple[date, ...]
    strikes: tuple[float, ...]
    start: datetime
    end: datetime
    exchange: str = DEFAULT_EXCHANGE
    breeze_symbol: str | None = None
    interval: str = DEFAULT_INTERVAL
    option_types: tuple[str, ...] = OPTION_TYPES
    lot_size: int | None = None
    is_weekly: int = 1


class HistoricalLoader:
    """Fetch and persist historical option candles."""

    def __init__(
        self,
        *,
        breeze_client: BreezeClient,
        clickhouse_client: Client,
        settings: Settings | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.breeze_client = breeze_client
        self.transformer = BreezeTransformer()
        self.insert_repository = InsertRepository(clickhouse_client, self.settings)

    def load(self, request: HistoricalLoadRequest) -> int:
        """Run a historical backfill and return inserted candle count."""

        contracts = [
            OptionContract(
                underlying=request.underlying,
                expiry_date=expiry,
                strike_price=strike,
                option_type=option_type,
                lot_size=request.lot_size or self.settings.default_lot_size,
                is_weekly=request.is_weekly,
            )
            for expiry in request.expiries
            for strike in request.strikes
            for option_type in request.option_types
        ]
        self.insert_repository.insert_option_contracts(contracts)

        total_inserted = 0
        stock_code = request.breeze_symbol or request.underlying
        for expiry in request.expiries:
            expiry_datetime = datetime.combine(expiry, time.min)
            for strike in request.strikes:
                for option_type in request.option_types:
                    for chunk_start, chunk_end in chunk_datetime_range(
                        request.start,
                        request.end,
                        self.settings.historical_chunk_days,
                    ):
                        try:
                            payload = self.breeze_client.get_historical_options(
                                underlying=stock_code,
                                exchange=request.exchange,
                                expiry_date=expiry_datetime,
                                strike_price=strike,
                                option_type=option_type,
                                from_date=chunk_start,
                                to_date=chunk_end,
                                interval=request.interval,
                            )
                            rows = self.transformer.to_ohlcv_rows(
                                payload,
                                underlying=request.underlying,
                                exchange=request.exchange,
                                expiry_date=expiry,
                                strike_price=strike,
                                option_type=option_type,
                            )
                            inserted = self.insert_repository.insert_options_ohlcv(rows, deduplicate=True)
                            total_inserted += inserted
                            logger.info(
                                "historical_chunk_loaded",
                                extra={
                                "underlying": request.underlying,
                                "breeze_symbol": stock_code,
                                "expiry_date": expiry.isoformat(),
                                "strike_price": strike,
                                    "option_type": option_type,
                                    "from_date": chunk_start.isoformat(),
                                    "to_date": chunk_end.isoformat(),
                                    "fetched_rows": len(rows),
                                    "inserted_rows": inserted,
                                },
                            )
                        except Exception as exc:
                            logger.exception(
                                "historical_chunk_failed",
                                extra={
                                "underlying": request.underlying,
                                "breeze_symbol": stock_code,
                                "expiry_date": expiry.isoformat(),
                                "strike_price": strike,
                                    "option_type": option_type,
                                    "from_date": chunk_start.isoformat(),
                                    "to_date": chunk_end.isoformat(),
                                    "error": str(exc),
                                },
                            )
        return total_inserted
