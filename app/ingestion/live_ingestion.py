"""Minute-by-minute ingestion loop."""

from __future__ import annotations

import signal
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from clickhouse_connect.driver.client import Client

from app.config.constants import DEFAULT_EXCHANGE, DEFAULT_INTERVAL, OPTION_TYPES
from app.config.settings import Settings, get_settings
from app.db.repository.insert import InsertRepository
from app.ingestion.breeze_client import BreezeClient
from app.ingestion.transformer import BreezeTransformer
from app.utils.datetime_utils import seconds_until_next_minute
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class LiveIngestionRequest:
    underlying: str
    expiry_date: date
    strikes: tuple[float, ...]
    exchange: str = DEFAULT_EXCHANGE
    interval: str = DEFAULT_INTERVAL
    option_types: tuple[str, ...] = OPTION_TYPES
    lookback_minutes: int = 3
    run_once: bool = False


class LiveIngestionRunner:
    """Poll Breeze once per minute and append new candles."""

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
        self._stop_requested = False

    def run(self, request: LiveIngestionRequest) -> None:
        """Run until interrupted, unless request.run_once is true."""

        self._install_signal_handlers()
        while not self._stop_requested:
            inserted = self.fetch_once(request)
            logger.info("live_ingestion_cycle_finished", extra={"inserted_rows": inserted})
            if request.run_once:
                return
            time_module.sleep(seconds_until_next_minute())

    def fetch_once(self, request: LiveIngestionRequest) -> int:
        """Fetch the most recent minute window once."""

        end = datetime.now().replace(second=0, microsecond=0)
        start = end - timedelta(minutes=request.lookback_minutes)
        expiry_datetime = datetime.combine(request.expiry_date, time.min)
        total_inserted = 0

        for strike in request.strikes:
            for option_type in request.option_types:
                try:
                    payload = self.breeze_client.get_historical_options(
                        underlying=request.underlying,
                        exchange=request.exchange,
                        expiry_date=expiry_datetime,
                        strike_price=strike,
                        option_type=option_type,
                        from_date=start,
                        to_date=end,
                        interval=request.interval,
                    )
                    rows = self.transformer.to_ohlcv_rows(
                        payload,
                        underlying=request.underlying,
                        exchange=request.exchange,
                        expiry_date=request.expiry_date,
                        strike_price=strike,
                        option_type=option_type,
                    )
                    total_inserted += self.insert_repository.insert_options_ohlcv(rows, deduplicate=True)
                except Exception as exc:
                    logger.exception(
                        "live_ingestion_contract_failed",
                        extra={
                            "underlying": request.underlying,
                            "expiry_date": request.expiry_date.isoformat(),
                            "strike_price": strike,
                            "option_type": option_type,
                            "error": str(exc),
                        },
                    )
        return total_inserted

    def _install_signal_handlers(self) -> None:
        def handle_stop(_signum: int, _frame: object) -> None:
            self._stop_requested = True
            logger.info("live_ingestion_stop_requested")

        signal.signal(signal.SIGINT, handle_stop)
        signal.signal(signal.SIGTERM, handle_stop)

