"""Breeze API wrapper."""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from breeze_connect import BreezeConnect

from app.config.constants import DEFAULT_INTERVAL, DEFAULT_PRODUCT_TYPE, OPTION_TYPE_TO_RIGHT
from app.config.settings import Settings, get_settings
from app.utils.datetime_utils import breeze_datetime
from app.utils.logger import get_logger
from app.utils.retry import retry

logger = get_logger(__name__)


class BreezeClient:
    """Thin, retrying wrapper around BreezeConnect."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._client = BreezeConnect(api_key=self.settings.breeze_api_key)
        self._last_request_at = 0.0
        self._lock = threading.Lock()
        self._connected = False

    @property
    def raw_client(self) -> BreezeConnect:
        return self._client

    def connect(self) -> None:
        """Generate Breeze session from configured credentials."""

        if self._connected:
            return
        if not all(
            [
                self.settings.breeze_api_key,
                self.settings.breeze_api_secret,
                self.settings.breeze_session_token,
            ]
        ):
            raise ValueError("Breeze credentials are incomplete; set BREEZE_API_KEY, BREEZE_API_SECRET, BREEZE_SESSION_TOKEN")
        self._client.generate_session(
            api_secret=self.settings.breeze_api_secret,
            session_token=self.settings.breeze_session_token,
        )
        self._connected = True
        logger.info("breeze_session_generated")

    def get_historical_options(
        self,
        *,
        underlying: str,
        exchange: str,
        expiry_date: datetime,
        strike_price: float,
        option_type: str,
        from_date: datetime,
        to_date: datetime,
        interval: str = DEFAULT_INTERVAL,
    ) -> Mapping[str, Any]:
        """Fetch historical option candles."""

        right = OPTION_TYPE_TO_RIGHT[option_type.upper()]
        return self._call_with_retry(
            self._client.get_historical_data_v2,
            interval=interval,
            from_date=breeze_datetime(from_date),
            to_date=breeze_datetime(to_date),
            stock_code=underlying,
            exchange_code=exchange,
            product_type=DEFAULT_PRODUCT_TYPE,
            expiry_date=breeze_datetime(expiry_date),
            right=right,
            strike_price=str(strike_price),
        )

    def get_option_chain_quotes(
        self,
        *,
        underlying: str,
        exchange: str,
        expiry_date: datetime,
        option_type: str | None = None,
        strike_price: float | None = None,
    ) -> Mapping[str, Any]:
        """Fetch option-chain quotes."""

        kwargs: dict[str, Any] = {
            "stock_code": underlying,
            "exchange_code": exchange,
            "expiry_date": breeze_datetime(expiry_date),
            "product_type": DEFAULT_PRODUCT_TYPE,
        }
        if option_type:
            kwargs["right"] = OPTION_TYPE_TO_RIGHT[option_type.upper()]
        if strike_price is not None:
            kwargs["strike_price"] = str(strike_price)

        return self._call_with_retry(self._client.get_option_chain_quotes, **kwargs)

    def _call_with_retry(self, func: Any, **kwargs: Any) -> Mapping[str, Any]:
        decorated = retry(
            attempts=self.settings.request_retry_attempts,
            base_delay_seconds=self.settings.request_retry_base_delay_seconds,
        )(self._call)
        return decorated(func, **kwargs)

    def _call(self, func: Any, **kwargs: Any) -> Mapping[str, Any]:
        self.connect()
        self._rate_limit()
        response = func(**kwargs)
        if not isinstance(response, Mapping):
            raise TypeError(f"unexpected Breeze response type: {type(response)!r}")

        if response.get("Status") not in (None, 200, "200") and not response.get("Success"):
            raise RuntimeError(f"Breeze request failed: {response}")
        if response.get("Error"):
            raise RuntimeError(f"Breeze returned error: {response['Error']}")
        return response

    def _rate_limit(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_request_at
            wait_seconds = self.settings.breeze_min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request_at = time.monotonic()

