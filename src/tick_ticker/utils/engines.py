"""Factory functions for external service clients."""

from __future__ import annotations

import threading
import time
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

import boto3
import httpx
from botocore.client import BaseClient

from tick_ticker.config import Settings, get_settings
from tick_ticker.utils.retry import retry


class BreezeClient:
    """Thin, rate-limited wrapper around BreezeConnect."""

    _rate_limit_lock = threading.Lock()
    _last_request_at = 0.0

    def __init__(self, settings: Settings | None = None) -> None:
        from breeze_connect import BreezeConnect

        self.settings = settings or get_settings()
        self._client = BreezeConnect(api_key=self.settings.breeze_api_key)
        self._connected = False
        self._connect_lock = threading.Lock()

    def connect(self) -> None:
        """Generate a Breeze session once."""

        if self._connected:
            return
        with self._connect_lock:
            if self._connected:
                return
            if not all([self.settings.breeze_api_key, self.settings.breeze_api_secret, self.settings.breeze_session_token]):
                raise ValueError("Set BREEZE_API_KEY, BREEZE_API_SECRET, and BREEZE_SESSION_TOKEN")
            self._rate_limit()
            self._client.generate_session(
                api_secret=self.settings.breeze_api_secret,
                session_token=self.settings.breeze_session_token,
            )
            self._connected = True

    def get_historical_cash(
        self,
        *,
        stock_code: str,
        from_date: str,
        to_date: str,
        interval: str,
        exchange_code: str,
        product_type: str,
    ) -> Mapping[str, Any]:
        """Fetch cash historical candles from Breeze historical v2."""

        return self._call_with_retry(
            self._client.get_historical_data_v2,
            interval=interval,
            from_date=from_date,
            to_date=to_date,
            stock_code=stock_code,
            exchange_code=exchange_code,
            product_type=product_type,
        )

    def _call_with_retry(self, func: Any, **kwargs: Any) -> Mapping[str, Any]:
        decorated = retry(
            attempts=self.settings.breeze_request_retry_attempts,
            base_delay_seconds=self.settings.breeze_request_retry_base_delay_seconds,
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
        with self._rate_limit_lock:
            elapsed = time.monotonic() - self.__class__._last_request_at
            wait_seconds = self.settings.breeze_min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self.__class__._last_request_at = time.monotonic()


class UpstoxClient:
    """Thin, rate-limited wrapper around Upstox historical candles."""

    _rate_limit_lock = threading.Lock()
    _last_request_at = 0.0

    def __init__(self, settings: Settings | None = None, *, timeout_seconds: float = 30.0) -> None:
        self.settings = settings or get_settings()
        self.timeout_seconds = timeout_seconds
        if not self.settings.upstox_access_token:
            raise ValueError("Set UPSTOX_ACCESS_TOKEN")
        self._base_url = self.settings.upstox_base_url.rstrip("/")

    def get_historical_cash(
        self,
        *,
        instrument_key: str,
        from_date: date | str,
        to_date: date | str,
        unit: str = "minutes",
        interval: str = "1",
    ) -> Mapping[str, Any]:
        """Fetch historical cash candles from Upstox History V3."""

        decorated = retry(
            attempts=self.settings.upstox_request_retry_attempts,
            base_delay_seconds=self.settings.upstox_request_retry_base_delay_seconds,
        )(self._get_historical_cash)
        return decorated(
            instrument_key=instrument_key,
            from_date=_date_segment(from_date),
            to_date=_date_segment(to_date),
            unit=unit,
            interval=interval,
        )

    def _get_historical_cash(
        self,
        *,
        instrument_key: str,
        from_date: str,
        to_date: str,
        unit: str,
        interval: str,
    ) -> Mapping[str, Any]:
        encoded_instrument_key = quote(instrument_key, safe="")
        url = f"{self._base_url}/v3/historical-candle/{encoded_instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
        self._rate_limit()
        response = httpx.get(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.settings.upstox_access_token}",
            },
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"Upstox HTTP {response.status_code}: {response.text}")

        body = response.json()
        if not isinstance(body, Mapping):
            raise TypeError(f"unexpected Upstox response type: {type(body)!r}")
        if body.get("status") not in (None, "success"):
            raise RuntimeError(f"Upstox request failed: {body}")
        return body

    def _rate_limit(self) -> None:
        with self._rate_limit_lock:
            elapsed = time.monotonic() - self.__class__._last_request_at
            wait_seconds = self.settings.upstox_min_request_interval_seconds - elapsed
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self.__class__._last_request_at = time.monotonic()


class D1Client:
    """Cloudflare D1 SQL API client."""

    def __init__(self, settings: Settings | None = None, *, timeout_seconds: float = 30.0) -> None:
        self.settings = settings or get_settings()
        self.timeout_seconds = timeout_seconds
        if not all([self.settings.cloudflare_account_id, self.settings.cloudflare_api_token, self.settings.d1_database_id]):
            raise ValueError("Set CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, and D1_DATABASE_ID")
        self._url = (
            "https://api.cloudflare.com/client/v4/accounts/"
            f"{self.settings.cloudflare_account_id}/d1/database/{self.settings.d1_database_id}/query"
        )

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return rows."""

        payload: dict[str, Any] = {"sql": sql}
        if params is not None:
            payload["params"] = list(params)

        response = httpx.post(
            self._url,
            headers={"Authorization": f"Bearer {self.settings.cloudflare_api_token}"},
            json=payload,
            timeout=self.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(f"D1 HTTP {response.status_code}: {response.text}")

        body = response.json()
        if not body.get("success", False):
            raise RuntimeError(f"D1 query failed: {body}")

        result = body.get("result") or []
        if not result:
            return []
        return list((result[0] or {}).get("results") or [])

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Execute SQL when returned rows are not needed."""

        self.query(sql, params)


class R2Client:
    """Cloudflare R2 S3-compatible client."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        if not all(
            [
                self.settings.r2_access_key_id,
                self.settings.r2_secret_access_key,
                self.settings.r2_s3_endpoint,
                self.settings.r2_bucket_name,
            ]
        ):
            raise ValueError("Set R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_S3_ENDPOINT, and R2_BUCKET_NAME")
        self._client: BaseClient = boto3.client(
            "s3",
            endpoint_url=self.settings.r2_s3_endpoint,
            aws_access_key_id=self.settings.r2_access_key_id,
            aws_secret_access_key=self.settings.r2_secret_access_key,
            region_name="auto",
        )

    def upload_file(self, local_path: str | Path, object_key: str) -> None:
        """Upload one file to the configured bucket."""

        self._client.upload_file(str(local_path), self.settings.r2_bucket_name, object_key)


def create_breeze_client(settings: Settings | None = None) -> BreezeClient:
    return BreezeClient(settings)


def create_upstox_client(settings: Settings | None = None) -> UpstoxClient:
    return UpstoxClient(settings)


def create_d1_client(settings: Settings | None = None) -> D1Client:
    return D1Client(settings)


def create_r2_client(settings: Settings | None = None) -> R2Client:
    return R2Client(settings)


def _date_segment(value: date | str) -> str:
    return value.isoformat() if isinstance(value, date) else value
