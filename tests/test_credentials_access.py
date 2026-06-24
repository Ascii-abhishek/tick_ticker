"""Live credential checks.

Run explicitly:

    RUN_CREDENTIAL_TESTS=1 uv run --with pyiceberg pytest tests/test_credentials_access.py -q

These tests intentionally touch external services and never print secret values.
"""

from __future__ import annotations

import os

import pytest

from tick_ticker.config import Settings
from tick_ticker.utils.engines import BreezeClient, D1Client


pytestmark = pytest.mark.credentials


def require_credential_tests_enabled() -> None:
    if os.getenv("RUN_CREDENTIAL_TESTS") != "1":
        pytest.skip("set RUN_CREDENTIAL_TESTS=1 to run live credential checks")


def test_required_env_values_are_set() -> None:
    require_credential_tests_enabled()

    settings = Settings()
    required = {
        "BREEZE_API_KEY": settings.breeze_api_key,
        "BREEZE_API_SECRET": settings.breeze_api_secret,
        "BREEZE_SESSION_TOKEN": settings.breeze_session_token,
        "CLOUDFLARE_ACCOUNT_ID": settings.cloudflare_account_id,
        "CLOUDFLARE_API_TOKEN": settings.cloudflare_api_token,
        "D1_DATABASE_ID": settings.d1_database_id,
        "R2_BUCKET_NAME": settings.r2_bucket_name,
    }

    missing = [key for key, value in required.items() if not value]
    assert not missing, f"missing required env values: {', '.join(missing)}"


def test_r2_data_catalog_access() -> None:
    require_credential_tests_enabled()

    pyiceberg = pytest.importorskip("pyiceberg.catalog.rest")
    settings = Settings()
    warehouse = f"{settings.cloudflare_account_id}_{settings.r2_bucket_name}"
    catalog_uri = f"https://catalog.cloudflarestorage.com/{settings.cloudflare_account_id}/{settings.r2_bucket_name}"

    catalog = pyiceberg.RestCatalog(
        name="cloudflare_r2",
        warehouse=warehouse,
        uri=catalog_uri,
        token=settings.cloudflare_api_token,
    )

    namespaces = catalog.list_namespaces()
    assert isinstance(namespaces, list)


def test_d1_access_and_required_tables() -> None:
    require_credential_tests_enabled()

    d1 = D1Client(Settings())
    try:
        rows = d1.query("SELECT 1 AS ok")
    except Exception as exc:
        pytest.fail(f"D1 access failed; check CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, and D1_DATABASE_ID: {exc}")
    assert rows and rows[0].get("ok") == 1

    tables = d1.query("SELECT name FROM sqlite_master WHERE type = 'table'")
    table_names = {row["name"] for row in tables}
    assert "equity_symbol_reference" in table_names
    assert "market_data_sync_state" in table_names


def test_breeze_session_access() -> None:
    require_credential_tests_enabled()

    try:
        BreezeClient(Settings()).connect()
    except Exception as exc:
        pytest.fail(f"Breeze session failed; refresh BREEZE_SESSION_TOKEN if expired: {exc}")
