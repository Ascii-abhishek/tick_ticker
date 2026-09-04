# Credentials

Required `.env` keys:

- `BREEZE_API_KEY`: Breeze app key.
- `BREEZE_API_SECRET`: Breeze secret key.
- `BREEZE_SESSION_TOKEN`: Breeze API session token from login flow.
- `UPSTOX_ACCESS_TOKEN`: Upstox API access token for History V3.
- `CLOUDFLARE_ACCOUNT_ID`: Cloudflare account id.
- `CLOUDFLARE_API_TOKEN`: Cloudflare API token with D1 query access.
- `D1_DATABASE_ID`: D1 database id.
- `R2_ACCESS_KEY_ID`: R2 S3-compatible access key.
- `R2_SECRET_ACCESS_KEY`: R2 S3-compatible secret key.
- `R2_S3_ENDPOINT`: R2 S3 endpoint, usually `https://<account_id>.r2.cloudflarestorage.com`.
- `R2_BUCKET_NAME`: Bucket name. Default: `market-data`.
- `R2_DATA_CATALOG_URI`: Optional R2 Data Catalog URI override. Defaults to Cloudflare's bucket URI.
- `R2_DATA_CATALOG_WAREHOUSE`: Optional warehouse override. Defaults to `<account_id>_<bucket>`.

Operational settings:

- `DATA_DIR`: Local staging directory. Default: `data`.
- `DEFAULT_INTERVAL`: Breeze candle interval. Default: `1minute`.
- `BREEZE_MIN_REQUEST_INTERVAL_SECONDS`: Process-wide minimum delay between Breeze API calls. Default: `0.65`.
- `BREEZE_MAX_REQUESTS_PER_RUN`: Historical Breeze requests the sync may reserve in one run. Default: `4500`; use `0` to disable.
- `UPSTOX_BASE_URL`: Upstox API base URL. Default: `https://api.upstox.com`.
- `UPSTOX_MIN_REQUEST_INTERVAL_SECONDS`: Process-wide minimum delay between Upstox API calls. Default: `1.0`.
- `UPSTOX_MAX_REQUESTS_PER_RUN`: Historical Upstox requests the sync may reserve in one run. Default: `1800`; use `0` to disable.
- `UPSTOX_REQUEST_RETRY_ATTEMPTS`: Upstox request retry attempts. Default: `3`.
- `UPSTOX_REQUEST_RETRY_BASE_DELAY_SECONDS`: Upstox retry base delay. Default: `1.0`.
- `CASH_EXCHANGE_CODE`: Cash exchange. Default: `NSE`.
- `CASH_PRODUCT_TYPE`: Breeze product type. Default: `cash`.
- `CASH_HISTORY_CHUNK_DAYS`: Fetch chunk size. Default: `1`.
- `CASH_SYMBOL_WORKERS`: Concurrent symbols for `sync-cash-data --all`. Default: `1`.
- `CASH_DOWNLOAD_WORKERS`: Concurrent Breeze download workers. Default: `1`.
- `CASH_UPLOAD_WORKERS`: Concurrent Iceberg upload workers. Default: `1`.
- `CASH_UPLOAD_BATCH_SIZE`: Local parquet files per Iceberg upload commit. Default: `25`.
- `CASH_SYMBOL_UPLOAD_WORKERS`: Concurrent local preparation workers for `backfill-cash-symbol-iceberg`. Default: `4`.
- `CASH_SYMBOL_UPLOAD_BATCH_SIZE`: Local parquet files per symbol/year backfill append. Default: `500`.
- `CASH_UPLOAD_RETRY_ATTEMPTS`: Iceberg upload retry attempts. Default: `3`.
- `CASH_UPLOAD_RETRY_BASE_DELAY_SECONDS`: Base delay between Iceberg upload retries. Default: `1.0`.
- `CASH_SYNC_MAX_DAYS_PER_RUN`: Safety limit for one run. Default: `30`.
- `CASH_SYNC_FROM_DATE`: Optional default start date.
- `CASH_SYNC_TO_DATE`: Optional default end date.
- `ICEBERG_CASH_NAMESPACE`: Default: `cash`.
- `ICEBERG_CASH_TABLE`: Default: `ohlcv_by_symbol`.
- `ICEBERG_CASH_SECOND_NAMESPACE`: Default: `cash`.
- `ICEBERG_CASH_SECOND_TABLE`: Default: `ohlcv_1s_by_symbol`.
- `CASH_SECOND_INTERVAL`: Default: `1second`.
- `CASH_SECOND_HISTORY_WINDOW_SECONDS`: Intraday Breeze request window. Default: `900`.
- `CASH_SECOND_MAX_CANDLES_PER_REQUEST`: Safety cap for one Breeze historical response. Default: `1000`.
- `CASH_SECOND_MARKET_OPEN_TIME`: Default: `09:15:00`.
- `CASH_SECOND_MARKET_CLOSE_TIME`: Default: `15:30:00`.
- `CASH_SECOND_SKIP_WEEKENDS`: Default: `true`.
- `CASH_SECOND_USE_MINUTE_EMPTY_DAYS`: Reuse empty 1-minute days to avoid 1-second Breeze calls. Default: `true`.
- `CASH_SECOND_SYMBOL_WORKERS`: Concurrent 1-second symbols. Default: `1`.
- `CASH_SECOND_DOWNLOAD_WORKERS`: Concurrent 1-second windows inside one symbol. Default: `1`.
- `CASH_SECOND_UPLOAD_WORKERS`: Concurrent 1-second Iceberg upload workers. Default: `1`.
- `CASH_SECOND_UPLOAD_BATCH_SIZE`: Local 1-second daily files per Iceberg upload commit. Default: `10`.
- `CASH_SECOND_SYNC_MAX_DAYS_PER_RUN`: Direct CLI safety limit. Default: `7`.
- `CASH_SECOND_SYNC_FROM_DATE`: Default: `2026-01-01`.
- `CASH_SECOND_SYNC_TO_DATE`: Optional default end date.
- `ICEBERG_OPTIONS_NAMESPACE`: Default: `options`.
- `ICEBERG_FUTURE_NAMESPACE`: Default: `future`.

Storage rules:

- Keep real values only in `.env`.
- Commit `.env.example`, not `.env`.
- Do not put credentials in notebooks, docs, or shell history.

Validate credentials:

```bash
RUN_CREDENTIAL_TESTS=1 uv run --with pyiceberg pytest tests/test_credentials_access.py -q
```

Checks covered:

- Required env values are set.
- R2 Data Catalog can list namespaces.
- D1 can query and has required tables.
- Breeze session can be generated.
- Upstox historical candles can be fetched.
