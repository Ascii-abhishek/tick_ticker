# Credentials

Required `.env` keys:

- `BREEZE_API_KEY`: Breeze app key.
- `BREEZE_API_SECRET`: Breeze secret key.
- `BREEZE_SESSION_TOKEN`: Breeze API session token from login flow.
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
- `CASH_EXCHANGE_CODE`: Cash exchange. Default: `NSE`.
- `CASH_PRODUCT_TYPE`: Breeze product type. Default: `cash`.
- `CASH_HISTORY_CHUNK_DAYS`: Fetch chunk size. Default: `1`.
- `CASH_SYNC_MAX_DAYS_PER_RUN`: Safety limit for one run. Default: `30`.
- `CASH_SYNC_FROM_DATE`: Optional default start date.
- `CASH_SYNC_TO_DATE`: Optional default end date.
- `ICEBERG_CASH_NAMESPACE`: Default: `cash`.
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
