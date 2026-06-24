# Tick Ticker

Small Python sync scripts for market data. The first implemented path is cash OHLCV:

```text
Cloudflare D1 equity_symbol_reference
  -> ICICI Breeze historical v2
  -> local Parquet: data/cash/YYYY/MM/DD/SYMBOL.parquet
  -> Cloudflare R2 Data Catalog Iceberg table: cash.ohlcv
  -> D1 market_data_sync_state status = completed
```

The script ensures three Iceberg namespaces/tables exist in the configured R2 bucket:

- `cash.ohlcv`
- `options.ohlcv`
- `future.ohlcv`

Practical docs live in `docs/`:

- `docs/credentials.md`
- `docs/storage.md`
- `docs/d1.md`
- `docs/cash-sync.md`

## Setup

```bash
uv sync --dev
cp .env.example .env
```

Fill the Breeze, D1, and R2 values in `.env`. The default bucket is `market-data`, configurable via `R2_BUCKET_NAME`. Enable R2 Data Catalog on that bucket before upload runs.

## D1 Reference Table

The existing `equity_symbol_reference` table is expected to include:

```text
nse_symbol
breeze_code
nse_company_name
listing_date
isin
```

Apply the sync-state migration once:

```bash
wrangler d1 execute "$D1_DATABASE_ID" --remote --file migrations/d1/001_market_data_sync_state.sql
```

The sync script also runs idempotent `CREATE TABLE` statements by default, so local/dev recovery is painless if the migration has not been applied yet.

## Cash Sync

Run the next pending symbol from D1:

```bash
uv run sync-cash-data --from-date 2026-01-01 --to-date 2026-01-31
```

For safety, ranges longer than `CASH_SYNC_MAX_DAYS_PER_RUN` are rejected unless explicitly allowed:

```bash
uv run sync-cash-data --from-date 2020-01-01 --to-date 2026-06-24 --allow-large-range
```

Resumability:

```bash
uv run sync-cash-data --fetch-only --from-date 2026-01-01 --to-date 2026-01-31
uv run sync-cash-data --upload-only --from-date 2026-01-01 --to-date 2026-01-31
```

Each symbol gets a manifest in `data/state/cash/SYMBOL.json`. If a run fails after some files are written or appended to Iceberg, rerun with the same date range and it resumes from the manifest.
