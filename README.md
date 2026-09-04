# Tick Ticker

Small Python sync scripts for market data. The cash OHLCV path can now use either Breeze or Upstox:

```text
Cloudflare D1 equity_symbol_reference
  -> ICICI Breeze historical v2 / Upstox History V3
  -> local Parquet: data/cash/YYYY/MM/DD/SYMBOL.parquet
  -> Cloudflare R2 Data Catalog Iceberg table: cash.ohlcv_by_symbol
  -> D1 market_data_sync_state status = completed
```

The script ensures Iceberg namespaces/tables exist in the configured R2 bucket:

- `cash.ohlcv_by_symbol`
- `cash.ohlcv_1s_by_symbol`
- `options.ohlcv`
- `future.ohlcv`

Practical docs live in `docs/`:

- `docs/credentials.md`
- `docs/storage.md`
- `docs/d1.md`
- `docs/cash-sync.md`
- `docs/cash-second-sync.md`

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

Run the same 1-minute cash sync through Upstox:

```bash
uv run sync-cash-upstox-data
```

With no `--nse-symbol`, the Upstox command walks every due cash symbol one by one by default. Use `--nse-symbol` for a single symbol:

```bash
uv run sync-cash-upstox-data --nse-symbol RELIANCE --from-date 2022-01-01 --to-date 2022-01-31
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

Each symbol gets a manifest in `data/state/cash/SYMBOL.json`. If a run fails after some files are written or uploaded to Iceberg, rerun with the same date range and it resumes from the manifest. Iceberg upload snapshots include the local source path, so retries can detect already committed files before appending.

The Upstox script uses the same local files, manifest, Iceberg table, and D1 sync state. It fetches month-bounded History V3 chunks and splits them into the same daily Parquet layout. Equity rows use `isin` as `NSE_EQ|<isin>`; index rows can store the full Upstox instrument key in `isin`, for example `NSE_INDEX|Nifty 50` for `NIFTY`.

Backfill the symbol-optimized query table from local Parquet:

```bash
uv run backfill-cash-symbol-iceberg --workers 4 --batch-size 500
```

Reconcile local JSON manifests and D1 state from local Parquet coverage:

```bash
uv run reconcile-cash-sync-state
```

## Cash 1-Second Sync

The 1-second path uses a separate table and local staging layout:

```bash
uv run sync-cash-second-data --ensure-table-only
scripts/fetch_bank_cash_1s_from_2026.sh
```

It defaults to `2026-01-01..today`, stages final daily files under `data/cash_1s/by_symbol`, and uploads to `cash.ohlcv_1s_by_symbol`. See `docs/cash-second-sync.md`.
