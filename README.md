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

The knowledge base starts at [`docs/README.md`](docs/README.md). It has market docs (NIFTY 50 membership, corporate actions, synthetic volume, provider quirks) and engineering docs (pipeline, D1 schema, audit, cron, backfill, decisions). The summary of the 2026-09 NIFTY volume work is [`docs/engineering/work-summary-2026-09.md`](docs/engineering/work-summary-2026-09.md).

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

With no symbol selection, the Upstox command walks every due cash symbol one by one by default. Use `--nse-symbol` for a single symbol:

```bash
uv run sync-cash-upstox-data --nse-symbol RELIANCE --from-date 2022-01-01 --to-date 2022-01-31
```

Select several symbols with `--nse-symbols ACC RELIANCE 'M&M'` (commas and
repeated `--nse-symbols` are also supported), or use the saved Nifty priority list:

```bash
uv run sync-cash-upstox-data --symbols-file scripts/nifty_priority_symbols.txt \
  --skip-missing-symbols --from-date 2016-01-01 --fetch-only \
  --symbol-workers 1 --upstox-max-requests 5000
```

For a single ready-to-run list, `scripts/nifty_priority_symbols_current.txt`
contains the 77 available securities in the original priority order, with the
four current names and comments for the three unavailable securities. The
original 80-name input is preserved in `scripts/nifty_priority_symbols.txt`.
A bounded live test, saving separately and making no uploads:

```bash
DATA_DIR=data/upstox_smoke_test uv run sync-cash-upstox-data \
  --symbols-file scripts/nifty_priority_symbols_current.txt \
  --from-date 2026-09-01 --to-date 2026-09-04 --fetch-only \
  --no-ensure-sync-table --upstox-max-requests 100
```

Only the selected symbols are processed. Duplicates are removed in first-seen
order; one symbol worker completes each symbol before starting the next. Files
accept whitespace/comma separators and `#` comments. `--nse-symbols` may be
combined with `--symbols-file` (CLI names first), but explicit selections cannot
be combined with `--all`. Missing D1 symbols fail before downloads unless
`--skip-missing-symbols` is passed, which logs the missing names. Historical names
are not automatically substituted with a different security.

As checked on September 6, 2026, the original priority list contains four renamed
symbols: `TATAMOTORS → TMPV`, `INFRATEL → INDUSTOWER`,
`IBULHSGFIN → SAMMAANCAP`, and `LTIM → LTM`. Fetch these explicitly under their
current D1 names:

```bash
uv run sync-cash-upstox-data --nse-symbols TMPV INDUSTOWER SAMMAANCAP LTM \
  --from-date 2016-01-01 --fetch-only --symbol-workers 1
```

`CAIRN`, `HDFC`, and `TATAMTRDVR` are also absent from the current D1 reference.
Live Upstox probes rejected the former HDFC and Tata Motors DVR instrument keys
with `UDAPI100011` (invalid instrument key); their histories are not substituted
with the acquiring companies' candles.

The script stores **1-minute** candles. [Upstox History V3 documentation](https://upstox.com/developer/api-documentation/v3/get-historical-candle-data/)
specifies minute/hour history from January 2022; daily/weekly/monthly history is
available from January 2000. Earlier requested dates are clamped to January 1,
2022, and every start date also respects the symbol's listing date. Daily candles
are not interchangeable with the minute data stored here. `--fetch-only` saves
real Parquet data and manifests without uploading to Iceberg or marking D1 sync
completion. Omit it for the normal upload flow. The request budget is a run cap,
separate from the API rate limiter; the default one-second request spacing stays
within Upstox's documented 2,000 requests per 30 minutes.

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

## NIFTY 50 Reference Data and Synthetic Volume

```bash
uv run load-reference-data --apply-migration      # reference/*.csv -> D1 (validated)
uv run repair-cash-data --from-date 2022-01-01    # audit gaps against the trading calendar
uv run generate-index-volume --from-date 2022-01-01 --publish
scripts/daily_cash_sync.sh                        # daily cron: sync, repair, volume
```

See `docs/engineering/pipeline-overview.md`.

## Cash 1-Second Sync

The 1-second path uses a separate table and local staging layout:

```bash
uv run sync-cash-second-data --ensure-table-only
scripts/fetch_bank_cash_1s_from_2026.sh
```

It defaults to `2026-01-01..today`, stages final daily files under `data/cash_1s/by_symbol`, and uploads to `cash.ohlcv_1s_by_symbol`. See `docs/cash-second-sync.md`.
