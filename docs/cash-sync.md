# Cash Sync

Run next pending cash symbol:

```bash
uv run sync-cash-data --from-date 2026-01-01 --to-date 2026-01-31
```

Run every pending or stale cash symbol:

```bash
uv run sync-cash-data --all
```

Run recent catch-up across many symbols:

```bash
uv run sync-cash-data --all --from-date 2026-07-01 --to-date 2026-07-05 --symbol-workers 20 --download-workers 1 --upload-workers 1
```

`--symbol-workers` parallelizes symbols. `--download-workers` parallelizes date chunks inside one symbol, so keep it low when symbol workers are high. Iceberg appends target `cash.ohlcv_by_symbol` by default and are serialized inside the process to avoid snapshot commit conflicts while downloads continue in parallel. When `--all` finds a symbol whose date range is larger than `CASH_SYNC_MAX_DAYS_PER_RUN`, it logs a skip and continues with the next symbol. Pass `--allow-large-range` only when you intentionally want those large backfills.

Run one NSE symbol:

```bash
uv run sync-cash-data --nse-symbol RELIANCE
```

Fetch only:

```bash
uv run sync-cash-data --fetch-only --from-date 2026-01-01 --to-date 2026-01-31
```

Local download only:

```bash
uv run sync-cash-data --local-only --nse-symbol RELIANCE
```

Upload existing local files to Iceberg only:

```bash
uv run sync-cash-data --upload-only --from-date 2026-01-01 --to-date 2026-01-31
```

Backfill the symbol-optimized Iceberg query table from local Parquet:

```bash
uv run backfill-cash-symbol-iceberg --workers 4 --batch-size 500
```

Reconcile local JSON manifests and D1 from existing local Parquet coverage:

```bash
uv run reconcile-cash-sync-state
```

Allow a large range:

```bash
uv run sync-cash-data --from-date 2020-01-01 --to-date 2026-06-24 --allow-large-range
```

What the script does:

- Ensures `market_data_sync_state` exists.
- If `--nse-symbol` is passed, reads that symbol from `equity_symbol_reference`.
- If `--all` is passed, reads every due cash symbol from D1 and processes them with up to `CASH_SYMBOL_WORKERS` workers.
- If neither `--nse-symbol` nor `--all` is passed, reads first due cash symbol from D1.
- A completed symbol is due again when its `to_date` is older than the requested `--to-date`, or older than today when `--to-date` is omitted.
- Uses `--from-date` when passed.
- If no `--from-date`, resumes from completed `to_date + 1`.
- If no previous completed state exists, starts from the later of `listing_date` and the provider-supported history start date.
- Uses `--to-date` when passed.
- If no `--to-date`, uses today.
- Uses `breeze_code` only for the Breeze API request.
- Stores local Parquet with `nse_symbol`.
- Ensures Iceberg namespaces/tables exist: `cash.ohlcv_by_symbol`, `options.ohlcv`, `future.ohlcv`.
- Appends cash files to `cash.ohlcv_by_symbol` unless `--local-only` or `--fetch-only` is passed.
- Marks `market_data_sync_state.status = 'completed'` only after Iceberg upload.

Resume behavior:

- Manifest path: `data/state/cash/NSE_SYMBOL.json`.
- Already fetched files are skipped.
- Already uploaded files are skipped.
- Manifest `from_date`, `to_date`, `fetched_files`, and `uploaded_files` describe the active/resumable run.
- Manifest `coverage_from_date`, `coverage_to_date`, `coverage_file_count`, and `coverage_row_count` describe durable local/uploaded coverage and are preserved when a new incremental run starts.
- Manifest `last_fetch` and `last_upload` keep the latest completed fetch/upload summary visible even after the active run range changes.
- If a run is interrupted after an Iceberg commit but before the manifest is saved, retrying checks committed snapshot source-path metadata before appending duplicates.
- Non-empty local parquet files are uploaded in batches controlled by `CASH_UPLOAD_BATCH_SIZE`.
- After a completed upload, a later date range resets the manifest for the new incremental run.
- Failed symbols stay retryable.

Safety:

- Default max range is `CASH_SYNC_MAX_DAYS_PER_RUN`.
- `--all` skips oversized ranges and continues unless `--allow-large-range` is passed.
- Breeze cash history starts on `2016-01-01`, so default backfills do not request earlier dates.
- Use `--allow-large-range` only for intentional backfills.
- Breeze lists a 100 calls/minute and 5000 calls/day API limit; historical v2 returns at most 1000 candles per request.
- Breeze rate limiting is process-wide and handled by `BREEZE_MIN_REQUEST_INTERVAL_SECONDS`. The default `0.65` seconds stays under 100 calls/minute.
- `BREEZE_MAX_REQUESTS_PER_RUN` defaults to `4500` historical requests to leave room for session/login and other API calls. Set `--breeze-max-requests 0` only when you intentionally manage the daily limit outside this script.
- Use `--symbol-workers 20` or `--symbol-workers 30` for short recent ranges. Avoid combining high symbol workers with high `--download-workers`.
- Use `--download-workers` and `--upload-workers` carefully; both default to `1`.
