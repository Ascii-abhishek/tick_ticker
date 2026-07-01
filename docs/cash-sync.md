# Cash Sync

Run next pending cash symbol:

```bash
uv run sync-cash-data --from-date 2026-01-01 --to-date 2026-01-31
```

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

Allow a large range:

```bash
uv run sync-cash-data --from-date 2020-01-01 --to-date 2026-06-24 --allow-large-range
```

What the script does:

- Ensures `market_data_sync_state` exists.
- If `--nse-symbol` is passed, reads that symbol from `equity_symbol_reference`.
- If `--nse-symbol` is not passed, reads first due cash symbol from D1.
- Uses `--from-date` when passed.
- If no `--from-date`, resumes from completed `to_date + 1`.
- If no previous completed state exists, starts from the later of `listing_date` and the provider-supported history start date.
- Uses `--to-date` when passed.
- If no `--to-date`, uses today.
- Uses `breeze_code` only for the Breeze API request.
- Stores local Parquet with `nse_symbol`.
- Ensures Iceberg namespaces/tables exist: `cash.ohlcv`, `options.ohlcv`, `future.ohlcv`.
- Appends cash files to `cash.ohlcv` unless `--local-only` or `--fetch-only` is passed.
- Marks `market_data_sync_state.status = 'completed'` only after Iceberg upload.

Resume behavior:

- Manifest path: `data/state/cash/NSE_SYMBOL.json`.
- Already fetched files are skipped.
- Already uploaded files are skipped.
- If a run is interrupted after an Iceberg commit but before the manifest is saved, retrying checks committed snapshot source-path metadata before appending duplicates.
- Non-empty local parquet files are uploaded in batches controlled by `CASH_UPLOAD_BATCH_SIZE`.
- After a completed upload, a later date range resets the manifest for the new incremental run.
- Failed symbols stay retryable.

Safety:

- Default max range is `CASH_SYNC_MAX_DAYS_PER_RUN`.
- Breeze cash history starts on `2016-01-01`, so default backfills do not request earlier dates.
- Use `--allow-large-range` only for intentional backfills.
- Breeze rate limit is handled by `BREEZE_MIN_REQUEST_INTERVAL_SECONDS`.
- Use `--download-workers` and `--upload-workers` carefully; both default to `1`.
