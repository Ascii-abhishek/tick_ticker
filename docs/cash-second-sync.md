# Cash 1-Second Sync

The 1-second cash path is separate from the 1-minute table and writes to:

```text
data/cash_1s/by_symbol/NSE_SYMBOL/YYYY/MM/DD.parquet
data/state/cash_1s/NSE_SYMBOL.json
cash.ohlcv_1s_by_symbol
```

Create or verify the Iceberg table:

```bash
uv run sync-cash-second-data --ensure-table-only
```

Run one symbol from `2026-01-01` to today:

```bash
uv run sync-cash-second-data --nse-symbol HDFCBANK --allow-large-range
```

Fetch only, then upload later:

```bash
uv run sync-cash-second-data --fetch-only --nse-symbol HDFCBANK --from-date 2026-01-01 --to-date 2026-01-31 --allow-large-range
uv run sync-cash-second-data --upload-only --nse-symbol HDFCBANK --from-date 2026-01-01 --to-date 2026-01-31 --allow-large-range
```

Run the selected bank-symbol backfill:

```bash
scripts/fetch_bank_cash_1s_from_2026.sh
```

Useful backfill controls:

- `BASE_FROM_DATE`: Default `2026-01-01`.
- `TO_DATE`: Default today.
- `DAILY_BREEZE_REQUEST_BUDGET`: Default `4500`.
- `BREEZE_REQUEST_BUDGET_MODE`: Default `cycle`. In `cycle` mode, `4500` is a per-cycle planning batch size and the script keeps going until all ranges finish or the session key expires. Use `run` to restore the old hard total-run budget behavior.
- `BREEZE_REQUESTS_ALREADY_USED`: Subtract calls already spent today.
- `PARALLEL_FETCH_JOBS`: Default `2`; each fetch process uses a slower per-process interval so aggregate calls stay under the Breeze per-minute limit.
- `DRY_RUN=1`: Plan only.
- `UPLOAD_AFTER_FETCH=0`: Leave final local Parquet files without uploading.
- `CASH_SECOND_D1_FALLBACK=1`: Allow D1 lookup only if the local 1-minute manifest does not have a Breeze code.

Request strategy:

- Breeze historical v2 supports `1second`, but response size is capped, so full trading days are split into intraday windows.
- Defaults use 900-second windows. A normal full NSE session is planned as 26 requests per symbol/day when including the closing second.
- The fetcher skips weekends.
- If a 1-minute local file exists and has zero rows for a symbol/day, the 1-second fetcher writes an empty 1-second daily file without calling Breeze.
- Temporary window files are saved under `data/cash_1s/_windows` and are merged into one sorted/deduped daily file after every window for that day is present.
- The bank launcher treats rate-limit and network failures as soft failures, waits, and replans. Expired Breeze sessions and disk-space failures stop the run.

Iceberg layout:

- Table: `cash.ohlcv_1s_by_symbol`.
- Columns match the 1-minute OHLCV schema.
- Partitioning: `nse_symbol`, `month(trade_date)`.
- Sort order: `nse_symbol`, `trade_date`, `datetime`.
- Table properties mark `tick_ticker.time_grain = 1second` and `tick_ticker.query_layout = symbol_intraday_time_range`.
