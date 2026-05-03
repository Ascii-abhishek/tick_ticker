# Operations Guide

## Project Structure

```text
app/
  config/
    settings.py          Environment-driven settings
    constants.py         Table columns and option constants
    underlyings.yml      Underlying catalog for initial seeding
    underlyings.py       YAML loader and typed config models
  db/
    client.py            ClickHouse client factory
    optimizations.py     Data-skipping index setup
    models/              Typed table row models
    repository/          Bulk insert and query access
  ingestion/
    breeze_client.py     Breeze API wrapper with retries and rate limiting
    transformer.py       Breeze response to DB row conversion
    historical_loader.py Batch backfill orchestration
    live_ingestion.py    Minute polling ingestion loop
  services/
    symbol_service.py    Canonical symbol and Breeze stock code resolution
    contract_service.py  Strike range and option contract helpers
  utils/
    logger.py            Structured JSON logging
    datetime_utils.py    Date parsing and chunking
    retry.py             Retry decorator
scripts/
  run_historical.py      Historical CLI
  run_live.py            Live polling CLI
notebooks/
  options_analysis_sample.ipynb
```

## Symbol Flow

The analytics tables should use canonical symbols, not broker-specific names.

```text
app/config/underlyings.yml
   |
   v
options-platform seed-underlyings
   |
   v
ClickHouse underlying_mapping
   |
   v
run_historical.py / run_live.py
   |
   |-- Breeze request stock_code = underlying_mapping.breeze_symbol
   |
   `-- Stored options_ohlcv.underlying = underlying_mapping.nse_symbol
```

For example, if a future broker symbol differs from the NSE/internal symbol, put both in `underlyings.yml`. The ingestion scripts will call Breeze with `breeze_symbol` and store `nse_symbol`.

## Add A New Underlying

Edit `app/config/underlyings.yml`:

```yaml
underlyings:
  - underlying_id: 1
    breeze_symbol: NIFTY
    nse_symbol: NIFTY
    display_name: Nifty 50
    exchange: NFO
    lot_size: 50
    tick_size: 0.05
    strike_step: 50
```

Then seed it:

```bash
uv run options-platform seed-underlyings
```

If a row already exists in `underlying_mapping`, the seed command skips it. ClickHouse rows are treated as authoritative at runtime, with YAML as a fallback for local development.

## Commands

Check ClickHouse connectivity:

```bash
uv run options-platform healthcheck
```

Seed all configured underlyings:

```bash
uv run options-platform seed-underlyings
```

Seed only default NIFTY:

```bash
uv run options-platform seed-nifty
```

Apply ClickHouse data-skipping indexes:

```bash
uv run options-platform apply-optimizations
```

Materialize those indexes for existing data parts:

```bash
uv run options-platform apply-optimizations --materialize
```

Materialization can be expensive on large tables. Run it during quiet periods.

## Historical Backfill

Exact strikes:

```bash
uv run python scripts/run_historical.py \
  --underlying NIFTY \
  --expiries 2026-05-07 \
  --strikes 22500,22550,22600 \
  --from-date 2026-05-01 \
  --to-date 2026-05-03
```

ATM-based strikes from spot:

```bash
uv run python scripts/run_historical.py \
  --underlying NIFTY \
  --expiries 2026-05-07 \
  --spot-price 22482 \
  --strike-window 5 \
  --from-date 2026-05-01 \
  --to-date 2026-05-03
```

The `strike_step` comes from `app/config/underlyings.yml`, falling back to `STRIKE_STEP`.

## Live Polling

Exact strikes:

```bash
uv run python scripts/run_live.py \
  --underlying NIFTY \
  --expiry 2026-05-07 \
  --strikes 22500,22550,22600
```

ATM-based strikes:

```bash
uv run python scripts/run_live.py \
  --underlying NIFTY \
  --expiry 2026-05-07 \
  --spot-price 22482 \
  --strike-window 5
```

Run one cycle for smoke testing:

```bash
uv run python scripts/run_live.py \
  --underlying NIFTY \
  --expiry 2026-05-07 \
  --strikes 22500 \
  --run-once
```

## Cron And Scheduling

Historical backfills are good cron jobs because they start, finish, and exit.

Example weekday cron:

```cron
30 18 * * 1-5 cd /Users/abhishekpathak/projects/personal/tick_ticker && uv run python scripts/run_historical.py --underlying NIFTY --expiries 2026-05-07 --spot-price 22482 --strike-window 10 --from-date 2026-05-03 --to-date 2026-05-03 >> logs/historical.log 2>&1
```

Live ingestion is a long-running process, so prefer `tmux`, `launchd` on macOS, `systemd` on Linux, or a supervisor. Cron can start it, but cron is not good at keeping one long-running process healthy.

## Performance And Idempotency

- Inserts use `clickhouse-connect` bulk inserts.
- `BATCH_SIZE` defaults to 1000 rows.
- The write repository avoids duplicate OHLCV rows using the natural key:

```text
underlying, expiry_date, strike_price, option_type, datetime
```

- The main table `ORDER BY` is already query-friendly for option-chain and contract time-series access:

```text
underlying, expiry_date, strike_price, option_type, datetime
```

- `apply-optimizations` adds ClickHouse data-skipping indexes for common ad hoc analytics filters without changing the base table schemas.
- Breeze calls are retried with exponential backoff and rate limited by `BREEZE_MIN_REQUEST_INTERVAL_SECONDS`.
- Historical requests are chunked by `HISTORICAL_CHUNK_DAYS` to avoid very large API calls.

