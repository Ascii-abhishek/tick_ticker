# Options Data Platform

A quant-ready Python platform for ingesting, storing, and analyzing 1-minute options candles from the ICICI Breeze API into ClickHouse.

The first target is NIFTY options, with the code organized so more underlyings, orchestration, Kafka, and BI tooling can be added later without rewriting the ingestion core.

## Architecture

```text
Breeze API
   |
   v
app.ingestion.breeze_client
   |
   v
app.ingestion.transformer
   |
   v
app.db.repository.insert  ---> ClickHouse
   |                               |
   v                               v
Historical loader / Live runner    Metabase / notebooks / analytics
```

## Project Layout

```text
app/
  config/        Pydantic settings and constants
  db/            ClickHouse client, typed row models, repositories
  ingestion/     Breeze wrapper, transformer, historical and live loaders
  services/      Contract and symbol helpers
  utils/         JSON logging, datetime helpers, retry decorator
  docs/          Common options knowledge
scripts/         CLI scripts for historical and live ingestion
notebooks/       Analysis examples
tests/           Unit tests
```

`data/clickhouse` is your local ClickHouse installation and data directory. It is intentionally ignored by Git and should not be edited by the application code.

## Setup

Install dependencies:

```bash
uv sync --dev
```

Configure `.env`:

```env
ENVIRONMENT=dev
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default

BREEZE_API_KEY=...
BREEZE_API_SECRET=...
BREEZE_SESSION_TOKEN=...

BATCH_SIZE=1000
STRIKE_STEP=50
STRIKE_WINDOW=10
```

Check ClickHouse connectivity:

```bash
uv run options-platform healthcheck
```

Seed the default NIFTY mapping:

```bash
uv run options-platform seed-nifty
```

## Historical Ingestion

Historical ingestion loops through:

```text
underlying -> expiry -> strike -> CE/PE -> date chunks
```

Rows are transformed into the exact `options_ohlcv` schema and inserted in batches. Before inserting, the repository checks existing natural keys:

```text
underlying, expiry_date, strike_price, option_type, datetime
```

This makes reruns idempotent at the application layer even though the existing ClickHouse table uses plain `MergeTree`.

Example:

```bash
uv run python scripts/run_historical.py \
  --underlying NIFTY \
  --expiries 2026-05-07 \
  --strikes 22500,22550,22600 \
  --from-date 2026-05-01 \
  --to-date 2026-05-03
```

## Live Ingestion

Live ingestion polls Breeze once per minute, fetches a short lookback window, and uses the same duplicate filter before insert.

Run continuously:

```bash
uv run python scripts/run_live.py \
  --underlying NIFTY \
  --expiry 2026-05-07 \
  --strikes 22500,22550,22600
```

Run one cycle:

```bash
uv run python scripts/run_live.py \
  --expiry 2026-05-07 \
  --strikes 22500,22550,22600 \
  --run-once
```

## Analytics

Use `QueryRepository` for application reads, or query ClickHouse directly from notebooks. A starter notebook is available at `notebooks/options_analysis_sample.ipynb`.

Example SQL:

```sql
SELECT
    datetime,
    strike_price,
    option_type,
    close,
    volume,
    open_interest
FROM options_ohlcv
WHERE underlying = 'NIFTY'
  AND expiry_date = '2026-05-07'
ORDER BY datetime, strike_price, option_type;
```

## Production Notes

- Inserts are batched with `BATCH_SIZE`, defaulting to 1000 rows.
- Breeze requests use retry with exponential backoff.
- Breeze requests are rate limited with `BREEZE_MIN_REQUEST_INTERVAL_SECONDS`.
- Logs are structured JSON and include contract identifiers on failures.
- The table schemas stay BI-friendly for future Metabase dashboards.
- Airflow, Kafka, and materialized views are intentionally not included yet.

