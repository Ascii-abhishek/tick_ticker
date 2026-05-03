# Data Structure

This document is the quick reference for how market data is stored, why ClickHouse is used, and how each table fits into ingestion and analytics.

## Why ClickHouse

ClickHouse is the storage engine for this project because options data is time-series heavy, append-oriented, and queried mostly through analytical filters.

It is a good fit because:

- It is columnar, so scans over `close`, `volume`, `open_interest`, or selected dimensions are fast.
- It handles high-volume append workloads well when data is inserted in batches.
- `MergeTree` partitioning and sorting make date-range and contract-specific queries efficient.
- `LowCardinality(String)` reduces storage and speeds filters for repeated symbols and exchange codes.
- It works well with BI tools like Metabase because the schema is flat and SQL-friendly.
- It avoids the overhead of row-store databases for workloads that mostly aggregate, filter, and backtest.

The key performance design is the `ORDER BY` layout. For the main candle table, data is sorted by:

```text
underlying, expiry_date, strike_price, option_type, datetime
```

That matches the most common query shape: “give me candles for this contract or expiry over time.”

## Table Overview

```text
app/config/underlyings.yml
   |
   v
underlying_mapping
   |
   |-- resolves Breeze stock_code
   |
   v
Breeze historical/live fetch
   |
   v
option_contracts       options_ohlcv
contract metadata ---> 1-minute OHLCV facts
                            |
                            v
                    options_snapshot
                    aggregate BI layer
```

## `options_ohlcv`

Main fact table for 1-minute option candles. Historical and live ingestion both write here.

Used for:

- Backtesting option price and OI behavior.
- Intraday analytics by underlying, expiry, strike, and option type.
- Metabase dashboards and notebook queries.
- Rebuilding derived snapshots if needed.

Primary query pattern:

```text
underlying + expiry_date + strike_price + option_type + datetime range
```

Schema:

```sql
CREATE TABLE options_ohlcv
(
    datetime            DateTime,
    underlying          LowCardinality(String),
    exchange            LowCardinality(String),

    expiry_date         Date,
    strike_price        Float32,
    option_type         Enum8('CE' = 1, 'PE' = 2),

    open                Float32,
    high                Float32,
    low                 Float32,
    close               Float32,

    volume              UInt32,
    open_interest       UInt32,

    ingestion_time      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(datetime)
ORDER BY (
    underlying,
    expiry_date,
    strike_price,
    option_type,
    datetime
);
```

Notes:

- `underlying` stores the canonical symbol from `underlying_mapping.nse_symbol`, not necessarily the Breeze stock code.
- Breeze is called with `underlying_mapping.breeze_symbol`.
- Duplicate protection is handled in application code using `(underlying, expiry_date, strike_price, option_type, datetime)`.
- `ingestion_time` is populated by ClickHouse.
- `PARTITION BY toYYYYMM(datetime)` keeps monthly data manageable for retention, maintenance, and range queries.

## `underlying_mapping`

Reference table for mapping internal/NSE symbols to Breeze API symbols.

Used for:

- Resolving the Breeze `stock_code` before API calls.
- Keeping analytics tables broker-independent.
- Supporting future underlyings without changing ingestion code.
- Storing lot size and tick size metadata needed by contract generation and analysis.

Schema:

```sql
CREATE TABLE underlying_mapping
(
    underlying_id       UInt32,
    breeze_symbol       String,
    nse_symbol          String,
    display_name        String,
    exchange            LowCardinality(String),
    lot_size            UInt16,
    tick_size           Float32,
    created_at          DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY underlying_id;
```

Notes:

- Seed from `app/config/underlyings.yml` with `uv run options-platform seed-underlyings`.
- Runtime resolution prefers ClickHouse rows and uses YAML as a fallback.
- `nse_symbol` is the canonical value stored in `options_ohlcv.underlying`.
- `breeze_symbol` is used only for Breeze API requests.

## `option_contracts`

Reference table for option contracts discovered or requested during ingestion.

Used for:

- Recording the contract universe being ingested.
- Joining with fact data for lot size and weekly/monthly classification.
- Avoiding repeated contract metadata setup in future workflows.
- Tracking which strikes/expiries are part of the loaded dataset.

Schema:

```sql
CREATE TABLE option_contracts
(
    contract_id        UInt64,
    underlying         LowCardinality(String),
    expiry_date        Date,
    strike_price       Float32,
    option_type        Enum8('CE' = 1, 'PE' = 2),
    lot_size           UInt16,
    is_weekly          UInt8,
    created_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (
    underlying,
    expiry_date,
    strike_price,
    option_type
);
```

Notes:

- Historical ingestion inserts contract rows before loading candles.
- `contract_id` is generated deterministically from `(underlying, expiry_date, strike_price, option_type)` when not supplied.
- Application code skips already-known contracts using the natural key.

## `options_snapshot`

Optional aggregate table for option-chain level snapshots.

Used for:

- BI-friendly put-call ratio views.
- Fast dashboard queries over total call/put OI.
- Future scheduled aggregations from `options_ohlcv` or option-chain quote responses.

Schema:

```sql
CREATE TABLE options_snapshot
(
    datetime        DateTime,
    underlying      LowCardinality(String),
    expiry_date     Date,
    atm_strike      Float32,
    total_call_oi   UInt64,
    total_put_oi    UInt64,
    pcr             Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(datetime)
ORDER BY (underlying, expiry_date, datetime);
```

Notes:

- This table exists for future aggregate ingestion and dashboarding.
- It is intentionally flat so BI tools can query it easily.
- `pcr` is usually `total_put_oi / total_call_oi`.

## Optimization Notes

The existing `ORDER BY` clauses are the primary indexes in ClickHouse. The app also provides optional data-skipping indexes for common ad hoc filters:

```bash
uv run options-platform apply-optimizations
```

For existing data, materialize indexes during a quiet period:

```bash
uv run options-platform apply-optimizations --materialize
```

Batch inserts are important. The ingestion repository uses `clickhouse-connect` bulk inserts with `BATCH_SIZE`, defaulting to `1000`.

