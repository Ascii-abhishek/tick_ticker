# Storage

Local staging:

```text
data/
  cash/
    YYYY/
      MM/
        DD/
          NSE_SYMBOL.parquet
  cash_1s/
    by_symbol/
      NSE_SYMBOL/
        YYYY/
          MM/
            DD.parquet
    _windows/
      NSE_SYMBOL/
        YYYY/
          MM/
            DD/
              HHMMSS-HHMMSS.parquet
  state/
    cash/
      NSE_SYMBOL.json
    cash_1s/
      NSE_SYMBOL.json
```

Iceberg catalog layout:

```text
cash.ohlcv_by_symbol
cash.ohlcv_1s_by_symbol
options.ohlcv
future.ohlcv
```

Iceberg table layout:

- `cash.ohlcv_by_symbol` partitions by `nse_symbol`, `year(trade_date)`; sort order is `nse_symbol`, `trade_date`, `datetime`. This is the preferred query table for symbol history and symbol/date-range reads.
- `cash.ohlcv_1s_by_symbol` partitions by `nse_symbol`, `month(trade_date)`; sort order is `nse_symbol`, `trade_date`, `datetime`. This is the preferred query table for 1-second symbol history, day reads, and intraday time-range reads.
- `options.ohlcv` partitions by `trade_date`, `underlying`, `expiry_date`; sort order is `underlying`, `expiry_date`, `strike_price`, `option_type`, `datetime`.
- `future.ohlcv` partitions by `trade_date`, `underlying`, `expiry_date`; sort order is `underlying`, `expiry_date`, `datetime`.
- The 1-second table uses a larger 256 MB target file size and 16 MB Parquet row groups so monthly symbol partitions remain compact while day/time filters still benefit from sorted file statistics.
- Snapshot expiration policy keeps at least 10 snapshots and targets 30 days, when a maintenance engine runs expiration.
- Metadata previous-version tracking is limited to 20; automatic metadata deletion is disabled for Cloudflare R2 Data Catalog compatibility.

Bucket:

- Default bucket is `market-data`.
- Override with `R2_BUCKET_NAME`.
- R2 Data Catalog must be enabled on the bucket.
- Catalog URI defaults to `https://catalog.cloudflarestorage.com/<account_id>/<bucket>`.
- Warehouse defaults to `<account_id>_<bucket>`.

Cash Parquet columns:

- `datetime`
- `trade_date`
- `nse_symbol`
- `exchange_code`
- `product_type`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `count`
- `ingested_at`

Local partitioning:

- 1-minute top level is market type: `cash`.
- 1-minute files are date-first: `cash/YYYY/MM/DD/NSE_SYMBOL.parquet`.
- 1-second final files are symbol-first: `cash_1s/by_symbol/NSE_SYMBOL/YYYY/MM/DD.parquet`.
- 1-second temporary window files live under `cash_1s/_windows` only while a partially fetched day is being resumed.
- Local partitioning is only staging; the durable query surface is the Iceberg table.
- 1-minute uploads target `cash.ohlcv_by_symbol`.
- 1-second uploads target `cash.ohlcv_1s_by_symbol`.

Notes:

- Breeze code is not stored in cash Parquet.
- NSE symbol is the analytics symbol.
- Local manifest files are for resume safety only.
- Manifest `uploaded_files` means files uploaded to Iceberg, not raw R2 objects.
