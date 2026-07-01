# Storage

Local staging:

```text
data/
  cash/
    YYYY/
      MM/
        DD/
          NSE_SYMBOL.parquet
  state/
    cash/
      NSE_SYMBOL.json
```

Iceberg catalog layout:

```text
cash.ohlcv
options.ohlcv
future.ohlcv
```

Iceberg table layout:

- `cash.ohlcv` partitions by `trade_date`, `nse_symbol`; sort order is `nse_symbol`, `trade_date`, `datetime`.
- `options.ohlcv` partitions by `trade_date`, `underlying`, `expiry_date`; sort order is `underlying`, `expiry_date`, `strike_price`, `option_type`, `datetime`.
- `future.ohlcv` partitions by `trade_date`, `underlying`, `expiry_date`; sort order is `underlying`, `expiry_date`, `datetime`.
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

- Top level is market type: `cash`.
- Then date: `YYYY/MM/DD`.
- Then symbol file: `NSE_SYMBOL.parquet`.
- Local partitioning is only staging; the durable query surface is the Iceberg table.

Notes:

- Breeze code is not stored in cash Parquet.
- NSE symbol is the analytics symbol.
- Local manifest files are for resume safety only.
- Manifest `uploaded_files` means files uploaded to Iceberg, not raw R2 objects.
