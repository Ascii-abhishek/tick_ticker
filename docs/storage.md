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
- Manifest `uploaded_files` means files appended to Iceberg, not raw R2 objects.
