# Pipeline overview

Part of the [knowledge base](../README.md). See also:
[daily cron](daily-cron.md) · [D1 reference schema](d1-reference-schema.md) ·
[index volume pipeline](index-volume-pipeline.md).

## Stores

| Store | Holds | Source of truth for |
|---|---|---|
| `reference/*.csv` (git) | Reviewed security identity, NIFTY membership, corporate actions | Reference content, which is reviewed through PRs |
| Cloudflare D1 | `equity_symbol_reference`, `market_data_sync_state`, the reference tables, `index_volume_state` | What scripts read at runtime; sync and volume status |
| `data/cash/YYYY/MM/DD/SYMBOL.parquet` | One file per symbol-day of 1-minute candles | Staging; what gets published |
| Iceberg `cash.ohlcv_by_symbol` on R2 | All published candles, partitioned by `nse_symbol`, `year(trade_date)` | What consumers query |

## Commands

| Command | Does | Writes |
|---|---|---|
| `sync-cash-upstox-data` | Fetches Upstox 1-minute candles (2022+) by month, splits them into daily files, uploads | local, Iceberg (replace), D1 sync state |
| `sync-cash-data` | Same for Breeze (2016+), grouping 2 known sessions per request | local, Iceberg (replace), D1 sync state |
| `repair-cash-data` | Audits local days against the trading calendar; repairs, re-fetches or republishes | local (with backup), Iceberg (replace), manifests |
| `load-reference-data` | Validates `reference/*.csv` and upserts it into D1 | D1 reference tables |
| `generate-index-volume` | Computes NIFTY synthetic volume and publishes complete days | NIFTY local files, Iceberg (replace), `index_volume_state` |
| `reconcile-cash-sync-state` | Rebuilds manifests and D1 sync state from local files | manifests, D1 |

## Order of operations

```text
1. load-reference-data            (only when reference/*.csv changes)
2. sync-cash-upstox-data --all --synced-only     (daily; keeps symbols current)
3. repair-cash-data --since-sessions 10 --repair-missing
4. generate-index-volume --last-sessions 10 --publish
```

`scripts/daily_cash_sync.sh` runs steps 2–4 ([daily cron](daily-cron.md)).
Historical work is separate and budget-bound; see
[historical backfill](historical-backfill.md).

## Invariants the code now enforces

1. **No partial days.** Every sync stops at yesterday (IST): `last_completed_session_bound()`.
2. **Idempotent publishing.** Every Iceberg write is a delete + insert for the symbol/days written (`replace_symbol_days` / `replace_symbol_range`). Re-running any step cannot duplicate candles.
3. **Gaps are found from the data, not from state.** `repair-cash-data` compares each symbol's non-empty files against the sessions implied by NIFTY and the other symbols. D1 "completed" status alone is not trusted ([audit](data-quality-audit-2026-09.md)).
4. **Replaced local files are kept** under `data/_superseded/<run>/`.
5. **Index volume is published only for complete days** ([rules](../market/synthetic-index-volume.md#completeness-rules)).
