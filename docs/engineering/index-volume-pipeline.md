# Index volume pipeline

Part of the [knowledge base](../README.md). The formula and its meaning are in
[synthetic index volume](../market/synthetic-index-volume.md); this page covers
how it runs.

Code: [`services/index_volume.py`](../../src/tick_ticker/services/index_volume.py)
(pure computation, unit-tested) and
[`scripts/generate_index_volume.py`](../../src/tick_ticker/scripts/generate_index_volume.py) (CLI).

## Flow per index day

1. Take the membership intervals active that day from D1 `index_membership` (or `reference/*.csv` with `--reference-source local`).
2. For each member, read `data/cash/Y/M/D/<storage_symbol>.parquet`. `expects_data` is false before `first_trade_date` (placeholders) and after `last_trade_date` (delisted).
3. Clean member rows: keep one candle per minute (last wins); drop non-finite or non-positive prices and impossible OHLC.
4. Sum `(O+H+L+C)/4 × V` per minute. Left-join onto the NIFTY minutes. Divide by the NIFTY close and round to an integer. Minutes in the pre-open (09:00–09:14) and post-close (15:30–15:59) windows get 0.
5. Status is `complete` or `partial`, with fingerprints of the basket and of the inputs (per member: row count, volume sum, close sum).

## Commands

```bash
# What would happen, with a per-day JSON report
uv run generate-index-volume --from-date 2022-01-01 --dry-run --report data/state/nifty_volume_report.json

# Missing member ranges and the provider to fetch each from
uv run generate-index-volume --from-date 2016-01-01 --dry-run --backfill-plan data/state/nifty50_backfill_plan.csv

# Publish complete days (local NIFTY files + Iceberg replace + D1 state)
uv run generate-index-volume --from-date 2022-01-01 --publish

# Daily: recompute the trailing sessions
uv run generate-index-volume --from-date "$(date -v-45d +%F)" --last-sessions 10 --publish
```

## Publishing

- The NIFTY local file for the day is rewritten atomically (temp file + rename) with the new `volume`. All other columns are unchanged.
- Iceberg: `replace_symbol_days("cash", …, nse_symbol="NIFTY")`, up to 60 days per commit. The snapshot summary records `tick_ticker.volume_formula` and `tick_ticker.volume_run`.
- D1 `index_volume_state` gets one row per day. `published_at` is set for the days written. A recompute that doesn't publish keeps the earlier `published_at` only if both fingerprints are unchanged.
- Partial days are not published: their NIFTY `volume` stays at the provider's 0. `--allow-partial` exists for experiments only.

## When to recompute

| Trigger | Detected by | Action |
|---|---|---|
| Member data fetched or repaired | `input_fingerprint` changes | Re-run for those dates with `--publish` |
| Membership corrected | `membership_fingerprint` changes | Same |
| Formula changed | `formula_version` | Bump `FORMULA_VERSION`, republish everything |
| New trading day | the daily cron | `--last-sessions 10` covers late provider data |

The run is idempotent: republishing a day replaces it.

## Performance

About 2,600 index days (2016–2026) compute in ~20 s with 8 workers, reading
local Parquet only. Publishing is dominated by Iceberg commits (~60 days per
commit).
