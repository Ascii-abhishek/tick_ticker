# Historical backfill (Breeze)

Part of the [knowledge base](../README.md). See also:
[provider characteristics](../market/provider-data-characteristics.md) ·
[decisions](decisions.md#breeze-backfill-is-fetch-only-then-republished).

Upstox minute history starts on 2022-01-01. To compute NIFTY volume for
2016–2021, and for 2022-01-03..2023-07-12 (HDFC is not on Upstox), constituent
candles must come from Breeze.

## Size (as of 2026-09-21)

- 69 securities; 73,468 member-sessions missing locally.
- At 2 sessions per Breeze request: **about 36,700 requests, roughly 8–9 days** at 4,500 requests/day.
- Largest single item: HDFC, 1,862 sessions (2016-01-01..2023-07-12).
- Delisted or renamed names whose Breeze codes have not been checked yet: HDFC (`HDFC`), CAIRN (`CAIIND`), TATAMTRDVR (`TMLDVR`), SAMMAANCAP/IBULHSGFIN (`INDHO`), INDUSTOWER/INFRATEL (`BHAINF`), TMPV/TATAMOTORS (`TATMOT`). The first run shows whether each returns data. Record the result in `reference/provider_mappings.csv`.
- NIFTY itself is missing 31 sessions in 2016–2017 (and RELIANCE 3). They are in the same plan.

## Steps

```bash
# 0. Fresh Breeze session token in .env (BREEZE_SESSION_TOKEN), valid for today.

# 1. Plan: missing member ranges, with the provider for each
uv run generate-index-volume --from-date 2016-01-01 --dry-run \
  --backfill-plan data/state/nifty50_backfill_plan.csv

# 2. Preview the commands
DRY_RUN=1 scripts/backfill_index_constituents_breeze.sh

# 3. Run; stops when today's budget (default 4500) is used, resumes next day
scripts/backfill_index_constituents_breeze.sh

# 4. After each day's run, publish NIFTY volume for days that became complete
uv run generate-index-volume --from-date 2016-01-01 --to-date 2021-12-31 --publish
```

Per plan row, the wrapper runs:
- `sync-cash-data --fetch-only …` (Breeze): writes local files and leaves D1 sync state untouched.
- `repair-cash-data --republish …`: replaces those dates in Iceberg in one commit and marks them uploaded in the symbol's manifest.

## Request planning

- Breeze returns at most 1,000 candles per request, and a session has up to 378 (with pre-open). So at most 2 sessions go in one request (`CASH_HISTORY_SESSIONS_PER_REQUEST=2`).
- Weekends and holidays are skipped using the sessions seen in local data (`CASH_HISTORY_USE_SESSION_CALENDAR=true`). Outside the known calendar the planner falls back to plain day chunks, so a real session is never skipped.
- Earlier, a chunk longer than one day could file the next day's candles under a holiday's file name. Rows are now always filed by their own `trade_date`.

## After the backfill

1. Re-run `generate-index-volume --from-date 2016-01-01 --dry-run` and check that the remaining `partial` days are only ones where Breeze truly has no data.
2. Update `provider_mappings.csv` availability for the Breeze codes (verified/unavailable) and `load-reference-data`.
3. Spot-check totals around rebalance dates ([sanity checks](../market/synthetic-index-volume.md#checking-it)).
