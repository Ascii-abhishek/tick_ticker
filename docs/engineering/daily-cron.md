# Daily cron

Part of the [knowledge base](../README.md). See also:
[pipeline overview](pipeline-overview.md) · [index volume pipeline](index-volume-pipeline.md).

Script: [`scripts/daily_cash_sync.sh`](../../scripts/daily_cash_sync.sh)

```cron
# 06:30 IST, Monday–Saturday (Saturday catches Friday + any special session)
30 6 * * 1-6  cd /path/to/tick_ticker && scripts/daily_cash_sync.sh
```

The cron's time zone must be IST, or convert the time. The script's own date
logic is in IST whatever the machine clock is set to.

## What it runs

1. `sync-cash-upstox-data --all --synced-only` brings every symbol that already has sync state up to yesterday. Never-synced reference symbols are skipped, so the request budget goes to keeping existing data current. Backfill new symbols deliberately.
2. `repair-cash-data --since-sessions 10 --repair-missing` first probes NIFTY for weekdays after the last known session (catches days the provider published late), then finds symbol/days in the last 10 sessions without candles (late provider data, failed requests), fetches them, and replaces them in Iceberg.
3. `generate-index-volume --last-sessions 10 --publish` recomputes NIFTY volume for those sessions and publishes the complete ones.

A lock directory (`data/state/daily_cash_sync.lock`) stops overlapping runs.
Logs go to `logs/daily_cash_sync_<timestamp>.log`. `DRY_RUN=1` runs only the
audit and the volume dry run.

## Credentials

- **Upstox access tokens expire daily.** The cron needs a fresh `UPSTOX_ACCESS_TOKEN` in `.env` before it runs; the token flow is not automated here. If the token is expired, step 1 fails with HTTP 401 and nothing is written. Fix the token and re-run.
- Breeze is not used daily. Its session token is only needed for the [historical backfill](historical-backfill.md).

## Failure behaviour

| Failure | Effect | Recovery |
|---|---|---|
| Token expired | Step 1 fails before writing | Refresh the token, re-run |
| One symbol's fetch fails | D1 marks it `failed`; it is retried the next day | Automatic |
| Provider publishes a day late | That day has no files; step 2 catches it within 10 sessions | Automatic |
| Transient D1 error (seen: a one-off HTTP 401) | Retried up to 4 times with backoff | Automatic |
| Iceberg commit conflict | Upload retries, then fails the symbol | Re-run; replace is idempotent |
| A constituent missing on a day | The NIFTY day stays `partial` (volume 0) | Fix the member's data; the next run within 10 sessions republishes it, or run `generate-index-volume` for that date |

## Timing

- About 20 s per symbol for the daily sync, mostly the Iceberg replace commit. That is roughly 45 minutes for 134 symbols. Table metadata checks now run once per process, not per symbol, which cut it from about 90 s.
- Upstox may not have the previous session at 01:00 IST. Running at 06:30 IST is safer, and the probe plus 10-session repair window covers late days either way.

## Checks after a run

```sql
SELECT status, COUNT(*) FROM index_volume_state
WHERE index_code = 'NIFTY50' AND trade_date >= date('now', '-30 days') GROUP BY status;
```
