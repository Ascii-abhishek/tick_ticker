#!/usr/bin/env bash
# Daily 1-minute cash sync + NIFTY 50 synthetic volume.
#
# Schedule after midnight IST (e.g. 06:30 IST): every step stops at the last
# completed session (yesterday, IST), so running earlier only waits a day.
#
#   30 6 * * 1-6  cd /path/to/tick_ticker && scripts/daily_cash_sync.sh
#
# Requires a fresh UPSTOX_ACCESS_TOKEN in .env (Upstox tokens expire daily).
#
# Environment:
#   REPAIR_SESSIONS  Trailing sessions re-checked for gaps (default 10).
#   VOLUME_SESSIONS  Trailing sessions whose NIFTY volume is recomputed (default 10).
#   UPSTOX_BUDGET    Upstox request cap for the sync step (default 1800).
#   DRY_RUN=1        Report only: no fetch, publish, or D1 writes.
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p logs data/state

REPAIR_SESSIONS="${REPAIR_SESSIONS:-10}"
VOLUME_SESSIONS="${VOLUME_SESSIONS:-10}"
UPSTOX_BUDGET="${UPSTOX_BUDGET:-1800}"
LOCK_DIR="data/state/daily_cash_sync.lock"
LOG_FILE="logs/daily_cash_sync_$(date +%Y%m%d_%H%M%S).log"

# mkdir is atomic; a second run exits instead of racing Iceberg commits.
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "daily_cash_sync: another run holds $LOCK_DIR; exiting" >&2
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT

exec > >(tee -a "$LOG_FILE") 2>&1
echo "daily_cash_sync started $(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [ "${DRY_RUN:-0}" = "1" ]; then
  uv run repair-cash-data --since-sessions "$REPAIR_SESSIONS"
  uv run generate-index-volume --index NIFTY50 --from-date "$(date -v-45d +%Y-%m-%d 2>/dev/null || date -d '45 days ago' +%Y-%m-%d)" \
    --last-sessions "$VOLUME_SESSIONS" --dry-run
  exit 0
fi

# 1. Bring every already-synced symbol (including NIFTY) up to yesterday.
uv run sync-cash-upstox-data --all --synced-only --upstox-max-requests "$UPSTOX_BUDGET"

# 2. Re-check the trailing sessions against the trading calendar and refetch
#    any symbol/day without candles (late provider data, failed requests).
uv run repair-cash-data --since-sessions "$REPAIR_SESSIONS" --repair-missing

# 3. Recompute NIFTY volume for the trailing sessions and publish complete days.
uv run generate-index-volume --index NIFTY50 \
  --from-date "$(date -v-45d +%Y-%m-%d 2>/dev/null || date -d '45 days ago' +%Y-%m-%d)" \
  --last-sessions "$VOLUME_SESSIONS" --publish

echo "daily_cash_sync finished $(date -u +%Y-%m-%dT%H:%M:%SZ)"
