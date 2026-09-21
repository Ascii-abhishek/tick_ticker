#!/usr/bin/env bash
# Backfill NIFTY 50 constituent 1-minute history from Breeze (pre-2022, and
# delisted names such as HDFC), then publish it to Iceberg.
#
# Works through the Breeze rows of a backfill plan written by:
#   uv run generate-index-volume --from-date 2016-01-01 --dry-run \
#     --backfill-plan data/state/nifty50_backfill_plan.csv
#
# Each range is fetched with --fetch-only (D1 sync state untouched), then
# published with `repair-cash-data --republish` (Iceberg replace, idempotent).
# Re-running skips days that already have local files, so run it once a day
# after refreshing BREEZE_SESSION_TOKEN until the plan is done.
#
# Environment:
#   PLAN_FILE        Default data/state/nifty50_backfill_plan.csv
#   BREEZE_BUDGET    Breeze requests per run (default 4500 of the 5000/day limit)
#   DRY_RUN=1        Print the commands only
set -uo pipefail

cd "$(dirname "$0")/.."
PLAN_FILE="${PLAN_FILE:-data/state/nifty50_backfill_plan.csv}"
BREEZE_BUDGET="${BREEZE_BUDGET:-4500}"
LOG_FILE="logs/breeze_backfill_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

[ -f "$PLAN_FILE" ] || { echo "missing $PLAN_FILE; generate it first (see header)" >&2; exit 1; }
exec > >(tee -a "$LOG_FILE") 2>&1

remaining="$BREEZE_BUDGET"
# storage_symbol,provider,from_date,to_date,sessions
tail -n +2 "$PLAN_FILE" | while IFS=, read -r symbol provider from_date to_date sessions; do
  [ "$provider" = "breeze" ] || continue
  # Two sessions per request; stop before a range that cannot fit today's budget.
  needed=$(( (sessions + 1) / 2 ))
  if [ "$needed" -gt "$remaining" ]; then
    echo "budget left $remaining < $needed for $symbol $from_date..$to_date; stopping for today"
    break
  fi
  fetch=(uv run sync-cash-data --fetch-only --no-ensure-sync-table --allow-large-range
         --nse-symbol "$symbol" --from-date "$from_date" --to-date "$to_date" --breeze-max-requests "$remaining")
  publish=(uv run repair-cash-data --republish --nse-symbols "$symbol" --from-date "$from_date" --to-date "$to_date")
  if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "${fetch[*]}"; echo "${publish[*]}"; continue
  fi
  if ! "${fetch[@]}"; then
    echo "fetch failed for $symbol $from_date..$to_date (expired session or budget); stopping"
    break
  fi
  "${publish[@]}" || { echo "publish failed for $symbol; rerun repair-cash-data --republish for it"; break; }
  remaining=$(( remaining - needed ))
done
