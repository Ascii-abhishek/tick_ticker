#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

today() {
  date +%F
}

BASE_FROM_DATE="${BASE_FROM_DATE:-2026-01-01}"
TO_DATE="${TO_DATE:-$(today)}"
PARALLEL_FETCH_JOBS="${PARALLEL_FETCH_JOBS:-2}"
DAILY_BREEZE_REQUEST_BUDGET="${DAILY_BREEZE_REQUEST_BUDGET:-4500}"
BREEZE_REQUEST_BUDGET_MODE="${BREEZE_REQUEST_BUDGET_MODE:-cycle}"
BREEZE_REQUESTS_ALREADY_USED="${BREEZE_REQUESTS_ALREADY_USED:-0}"
BREEZE_PREFLIGHT="${BREEZE_PREFLIGHT:-1}"
AUTO_CONTINUE="${AUTO_CONTINUE:-1}"
UPLOAD_AFTER_FETCH="${UPLOAD_AFTER_FETCH:-1}"
DRY_RUN="${DRY_RUN:-0}"
CASH_SECOND_D1_FALLBACK="${CASH_SECOND_D1_FALLBACK:-0}"
ENSURE_ICEBERG_TABLE="${ENSURE_ICEBERG_TABLE:-1}"
MIN_FREE_SPACE_GB="${MIN_FREE_SPACE_GB:-20}"
SOFT_FETCH_RETRY_DELAY_SECONDS="${SOFT_FETCH_RETRY_DELAY_SECONDS:-60}"
LOG_ROOT="${LOG_ROOT:-logs/cash-second-bank-backfill}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="$LOG_ROOT/$RUN_ID"

# SYMBOLS=(
#   HDFCBANK
#   ICICIBANK
#   SBIN
#   AXISBANK
#   KOTAKBANK
#   BANKBARODA
#   PNB
#   FEDERALBNK
#   INDUSINDBK
#   AUBANK
#   IDFCFIRSTB
#   BANDHANBNK
# )

SYMBOLS=(
  NIFTY
)
SYMBOL_CODE_VALUES=()
SYMBOL_CODE_ARGS=()

die() {
  echo "error: $*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --symbol-code)
      [ "$#" -ge 2 ] || die "--symbol-code requires SYMBOL:BREEZE_CODE"
      SYMBOL_CODE_VALUES[${#SYMBOL_CODE_VALUES[@]}]="$2"
      SYMBOL_CODE_ARGS[${#SYMBOL_CODE_ARGS[@]}]="--symbol-code"
      SYMBOL_CODE_ARGS[${#SYMBOL_CODE_ARGS[@]}]="$2"
      shift 2
      ;;
    --symbol-code=*)
      value="${1#--symbol-code=}"
      [ -n "$value" ] || die "--symbol-code requires SYMBOL:BREEZE_CODE"
      SYMBOL_CODE_VALUES[${#SYMBOL_CODE_VALUES[@]}]="$value"
      SYMBOL_CODE_ARGS[${#SYMBOL_CODE_ARGS[@]}]="--symbol-code"
      SYMBOL_CODE_ARGS[${#SYMBOL_CODE_ARGS[@]}]="$value"
      shift
      ;;
    *)
      die "unsupported argument $1; use environment variables or --symbol-code SYMBOL:BREEZE_CODE"
      ;;
  esac
done

is_non_negative_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

is_positive_int() {
  is_non_negative_int "$1" && [ "$1" -gt 0 ]
}

is_boolean_flag() {
  [ "$1" = "0" ] || [ "$1" = "1" ]
}

is_positive_int "$PARALLEL_FETCH_JOBS" || die "PARALLEL_FETCH_JOBS must be >= 1"
is_non_negative_int "$DAILY_BREEZE_REQUEST_BUDGET" || die "DAILY_BREEZE_REQUEST_BUDGET must be >= 0"
case "$BREEZE_REQUEST_BUDGET_MODE" in
  cycle|run) ;;
  *) die "BREEZE_REQUEST_BUDGET_MODE must be cycle or run" ;;
esac
is_non_negative_int "$BREEZE_REQUESTS_ALREADY_USED" || die "BREEZE_REQUESTS_ALREADY_USED must be >= 0"
is_boolean_flag "$BREEZE_PREFLIGHT" || die "BREEZE_PREFLIGHT must be 0 or 1"
is_boolean_flag "$AUTO_CONTINUE" || die "AUTO_CONTINUE must be 0 or 1"
is_boolean_flag "$UPLOAD_AFTER_FETCH" || die "UPLOAD_AFTER_FETCH must be 0 or 1"
is_boolean_flag "$DRY_RUN" || die "DRY_RUN must be 0 or 1"
is_boolean_flag "$CASH_SECOND_D1_FALLBACK" || die "CASH_SECOND_D1_FALLBACK must be 0 or 1"
is_boolean_flag "$ENSURE_ICEBERG_TABLE" || die "ENSURE_ICEBERG_TABLE must be 0 or 1"
is_non_negative_int "$SOFT_FETCH_RETRY_DELAY_SECONDS" || die "SOFT_FETCH_RETRY_DELAY_SECONDS must be >= 0"

AVAILABLE_BREEZE_REQUEST_BUDGET=$((DAILY_BREEZE_REQUEST_BUDGET - BREEZE_REQUESTS_ALREADY_USED))
if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 0 ]; then
  die "BREEZE_REQUESTS_ALREADY_USED is greater than DAILY_BREEZE_REQUEST_BUDGET"
fi

DEFAULT_INTERVAL_SECONDS="$(awk -v jobs="$PARALLEL_FETCH_JOBS" 'BEGIN { printf "%.2f", jobs * 0.70 }')"
PER_PROCESS_INTERVAL_SECONDS="${BREEZE_MIN_REQUEST_INTERVAL_SECONDS:-$DEFAULT_INTERVAL_SECONDS}"

LOCK_DIR="${TMPDIR:-/tmp}/tick_ticker_bank_cash_second_backfill.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  die "another selected cash 1-second backfill appears to be running; remove $LOCK_DIR only if it is stale"
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

mkdir -p "$LOG_DIR"

d1_arg() {
  if [ "$CASH_SECOND_D1_FALLBACK" = "1" ]; then
    echo "--d1-fallback"
  else
    echo "--no-d1-fallback"
  fi
}

check_free_space() {
  data_path="$ROOT_DIR/data"
  mkdir -p "$data_path"

  available_kb="$(df -Pk "$data_path" | awk 'NR == 2 { print $4 }')"
  required_kb="$(awk -v gb="$MIN_FREE_SPACE_GB" 'BEGIN { printf "%.0f", gb * 1024 * 1024 }')"
  if [ -z "$available_kb" ] || [ "$available_kb" -lt "$required_kb" ]; then
    die "low disk space under $data_path; require at least ${MIN_FREE_SPACE_GB}GB free"
  fi
}

breeze_preflight() {
  if [ "$DRY_RUN" = "1" ] || [ "$BREEZE_PREFLIGHT" = "0" ]; then
    return
  fi
  if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 1 ]; then
    die "not enough Breeze budget left for the preflight session check"
  fi
  echo "Checking Breeze session token before launching 1-second fetch workers..."
  preflight_log="$LOG_DIR/breeze_preflight.log"
  if ! uv run python - > "$preflight_log" 2>&1 <<'PY'
from tick_ticker.config import Settings
from tick_ticker.utils.engines import create_breeze_client

create_breeze_client(Settings()).connect()
PY
  then
    tail -n 20 "$preflight_log" >&2
    die "Breeze session preflight failed; refresh BREEZE_SESSION_TOKEN and rerun"
  fi
  AVAILABLE_BREEZE_REQUEST_BUDGET=$((AVAILABLE_BREEZE_REQUEST_BUDGET - 1))
}

ensure_iceberg_table() {
  if [ "$DRY_RUN" = "1" ] || [ "$ENSURE_ICEBERG_TABLE" = "0" ]; then
    return
  fi
  echo "Ensuring 1-second Iceberg table exists..."
  ensure_log="$LOG_DIR/ensure_iceberg_table.log"
  if ! uv run sync-cash-second-data --ensure-table-only > "$ensure_log" 2>&1; then
    tail -n 20 "$ensure_log" >&2
    die "failed to ensure 1-second Iceberg table; check Cloudflare/R2 credentials"
  fi
}

write_plan() {
  plan_file="$1"
  budget="$2"
  fallback="$3"

  uv run python - "$BASE_FROM_DATE" "$TO_DATE" "$budget" "$fallback" "${#SYMBOL_CODE_VALUES[@]}" "${SYMBOL_CODE_VALUES[@]+"${SYMBOL_CODE_VALUES[@]}"}" "${SYMBOLS[@]}" > "$plan_file" <<'PY'
from __future__ import annotations

import sys
from datetime import timedelta

from tick_ticker.config import Settings
from tick_ticker.services.cash_second_data import CashSecondSyncManifest, cash_second_manifest_path
from tick_ticker.scripts.sync_cash_second_data import (
    count_missing_fetch_requests,
    parse_symbol_codes,
    resolve_cash_second_symbols,
)
from tick_ticker.utils.datetime import parse_date


def clean_note(value: str) -> str:
    return value.replace("\t", " ").replace("\n", " ")


def missing_requests(settings: Settings, symbol, from_date, to_date, manifest) -> int:
    return count_missing_fetch_requests(settings, symbol, from_date, to_date, manifest)


base_from = parse_date(sys.argv[1])
to_date = parse_date(sys.argv[2])
remaining_budget = int(sys.argv[3])
d1_fallback = sys.argv[4] == "1"
symbol_code_count = int(sys.argv[5])
symbol_code_end = 6 + symbol_code_count
explicit_codes = parse_symbol_codes(sys.argv[6:symbol_code_end])
raw_symbols = [symbol.upper() for symbol in sys.argv[symbol_code_end:]]

settings = Settings()
symbols = resolve_cash_second_symbols(
    settings,
    requested_symbols=raw_symbols,
    explicit_codes=explicit_codes,
    d1_fallback=d1_fallback,
)

print("kind\tsymbol\tfrom_date\tto_date\tmissing_requests\tbudget_cost\tnote")

for symbol in symbols:
    nse_symbol = symbol.nse_symbol.upper()
    start_floor = max(base_from, symbol.listing_date or base_from)
    manifest_path = cash_second_manifest_path(settings.data_dir, nse_symbol)
    manifest = CashSecondSyncManifest.load(manifest_path)
    if manifest is None:
        manifest_exists = False
        manifest = CashSecondSyncManifest(
            nse_symbol=nse_symbol,
            breeze_code=symbol.breeze_code,
            from_date=start_floor,
            to_date=to_date,
        )
    else:
        manifest_exists = True
    from_date = start_floor
    note_parts = [f"floor={start_floor}"]

    fully_uploaded = bool(manifest.fetched_files) and set(manifest.fetched_files).issubset(set(manifest.uploaded_files))
    if manifest.coverage_to_date and manifest.coverage_to_date >= start_floor and fully_uploaded:
        from_date = manifest.coverage_to_date + timedelta(days=1)
        note_parts.append(f"coverage_to={manifest.coverage_to_date}")
    elif manifest_exists and manifest.from_date:
        from_date = max(start_floor, manifest.from_date)
        note_parts.append(f"active_from={manifest.from_date}")
    else:
        note_parts.append("state=none")

    if from_date > to_date:
        print(f"SKIP\t{nse_symbol}\t{from_date}\t{to_date}\t0\t0\t{clean_note(';'.join(note_parts + ['already_complete']))}")
        continue

    total_missing = missing_requests(settings, symbol, from_date, to_date, manifest)
    if total_missing == 0:
        print(f"RUN\t{nse_symbol}\t{from_date}\t{to_date}\t0\t0\t{clean_note(';'.join(note_parts + ['local_files_present']))}")
        continue

    selected_end = None
    selected_missing = 0
    budget_cost = 0
    current = from_date
    while current <= to_date:
        day_missing = missing_requests(settings, symbol, current, current, manifest)
        incremental_cost = day_missing
        if day_missing and selected_missing == 0:
            incremental_cost += 1

        if incremental_cost and budget_cost + incremental_cost > remaining_budget:
            break

        selected_end = current
        budget_cost += incremental_cost
        selected_missing += day_missing
        current += timedelta(days=1)

    if selected_end is None:
        deferred_cost = total_missing + 1
        print(f"DEFER\t{nse_symbol}\t{from_date}\t{to_date}\t{total_missing}\t{deferred_cost}\t{clean_note(';'.join(note_parts + ['budget_exhausted']))}")
        continue

    print(f"RUN\t{nse_symbol}\t{from_date}\t{selected_end}\t{selected_missing}\t{budget_cost}\t{clean_note(';'.join(note_parts))}")
    remaining_budget -= budget_cost

    next_from = selected_end + timedelta(days=1)
    if next_from <= to_date:
        deferred_missing = missing_requests(settings, symbol, next_from, to_date, manifest)
        deferred_cost = deferred_missing + 1 if deferred_missing else 0
        print(f"DEFER\t{nse_symbol}\t{next_from}\t{to_date}\t{deferred_missing}\t{deferred_cost}\t{clean_note(';'.join(note_parts + ['remaining_range']))}")
PY
}

print_plan() {
  plan_file="$1"

  awk -F '\t' '{
    if ($1 == "kind") next
    printf "  %-6s %-12s %-10s %-10s missing=%-6s budget=%-6s %s\n", $1, $2, $3, $4, $5, $6, $7
  }' "$plan_file"
}

load_run_plan() {
  plan_file="$1"

  RUN_SYMBOLS=()
  RUN_FROM_DATES=()
  RUN_TO_DATES=()
  RUN_MISSING_REQUESTS=()
  RUN_BUDGET_COSTS=()

  while IFS=$'\t' read -r kind symbol from_date to_date missing_requests budget_cost note; do
    [ "$kind" = "kind" ] && continue
    if [ "$kind" = "RUN" ]; then
      RUN_SYMBOLS[${#RUN_SYMBOLS[@]}]="$symbol"
      RUN_FROM_DATES[${#RUN_FROM_DATES[@]}]="$from_date"
      RUN_TO_DATES[${#RUN_TO_DATES[@]}]="$to_date"
      RUN_MISSING_REQUESTS[${#RUN_MISSING_REQUESTS[@]}]="$missing_requests"
      RUN_BUDGET_COSTS[${#RUN_BUDGET_COSTS[@]}]="$budget_cost"
    fi
  done < "$plan_file"
}

wait_for_fetch_slot() {
  while [ "$(jobs -pr | wc -l | tr -d ' ')" -ge "$PARALLEL_FETCH_JOBS" ]; do
    sleep 1
  done
}

start_fetch_job() {
  idx="$1"
  cycle="$2"
  symbol="${RUN_SYMBOLS[$idx]}"
  from_date="${RUN_FROM_DATES[$idx]}"
  to_date="${RUN_TO_DATES[$idx]}"
  missing_requests="${RUN_MISSING_REQUESTS[$idx]}"
  log_file="$LOG_DIR/fetch_cycle${cycle}_${symbol}_${from_date}_${to_date}.log"
  max_requests=$((missing_requests + 2))

  echo "Starting 1-second fetch: $symbol $from_date..$to_date (missing=$missing_requests, log=$log_file)"
  (
    export BREEZE_MIN_REQUEST_INTERVAL_SECONDS="$PER_PROCESS_INTERVAL_SECONDS"
    uv run sync-cash-second-data \
      --fetch-only \
      --nse-symbol "$symbol" \
      --from-date "$from_date" \
      --to-date "$to_date" \
      --allow-large-range \
      --download-workers 1 \
      --breeze-max-requests "$max_requests" \
      "${SYMBOL_CODE_ARGS[@]+"${SYMBOL_CODE_ARGS[@]}"}" \
      "$(d1_arg)"
  ) > "$log_file" 2>&1 &

  PIDS[${#PIDS[@]}]="$!"
  PID_LABELS[${#PID_LABELS[@]}]="$symbol"
  PID_LOGS[${#PID_LOGS[@]}]="$log_file"
}

classify_fetch_failure() {
  log_file="$1"

  if grep -Eqi 'Session key is expired|SESSIONKEY_EXPIRED' "$log_file"; then
    echo "expired Breeze session"
    return 2
  fi
  if grep -Eqi 'No space left on device|Disk quota exceeded|OSError: \[Errno 28\]' "$log_file"; then
    echo "disk space"
    return 2
  fi
  if grep -Eqi 'ConnectError|ConnectionError|Network is unreachable|NameResolutionError|Temporary failure in name resolution|ConnectTimeout|ReadTimeout|timed out' "$log_file"; then
    echo "network"
    return 0
  fi
  if grep -Eqi '429|rate.?limit|too many requests|limit exceeded|limit is exceeded' "$log_file"; then
    echo "rate limit"
    return 0
  fi

  echo "transient Breeze/API error"
  return 0
}

estimate_failed_fetch_budget_cost() {
  log_file="$1"
  history_calls="$(grep -E -c 'Get Historical data V2 response|Exception in get_historical_data_v2' "$log_file" || true)"
  session_calls=0
  if grep -Eq 'cash_second_breeze_requests_reserved .*requests=[1-9][0-9]*' "$log_file"; then
    session_calls=1
  fi
  echo $((history_calls + session_calls))
}

run_fetch_phase() {
  cycle="$1"

  PIDS=()
  PID_LABELS=()
  PID_LOGS=()
  SUCCESS_SYMBOLS=()
  SUCCESS_FROM_DATES=()
  SUCCESS_TO_DATES=()
  FETCH_SOFT_FAILED=0
  FETCH_BUDGET_SPENT=0

  echo
  echo "1-second fetch phase $cycle"
  idx=0
  while [ "$idx" -lt "${#RUN_SYMBOLS[@]}" ]; do
    wait_for_fetch_slot
    start_fetch_job "$idx" "$cycle"
    idx=$((idx + 1))
  done

  idx=0
  while [ "$idx" -lt "${#PIDS[@]}" ]; do
    if wait "${PIDS[$idx]}"; then
      echo "1-second fetch done: ${PID_LABELS[$idx]}"
      SUCCESS_SYMBOLS[${#SUCCESS_SYMBOLS[@]}]="${RUN_SYMBOLS[$idx]}"
      SUCCESS_FROM_DATES[${#SUCCESS_FROM_DATES[@]}]="${RUN_FROM_DATES[$idx]}"
      SUCCESS_TO_DATES[${#SUCCESS_TO_DATES[@]}]="${RUN_TO_DATES[$idx]}"
      FETCH_BUDGET_SPENT=$((FETCH_BUDGET_SPENT + RUN_BUDGET_COSTS[$idx]))
    else
      reason_status=0
      reason="$(classify_fetch_failure "${PID_LOGS[$idx]}")" || reason_status="$?"
      if [ "$reason_status" -eq 2 ]; then
        echo "1-second fetch failed hard: ${PID_LABELS[$idx]} ($reason; see ${PID_LOGS[$idx]})" >&2
        die "halting because fetch hit $reason"
      fi
      actual_cost="$(estimate_failed_fetch_budget_cost "${PID_LOGS[$idx]}")"
      if [ "$actual_cost" -lt 1 ]; then
        actual_cost=1
      fi
      FETCH_BUDGET_SPENT=$((FETCH_BUDGET_SPENT + actual_cost))
      echo "1-second fetch soft-failed: ${PID_LABELS[$idx]} ($reason, estimated Breeze calls=$actual_cost; see ${PID_LOGS[$idx]})" >&2
      FETCH_SOFT_FAILED=1
    fi
    idx=$((idx + 1))
  done
}

run_upload_phase() {
  cycle="$1"

  if [ "${#SUCCESS_SYMBOLS[@]}" -eq 0 ]; then
    echo
    echo "No completed 1-second fetch jobs to upload in cycle $cycle."
    return
  fi

  echo
  echo "1-second upload phase $cycle"
  idx=0
  while [ "$idx" -lt "${#SUCCESS_SYMBOLS[@]}" ]; do
    symbol="${SUCCESS_SYMBOLS[$idx]}"
    from_date="${SUCCESS_FROM_DATES[$idx]}"
    to_date="${SUCCESS_TO_DATES[$idx]}"
    log_file="$LOG_DIR/upload_cycle${cycle}_${symbol}_${from_date}_${to_date}.log"

    echo "Uploading 1-second files: $symbol $from_date..$to_date (log=$log_file)"
    if ! uv run sync-cash-second-data \
      --upload-only \
      --nse-symbol "$symbol" \
      --from-date "$from_date" \
      --to-date "$to_date" \
      --allow-large-range \
      --upload-workers 1 \
      "${SYMBOL_CODE_ARGS[@]+"${SYMBOL_CODE_ARGS[@]}"}" \
      "$(d1_arg)" > "$log_file" 2>&1
    then
      die "1-second upload failed for $symbol; see $log_file"
    fi
    echo "1-second upload done: $symbol"

    idx=$((idx + 1))
  done
}

check_free_space
breeze_preflight
ensure_iceberg_table

echo "Selected bank cash 1-second backfill"
echo "  range floor: $BASE_FROM_DATE"
echo "  to date:     $TO_DATE"
echo "  budget:      $AVAILABLE_BREEZE_REQUEST_BUDGET Breeze calls available for cycle 1"
echo "  budget mode: $BREEZE_REQUEST_BUDGET_MODE"
echo "  fetch jobs:  $PARALLEL_FETCH_JOBS"
echo "  interval:    ${PER_PROCESS_INTERVAL_SECONDS}s per fetch process"
echo "  d1 fallback: $CASH_SECOND_D1_FALLBACK"
echo "  auto continue: $AUTO_CONTINUE"
echo "  logs:        $LOG_DIR"

cycle=1
while :; do
  check_free_space
  PLAN_FILE="$LOG_DIR/plan_cycle${cycle}.tsv"
  PLAN_ERROR_LOG="$LOG_DIR/plan_cycle${cycle}.err.log"
  if ! write_plan "$PLAN_FILE" "$AVAILABLE_BREEZE_REQUEST_BUDGET" "$CASH_SECOND_D1_FALLBACK" 2> "$PLAN_ERROR_LOG"; then
    tail -n 20 "$PLAN_ERROR_LOG" >&2
    die "planning failed; check local minute manifests or set CASH_SECOND_D1_FALLBACK=1"
  fi
  rm -f "$PLAN_ERROR_LOG"
  cp "$PLAN_FILE" "$LOG_DIR/plan.tsv"

  echo
  echo "Plan cycle $cycle:"
  print_plan "$PLAN_FILE"
  load_run_plan "$PLAN_FILE"

  if [ "${#RUN_SYMBOLS[@]}" -eq 0 ]; then
    echo
    echo "No runnable 1-second ranges remain for $TO_DATE."
    echo "Done. Plans and logs are in $LOG_DIR"
    exit 0
  fi

  PLAN_BUDGET_COST="$(awk -F '\t' '$1 == "RUN" { total += $6 } END { print total + 0 }' "$PLAN_FILE")"
  echo
  echo "Runnable symbols/ranges: ${#RUN_SYMBOLS[@]} (planned Breeze budget cost: $PLAN_BUDGET_COST, budget left before cycle: $AVAILABLE_BREEZE_REQUEST_BUDGET)"

  if [ "$DRY_RUN" = "1" ]; then
    echo "DRY_RUN=1, stopping before Breeze fetch/upload."
    exit 0
  fi

  run_fetch_phase "$cycle"

  if [ "$UPLOAD_AFTER_FETCH" = "0" ]; then
    echo
    echo "UPLOAD_AFTER_FETCH=0, leaving fetched 1-second files in local Parquet/manifests only."
    exit 0
  fi

  check_free_space
  run_upload_phase "$cycle"

  echo
  echo "Cycle $cycle complete. Estimated Breeze budget spent: $FETCH_BUDGET_SPENT."

  if [ "$AUTO_CONTINUE" = "0" ]; then
    echo "AUTO_CONTINUE=0, stopping after one cycle."
    break
  fi
  if [ "$BREEZE_REQUEST_BUDGET_MODE" = "run" ]; then
    AVAILABLE_BREEZE_REQUEST_BUDGET=$((AVAILABLE_BREEZE_REQUEST_BUDGET - FETCH_BUDGET_SPENT))
    if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 0 ]; then
      AVAILABLE_BREEZE_REQUEST_BUDGET=0
    fi
    echo "Budget left for this run: $AVAILABLE_BREEZE_REQUEST_BUDGET"
  else
    AVAILABLE_BREEZE_REQUEST_BUDGET="$DAILY_BREEZE_REQUEST_BUDGET"
    echo "Continuing with a fresh per-cycle Breeze planning budget: $AVAILABLE_BREEZE_REQUEST_BUDGET"
  fi
  if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 1 ]; then
    echo "Breeze budget exhausted for this run."
    break
  fi
  if [ "$FETCH_SOFT_FAILED" = "1" ] && [ "$SOFT_FETCH_RETRY_DELAY_SECONDS" -gt 0 ]; then
    echo "Soft fetch failures occurred; retrying after ${SOFT_FETCH_RETRY_DELAY_SECONDS}s."
    sleep "$SOFT_FETCH_RETRY_DELAY_SECONDS"
  fi

  cycle=$((cycle + 1))
done

echo
echo "Done. Plans and logs are in $LOG_DIR"
