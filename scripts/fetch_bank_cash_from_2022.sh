#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

default_to_date() {
  if date -v-1d +%F >/dev/null 2>&1; then
    date -v-1d +%F
    return
  fi
  date -d yesterday +%F
}

BASE_FROM_DATE="${BASE_FROM_DATE:-2022-01-01}"
TO_DATE="${TO_DATE:-$(default_to_date)}"
PARALLEL_FETCH_JOBS="${PARALLEL_FETCH_JOBS:-3}"
DAILY_BREEZE_REQUEST_BUDGET="${DAILY_BREEZE_REQUEST_BUDGET:-4500}"
BREEZE_REQUESTS_ALREADY_USED="${BREEZE_REQUESTS_ALREADY_USED:-0}"
BREEZE_PREFLIGHT="${BREEZE_PREFLIGHT:-1}"
AUTO_CONTINUE="${AUTO_CONTINUE:-1}"
UPLOAD_AFTER_FETCH="${UPLOAD_AFTER_FETCH:-1}"
DRY_RUN="${DRY_RUN:-0}"
MIN_FREE_SPACE_GB="${MIN_FREE_SPACE_GB:-2}"
SOFT_FETCH_RETRY_DELAY_SECONDS="${SOFT_FETCH_RETRY_DELAY_SECONDS:-60}"
LOG_ROOT="${LOG_ROOT:-logs/cash-bank-backfill}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_DIR="$LOG_ROOT/$RUN_ID"

SYMBOLS=(
  HDFCBANK
  ICICIBANK
  SBIN
  AXISBANK
  KOTAKBANK
  BANKBARODA
  PNB
  FEDERALBNK
  INDUSINDBK
  AUBANK
  IDFCFIRSTB
  BANDHANBNK
)

die() {
  echo "error: $*" >&2
  exit 1
}

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
is_non_negative_int "$BREEZE_REQUESTS_ALREADY_USED" || die "BREEZE_REQUESTS_ALREADY_USED must be >= 0"
is_boolean_flag "$BREEZE_PREFLIGHT" || die "BREEZE_PREFLIGHT must be 0 or 1"
is_boolean_flag "$AUTO_CONTINUE" || die "AUTO_CONTINUE must be 0 or 1"
is_boolean_flag "$UPLOAD_AFTER_FETCH" || die "UPLOAD_AFTER_FETCH must be 0 or 1"
is_boolean_flag "$DRY_RUN" || die "DRY_RUN must be 0 or 1"
is_non_negative_int "$SOFT_FETCH_RETRY_DELAY_SECONDS" || die "SOFT_FETCH_RETRY_DELAY_SECONDS must be >= 0"

AVAILABLE_BREEZE_REQUEST_BUDGET=$((DAILY_BREEZE_REQUEST_BUDGET - BREEZE_REQUESTS_ALREADY_USED))
if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 0 ]; then
  die "BREEZE_REQUESTS_ALREADY_USED is greater than DAILY_BREEZE_REQUEST_BUDGET"
fi

DEFAULT_INTERVAL_SECONDS="$(awk -v jobs="$PARALLEL_FETCH_JOBS" 'BEGIN { printf "%.2f", jobs * 0.70 }')"
PER_PROCESS_INTERVAL_SECONDS="${BREEZE_MIN_REQUEST_INTERVAL_SECONDS:-$DEFAULT_INTERVAL_SECONDS}"

LOCK_DIR="${TMPDIR:-/tmp}/tick_ticker_bank_cash_backfill.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  die "another selected cash backfill appears to be running; remove $LOCK_DIR only if it is stale"
fi
trap 'rm -rf "$LOCK_DIR"' EXIT

mkdir -p "$LOG_DIR"

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
  echo "Checking Breeze session token before launching fetch workers..."
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

write_plan() {
  plan_file="$1"
  budget="$2"

  uv run python - "$BASE_FROM_DATE" "$TO_DATE" "$budget" "${SYMBOLS[@]}" > "$plan_file" <<'PY'
from __future__ import annotations

import sys
from datetime import timedelta

from tick_ticker.config import Settings
from tick_ticker.db.repositories import EquitySymbolReferenceRepository, MarketDataSyncStateRepository
from tick_ticker.services.cash_data import cash_local_path
from tick_ticker.utils.datetime import iter_date_chunks, parse_date
from tick_ticker.utils.engines import create_d1_client


def clean_note(value: str) -> str:
    return value.replace("\t", " ").replace("\n", " ")


def missing_requests(settings: Settings, symbol: str, from_date, to_date) -> int:
    requests = 0
    for chunk_start, _chunk_end in iter_date_chunks(from_date, to_date, chunk_days=settings.cash_history_chunk_days):
        if not cash_local_path(settings.data_dir, chunk_start, symbol).exists():
            requests += 1
    return requests


base_from = parse_date(sys.argv[1])
to_date = parse_date(sys.argv[2])
remaining_budget = int(sys.argv[3])
symbols = sys.argv[4:]

settings = Settings()
d1_client = create_d1_client(settings)
symbol_repo = EquitySymbolReferenceRepository(d1_client)
sync_repo = MarketDataSyncStateRepository(d1_client)
sync_repo.ensure_table()

print("kind\tsymbol\tfrom_date\tto_date\tmissing_requests\tbudget_cost\tnote")

for raw_symbol in symbols:
    nse_symbol = raw_symbol.upper()
    symbol = symbol_repo.get_by_nse_symbol(nse_symbol)
    if symbol is None:
        raise SystemExit(f"NSE symbol not found in equity_symbol_reference: {nse_symbol}")

    start_floor = max(base_from, symbol.listing_date or base_from)
    state = sync_repo.get_state(market_type="cash", nse_symbol=nse_symbol)
    from_date = start_floor
    note_parts = [f"floor={start_floor}"]

    if state is None:
        note_parts.append("state=none")
    else:
        note_parts.append(f"state={state.status}")
        if state.to_date:
            note_parts.append(f"state_to={state.to_date}")
        if state.from_date:
            note_parts.append(f"state_from={state.from_date}")
        if state.status == "completed" and state.to_date and state.to_date >= start_floor:
            from_date = state.to_date + timedelta(days=1)
        elif state.status in {"failed", "in_progress"} and state.from_date:
            from_date = max(start_floor, state.from_date)

    if from_date > to_date:
        print(f"SKIP\t{nse_symbol}\t{from_date}\t{to_date}\t0\t0\t{clean_note(';'.join(note_parts + ['already_complete']))}")
        continue

    total_missing = missing_requests(settings, nse_symbol, from_date, to_date)
    if total_missing == 0:
        print(f"RUN\t{nse_symbol}\t{from_date}\t{to_date}\t0\t0\t{clean_note(';'.join(note_parts + ['local_files_present']))}")
        continue

    selected_end = None
    selected_missing = 0
    budget_cost = 0

    for chunk_start, chunk_end in iter_date_chunks(from_date, to_date, chunk_days=settings.cash_history_chunk_days):
        chunk_missing = not cash_local_path(settings.data_dir, chunk_start, nse_symbol).exists()
        incremental_cost = 0
        if chunk_missing:
            incremental_cost = 1
            if selected_missing == 0:
                incremental_cost += 1  # one Breeze session-generation call for this process

        if incremental_cost and budget_cost + incremental_cost > remaining_budget:
            break

        selected_end = chunk_end
        budget_cost += incremental_cost
        if chunk_missing:
            selected_missing += 1

    if selected_end is None:
        deferred_cost = total_missing + 1
        print(f"DEFER\t{nse_symbol}\t{from_date}\t{to_date}\t{total_missing}\t{deferred_cost}\t{clean_note(';'.join(note_parts + ['budget_exhausted']))}")
        continue

    print(f"RUN\t{nse_symbol}\t{from_date}\t{selected_end}\t{selected_missing}\t{budget_cost}\t{clean_note(';'.join(note_parts))}")
    remaining_budget -= budget_cost

    next_from = selected_end + timedelta(days=1)
    if next_from <= to_date:
        deferred_missing = missing_requests(settings, nse_symbol, next_from, to_date)
        deferred_cost = deferred_missing + 1 if deferred_missing else 0
        print(f"DEFER\t{nse_symbol}\t{next_from}\t{to_date}\t{deferred_missing}\t{deferred_cost}\t{clean_note(';'.join(note_parts + ['remaining_range']))}")
PY
}

print_plan() {
  plan_file="$1"

  awk -F '\t' '{
    printf "  %-6s %-12s %-10s %-10s missing=%-5s budget=%-5s %s\n", $1, $2, $3, $4, $5, $6, $7
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

  echo "Starting fetch: $symbol $from_date..$to_date (missing=$missing_requests, log=$log_file)"
  (
    export BREEZE_MIN_REQUEST_INTERVAL_SECONDS="$PER_PROCESS_INTERVAL_SECONDS"
    uv run sync-cash-data \
      --fetch-only \
      --nse-symbol "$symbol" \
      --from-date "$from_date" \
      --to-date "$to_date" \
      --allow-large-range \
      --download-workers 1 \
      --breeze-max-requests "$max_requests"
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
  if grep -Eqi 'ConnectError|ConnectionError|Network is unreachable|nodename nor servname|NameResolutionError|Temporary failure in name resolution|ConnectTimeout|ReadTimeout|timed out' "$log_file"; then
    echo "network"
    return 2
  fi
  if grep -Eqi '429|rate.?limit|too many requests|limit exceeded|limit is exceeded' "$log_file"; then
    echo "rate limit"
    return 2
  fi

  echo "transient Breeze/API error"
  return 0
}

estimate_failed_fetch_budget_cost() {
  log_file="$1"
  history_calls="$(grep -E -c 'Get Historical data V2 response|Exception in get_historical_data_v2' "$log_file" || true)"
  session_calls=0
  if grep -Eq 'cash_breeze_requests_reserved .*requests=[1-9][0-9]*' "$log_file"; then
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
  echo "Fetch phase $cycle"
  idx=0
  while [ "$idx" -lt "${#RUN_SYMBOLS[@]}" ]; do
    wait_for_fetch_slot
    start_fetch_job "$idx" "$cycle"
    idx=$((idx + 1))
  done

  fetch_failed=0
  idx=0
  while [ "$idx" -lt "${#PIDS[@]}" ]; do
    if wait "${PIDS[$idx]}"; then
      echo "Fetch done: ${PID_LABELS[$idx]}"
      SUCCESS_SYMBOLS[${#SUCCESS_SYMBOLS[@]}]="${RUN_SYMBOLS[$idx]}"
      SUCCESS_FROM_DATES[${#SUCCESS_FROM_DATES[@]}]="${RUN_FROM_DATES[$idx]}"
      SUCCESS_TO_DATES[${#SUCCESS_TO_DATES[@]}]="${RUN_TO_DATES[$idx]}"
      FETCH_BUDGET_SPENT=$((FETCH_BUDGET_SPENT + RUN_BUDGET_COSTS[$idx]))
    else
      reason_status=0
      reason="$(classify_fetch_failure "${PID_LOGS[$idx]}")" || reason_status="$?"
      if [ "$reason_status" -eq 2 ]; then
        echo "Fetch failed hard: ${PID_LABELS[$idx]} ($reason; see ${PID_LOGS[$idx]})" >&2
        die "halting because fetch hit $reason"
      fi
      actual_cost="$(estimate_failed_fetch_budget_cost "${PID_LOGS[$idx]}")"
      if [ "$actual_cost" -lt 1 ]; then
        actual_cost=1
      fi
      FETCH_BUDGET_SPENT=$((FETCH_BUDGET_SPENT + actual_cost))
      echo "Fetch soft-failed: ${PID_LABELS[$idx]} ($reason, estimated Breeze calls=$actual_cost; see ${PID_LOGS[$idx]})" >&2
      FETCH_SOFT_FAILED=1
    fi
    idx=$((idx + 1))
  done
}

run_upload_phase() {
  cycle="$1"

  if [ "${#SUCCESS_SYMBOLS[@]}" -eq 0 ]; then
    echo
    echo "No completed fetch jobs to upload in cycle $cycle."
    return
  fi

  echo
  echo "Upload/status phase $cycle"
  idx=0
  while [ "$idx" -lt "${#SUCCESS_SYMBOLS[@]}" ]; do
    symbol="${SUCCESS_SYMBOLS[$idx]}"
    from_date="${SUCCESS_FROM_DATES[$idx]}"
    to_date="${SUCCESS_TO_DATES[$idx]}"
    log_file="$LOG_DIR/upload_cycle${cycle}_${symbol}_${from_date}_${to_date}.log"

    echo "Uploading: $symbol $from_date..$to_date (log=$log_file)"
    if ! uv run sync-cash-data \
      --upload-only \
      --nse-symbol "$symbol" \
      --from-date "$from_date" \
      --to-date "$to_date" \
      --allow-large-range \
      --upload-workers 1 > "$log_file" 2>&1
    then
      die "upload/status failed for $symbol; see $log_file"
    fi
    echo "Upload/status done: $symbol"

    idx=$((idx + 1))
  done
}

check_free_space
breeze_preflight

echo "Selected bank cash backfill"
echo "  range floor: $BASE_FROM_DATE"
echo "  to date:     $TO_DATE"
echo "  budget:      $AVAILABLE_BREEZE_REQUEST_BUDGET Breeze calls available for this run"
echo "  fetch jobs:  $PARALLEL_FETCH_JOBS"
echo "  interval:    ${PER_PROCESS_INTERVAL_SECONDS}s per fetch process"
echo "  auto continue: $AUTO_CONTINUE"
echo "  logs:        $LOG_DIR"

cycle=1
while :; do
  check_free_space
  PLAN_FILE="$LOG_DIR/plan_cycle${cycle}.tsv"
  PLAN_ERROR_LOG="$LOG_DIR/plan_cycle${cycle}.err.log"
  if ! write_plan "$PLAN_FILE" "$AVAILABLE_BREEZE_REQUEST_BUDGET" 2> "$PLAN_ERROR_LOG"; then
    tail -n 20 "$PLAN_ERROR_LOG" >&2
    die "planning failed; check internet/D1 credentials and rerun"
  fi
  rm -f "$PLAN_ERROR_LOG"
  cp "$PLAN_FILE" "$LOG_DIR/plan.tsv"

  echo
  echo "Plan cycle $cycle:"
  print_plan "$PLAN_FILE"
  load_run_plan "$PLAN_FILE"

  if [ "${#RUN_SYMBOLS[@]}" -eq 0 ]; then
    echo
    echo "No runnable ranges remain for $TO_DATE."
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
    echo "UPLOAD_AFTER_FETCH=0, leaving fetched files in local Parquet/manifests only."
    exit 0
  fi

  check_free_space
  run_upload_phase "$cycle"

  AVAILABLE_BREEZE_REQUEST_BUDGET=$((AVAILABLE_BREEZE_REQUEST_BUDGET - FETCH_BUDGET_SPENT))
  if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 0 ]; then
    AVAILABLE_BREEZE_REQUEST_BUDGET=0
  fi
  echo
  echo "Cycle $cycle complete. Estimated Breeze budget spent: $FETCH_BUDGET_SPENT. Budget left: $AVAILABLE_BREEZE_REQUEST_BUDGET"

  if [ "$AUTO_CONTINUE" = "0" ]; then
    echo "AUTO_CONTINUE=0, stopping after one cycle."
    break
  fi
  if [ "$AVAILABLE_BREEZE_REQUEST_BUDGET" -lt 1 ]; then
    echo "Breeze budget exhausted for this run. Rerun after the daily API window resets, or lower BREEZE_REQUESTS_ALREADY_USED if you know the budget is available."
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
