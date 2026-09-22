# Runbook: an index constituent change

Part of the [knowledge base](../README.md). See also:
[D1 reference schema](d1-reference-schema.md) ·
[constituents](../market/nifty50-constituents.md) ·
[index volume pipeline](index-volume-pipeline.md).

Volume generation reads the basket **in force on each trade date**, so a change
is applied by editing dated rows, never by rewriting history. Nothing needs to
be recomputed by hand: a changed basket changes the day's membership
fingerprint, and the day is recomputed on the next run.

## A. Ordinary review change (e.g. WIPRO out, BSE in, effective 2026-09-30)

1. **Edit [`reference/index_membership.csv`](../../reference/index_membership.csv)**
   - Leaving security: set `valid_to` to the **effective date** (the first session on the new basket). It stays a member through the previous session.
   - Entering security: add a row with `valid_from` = effective date, `valid_to` empty, `membership_type = regular`, `symbol_as_announced`, the press-release URL in `inclusion_source`, and `verification = pending` until it takes effect (then `verified`).
2. **Make sure the incoming security exists and is synced.**
   - In [`reference/securities.csv`](../../reference/securities.csv) with a `storage_symbol`, and in D1 `equity_symbol_reference` (the fetchers use it).
   - Data must exist from the effective date: `uv run sync-cash-upstox-data --nse-symbols BSE --from-date 2026-09-01`.
3. **Load:** `uv run load-reference-data` (validates keys, intervals, overlaps and basket size first; `--validate-only` to check alone).
4. **Nothing else.** The daily cron recomputes the trailing sessions. For older dates: `uv run generate-index-volume --from-date <effective> --publish`.

Already staged: WIPRO's `valid_to` is 2026-09-30 and BSE's row is `pending` from
2026-09-30. BSE is already synced, so on 2026-09-30 the basket switches by
itself. Flip both rows to `verified` when it happens.

## B. Rename of a current constituent (e.g. TATAMOTORS → TMPV)

1. Close the old symbol in [`reference/security_identifiers.csv`](../../reference/security_identifiers.csv) (`valid_to` = change date) and add the new one.
2. **Keep `storage_symbol` unchanged** so history stays in one Iceberg partition.
3. If D1 `equity_symbol_reference.nse_symbol` is renamed too, follow the [rename procedure](decisions.md#storage-symbol-is-fixed-per-security) so new candles don't land under a second name.
4. `load-reference-data`. Membership rows need no change: they key on `security_id`.

## C. Demerger with a placeholder (e.g. TMCV)

1. Add the new company to `securities.csv` with `first_trade_date` = its **listing** date, so the pipeline knows it cannot trade before then.
2. Add a membership row: `valid_from` = the parent's **ex-date**, `membership_type = demerger_placeholder`, `symbol_as_announced` = the dummy symbol (DUMMYTATAM, DUMMYITC).
3. When NSE announces the exclusion, set `valid_to` to that effective date.
4. Add the event to `corporate_actions.csv`.
5. Sync the new security from its listing date, then `load-reference-data`.

The basket then has 51 securities for those weeks, which is correct. Days before
listing count the placeholder as "not trading", so they still qualify as
complete.

## D. A constituent stops trading (merger, suspension)

1. In `securities.csv`: `status`, `last_trade_date`, `successor_security_id`.
2. Close the membership at the replacement's effective date.
3. Mark the Upstox mapping `unavailable` in `provider_mappings.csv` once the ISIN is rejected, and plan a Breeze fetch for the tail of its history (as with HDFC).

## Checks after any change

```bash
uv run load-reference-data --validate-only     # intervals, overlaps, basket size
uv run generate-index-volume --from-date <effective-date> --dry-run
```

The dry run prints the status per day. A day that turns `partial` right after a
change usually means the incoming security has no candles yet.

```sql
SELECT trade_date, status, observed_members, missing_members
FROM index_volume_state WHERE index_code='NIFTY50' AND trade_date >= '<effective-date>';
```
