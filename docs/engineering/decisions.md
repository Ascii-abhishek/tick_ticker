# Decision log

Part of the [knowledge base](../README.md). Each entry gives the choice, the
alternatives, and the merits and costs, so it can be revisited with context.

## Synthetic volume goes into the `volume` column
**Chosen:** write the synthetic value into NIFTY's own `volume` column (int64), in the same Parquet and Iceberg rows.
**Alternatives:** new nullable columns (`synthetic_volume`, `estimated_turnover`, `volume_status`) on the shared cash schema; a separate table.
- **Merits:** consumers read NIFTY like any stock, with no schema change for 134+ symbols. The provider value it replaces is always 0, so nothing is lost. This matches how the data team planned to use it.
- **Costs:** there is no per-row flag in Iceberg saying the volume is synthetic or that a day is unpublished (0). That information lives in D1 `index_volume_state` and in the snapshot summary. Turnover is not stored per minute (daily totals are in D1).
- **Revisit if** other indices or consumers need per-minute provenance. A schema-evolution column can be added later without rewriting data.

## Membership as dated intervals in D1, reviewed as CSV in git
**Chosen:** `index_membership` rows with `[valid_from, valid_to)`, loaded from `reference/index_membership.csv` by a validating loader.
**Alternatives:** store the basket snapshots as received; D1 as the only copy.
- **Merits:** point-in-time lookup is one range filter. Each change is a one-row edit. Overlaps and wrong basket sizes are caught mechanically. Git keeps review history and source links; D1 keeps runtime access and ad hoc SQL.
- **Costs:** two copies. The loader is the only writer, and `--prune` keeps D1 equal to git. Direct D1 edits would be overwritten by the next load.

## Stable `security_id`, never a symbol string, for joins across time
**Chosen:** `security_master` plus `security_identifier_history` (dated symbols and ISINs), with provider keys in their own table.
**Alternatives:** globally replace old symbols with new ones in the basket; key by ISIN.
- **Merits:** renames, mergers (HDFC ≠ HDFCBANK), demergers (TMPV ≠ TMCV) and ISIN changes (NESTLEIND) each map correctly. ISIN alone fails for ISIN changes and DVR classes.
- **Costs:** one more lookup table to maintain when NSE renames something.

## Storage symbol is fixed per security
**Chosen:** candles for a security are always stored under one `nse_symbol` (its `storage_symbol`). Today that is the D1 symbol at first ingestion (TMPV, LTM, ETERNAL…).
- **Why:** Iceberg is partitioned by `nse_symbol`. Splitting one security's history across two names breaks symbol queries and the index join.
- **Open risk:** the fetchers write under `equity_symbol_reference.nse_symbol`. If a future rename is applied to that table, new candles land under the new name. **Procedure on rename:** either keep `equity_symbol_reference.nse_symbol` as the storage symbol and record the new ticker only in `security_identifier_history`, or rewrite the old partition under the new name (`repair-cash-data --republish` after moving the local files) and update `storage_symbol`. Choose one per case; don't leave history split.

## Replace, never append, when publishing to Iceberg
**Chosen:** every write deletes the symbol/days it covers and inserts the new rows in one snapshot.
**Alternative (previous):** append, skipping files whose path appears in snapshot metadata.
- **Why:** the path check is fragile. 11 symbols had doubled rows for 2022–2026 ([audit](data-quality-audit-2026-09.md)). Replace makes retries, re-uploads and repairs safe.
- **Cost:** a replace reads file metadata in the affected partitions, so it is slightly slower than a blind append. The fetch dominates anyway.

## Syncs stop at yesterday (IST)
**Chosen:** `to_date` defaults to, and is clamped to, `last_completed_session_bound()`.
- **Why:** fetching the running session produced empty or partial files. Those were later treated as complete, and D1 recorded a `to_date` beyond the real data, leaving permanent gaps.
- **Cost:** data arrives the next morning, which is fine for a daily cron.

## Gaps are audited from files against a derived trading calendar
**Chosen:** the calendar is the NIFTY days plus any day where ≥20 symbols (≥3 for Breeze planning) have candles.
**Alternative:** a holiday list.
- **Merits:** no external holiday file to maintain. Special sessions (Muhurat, budget Saturdays, DR drills) are picked up automatically.
- **Cost:** a session that *no* symbol has yet (the provider published it late) would be invisible. `repair-cash-data --repair-missing` handles this by probing NIFTY for weekdays after the last known session. If NIFTY has candles, the day joins the calendar and every symbol missing it is repaired in the same run. A holiday costs one NIFTY request and stops being probed once a later session exists. This was seen on 2026-09-22: Upstox had nothing yet for 2026-09-21 at 00:50 IST.

## Only complete days get synthetic volume
**Chosen:** a day with any missing trading member keeps volume 0 and is marked `partial`.
**Alternatives:** publish partial sums; scale up by the missing weight.
- **Why:** partial sums create artificial drops, and scaling invents data. Status and missing members are queryable, so consumers can filter.

## Breeze backfill is fetch-only, then republished
**Chosen:** `sync-cash-data --fetch-only` followed by `repair-cash-data --republish`, driven by a plan generated from missing members.
- **Why:** a normal sync over an old range rewrites the symbol's D1 sync state and manifest range, which would derail the daily Upstox sync. Fetch-only leaves D1 alone. The republish marks the manifest range uploaded.

## Future data types
The same pieces carry over:
- **1-second cash:** `replace_symbol_days("cash_1s", …)`, gap audit per day.
- **Futures and options:** `security_id` as the underlying key; expiry/strike reference tables already exist in D1.
- **Other indices:** add rows to `index_definition` and `index_membership`.

The volume formula is index-agnostic.
