# Work summary: NIFTY 50 volume and cash 1-minute data, September 2026

Written for the data-collection owner and the data team. Part of the
[knowledge base](../README.md). Each item links to the page with the detail.

## What was asked

1. Generate a synthetic volume for NIFTY 50 from its constituents, based on the basket in force on each day.
2. Review an LLM-written analysis of the basket CSV (renames, mergers, demergers). Fix what was wrong and design D1 storage for the NIFTY history.
3. Make the cash 1-minute sync ready for a daily cron. Document the market side and the engineering side separately, as one linked knowledge base.

## Review of the LLM analysis: what held up and what didn't

| LLM claim | Verdict |
|---|---|
| The formula measures constituent turnover in index units, not index-weighted volume | **Correct.** Kept, and documented as such ([formula](../market/synthetic-index-volume.md)). |
| It is split-safe only if price and volume are adjusted consistently | **Correct**, and verified: Upstox adjusts both together. |
| Missing demerger placeholders: JIOFIN 2023, ITC Hotels 2025 | **Correct**, and TMCV 2025 was missing too. Exact dates are now verified from NSE press releases. |
| HDFC's last trading day was 2023-07-12, not 06-12; keep HDFC separate from HDFCBANK | **Correct.** |
| The first 2015 snapshots are wrong (Bosch joined in May 2015) | Correct, but **irrelevant**: minute data starts in 2016. |
| Don't enforce exactly 50 securities (DVRs) | **Correct.** The validator counts companies, allowing for DVRs and placeholders. |
| The datetime helper "strips timezone without converting" | **Not a bug.** Both providers send IST, and naive IST is stored consistently ([timestamps](../market/provider-data-characteristics.md#timestamps)). |
| Store synthetic volume in new columns | **Not adopted.** It goes into `volume`, matching the data team's plan ([decision](decisions.md#synthetic-volume-goes-into-the-volume-column)). |
| Five D1 tables including snapshot "basket versions" | **Replaced** by interval membership plus identity tables ([schema](d1-reference-schema.md)). |

What it **missed**, found in this review:
- **The basket CSV removes LUPIN six months early.** It left on 2018-09-28, not 2018-04-02.
- **11 symbols had duplicated candles in Iceberg.**
- **13 symbols mixed Breeze and Upstox price bases.**
- **The sync wrote empty files for unfinished days and recorded dates it never fetched.**
- **NESTLEIND was missing 18 months** because of NSE's listing-date reset.

Full list: [data quality audit](data-quality-audit-2026-09.md).

The data team's notes (Tejasvi) all match the verified facts:
- TMCV was in the index briefly.
- The HDFC-from-HDFCBANK factor doesn't work, so real HDFC data is needed.
- Breeze should be used where Upstox has no history.

## What was built

| Area | Change | Where |
|---|---|---|
| Reference data | Verified NIFTY 50 membership 2015–2026 as dated intervals (85 rows, 82 securities, every change linked to its NSE press release), security identity and rename history, provider mappings, corporate actions | [`reference/`](../../reference), [constituents](../market/nifty50-constituents.md) |
| D1 | 7 new tables plus a view (migration 002), loaded and checked: D1 matches the CSVs row for row | [D1 schema](d1-reference-schema.md) |
| Volume | `generate-index-volume`: point-in-time basket, completeness status, publish-only-complete, per-day state in D1, backfill plan output | [pipeline](index-volume-pipeline.md) |
| Iceberg writes | Append replaced by **replace** (delete + insert) everywhere in the 1-minute path | [decision](decisions.md#replace-never-append-when-publishing-to-iceberg) |
| Sync correctness | `to_date` clamped to yesterday (IST); `--synced-only`; `--ignore-listing-date`; D1 retries | [cash-sync.md](../cash-sync.md) |
| Repair | `repair-cash-data`: calendar-based gap audit, NIFTY probe for late sessions, repair / refetch / republish with backups | [pipeline overview](pipeline-overview.md) |
| Breeze | Session-aware request planning (2 sessions per request, skips holidays), misfiled-chunk bug fixed, fetch-only backfill wrapper | [historical backfill](historical-backfill.md) |
| Cron | `scripts/daily_cash_sync.sh`: sync → repair → volume, with a lock and logs | [daily cron](daily-cron.md) |
| Speed | Table metadata ensured once per process: per-symbol upload went from ~90 s to ~20–30 s | [daily cron](daily-cron.md#timing) |
| Tests | 84 passing, including Iceberg replace against a local catalog, volume math, reference validation and chunk planning | `tests/` |

## Data actions performed (2026-09-22)

- **D1:** migration 002 applied; reference data loaded.
- **Re-fetched from Upstox and republished** RELIANCE and 12 banks for 2022-01-01..2026-09-04. This removed the duplicates and the price-basis mix. The previous Breeze files are in `data/_superseded/`.
- **NESTLEIND** 2022-01..2023-07 fetched and published (391 sessions).
- **Gap repair from 2022:** 50 symbols, 92 requests, every gap filled.
- **TMCV and ITCHOTELS** synced from listing.
- **Forward sync:** every symbol (and NIFTY) is current to **2026-09-21**. A full re-audit over 2022-01-03..2026-09-21 (1,170 sessions, 134 symbols) reports zero gaps.
- **All 2022+ data now comes from Upstox on one adjustment basis:** the 13 mixed symbols plus a further 25 Breeze-sourced non-index symbols were re-fetched and republished.
- **NIFTY volume published for 792 complete days (2023-07-13..2026-09-21)**, locally and in Iceberg; verified with no duplicate rows. The 1,833 earlier days are recorded as `partial` in D1 with their missing members: all of 2016–2021 (Breeze backfill pending), plus 2022-01-03..2023-07-12, where HDFC is the only missing member. Cross-check: 2026-09-02 09:15 = 375,192, matching the independent estimate in the reviewed analysis.

## Still open

| Item | Why it is open | Next step |
|---|---|---|
| 2016–2021 constituents and HDFC (to 2023-07-12) | Needs Breeze; the session token had expired. About 36.7k requests, 8–9 days of quota. | [historical backfill](historical-backfill.md) |
| Breeze codes for delisted or renamed names | Not verifiable without a session | First backfill run; update `provider_mappings.csv` |
| Cron credentials | Upstox and Breeze tokens expire daily | Automate the token refresh, or refresh before 06:30 IST |
| Scheduler is a macOS launchd agent | `crontab` needs Full Disk Access; the Mac must be awake at 06:30 | Move to a server or keep the Mac awake ([daily cron](daily-cron.md)) |
| 1-second path (`sync-cash-second-data`) still appends | Out of scope (1-minute focus) | Port it to `replace_symbol_days("cash_1s", …)` |
| Storage-symbol procedure on future renames | Fetchers use the D1 current symbol | Follow the [rename procedure](decisions.md#storage-symbol-is-fixed-per-security) |
| Mixed adjustment basis in raw prices | Providers adjust at fetch time | Use `corporate_action` for return series; re-fetch symbols after splits if needed |

## Where to look

- **What a column or number means:** [market docs](../README.md#market-what-the-data-means)
- **Which stocks were in NIFTY on a date:** D1 `v_index_membership`, or [`reference/index_membership.csv`](../../reference/index_membership.csv)
- **Whether NIFTY volume on a date is real:** D1 `index_volume_state` (`status`, `published_at`, `missing_members`)
- **Why something was designed this way:** [decisions](decisions.md)
- **Run logs from this work:** `logs/repair_*_2026092*.log`, `logs/sync_*_20260922.log` (local only)
