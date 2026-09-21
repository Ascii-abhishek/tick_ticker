# Data quality audit, 2026-09

Part of the [knowledge base](../README.md). See also:
[decisions](decisions.md) · [provider characteristics](../market/provider-data-characteristics.md).

Audit of 1-minute cash data (local Parquet, Iceberg `cash.ohlcv_by_symbol`,
D1 sync state) done on 2026-09-21/22 before generating NIFTY volume. D1 showed
134 symbols as `completed`. That status turned out not to prove coverage.

## Findings

| # | Finding | Scope | Root cause |
|---|---|---|---|
| 1 | **Duplicate candles in Iceberg**: about 2× rows per unique timestamp | 11 symbols (RELIANCE, HDFCBANK, ICICIBANK, SBIN, AXISBANK, KOTAKBANK, BANKBARODA, PNB, INDUSINDBK, plus a few 2023 rows in FEDERALBNK and IDFCFIRSTB), 2022–2026 | Uploads were append-only, deduplicated only by source path in snapshot metadata. The files were uploaded again (bank-script run, then Upstox run, after the table rebuild), and the check didn't catch it. |
| 2 | **Mixed provider and price basis in one series** | RELIANCE and 12 banks, 2022-01 to 2026-07 | `fetch_bank_cash_from_2022.sh` used Breeze: unadjusted for later bonuses, with pre-open candles. Later Upstox runs skipped existing files, so e.g. RELIANCE jumped from ₹2,365 (Breeze) to ₹1,257 (Upstox) between days. |
| 3 | **Empty files for real sessions**, never retried | e.g. RELIANCE 2026-07-07..31; banks 2026-07-27..31; many symbols 2022-03-29 and 2022-06-14 | Syncs ran with `to_date = today`. Files for the running or future session were written empty (RELIANCE 2026-07-07 was created at 01:12 that day), and any existing file counts as fetched. |
| 4 | **D1 `to_date` ahead of real data** | e.g. `to_date` 2026-09-06 but data to 09-03/04 (NIFTY, ACC, …) | Completion recorded the *requested* end date. The next run started after it, so the gap was permanent. |
| 5 | **NESTLEIND missing 2022-01..2023-07** | 391 sessions | `listing_date` 2023-08-01, copied from NSE after its ISIN change, was used as the history floor. |
| 6 | **Stale symbols** | CANHLIFE, CPPLUS, EMMVEE, TENNIND: no data after 2026-07-03; IDFCFIRSTB, FEDERALBNK, BANDHANBNK after 2026-07-24 | Same as 3 and 4. |
| 7 | **Constituents never synced** | HDFC, CAIRN, TATAMTRDVR (delisted; not on Upstox), TMCV, ITCHOTELS | Not in the sync lists. The first three need Breeze. |
| 8 | **No pre-2022 constituent data** | 67 of 69 historical members | Upstox starts in 2022; only NIFTY and RELIANCE had Breeze history. |
| 9 | NIFTY missing 31 sessions in 2016–2017 | Breeze era | Fetched with empty results at the time; needs Breeze. |
| 10 | Latent bug: multi-day Breeze chunks misfile rows | Only if `CASH_HISTORY_CHUNK_DAYS > 1` | The chunk-start file name was used for rows of another date. |

Everything else matched: for every other symbol and year, Iceberg day counts
equalled the local non-empty day counts.

## Fixes applied (2026-09-22)

| Finding | Code change | Data action |
|---|---|---|
| 1 | Uploads now **replace** symbol/days (`replace_parquet_files`, `replace_symbol_range`) | 13 symbols republished 2022-01-01..2026-09-04, one commit each; now 1,160 days and ~433k rows per symbol, no duplicates |
| 2 | — | Same 13 symbols **re-fetched from Upstox** for 2022-01..2026-09-04. The Breeze files are kept under `data/_superseded/20260921T*/` |
| 3, 4 | `to_date` clamped to yesterday (IST); `repair-cash-data` audits files against the trading calendar | `repair-cash-data --repair-missing` from 2022: 50 symbols, 92 requests, every gap filled (no day where the provider had no candles) |
| 5 | `--ignore-listing-date`; `security_master.first_trade_date` left blank for NESTLEIND | NESTLEIND 2022-01..2023-07 fetched and published |
| 6 | `--synced-only` daily sync plus repair window | Brought current by the forward sync |
| 7 | Reference data lists them with provider availability | TMCV and ITCHOTELS synced from Upstox. HDFC, CAIRN and TATAMTRDVR are in the [Breeze plan](historical-backfill.md). |
| 8, 9 | Calendar-aware Breeze planning; backfill wrapper | Pending: needs a Breeze session (about 8–9 days of quota) |
| 10 | Rows always filed by their own `trade_date` | — |

## How to re-run the audit

```bash
uv run repair-cash-data --from-date 2022-01-01 --report data/state/coverage_report.json
uv run generate-index-volume --from-date 2016-01-01 --dry-run --report data/state/nifty_volume_report.json
```

Duplicate check against Iceberg (slow, about 10 minutes for every symbol):
scan `nse_symbol, trade_date, datetime` per symbol and compare the row count
with the count of unique `datetime` values.
