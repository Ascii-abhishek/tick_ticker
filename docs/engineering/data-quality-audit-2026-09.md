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

## Answers to the data team's questions (2026-09-22)

### 1. Which symbols had the false price jump, and over what period?

Thirteen symbols were fetched by the Breeze bank script instead of Upstox for
**2022-01-03 .. 2026-07-24** (RELIANCE: .. 2026-07-06); later days came from
Upstox. Breeze prices are on a different adjustment basis and include a 09:07
pre-open candle.

The stored price level differed from the Upstox basis for **4 of them**
(`old ÷ new` price ratio, measured file by file):

| Symbol | Ratio and period | Reason |
|---|---|---|
| RELIANCE | 2.0982 (2022-01-03..2023-07-19), 2.0 (2023-07-20..2024-10-25), 1.0 after | 1:1 bonus ex 2024-10-28; the 4.9% step on 2023-07-20 is the Jio demerger |
| HDFCBANK | 2.0 (2022-01-03..2025-08-25), 1.0 after | 1:1 bonus ex 2025-08-27 |
| KOTAKBANK | 5.0 (2022-01-03..2026-01-13), 1.0 after | 1:5 split ex 2026-01-14 |
| BANKBARODA | drifts 1.10 → 1.03 (2022-01-03..2024-06-27), 1.0 after | looks like dividend adjustment in the Breeze series |

The other nine (ICICIBANK, SBIN, AXISBANK, INDUSINDBK, PNB, FEDERALBNK, AUBANK,
BANDHANBNK, IDFCFIRSTB) matched exactly (ratio 1.0); they had duplicates and
pre-open candles but correct price levels.

The jumps were visible as **single-day spikes**: days inside the Breeze window
that Breeze had missed were filled from Upstox, so they sat at a different
basis than their neighbours. For RELIANCE: 2022-10-24, 2023-02-03, 2023-07-20,
2023-09-18, 2023-11-12, 2024-11-01, 2025-01-06, 2025-10-14, 2025-10-21. A
further step existed where Breeze data ends and Upstox begins (2026-07-27, and
2026-08-03 for some).

**Fixed:** all 13 re-fetched from Upstox for 2022-01-01..2026-09-04 and
republished; the old files are in `data/_superseded/20260921T182813/`.

A further 25 symbols were Breeze-sourced in 2022+, none of them in NIFTY 50
(360ONE, 3MINDIA, AADHARHFC, AARTIIND, AAVAS, ABB, ABBOTINDIA, ABCAPITAL, ABDL,
ABLBL, ABSLAMC, ACMESOLAR, ACUTAAS, AEGISVOPAK, AFCONS, AIIL, ANANDRATHI,
ANGELONE, ANTHEM, ANURAS, ATHERENERG, CANHLIFE, CPPLUS, EMMVEE, TENNIND). They
were re-fetched from Upstox for 2022-01-01..2026-09-21 as well, so **no
Breeze-sourced day remains in the 2022+ data** (verified: no pre-open candles on
sampled days across 2022–2026).

**Synthetic volume was never affected**: it uses price × volume, which is
invariant to the adjustment basis, and it reads the local daily files, which
were never duplicated (the duplication was in Iceberg only).

### 2. Where were the empty-file gaps?

Audit of 2022-01-01..2026-09-20 (1,160 sessions) before the repair: 63 symbols
with gaps.

| Missing days | Symbols | Why |
|---|---|---|
| 2026-09-04 | 45 | fetched on the morning of 09-04, before the session existed |
| 2022-03-29 | 24 | Breeze returned nothing; the empty file was never retried |
| 2022-06-14 | 24 | same |
| 2026-07-27..07-31 | 7 (HDFCBANK, ICICIBANK, SBIN, KOTAKBANK, BANKBARODA, PNB, INDUSINDBK) | bank script stopped; state said "completed" to a later date |
| 2026-07-27..09-04 | 3 (BANDHANBNK, FEDERALBNK, IDFCFIRSTB) | same |
| 2026-07-07..07-31 | RELIANCE | empty file written at 01:12 on 2026-07-07 |
| 2026-07-06..09-04 | 4 (CANHLIFE, CPPLUS, EMMVEE, TENNIND) | same |
| 2026-09-03..09-04 | NIFTY | same |
| 2026-07-02 | 4 | same |
| 2022-09-01..2022-12-30 | 360ONE | Breeze range never refetched |
| 2022-10-19 | AARTIIND | single failed request |
| 2026-06-25 | TENNIND | single failed request |

All of these are filled. The repair logged no day where Upstox had no candles.
A re-audit on 2026-09-23 over 2022-01-03..2026-09-21 (1,170 sessions, all 134
symbols) reports **zero gaps**.

### 3. What period of NESTLEIND was missing?

**2022-01-03 .. 2023-07-31, 391 sessions.** NSE's equity list gives NESTLEIND a
"date of listing" of 2023-08-01, which is when its **ISIN changed**
(INE239A01016 → INE239A01024, 1:10 split, record date 2024-01-05 — NSE re-dated
the listing earlier). The sync used `listing_date` as the history floor, so it
never asked for anything before it. Upstox does serve the older candles under
the current ISIN, adjusted: February 2022 comes back near ₹930 rather than the
₹18,600 traded then. Now fetched and published.

### 4. Upstox has data from 2022, so why does published volume start 2023-07-13?

Because of **HDFC (INE001A01036)**. It was a NIFTY 50 constituent until its last
trading day, **2023-07-12** (merged into HDFC Bank; LTIM replaced it on 07-13),
and Upstox rejects its delisted ISIN with `UDAPI100011`. Its minute data can
only come from Breeze, which is not backfilled yet.

So from 2022-01-03 to 2023-07-12 every index day is one constituent short.
Those 378 days are marked `partial` in `index_volume_state`, with
`missing_members = ["HDFC"]`, and are deliberately **not published**: the volume
column stays 0 rather than carrying a sum that silently omits a constituent
worth roughly 6% of the index. 2023-07-13 is simply the first day the basket is
complete from Upstox alone.

Once the Breeze backfill lands, 2022-01-03..2023-07-12 gets published first, and
2016-2021 after that. Nothing else in that window is missing.
