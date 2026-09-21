# Provider data characteristics

Part of the [knowledge base](../README.md). See also:
[corporate actions](corporate-actions-and-identity.md) ·
[data quality audit](../engineering/data-quality-audit-2026-09.md) ·
[historical backfill](../engineering/historical-backfill.md).

Everything below was observed directly (probes and stored files, 2026-09-21)
unless marked otherwise.

| | Upstox History V3 | ICICI Breeze historical v2 |
|---|---|---|
| 1-minute history starts | 2022-01-01 | 2016-01-01 |
| Request unit | Month-bounded range, one request | ≤1000 candles per request (about 2 sessions) |
| Rate limits | 2000 requests / 30 min | 100 / min, 5000 / day |
| Credentials | Access token, expires daily | Session token, expires daily (manual login) |
| Instrument key | `NSE_EQ\|<current ISIN>`, `NSE_INDEX\|Nifty 50` | ICICI `stock_code` (e.g. `RELIND`, `HDFBAN`) |
| Delisted securities | **Rejected** (`UDAPI100011`: HDFC, TATAMTRDVR, old NESTLEIND ISIN) | Expected to work; to be verified per code |
| Candles per normal day | 375 (09:15–15:29) | 375–378: adds a 09:07 pre-open candle; NIFTY also has 15:30/15:31 |
| Timestamp label | Candle start, `+05:30` | Candle start, IST without an offset |
| Price adjustment | Adjusted for splits and bonuses as of the fetch date; price and volume together | Inconsistent: RELIANCE 2016 shows ~₹500 (2017 bonus applied, actual was ~₹1,000), but Jan 2022 shows ₹2,365 (2024 bonus not applied; Upstox ₹1,257). Traded value per candle still looks consistent |
| Index volume | 0 | 0, except 2021-03-01..2021-12-31 (unexplained non-zero values) |
| Demerger parent | Not adjusted for demergers (TMPV 2022 shows actual ~₹490) | Not checked |

## Price adjustment

Upstox returns history back-adjusted for later corporate actions, using the
ratios known on the day you fetch. So:

- Two fetches of the same day, one before and one after a split, differ by the split ratio.
- Mixing Breeze and Upstox rows in one symbol's history can create false jumps. This happened for RELIANCE and 12 banks in 2022–2026; it is now fixed ([audit](../engineering/data-quality-audit-2026-09.md)).
- Traded value per candle (price × volume) is the same on every basis. That is the property the [synthetic volume](synthetic-index-volume.md) relies on.

## Timestamps

Both providers label a candle by its **start minute** in IST. The loader strips
the offset and stores naive IST timestamps (`datetime` column). All joins are
by this naive IST minute; no UTC conversion happens anywhere.

## Listing dates

NSE's `EQUITY_L.csv` "date of listing" is the date the current ISIN was listed,
not the company's first trading day. NESTLEIND shows 2023-08-01. Do not use it
as a history floor for securities with an ISIN change.

## Day completeness

Upstox historical data for a day is only final after that session. A fetch
that includes the current day returns nothing or a partial day. The syncs now
stop at yesterday (IST) and re-check recent sessions every day
([daily cron](../engineering/daily-cron.md)).
