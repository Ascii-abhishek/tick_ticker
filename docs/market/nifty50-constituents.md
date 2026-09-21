# NIFTY 50 constituents

Part of the [knowledge base](../README.md). See also:
[corporate actions](corporate-actions-and-identity.md) ·
[synthetic volume](synthetic-index-volume.md) ·
[D1 reference schema](../engineering/d1-reference-schema.md).

## How membership changes

- **Semi-annual reviews.** NSE Indices reviews the index in February and August, using average free-float market cap and liquidity. Changes take effect on the first trading day after the last Thursday/Friday of **March and September** (for example 2025-03-28, 2025-09-30). Some reviews change nothing (Sept 2016, Sept 2021, Mar 2023, Sept 2023, Mar 2026).
- **Ad hoc changes.** These happen between reviews when a constituent merges, delists, is suspended or restructures:
  - HDFC → LTIM on 2023-07-13
  - YESBANK → SHREECEM on 2020-03-19
  - VEDL → HDFCLIFE on 2020-07-31
  - GRASIM → VEDL on 2017-05-26
- **DVR share classes.** Differential voting right shares are counted as a separate security under the same company. From 2016-04-01 to 2017-09-28 the index held **51 securities for 50 companies**, TATAMOTORS plus TATAMTRDVR.
- **Demerger placeholders** (rule since 2023-04-26). When a constituent spins off a business, NSE adds the unlisted spun-off entity as an extra security from the ex-date. It is held at a fixed price, sometimes zero. It stays in the index until a few days after it lists, then is excluded. For those weeks the index again holds **51 securities**. It happened three times:

  | Parent | Placeholder | In index | Listed | Excluded from |
  |---|---|---|---|---|
  | RELIANCE | Jio Financial Services (JIOFIN) | 2023-07-20 | 2023-08-21 | 2023-09-07 |
  | ITC | ITC Hotels (DUMMYITC → ITCHOTELS) | 2025-01-06 | 2025-01-29 | 2025-02-10 |
  | TATAMOTORS | Tata Motors CV (DUMMYTATAM → TMCV) | 2025-10-14 | 2025-11-12 | 2025-11-17 |

  A placeholder has no trades before it lists. It contributes real traded value only on the days between listing and exclusion (TMCV: 2025-11-12 to 2025-11-14).

"Effective date" throughout means the **first trading day on the new basket**.
NSE announces changes "effective from close of" the previous trading day.

## Verified history

Every row below is backed by an NSE Indices press release,
`https://www.niftyindices.com/Press_Release/<file>`. The machine-readable
version is [`reference/index_membership.csv`](../../reference/index_membership.csv),
also stored in D1 as `index_membership`.

| Effective | Out | In | Press release |
|---|---|---|---|
| 2015-09-28 | NMDC | ADANIPORTS | news, start of our basket |
| 2016-04-01 | CAIRN, PNB, VEDL | AUROPHARMA, INFRATEL, EICHERMOT, TATAMTRDVR | ind_prs22022016_2.pdf |
| 2017-03-31 | BHEL, IDEA | IBULHSGFIN, IOC | ind_prs16022017.pdf |
| 2017-05-26 | GRASIM | VEDL | ind_prs27042017.pdf |
| 2017-09-29 | ACC, BANKBARODA, TATAPOWER, TATAMTRDVR | BAJFINANCE, HINDPETRO, UPL | ind_prs28082017.pdf |
| 2018-04-02 | AMBUJACEM, AUROPHARMA, BOSCHLTD | BAJAJFINSV, GRASIM, TITAN | ind_prs21022018.pdf |
| 2018-09-28 | LUPIN | JSWSTEEL | ind_prs28082018.pdf |
| 2019-03-29 | HINDPETRO | BRITANNIA | ind_prs25022019.pdf |
| 2019-09-27 | IBULHSGFIN | NESTLEIND | ind_prs28082019.pdf |
| 2020-03-19 | YESBANK | SHREECEM | ind_prs16032020.pdf |
| 2020-07-31 | VEDL | HDFCLIFE | ind_prs02072020_1.pdf |
| 2020-09-25 | INFRATEL, ZEEL | DIVISLAB, SBILIFE | ind_prs20082020.pdf |
| 2021-03-31 | GAIL | TATACONSUM | ind_prs23022021.pdf |
| 2022-03-31 | IOC | APOLLOHOSP | ind_prs24022022_1.pdf |
| 2022-09-30 | SHREECEM | ADANIENT | ind_prs01092022.pdf |
| 2023-07-13 | HDFC | LTIM | ind_prs04072023.pdf |
| 2023-07-20 | — | JIOFIN (placeholder) | ind_prs17072023.pdf |
| 2023-09-07 | JIOFIN | — | ind_prs05092023.pdf |
| 2024-03-28 | UPL | SHRIRAMFIN | ind_prs28022024.pdf |
| 2024-09-30 | DIVISLAB, LTIM | BEL, TRENT | ind_prs23082024.pdf |
| 2025-01-06 | — | ITC Hotels (placeholder) | ind_prs30122024.pdf |
| 2025-02-10 | ITCHOTELS | — | ind_prs06022025_1.pdf (NSE archive) |
| 2025-03-28 | BPCL, BRITANNIA | JIOFIN, ZOMATO (now ETERNAL) | ind_prs21022025.pdf |
| 2025-09-30 | HEROMOTOCO, INDUSINDBK | INDIGO, MAXHEALTH | ind_prs22082025.pdf |
| 2025-10-14 | — | TMCV (placeholder) | ind_prs07102025.pdf |
| 2025-11-17 | TMCV | — | ind_prs13112025.pdf |
| 2026-09-30 *(pending)* | WIPRO | BSE | ind_prs10082026.pdf |

## Corrections to the received basket CSV

The original [`extra/NIFTY50_historical_baskets_2015_2026.csv`](../../extra/NIFTY50_historical_baskets_2015_2026.csv)
is kept unchanged as source evidence. The reviewed membership differs from it in these ways:

1. **LUPIN exit date.** The CSV removes LUPIN on 2018-04-02. It actually left on 2018-09-28, replaced by JSWSTEEL. As written, the CSV basket has 49 names from April to September 2018.
2. **Three demerger placeholders are missing:** JIOFIN (2023), ITC Hotels (2025) and TMCV (2025). Their traded days inside the index belong in the volume sum.
3. **Renames while in the index.** Zomato traded as ZOMATO until 2025-04-08 and as ETERNAL from 2025-04-09. TATAMOTORS became TMPV on 2025-10-24. The CSV uses current names; the reference data keeps both through dated identifiers.
4. **Duplicate 2015 rows.** The first four 2015 snapshots are identical and not accurate for early 2015 (Bosch joined in May 2015). Minute data starts in 2016, so they don't matter. Our membership starts from the 2015-09-28 basket, which agrees with every later press release.
5. **2026-09-30 row.** It is confirmed (announced 2026-08-10) and is stored as `pending` until it takes effect. The CSV row count of 50 per snapshot is fine, but code should **not** enforce exactly 50 securities because of DVRs and placeholders.

From the data team (Tejasvi, 2026-09-20): the 2025 list "could have a little
change". Those changes are points 2 and 3. TMCV was indeed in the index briefly
(listed 2025-11-12, excluded 2025-11-17). He also said Breeze data should be
used wherever Upstox history does not reach.

## Symbols used for storage vs. announced symbols

The index membership stores a stable `security_id`. Candles are stored under
that security's **storage symbol**, which is the NSE symbol at the time we first
ingested it. Mapping used by NIFTY 50 history:

| Announced as | Stored as | Why |
|---|---|---|
| TATAMOTORS | TMPV | Same ISIN; renamed after the CV demerger |
| INFRATEL | INDUSTOWER | Same ISIN; renamed 2020-12-18 |
| IBULHSGFIN | SAMMAANCAP | Same ISIN; renamed 2024-07-26 |
| LTIM | LTM | Same ISIN; LTI → LTIM → LTM |
| ZOMATO | ETERNAL | Same ISIN; renamed 2025-04-09 |
| HDFC, CAIRN, TATAMTRDVR | unchanged | Delisted; history only from Breeze |

Details and the reasoning for each are in
[corporate-actions-and-identity.md](corporate-actions-and-identity.md).
