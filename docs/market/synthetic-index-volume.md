# Synthetic index volume

Part of the [knowledge base](../README.md). See also:
[constituents](nifty50-constituents.md) ·
[index volume pipeline](../engineering/index-volume-pipeline.md) ·
[decision log](../engineering/decisions.md#synthetic-volume-goes-into-the-volume-column).

## Why it is needed

An index is a calculated number, not a traded instrument, so NIFTY 50 candles
have no volume. Upstox returns 0. Breeze returns 0, except for an unexplained
stretch from 2021-03-01 to 2021-12-31 that we do not rely on. Models trained on
stock candles expect a volume feature, so we derive one from the constituents.

## Formula (`ohlc4_turnover_over_close_v1`)

For each trading minute *t* (all index candles except the 09:00–09:14 pre-open
and 15:30–15:59 post-close windows) and the basket
*B(t)* in force on that trade date:

```text
turnover_t = Σ_{s ∈ B(t)} ((O_s,t + H_s,t + L_s,t + C_s,t) / 4) × V_s,t
volume_t   = round(turnover_t / C_NIFTY,t)
```

- `(O+H+L+C)/4 × V` approximates the rupee value traded in that minute.
- Dividing by the index close converts rupees into "index units": how many units of the index, at that minute's level, the constituents' traded value would buy. The result has a stable scale across years and index levels.
- Example: on 2026-09-02 at 09:15, the result is about 375k units.

### What it corrects and what it doesn't

| Effect | Handled? | Why |
|---|---|---|
| Stock splits and bonuses | **Yes** | Price × volume is unchanged by a split when price and volume are adjusted together (both providers do this). Tested in `tests/test_index_volume.py`. |
| Constituent changes | **Yes** | The basket is the one in force on that day, including DVRs and demerger placeholders. |
| Different share prices | **Yes** | A ₹100 stock and a ₹10,000 stock contribute by rupee value, not share count. |
| Index weights | **No** | NIFTY weights by free-float market cap. This measure weights by *traded value*: a heavily traded mid-weight stock counts more than its index weight. The measure is "constituent turnover in index units", not "index volume". |
| Exact traded value | **Approximate** | OHLC average × volume is not the VWAP; true turnover would need tick data. |
| Pre-open auction | **Excluded** | Breeze has a 09:07 pre-open candle; Upstox does not. It is excluded so both eras are comparable. NIFTY rows in the pre-open and post-close windows get volume 0. |
| Special sessions | **Included** | Excluding fixed windows, not keeping only 09:15–15:29, counts evening Muhurat trading (e.g. 2023-11-12, 18:15). |

A minute where a member has no candle counts as zero trades for that member.
That is correct for illiquid minutes. A member with **no candles for the whole
day** is treated as missing data, not as zero.

## Completeness rules

Each index day gets a status in D1 `index_volume_state`:

- **complete**: every basket member that could trade that day has candles. Placeholders before listing and delisted names after their last trade are "not trading", and their absence is expected.
- **partial**: at least one member that should have traded has no candles. Partial days are **not published** by default: the index keeps volume 0, and the missing members are listed in the state row. Scaling a partial sum up to 50 names would make up data.
- **no_index_data**: no NIFTY candles that day.

## Known limitations

1. Before 2022, constituents need Breeze history, which is still being backfilled ([plan](../engineering/historical-backfill.md)). Until it lands, 2016–2021 days are partial.
2. HDFC (to 2023-07-12) exists only on Breeze. Days from 2022-01-03 to 2023-07-12 stay partial until HDFC is fetched.
3. Stored prices mix adjustment bases (see [corporate actions](corporate-actions-and-identity.md#consequences-for-stored-data)). The volume formula is immune to this; raw price series are not.
4. For coarser candles (5-minute, daily), **sum the minute volumes**. Recomputing from aggregated OHLC gives a different number.
5. If the formula changes, bump `FORMULA_VERSION` in `services/index_volume.py` and republish. The state table records the version for each day.

## Checking it

```sql
-- Days not yet complete, most recent first
SELECT trade_date, status, observed_members, expected_members, missing_members
FROM index_volume_state WHERE index_code = 'NIFTY50' AND status != 'complete'
ORDER BY trade_date DESC LIMIT 20;
```

Sanity checks worth running after any backfill:
- Daily totals should move smoothly across rebalance dates. A jump on an effective date points to a wrong basket.
- `minutes_with_turnover` should be close to `session_minutes`, which is 375 on a normal day.
