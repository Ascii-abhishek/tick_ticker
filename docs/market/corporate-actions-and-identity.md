# Corporate actions and security identity

Part of the [knowledge base](../README.md). See also:
[constituents](nifty50-constituents.md) ·
[provider characteristics](provider-data-characteristics.md) ·
[D1 reference schema](../engineering/d1-reference-schema.md).

A ticker symbol is not an identity. Over ten years, NIFTY 50 constituents
renamed, merged, demerged and changed ISINs. Each event has different
consequences for a minute-candle dataset.

## Identity model

| Concept | Meaning | Changes when |
|---|---|---|
| `security_id` | Our stable key for one tradable security | Never |
| `storage_symbol` | The `nse_symbol` value in Parquet/Iceberg rows | Never (set at first ingestion) |
| NSE symbol | Exchange ticker | Renames |
| ISIN | Depository identifier | Some splits and face-value changes, share-class changes |
| Provider key | Upstox `NSE_EQ\|<ISIN>`, Breeze `stock_code` | Follows ISIN (Upstox) or ICICI's own code (Breeze) |

Each identifier is stored with its own validity interval in
`security_identifier_history`, so the question "what was this called on date X"
has one answer.

## Event types and how each is handled

### Rename (same ISIN)
Examples: TATAMOTORS→TMPV, ZOMATO→ETERNAL, LTI→LTIM→LTM, INFRATEL→INDUSTOWER,
IBULHSGFIN→SAMMAANCAP, TATAGLOBAL→TATACONSUM, SRTRANSFIN→SHRIRAMFIN.

- Same company, same share, continuous price history. Upstox returns the whole history under the current ISIN.
- **Handling:** store under one storage symbol. Record each old symbol as an `nse_symbol` identifier with dates. Never duplicate the history under two names.
- **Watch out:** if a renamed stock is ever re-fetched under its new D1 symbol, new rows would land under a different `nse_symbol` than old ones. See [decisions: storage symbol](../engineering/decisions.md#storage-symbol-is-fixed-per-security).

### Merger (target disappears)
Examples: HDFC into HDFCBANK (2023-07-01; last trade 2023-07-12; 42 HDFCBANK per 25 HDFC), CAIRN into VEDL (2017).

- The acquired company's shares stop trading and it is removed from the index.
- **HDFC is not a rename of HDFCBANK.** Both traded, separately, until 2023-07-12. Both were in NIFTY 50 and both contribute to its volume.
- Tejasvi tried scaling HDFCBANK volume by a factor to stand in for HDFC. It did not work, because the volume shift around the merger was gradual, not a step. **Decision:** use real HDFC candles from Breeze (Upstox rejects the delisted ISIN). Days without them stay `partial`.
- **Handling:** a separate `security_id` with `status = merged`, `last_trade_date` and `successor_security_id`.

### Demerger (new company spun out)
Examples: RELIANCE→JIOFIN (2023), ITC→ITCHOTELS (2025), TATAMOTORS→TMCV (2025), GRASIM/ABNL (2017).

- The parent's price drops on the ex-date by the value of the spun-off business. The new company lists weeks later.
- NSE keeps the spun-off entity in the index as a placeholder until shortly after it lists (see [constituents](nifty50-constituents.md#how-membership-changes)).
- **Tata Motors specifically:** the *old* listed company (ISIN INE155A01022) kept the passenger-vehicle business and was renamed Tata Motors Passenger Vehicles (**TMPV**). The *new* listed company, TML Commercial Vehicles, is confusingly also named "Tata Motors Limited" (**TMCV**, INE1TAE01010). So TATAMOTORS history belongs to TMPV, and TMCV is a new security from 2025-11-12.
- **Handling:** the new entity gets its own `security_id` with `first_trade_date = listing date`. Membership before that date is a placeholder that is expected to have no candles.

### DVR share class
Example: TATAMTRDVR (2008–2024), in NIFTY 50 from 2016-04-01 to 2017-09-28.

- A second listed share class of the same company. It has its own ISIN and its own trades. NSE counts it as a separate index security.
- It was cancelled for 7 ordinary shares per 10 DVR, last traded 2024-08-29. Upstox rejects its ISIN; Breeze code `TMLDVR` is still to be verified.

### Split, bonus and ISIN change
Examples:
- NESTLEIND: 1:10 split (2024-01-05), ISIN INE239A01016→INE239A01024; 1:1 bonus (2025-08-08)
- RELIANCE: 1:1 bonus (2024-10-28)
- HDFCBANK: 1:1 bonus (2025-08-27)
- BAJFINANCE: 1:2 split plus 4:1 bonus (2025-06-16)
- KOTAKBANK: 1:5 split (2026-01-14)

The full list is in [`reference/corporate_actions.csv`](../../reference/corporate_actions.csv).

- Share count and price change by the ratio. Traded value (price × volume) does not.
- **Providers return adjusted history.** Upstox divides old prices and multiplies old volumes by the cumulative ratio *as of the fetch date*. For example, NESTLEIND 2022 candles come back at about ₹930, not ₹18,600. See [provider characteristics](provider-data-characteristics.md#price-adjustment).
- NSE resets the "date of listing" for the new ISIN in its equity list: NESTLEIND shows 2023-08-01. The D1 `listing_date` copied that, so the fetcher silently skipped NESTLEIND before August 2023. `security_master.first_trade_date` is deliberately blank for NESTLEIND. `sync-cash-upstox-data --ignore-listing-date` fetches the earlier history.

## Consequences for stored data

1. **Prices are on a mixed adjustment basis over time.** Data fetched before a split is on the old basis; data fetched after is on the new one. Anyone computing returns across a corporate action should use the `corporate_action` table, or re-fetch the full symbol history after each split or bonus.
2. **Volume-weighted measures are safe.** Price × volume is invariant to adjustment when price and volume are adjusted together, which both providers do. This is why the [synthetic index volume](synthetic-index-volume.md) is robust to splits.
3. **Joins across time must go through `security_id`,** not the symbol string.
