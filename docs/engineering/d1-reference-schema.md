# D1 reference schema

Part of the [knowledge base](../README.md). See also:
[corporate actions and identity](../market/corporate-actions-and-identity.md) ·
[constituents](../market/nifty50-constituents.md) · [decisions](decisions.md).

Migration: [`migrations/d1/002_security_and_index_reference.sql`](../../migrations/d1/002_security_and_index_reference.sql)
(idempotent; `load-reference-data --apply-migration` runs it).
The older tables `equity_symbol_reference` and `market_data_sync_state` are
unchanged ([d1.md](../d1.md)).

## Conventions

- Dates are ISO text. Intervals are **`[valid_from, valid_to)`**: the start is inclusive, the end exclusive, and `valid_to` NULL means "still valid". `1900-01-01` means "since before our coverage".
- `security_id` is stable and never reused. It equals the `storage_symbol`, the `nse_symbol` under which candles are stored.
- Membership is stored as **intervals, not snapshots**. An index change is one new row plus one `valid_to` update. The basket on day *d* is `WHERE valid_from <= d AND (valid_to IS NULL OR valid_to > d)`.

## Tables

| Table | Key | Purpose |
|---|---|---|
| `security_master` | `security_id` | One row per security: storage symbol, type (`equity`, `equity_dvr`, `index`), status (`active`/`merged`/`delisted`), `first_trade_date` / `last_trade_date` (bounds when candles can exist), successor |
| `security_identifier_history` | `identifier_type, identifier_value, valid_from` | Dated NSE symbols and ISINs per security (renames, ISIN changes) |
| `security_provider_mapping` | `security_id, provider, valid_from` | Upstox instrument key or Breeze code, with availability `verified`/`unverified`/`unavailable` and when it was checked |
| `corporate_action` | `action_id` | Splits, bonuses, mergers, demergers, renames, delistings, with ratio, dates, source and verification (`verified` = primary source, `news`, `unverified`) |
| `index_definition` | `index_code` | `NIFTY50` → storage symbol `NIFTY`, target of 50 companies |
| `index_membership` | `index_code, security_id, valid_from` | Dated membership, `regular` or `demerger_placeholder`, the symbol as announced, inclusion/exclusion reason and source PDF, verification (`verified`, `derived`, `pending` for announced future changes) |
| `reference_source` | `source_id` | Where the reference data came from, including the checksum of the original basket CSV |
| `index_volume_state` | `index_code, trade_date` | Per-day synthetic volume result: status, formula version, fingerprints, member counts, missing members, totals, `published_at` |
| view `v_index_membership` | | Membership joined with company name and current NSE symbol |

## Useful queries

```sql
-- NIFTY 50 basket on a date
SELECT security_id, current_nse_symbol, membership_type
FROM v_index_membership
WHERE index_code = 'NIFTY50' AND valid_from <= '2023-08-25'
  AND (valid_to IS NULL OR valid_to > '2023-08-25');

-- What was a security called on a date
SELECT identifier_value FROM security_identifier_history
WHERE security_id = 'TMPV' AND identifier_type = 'nse_symbol'
  AND valid_from <= '2025-06-01' AND (valid_to IS NULL OR valid_to > '2025-06-01');

-- Providers that can't serve a member
SELECT security_id, provider, availability, notes FROM security_provider_mapping
WHERE availability != 'verified' ORDER BY security_id;
```

## Updating

The CSV files in [`reference/`](../../reference) are the reviewed source. Change them, then:

```bash
uv run load-reference-data --validate-only     # checks keys, dates, overlaps, basket size
uv run load-reference-data                     # upsert into D1
uv run load-reference-data --prune             # also delete D1 rows removed from the CSVs
```

The validator rejects:
- overlapping intervals per security
- one symbol or ISIN on two securities at once
- unknown security references
- malformed dates

It warns when a basket does not add up to 50 companies once DVRs and placeholders are allowed for.

Common edits:

| Event | Edit |
|---|---|
| Semi-annual review announced | For each exclusion, set `valid_to` = effective date. Add a row for each inclusion with `verification = pending`. Switch to `verified` once it takes effect. |
| Demerger with placeholder | Add the new company to `security_master` (`first_trade_date` = listing date). Add a `demerger_placeholder` membership from the ex-date. Set `valid_to` when NSE announces the exclusion. |
| Rename | Close the old `nse_symbol` identifier (`valid_to` = change date) and add the new one. Keep `storage_symbol`. If `equity_symbol_reference` is renamed too, see [decisions](decisions.md#storage-symbol-is-fixed-per-security). |
| Merger or delisting | Set `status`, `last_trade_date` and `successor_security_id`. Close the membership. Mark the Upstox mapping `unavailable` once the key is rejected. |
| Split or bonus | Add a `corporate_action` row. Stored prices keep their fetch-time basis ([why](../market/corporate-actions-and-identity.md#consequences-for-stored-data)). |

After a membership edit, re-run `generate-index-volume --publish` for the
affected dates. The membership fingerprint in `index_volume_state` changes, and
the day is recomputed.
