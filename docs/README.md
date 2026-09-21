# Tick Ticker knowledge base

The docs have two halves that link to each other:

- **Market** docs cover how the exchange, the index and the companies behave.
- **Engineering** docs cover how this repository fetches, stores and repairs the data.

Start with the page that matches your question. Follow links to go deeper.
For what changed in September 2026 and what is still open, read
[the work summary](engineering/work-summary-2026-09.md).

## Market (what the data means)

| Page | Answers |
|---|---|
| [NIFTY 50 constituents](market/nifty50-constituents.md) | Which stocks were in NIFTY 50 on a given day, and how index changes work: reviews, ad hoc changes, DVRs, demerger placeholders |
| [Corporate actions and security identity](market/corporate-actions-and-identity.md) | Renames, mergers, demergers, splits and ISIN changes, and how each one affects stored data |
| [Synthetic index volume](market/synthetic-index-volume.md) | Why NIFTY has no volume, the formula used instead, what it means and where it breaks |
| [Provider data characteristics](market/provider-data-characteristics.md) | Upstox vs Breeze: history limits, price adjustment, candle timestamps, pre-open candles, delisted names |

## Engineering (how the data is produced)

| Page | Answers |
|---|---|
| [Pipeline overview](engineering/pipeline-overview.md) | End-to-end flow, the commands and the order they run in |
| [D1 reference schema](engineering/d1-reference-schema.md) | Tables for identity, membership, provider mappings and volume state; how to update them |
| [Index volume pipeline](engineering/index-volume-pipeline.md) | `generate-index-volume`: inputs, completeness rules, publishing, recompute triggers |
| [Data quality audit, 2026-09](engineering/data-quality-audit-2026-09.md) | Problems found in the existing data, their root causes, and the fixes applied |
| [Daily cron](engineering/daily-cron.md) | The scheduled job, credentials and failure handling |
| [Historical backfill](engineering/historical-backfill.md) | Getting 2016–2021 constituents and HDFC from Breeze; request estimates |
| [Decision log](engineering/decisions.md) | Choices made, the alternatives, and their merits and drawbacks |

Operational reference pages (older, still current):

- [cash-sync.md](cash-sync.md), [cash-second-sync.md](cash-second-sync.md): per-command options
- [storage.md](storage.md): local layout and Iceberg tables
- [d1.md](d1.md): `equity_symbol_reference` and `market_data_sync_state`
- [credentials.md](credentials.md): `.env` keys

## How the pieces connect

```text
reference/*.csv ──load-reference-data──▶ D1 reference tables ─┐
                                                              │ membership, identity
Upstox / Breeze ──sync-cash-*-data──▶ data/cash/*.parquet ────┼──▶ generate-index-volume ──▶ NIFTY volume
                        ▲                     │               │                              (local + Iceberg)
                        └── repair-cash-data ◀┘               └──▶ index_volume_state (D1)
                                              │
                                              └──▶ Iceberg cash.ohlcv_by_symbol (replace, never duplicate)
```

Scope today is NSE cash 1-minute candles. The same identity, membership and
repair ideas apply to the planned 1-second, futures and options data.
[decisions.md](engineering/decisions.md#future-data-types) lists what will
carry over.
