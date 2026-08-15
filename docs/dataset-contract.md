# Public interchange/import contract 1.0.0

This contract preserves evidence likely needed by a later Linux factory adapter. It is deliberately
not a private-factory compatibility claim.

## Immutable release layout

```text
release-vN/
  index.json                 # release cutoff, ordered partition manifests, their digests
  NOTICE.json                # source attribution/terms snapshot
  asset=DOGE/timeframe=5m/date=YYYY-MM-DD/
    markets.parquet
    book-events.parquet
    book-200ms.parquet
    underlying.parquet
    exclusions.parquet
    manifest.json
```

The JSON Schemas under `schemas/` are normative for Chapter 2. Parquet receives an Arrow schema
with the same logical fields and pinned physical types. Prices/sizes are decimal strings in raw
JSON and fixed-scale decimals in Parquet—never binary floating point. Timestamps are UTC signed
int64 nanoseconds; source precision is recorded in provenance and must not be fabricated.

`markets` carries asset/venue/timeframe, Gamma event and market IDs, condition ID, both outcome
token IDs and mapping, exact start/end, captured rules digest/source URL, official outcome and
resolution timestamp/evidence, quality tier and exclusion reason. `book-events` preserves source
and receive timestamps, stable within-partition sequence, snapshots/updates, all available depth,
tick-size changes and public last-trade events. It does not call WebSocket last-price messages an
authoritative trade tape without later on-chain/API confirmation. `underlying` identifies venue,
instrument type (`spot` or `usds_m_perpetual`), symbol, observation kind, exchange/source time,
receive time if present, value, source archive path/checksum, and explicitly `is_settlement=false`.

Each file descriptor includes path, exact byte length and SHA-256. Each manifest includes schema
version, partition identity, inputs and their digests/ETags/checksums, tool Git commit, deterministic
parameters, row counts/min/max timestamps, provenance and exclusions. Canonical JSON is UTF-8,
sorted keys, compact separators, LF terminated. Exact partition ID plus identical manifest digest
means verified no-op; a different digest for an existing ID is a hard conflict and cannot overwrite.

## Reconstruction and causal 200 ms derivation

For each token, order source events deterministically by `(timestamp_received, timestamp,
source_object, source_row)`. A `book` replaces the full state. A `price_change` sets its side/price
level, and size zero deletes it. Tick changes update metadata; trade-price events do not mutate book
state unless an accompanying official book event does. Validate nonnegative sizes, bids below asks
unless a documented transient is preserved, event identity and snapshot availability.

Generate grid points `market_start + n*200ms`, for `n = 0..1499`. At a grid point emit only the
latest state whose **receive timestamp is <= grid time**; never look ahead. Preserve the event
sequence/source time used, receive time, staleness and full available depth. Before the first valid
snapshot emit no row (and record the gap), not an invented empty book. Tier B 1 Hz data may be
forward-filled onto the grid only with `quality_tier=TIER_B`, native interval/staleness fields, and
no claim of 200 ms fidelity. Native events remain alongside every derived representation.

## Later private-factory verification on Linux

The private adapter must verify field names/types, nanosecond and boundary conventions, token/outcome
mapping, book-side semantics, depth ordering, treatment of no-quote/stale states, fee/slippage model,
trade authority, partition/index discovery, accepted compression/Parquet versions, and digest/NOTICE
handling. It must run representative replay parity and conservative backtest acceptance. Any required
adapter is private and must not be copied here.
