# Source authority and scope evidence

Audit date: 2026-08-07 UTC. Availability is not authority. A source is accepted only for the role
its producer and market rules support.

| Source | Class | Proven coverage and contents | Authority / limitation |
|---|---|---|---|
| [PMXT v2 overview](https://archive.pmxt.dev/docs/v2-data-overview) | PRIMARY | Polymarket public market-channel events, hourly UTC Parquet, exact archive start `2026-04-13T19`, source and exporter-receive timestamps; `book`, `price_change`, `last_trade_price`, `tick_size_change`; full depth only in `book`, level updates thereafter | Primary historical order-book recording, not settlement authority. CC BY 4.0. Files are sorted by condition/token/receive time, not globally chronological; rebuild each token from a book snapshot, apply zero-size deletion and later deltas ordered by receive time with source time retained. Missing initial snapshot or ambiguity excludes the segment. |
| [Polymarket Gamma/CLOB overview](https://docs.polymarket.com/market-data/overview), [market schema](https://docs.polymarket.com/api-reference/markets/get-market-by-id), [market channel](https://docs.polymarket.com/market-data/websocket/market-channel) | PRIMARY | Event, market, condition and outcome-token identity; start/end, rules, resolution source; public L2 events and official `market_resolved` winning token/outcome | Identity, rules and result authority. Gamma's mutable response is captured with retrieval timestamp and digest. Official resolution outranks all inferred outcomes. |
| Polymarket market rules: [DOGE](https://polymarket.com/event/doge-updown-5m-1785191400), [BNB](https://polymarket.com/event/bnb-updown-5m-1778963100), [HYPE](https://polymarket.com/event/hype-updown-5m-1778255400) | PRIMARY | Bounded 5-minute examples name Chainlink DOGE/USD, BNB/USD and HYPE/USD streams. Up means end value greater than or equal to start; otherwise Down. | Per-market captured rules are controlling. These examples prove the product/rule family, not that every historical market has identical rules. Binance is explicitly not settlement evidence. |
| [Kacho dataset card](https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets) | BACKFILL | DOGE/BNB/HYPE 5m only, `2026-04-05`–`2026-05-18` UTC; 12,258–12,259 markets/asset; ~300 best-effort 1 Hz rows/market; best bid/ask and sizes for both tokens; bid depth within 5 cents only. Shared outages include 2026-04-18 10:35–11:55 UTC. | CC0 validation/backfill. Outcome is inferred from final book and is never imported as official; no ask-depth history, no trades, no tick-size events, no receive timestamp. Tier B only. |
| [Binance public data](https://github.com/binance/binance-public-data/blob/master/README.md) | VALIDATION | Public daily/monthly spot and USD-M files; trades, aggTrades and klines, plus futures mark/index/premium klines in archive layout; SHA-256 sidecars. Spot timestamps are microseconds from 2025-01-01; futures examples are milliseconds. | Venue observations only. DOGEUSDT and BNBUSDT are spot; HYPEUSDT is USDⓈ-M perpetual and must be labeled so. None is the Chainlink settlement stream or official outcome. |
| Chainlink Data Streams UI | EXCLUDED | Named by the rules as reference authority | No zero-cost historical bulk/archive and redistribution contract was established in Chapter 1. Preserve its URL/rules evidence; use official Polymarket outcome for settlement. |
| Other third-party datasets/vendors | EXCLUDED | Some advertise broader timeframes | Excluded because authority, complete public access, stable zero-cost terms, or material improvement was not proved. Re-evaluate only by a documented source-change proposal. |

## Frozen discovery and precedence

Discover candidates from Gamma by exact slug family `^(doge|bnb|hype)-updown-5m-[0-9]{10}$`, then
validate the title/rules, exactly two outcomes mapped to their CLOB token IDs, a 300-second UTC
window, condition ID, and official resolution. Slug matching alone never admits a market.

Precedence is: per-market official Polymarket identity/rules/resolution; PMXT v2 event recording;
Kacho only for validated Tier B gaps; Binance only as a separately labeled venue observation.
Conflicts are retained as provenance and fail closed: identity/rules/outcome conflicts exclude the
market; event conflicts split/quarantine the segment; validation differences never rewrite Tier A.

## Frozen dates and tiers

- **TIER_A:** DOGE/BNB/HYPE 5m markets on or after `2026-04-13T19:00:00Z`, only where PMXT events
  reconstruct cleanly and official identity/rules/result validate. End is a release manifest cutoff;
  only resolved markets ending at or before it enter.
- **TIER_B:** DOGE/BNB/HYPE 5m from `2026-04-05T00:00:00Z` through the Kacho fixed endpoint
  `2026-05-18T23:59:59Z`, when identity/rules/result are independently replaced/confirmed from
  official Polymarket sources. It remains 1 Hz/top-of-book fidelity and is never promoted.
- **EXCLUDED:** before 2026-04-05, 15m, unresolved or identity-ambiguous markets, reconstruction
  without a usable snapshot, gaps presented as continuous, inferred settlement, or unproved fields.

No exact universal end date is frozen because PMXT is updating. Each immutable release freezes its
own cutoff and source-object inventory. A later release is a new version, never a mutation.

## Binance bounded coverage verification

Archive HEAD checks and checksum-verified first rows establish exact earliest observed samples:

| Instrument/category | First verified source timestamp (UTC) | Prior-month object |
|---|---:|---|
| BNBUSDT spot 1m kline | 2017-11-06 03:54:00 | 2017-10: 404 |
| BNBUSDT spot trades/aggTrades | 2017-11-06 03:54:23 | 2017-10: not present via kline boundary check |
| DOGEUSDT spot 1m kline | 2019-07-05 12:00:00 | 2019-06: 404 |
| DOGEUSDT spot trades/aggTrades | 2019-07-05 12:00:01.957 | 2019-06: not present via kline boundary check |
| HYPEUSDT USDⓈ-M perpetual mark/index 1m | 2025-05-30 09:34:00 | 2025-04: 404 |
| HYPEUSDT USDⓈ-M perpetual kline | 2025-05-30 10:30:00 | 2025-04: 404 |
| HYPEUSDT USDⓈ-M perpetual trades/aggTrades | 2025-05-30 10:30:01.237 | 2025-04: absent at product boundary |

These are earliest **verified archive observations**, not claims about asset inception, complete
continuity, spot availability for HYPE, or settlement authority. Availability of checksum sidecars
was verified for all sampled contract/trade files. Chapter 2 inventories gaps and premium-index
availability before use.
