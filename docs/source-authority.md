# Source authority

Source claims below were reviewed on 2026-08-15. The canary must capture new per-market evidence;
these references do not assert that any particular 15-minute market exists.

| Source | Role | 15m authority |
|---|---|---|
| [Polymarket market-data documentation](https://docs.polymarket.com/market-data/overview) | Official Gamma/CLOB identity, token mapping, rules, and final outcome | Controlling for exact slug, window, condition, tokens, rules, and outcome. Each payload is captured and hashed. |
| [PMXT v2 overview](https://archive.pmxt.dev/docs/v2-data-overview) and [v2 catalog](https://archive.pmxt.dev/Polymarket/v2) | Historical order-book events | The catalog was refreshed on 2026-08-15: 2,835 objects span 2026-04-13T19 through 2026-08-10T00, with only 2026-06-11T04/T05/T06 absent. PMXT documents that empty hours are skipped and a missing key means zero events. The first object is reconstruction warm-up, so validated 15m coverage is 2026-04-13T20:00Z through the exclusive 2026-08-10T01:00Z cutoff. Events are accepted only after filtering by official 15m condition and token identifiers. No 5m-derived row is authority. |
| [Chainlink Data Streams](https://data.chain.link/streams) | Rules-named settlement feed | The exact spot or TWAP URL must be bound from each controlling market's rules and Gamma resolution-source field. No Binance value or inferred terminal book value may substitute. |
| [Kacho 5-minute dataset](https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets) | Excluded | Product-specific 5m evidence is never imported or used for 15m quality or settlement. |

Discovery is limited to `^(btc|eth|sol|xrp|doge|bnb|hype)-updown-15m-[0-9]{10}$`.
The timestamp must be 15-minute aligned and the official window must be exactly 900 seconds.
Unknown identity, rules, outcome, stream binding, or source fidelity fails closed.
Canary candidates must be inside the frozen validation coverage, avoid the three catalog-proven
empty hours, and pass bounded HEAD checks for both their warm-up and market-hour objects. The single
canary candidate is additionally pinned by `config/canary-source-evidence.json`: both official
outcome tokens for all seven conditions have a full PMXT `book` before the window begins. A 404 for
an object listed by the frozen catalog is an authority conflict and aborts; it is not an exclusion.

PMXT hourly files are bounded slices of the continuous official market-channel stream. The official
channel defines `book` as a full order-book snapshot; `price_change` and `tick_size_change` are
incremental updates. Therefore an hourly slice may begin with unusable incremental events. That
prefix is discarded, never applied; reconstruction starts only at the first full snapshot. No snapshot is
`NO_INITIAL_SNAPSHOT`, a snapshot after market start produces `SOURCE_GAP`, and any inconsistency
after the snapshot remains `EVENT_CONFLICT`.
