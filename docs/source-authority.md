# Source authority

Source claims below were reviewed on 2026-08-15. The canary must capture new per-market evidence;
these references do not assert that any particular 15-minute market exists.

| Source | Role | 15m authority |
|---|---|---|
| [Polymarket market-data documentation](https://docs.polymarket.com/market-data/overview) | Official Gamma/CLOB identity, token mapping, rules, and final outcome | Controlling for exact slug, window, condition, tokens, rules, and outcome. Each payload is captured and hashed. |
| [PMXT v2 overview](https://archive.pmxt.dev/docs/v2-data-overview) | Historical order-book events | Hourly objects are timeframe-neutral. The inherited bounded catalog probe recorded the frozen v2 coverage boundary as 2026-04-13T19:00:00Z on 2026-08-07; its exact extraction is preserved in `timeframe-neutral-evidence.json`. Events are accepted only after filtering by official 15m condition and token identifiers. No 5m-derived row is authority. |
| [Chainlink Data Streams](https://data.chain.link/streams) | Rules-named settlement feed | The exact spot or TWAP URL must be bound from each controlling market's rules and Gamma resolution-source field. No Binance value or inferred terminal book value may substitute. |
| [Kacho 5-minute dataset](https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets) | Excluded | Product-specific 5m evidence is never imported or used for 15m quality or settlement. |

Discovery is limited to `^(btc|eth|sol|xrp|doge|bnb|hype)-updown-15m-[0-9]{10}$`.
The timestamp must be 15-minute aligned and the official window must be exactly 900 seconds.
Unknown identity, rules, outcome, stream binding, or source fidelity fails closed.
