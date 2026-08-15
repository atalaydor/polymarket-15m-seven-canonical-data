# Chapter 3 representative pilot and calibration

Access date: 2026-08-07 UTC. The fixed pilot is `2026-04-13 19:40–19:55 UTC`. It is the smallest
interval in PMXT's first archive hour containing the naturally incomplete 19:40 market plus two
ordinary future-listed markets for DOGE, BNB and HYPE. One ordinary BTC 19:45 market is temporary
control evidence only. Sources were the [PMXT object](https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T19.parquet),
[official Gamma API](https://gamma-api.polymarket.com), [Kacho dataset](https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets),
and [Binance public archive](https://data.binance.vision/).

The measured results are normative in `pilot-results.json`; production parameters are normative in
`config/production-plan.json`. All nine target identities, windows and token mappings agreed between
official evidence and Kacho. Kacho's inferred outcome happened to agree on all nine but was ignored
for settlement. Binance supplied 15 one-minute observations per asset; HYPE remained explicitly
`HYPEUSDT` USDⓈ-M perpetual and `is_settlement=false`.

PMXT's flattened price-change rows share an atomic exporter receive timestamp while retaining
distinct source timestamps. Native BBO values can prove that old better levels disappeared without
an explicit zero-size row. Reconstruction now processes one row group at a time, applies an entire
receive-time batch, prunes only stale better levels proved absent by native BBO, interprets 0/1 as
empty-side sentinels, and rejects a batch when its claimed new best level is still unavailable.
Consequently one DOGE market qualified TIER_A; the other eight markets used matching Kacho as
explicit TIER_B because of the archive-start gap or unresolved PMXT BBO/depth inconsistency. No
inconsistency was averaged or promoted.

Across 7,226 comparable bid/ask points, exact PMXT/Kacho BBO agreement was 93.83% DOGE, 96.76% BNB
and 88.52% HYPE. Mean absolute spread differences were 0.00342, 0.00189 and 0.00451 respectively.
All 27,000 derived rows passed receive/source-as-of causality. Clean and interrupted reruns were
byte-identical. The authenticated noncanonical GitHub draft contains 18 partition assets plus a
portable index and NOTICE; all 20 were downloaded and SHA-256 verified, and a restart reused them.

The fixed Chapter 4 plan has 375 daily asset partitions across five calendar-month draft releases.
Concurrency remains one. Measured rates imply about 700 million target PMXT events, 0.9–1.5 TB
worst-case source transfer, 12–20 GB canonical output, 55–110 wall-hours and 90–180 core-hours.
The low case fits one free monthly compute allowance; the high case uses two allowance periods.
Sequential hourly inputs, immediate verified publication and deletion keep working storage below the
measured 0.37 GB pilot footprint plus the frozen 8 GB disk headroom.
