# Chapter 4 recovery evidence

Access date: 2026-08-14 UTC.

After Actions run `31573320747`, authenticated GitHub API metadata proves 359 complete
content-addressed partitions: 137 TIER_A, 132 TIER_B, and 90 EXCLUDED. Sixteen partitions are
unfinished. The only partial publication is `HYPE/5m/2026-07-29`, whose sole remote asset is in
non-durable `starter` state. There are no divergent logical assets, wrong-month publications, or
out-of-plan partitions.

The PMXT object
[`2026-06-09T15`](https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-09T15.parquet) reports a stable
764,905,077-byte identity with byte-range support, above the former 750,000,000-byte cap. The cap is
therefore raised only to 800,000,000 bytes.

HEAD inventory confirms that the expected PMXT paths for
[`2026-06-11T04`](https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T04.parquet),
[`2026-06-11T05`](https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T05.parquet), and
[`2026-06-11T06`](https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T06.parquet) return HTTP 404,
while the surrounding hourly paths return stable objects. Only these three exact absences are
declared; every other unexpected HTTP absence remains fail-closed.

The bounded `recovery-canary` workflow mode contains exactly the six failed UTC days. It derives
unfinished assets from remote durable evidence, preserves already complete July 29 DOGE/BNB
partitions, and certifies only after all 375 planned partitions are durable with no publication
anomalies. Draft dataset Releases remain staged for Chapter 5 final assembly and publication.

The August 7 recovery also recognizes Gamma's dedicated `resolutionSource` field as
the frozen stream authority when it is exactly the configured per-asset Chainlink URL
and the controlling rules independently name that asset and retain the Up/Down
comparison semantics. A blank or different source, a cross-asset rule, or missing
comparison semantics remains an identity failure.

The August 7 official Gamma rules and `resolutionSource` agree on Chainlink's exact
[DOGE/USD 30-second TWAP stream](https://data.chain.link/streams/doge-usd-twap-30s-streams),
and explicitly name Dogecoin, Chainlink, and TWAP. Recovery accepts only the finite
per-asset spot or 30-second-TWAP stream identities; TWAP additionally requires that
exact URL in the controlling rules plus matching Chainlink, asset, and TWAP wording.
