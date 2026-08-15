# Dataset contract

The dataset identity is `polymarket-15m-seven-v1`. The only production partitions are:

```text
asset={BTC|ETH|SOL|XRP|DOGE|BNB|HYPE}/timeframe=15m/date=YYYY-MM-DD/
```

Each partition contains `markets.parquet`, `book-events.parquet`, `book-200ms.parquet`,
`underlying.parquet`, `exclusions.parquet`, and `manifest.json`. The empty underlying table is
retained for interchange compatibility; it is not settlement authority.

Manifests bind dataset, asset, timeframe, UTC coverage start/cutoff, source provenance, tool commit,
file checksums, statistics, quality, and exclusions. Release assets are content-addressed. A durable
partition is exactly one uploaded asset for every required logical file with matching embedded
digests. Published partitions are immutable.

Canary partitions use `polymarket-15m-seven-canary-v1-*` Releases and cover one declared 15-minute
window. Production partitions use `polymarket-15m-seven-v1-YYYY-MM` Releases and are the only
authority considered by the full planner.
