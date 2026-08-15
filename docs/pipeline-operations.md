# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The canary searches a finite list of exact, resolved 15-minute windows, stops at the newest window
common to all seven assets, downloads the least hourly PMXT inventory needed for that window, and
publishes seven independently verified partitions to an isolated namespace. It then performs a
second authenticated redownload/no-op verification and records network, runtime, RSS, disk, timeout,
settlement, exclusion-contract, and source-reuse evidence.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
