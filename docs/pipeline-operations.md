# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The adaptive canary first redownloads and authenticates the v4 BTC and ETH Tier A partitions, then
requires current Gamma to match each accepted row's complete minimum semantic projection. Drift or
unresolved authority invalidates reuse and returns that asset to search. The remaining assets run
through at most eight new, stratified eight-window rounds across previously untested source-reuse
days and UTC phases. Each round completes Gamma identity/rules/outcome binding and three PMXT HEAD
checks before one shared source acquisition. An asset leaves the search after its first authenticated
Tier A partition. The controller stops on complete coverage and computes the exact minimum cover
across prior and new proofs; exclusions never count. Aggregate bounds are 64 new candidates, 464
Gamma requests including 16 prior-proof revalidations, 24 source objects, 19.2 GB transferred, and
five hours. Each new round publishes to its own isolated v5 namespace; v4 remains immutable.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
