# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The adaptive canary first redownloads and authenticates the v4/v5/v6 BTC, ETH, SOL, XRP, DOGE, and BNB Tier A partitions, then
requires current Gamma to match each accepted row's complete minimum semantic projection. Drift or
unresolved authority invalidates reuse and returns that asset to search. The remaining assets run
through at most four new, source-active full UTC-day rounds with 96 markets per day. Each round
completes Gamma identity/rules/outcome binding and 25 PMXT HEAD checks before one sequential shared
source acquisition. An asset leaves the search after its first authenticated
Tier A partition. The controller stops on complete coverage and computes the exact minimum cover
across prior and new proofs; exclusions never count. Aggregate bounds are 384 new candidates, 2,736
Gamma requests including 48 prior-proof revalidations, 100 source objects, 80 GB transferred, and
five hours. Each new round publishes to its own isolated v7 namespace; v4/v5/v6 remain immutable.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
