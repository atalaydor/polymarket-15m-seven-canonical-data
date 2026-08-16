# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The adaptive canary first redownloads and authenticates the v4/v5/v6/v7 Tier A partitions for all seven assets, then
requires current Gamma to match each accepted row's complete minimum semantic projection. Drift or
unresolved authority invalidates reuse and returns that asset to search. The remaining assets run
through at most four new, source-active full UTC-day rounds with 96 markets per day. Each round
completes Gamma identity/rules/outcome binding and 25 PMXT HEAD checks before one sequential shared
source acquisition. An asset leaves the search after its first authenticated
Tier A partition. The controller stops on complete coverage and computes the exact minimum cover
across prior and new proofs; exclusions never count. When all seven prior proofs revalidate, this is
an authenticated zero-transfer no-op. Aggregate bounds are 384 new candidates, 2,832 Gamma requests
including 144 prior-proof revalidations, 100 source objects, 80 GB transferred, and five hours. Each
new round publishes to its own isolated v7 namespace; earlier publications remain immutable.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
