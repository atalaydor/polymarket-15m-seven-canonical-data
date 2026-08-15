# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The canary asks official Gamma about eight starts from 2026-08-09T23:45Z through 22:00Z. Only after
a complete resolved seven-asset identity/rules/outcome binding does it HEAD the required PMXT
objects. The search is capped at 56 Gamma requests, three unique source objects, and 2.4 GB of
source transfer. All qualified windows execute together through one shared PMXT acquisition and
seven independently published partitions in an isolated v3 run namespace. Authenticated remote
market rows are then searched for a common Tier A window first and otherwise an exact minimum
multi-window cover. Exclusions remain evidence but never count as usable coverage. A second
authenticated redownload/no-op verification records network, runtime, RSS, disk, timeout,
settlement, exclusion-contract, and source-reuse evidence.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
