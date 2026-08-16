# Operations

The workflow exposes only `canary` and `full-backfill` dispatch modes. Do not dispatch full backfill
until the canary has committed `config/canary-receipt.json`; the planner also enforces this lock.

The adaptive canary first revalidates BTC's digest-pinned v3 Tier A proof against fresh official
Gamma identities and the exact remote partition. It then runs only uncovered assets through at most
four eight-window rounds. The rounds are separated by 24 days and rotate through 18:00, 12:00,
06:00, and 00:00 UTC phases because v3 showed that adjacent-window PMXT conflicts are highly
correlated. Each round completes Gamma identity/rules/outcome binding and three PMXT HEAD checks
before one shared source acquisition. An asset leaves the search after its first authenticated Tier A
partition. The controller stops on complete coverage and computes the exact minimum cover across
the prior and new proofs; exclusions never count. Aggregate bounds are 32 new candidates, 200 Gamma
requests including prior-proof revalidation, 12 source objects, 9.6 GB transferred, and five hours.
Each round publishes to its own isolated v4 namespace.

Full execution is one UTC day per job. Before doing work, the executor reconciles production Release
assets, rejects anomalies, and selects only unfinished assets. Each successful asset publication is
durable even if a later asset or checkpoint commit fails; the next run discovers it remotely and
does not repeat it.
