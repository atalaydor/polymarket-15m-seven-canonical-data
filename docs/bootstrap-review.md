# Bootstrap independent review

Review date: 2026-08-15. This is a code/configuration review; real historical evidence remains the
responsibility of the one Actions canary.

- **15m semantics:** slug, alignment, and 900-second duration are enforced; official outcome and
  exact Chainlink spot/TWAP binding fail closed.
- **Seven-asset coverage:** one ordered asset enum drives discovery, plan generation, release-name
  validation, canary requirements, and receipt authorization.
- **Publication isolation:** canary and production Release prefixes are disjoint; the canary compares
  production inventory before and after publication.
- **Source reuse:** official identities are per asset, while combined condition/token filters feed one
  shared PMXT spool and transfer bytes are charged to exactly one asset.
- **Fail-closed integrity:** unknown semantics, missing fidelity, divergent Release state, and
  evidence-free exclusions abort or exclude; compatible partial uploads remain unfinished for
  restart-safe completion, and Kacho 5m data is explicitly excluded.
- **Resumability:** plans derive from remote durable assets, days appear once, durable partitions zero
  times, and each day recomputes only unfinished assets before independent publication.

The review found and repaired two locally solvable issues: inherited dates were replaced by the
timeframe-neutral PMXT coverage boundary plus a new finite cutoff, and monthly Release buckets were
split into bounded half-month groups. No unresolved local conflict remains after focused
verification. The remaining boundary is one real Actions canary dispatch; bulk production is
intentionally not launched.
