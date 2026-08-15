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

The source-404 review corrected inherited raw-object metadata that had been reused as market
coverage and a finite cutoff that extended beyond the authoritative PMXT catalog. The first raw
hour is now reserved for reconstruction warm-up, the cutoff is bounded by the last catalog object,
and canary candidates verify their two required source objects before discovery. No unresolved
local conflict remains after focused verification. The remaining boundary is one real Actions
canary dispatch; bulk production is intentionally not launched.
