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
and canary candidates stay within that coverage. No unresolved local conflict remains after focused
verification. The remaining boundary is one real Actions canary dispatch; bulk production is
intentionally not launched.

Run 31899002607 then proved shared acquisition and real seven-asset filtering but exposed a common
reconstruction defect: every bounded stream began with an incremental update, and the implementation
aborted before its first full snapshot. A bounded exact-market source probe showed that the 00:00
window was also unsuitable: BNB and HYPE did not receive their first full snapshots until after the
window started. The replacement 23:30 window has pre-start snapshots for both tokens of all seven
assets and is frozen in `config/canary-source-evidence.json`. The 42 digest-verified v1 draft assets
remain isolated evidence as recorded in `canary-v1-exclusion-reconciliation.json`; corrected canary
output used the v2 namespace.

Run 31902521041 proved that the v2 23:30 window was also unsuitable: every asset had a native
post-snapshot BBO/depth contradiction and was legitimately published as `EVENT_CONFLICT`. The v1
and v2 Releases remain immutable isolated evidence. Replacement qualification now runs only on
GitHub Actions: eight candidates share one bounded source bundle, fresh Gamma evidence precedes
PMXT HEADs, and authenticated Tier A rows select a common window or the exact minimum multi-window
cover. Run 31914715144 then found BTC Tier A evidence, but 47 of the 48 non-BTC dispositions were
legitimate post-snapshot `EVENT_CONFLICT` results and one was a legitimate XRP `SOURCE_GAP`.
The authenticated matrix is frozen in `canary-v3-coverage-reconciliation.json`. Run 31917413125
proved the v3 record retained only mutable whole-Gamma-payload hashes, not the historical minimum
authoritative projection, so BTC reuse was invalidated rather than treating serialization drift as
semantic drift. Run 31919920497 then produced authenticated v4 Tier A evidence for BTC and ETH;
all other v4 dispositions remained legitimate `EVENT_CONFLICT`, `SOURCE_GAP`, or
`NO_INITIAL_SNAPSHOT` exclusions. The v5 controller revalidates the full minimum semantic
projection of those Tier A rows and searches eight new source-reused, stratified rounds only for
assets still uncovered. Run 31923100373 added authenticated Tier A evidence for XRP, SOL, and DOGE;
its remaining BNB/HYPE results were legitimate `EVENT_CONFLICT`, `SOURCE_GAP`, or
`NO_INITIAL_SNAPSHOT` exclusions. The v6 controller revalidates all five proofs and searches up to
12 later-period source-reused rounds only for BNB/HYPE. Run 31924347931 added authenticated BNB
Tier A evidence; HYPE remained legitimately excluded by 55 `EVENT_CONFLICT` and 41 `SOURCE_GAP`
dispositions. The v7 controller revalidates all six proofs and searches up to four complete,
source-active UTC days only for HYPE. Production remains locked pending the receipt.
