# Data-quality policy

`TIER_A` requires a resolved official 15-minute market, exact Up/Down token mapping, a rules-bound
Chainlink stream, and gap-free PMXT reconstruction across the market window.

`EXCLUDED` is the only fallback. Every exclusion must name a reason and bind evidence such as the
official payload digest, condition identifier, official outcome, stream URL, or source-gap record.
An excluded partition with no explicit exclusion evidence is invalid. There is no 15m Tier B and no
inferred settlement.

Missing initial snapshots, reconstruction conflicts, unresolved outcomes, identity conflicts,
unbound rules, unexplained source failures, and unsupported scopes all fail closed.
