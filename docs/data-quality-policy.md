# Data quality policy

Quality is evidence-based, never inferred from file presence.

- `TIER_A`: official identity/rules/result plus complete, deterministic PMXT reconstruction for the
  claimed interval, including an initial full snapshot and monotonic receive ordering.
- `TIER_B`: official identity/rules/result plus Kacho's known 1 Hz top-of-book fidelity, including
  explicit missing intervals and absent ask-depth/trade/tick data.
- `EXCLUDED`: anything else. Reason codes include `IDENTITY_CONFLICT`, `RULES_UNVERIFIED`,
  `OUTCOME_UNVERIFIED`, `NO_INITIAL_SNAPSHOT`, `EVENT_CONFLICT`, `SOURCE_GAP`, `UNSUPPORTED_TIMEFRAME`,
  `OUT_OF_SCOPE_DATE`, `LICENSE_UNRESOLVED`, and `UNRESOLVED_MARKET`.

Coverage is calculated against expected 300-second windows and separately for markets, native
events and derived grid points. A gap remains a gap; cross-source repair creates Tier B lineage and
never alters Tier A. Conflicts retain both source digests in a quarantine record and stop publication
of the affected partition. Changes require a new schema/release version and migration note.

Conservative replay uses receive time when available, rejects lookahead, marks staleness, applies
actual tick size and available depth, and never assumes fills from midpoint or inferred outcomes.
