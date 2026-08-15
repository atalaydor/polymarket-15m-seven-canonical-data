# DOGE / BNB / HYPE canonical historical data

This public repository implements the zero-cost, resumable production pipeline for Polymarket DOGE,
BNB, and HYPE **5-minute Up/Down** markets. Chapter 1 froze feasibility and architecture; Chapter 2
implements the complete pipeline without building the historical dataset.

The target is a portable immutable public release that a Linux consumer can download and verify.
It is a sanitized **interchange/import contract**, not a claim of compatibility with any private
factory. Windows is gateway-only; processing occurs in a 2-core GitHub Codespace without Actions.

## Frozen scope

- Tier A: event-level PMXT v2 book reconstruction plus official Polymarket identity/rules/outcome,
  from `2026-04-13T19:00:00Z`, for verified 5-minute DOGE/BNB/HYPE markets.
- Tier B: Kacho 1 Hz top-of-book validation/backfill for `2026-04-05` through `2026-05-18`, with
  inferred outcomes discarded; Binance venue observations are non-settlement validation only.
- Excluded: 15-minute markets, pre-2026-04-05 history, unsupported fields, private/paid sources,
  and any market whose identity, rules, timestamps, or official result cannot be verified.

See [source authority](docs/source-authority.md), [contract](docs/dataset-contract.md), and
[architecture](docs/storage-and-execution-architecture.md). Operational ownership is documented in
[pipeline operations](docs/pipeline-operations.md). [Chapter 3 pilot calibration](docs/pilot-calibration.md), its
[machine-readable measurements](docs/pilot-results.json), and the
[production plan](config/production-plan.json) freeze the Chapter 4 execution parameters.

Run:

```bash
python -m unittest discover -s tests -v
python -m canonical_data.audit --offline
ruff check .
mypy
```

No combined dataset license is selected. Source obligations remain attached to each record and
asset; see [licensing](docs/licensing.md).
