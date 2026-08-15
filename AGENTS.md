# Repository instructions

- This public repository contains only canonical historical-data tooling, contracts, evidence, and releases for actual Polymarket 15-minute Up/Down markets for BTC, ETH, SOL, XRP, DOGE, BNB, and HYPE.
- Never add credentials, wallets, private strategy/factory material, 5-minute production, other assets, other market durations, other venues, or live-trading functionality or data.
- GitHub Actions may be used only for this repository’s finite public historical-data processing, verification, publication, and certification on standard public-repository GitHub-hosted runners at zero cost. Paid runners/overage, private Actions minutes, scheduled/perpetual or unrelated compute, and Actions artifacts/cache as canonical dataset authority remain prohibited.
- Historical source inputs are temporary and must not be committed. Published partitions are immutable and content-addressed.
- Source claims require a URL and an access date. Official market rules and outcomes outrank inferred values.
- Run `python -m unittest`, Ruff, strict mypy, and `git diff --check` for affected Chapter 1 changes.
