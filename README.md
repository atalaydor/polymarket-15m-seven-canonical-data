# Polymarket 15-minute seven-asset canonical data

This repository is the isolated control plane for a new canonical historical dataset of actual
Polymarket 15-minute Up/Down markets for BTC, ETH, SOL, XRP, DOGE, BNB, and HYPE. It does not
convert or resample 5-minute markets.

Production is locked until one Actions canary establishes usable evidence for all seven assets,
preferring a common resolved 15-minute window and otherwise using the minimum qualifying windows.
It also proves official settlement bindings, PMXT availability, shared-source behavior, isolated
publication, authenticated no-op verification, and runner safety margins. The canary uses its own
Release namespace and cannot create production authority.

After a passing receipt is committed, the finite planner derives unfinished asset/day partitions
from content-addressed remote Releases. One UTC day is processed at a time, with one PMXT source
pass shared by every unfinished asset and independent durable publication per partition.

See [source authority](docs/source-authority.md), [dataset contract](docs/dataset-contract.md),
[quality policy](docs/data-quality-policy.md), and [operations](docs/pipeline-operations.md).
