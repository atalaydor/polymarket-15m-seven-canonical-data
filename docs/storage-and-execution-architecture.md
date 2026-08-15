# Storage and Codespace execution architecture

## Selection

GitHub Releases is the durable public store. GitHub documents up to 1,000 assets per release, each
under 2 GiB, with no total release-size or bandwidth limit ([release quotas](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases)). It is public, tag/version bound,
Linux-downloadable, supports API upload, and costs no storage fee for this public repository.
Release assets are not assumed intrinsically immutable: published filenames are content-addressed,
the signed-off index pins length/SHA-256, and tooling refuses replacement. A superseding release is
new and retains the old one.

Hugging Face dataset repositories are a useful source/secondary mirror but their hosted Git/LFS/Xet
workflow and mutable branch tips add machinery and policy dependency. Git LFS has quotas; Packages
has small account quotas; repository Git is unsuitable for binaries; external object stores either
lack an established perpetual free tier/capacity or introduce credentials/billing. Therefore Releases
is the smallest proved zero-cost architecture. A mirror may be added only as non-authoritative.

## Sequential process

1. Freeze one `asset/timeframe/UTC-date` work item and official candidate identity list.
2. Fetch PMXT Parquet footer/ranges when the server honors byte ranges; prune by condition/token row
   group. Because PMXT row groups are large and sorted across all markets, transfer is measured and
   capped. If pruning exceeds cap, download exactly one hourly object, transform immediately, verify,
   then delete it—never accumulate hours.
3. Fetch only the matching official metadata/evidence and small Binance/Kacho inputs required.
4. Reconstruct with one worker and bounded buffers; write a temporary partition, fsync, validate
   schema/coverage/hash, and create its canonical manifest.
5. If remote partition digest matches, verify/no-op. If absent, upload content-addressed assets and
   verify remote length/hash. If different, fail closed. Delete local raw and completed outputs only
   after remote verification. Keep manifest/checkpoint in Git.
6. Maintain at least 8 GiB disk headroom and 2 GiB RAM headroom; stop acquisition at either limit.

No Actions, Windows processing, factory VM, paid overage, credentialed source, parallel partition,
or permanent Codespace archive is part of this design.

## Measured budget and estimate

On 2026-08-07 this Codespace reported 2 cores, 7.8 GiB RAM (6.4 GiB available), a 32 GiB filesystem
with 20 GiB free. PMXT's listing held 2,782 hourly objects from 2026-04-13 19 through 2026-08-07 15;
the first was 127 MB and recent samples were roughly 491–655 MB. A current HEAD probe returned
133,193,857 bytes for the first file and 562,699,309 for a recent file, with `Accept-Ranges: bytes`.
The archive's own early measurement was 31 GB/94 hours; extrapolating observed hourly sizes gives
roughly 0.9–1.5 TB total PMXT input if whole files were fetched. That is explicitly not the plan.

A date partition is practical output granularity; the temporary input unit remains one PMXT hour.
Design caps are 750 MB source object, 1.5 GiB temporary raw, 1 GiB transformed partition, and one
worker. Targeted three-asset traffic is not measured yet, so output and runtime are ranges, not facts:
estimate 25–100 MB canonical/day (three assets, native events plus compressed 200 ms snapshots),
3–12 GB for 116 days through this audit date, and 35–90 Codespace wall-hours (70–180 core-hours).
Whole-file transfer could be ~1 TB; successful range pruning should reduce it by one to two orders
of magnitude. Chapter 3 must calibrate before backfill.

GitHub Free personal accounts currently include 120 core-hours and 15 GB-month Codespaces storage
([billing](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces)). Thus the
low estimate fits one monthly allowance; the high estimate requires resumable work across two free
monthly resets, still zero-cost. Immediate Releases upload plus deletion keeps incremental storage
well below 15 GB-month. Paid overage remains disabled. If the Chapter 3 pilot exceeds 90 core-hours
or range transfer fails to reduce source bytes sufficiently, split production across monthly quota
resets; that lawful zero-cost fallback is slower, not architecturally blocked.
