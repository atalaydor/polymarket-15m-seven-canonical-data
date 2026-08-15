# Closed roadmap after Chapter 1

## Chapter 2 — pipeline implementation

Implement official discovery/evidence capture, PMXT range reader and reconstruction, Binance/Kacho
adapters, pinned Arrow schemas, causal 200 ms derivation, validation, checkpoints, conflict-safe
GitHub Release publication, NOTICE/index creation, and unit/integration fixtures. No broad backfill.

## Chapter 3 — representative pilot and calibration

Run representative DOGE/BNB/HYPE dates including PMXT/Kacho overlap and known outages. Measure row
group selectivity, bytes, CPU/RAM/disk, canonical ratios, replay behavior, cross-source differences,
and repair rules. Recalculate the finite budget and accept or narrow the backfill plan.

## Chapter 4 — finite historical backfill and certification

Process sequential daily partitions from the frozen start to a declared cutoff, resume across free
quota periods, repair only under the tier policy, publish each verified partition, inventory gaps,
and issue quality/cost/coverage certification. No scope expansion mid-run.

## Chapter 5 — final immutable release and Linux handoff

Publish the final content-addressed index, manifests, NOTICE, checksums and documentation; verify a
clean Linux download and all digests; run the private factory adapter acceptance outside this public
repository; record only sanitized acceptance status here. Never require rebuilding history.
