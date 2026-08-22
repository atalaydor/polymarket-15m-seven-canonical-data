# Storage and execution architecture

```text
remote production Releases -> unfinished asset/day plan
UTC day -> official discovery for unfinished assets
        -> acquire each PMXT hourly object once
        -> scan/decode it once for every exact unfinished identity
        -> finalize each market when its second causal source hour closes
        -> retain only compressed future-window and canonical-result fragments
        -> assemble, verify, publish, redownload, and delete each asset independently
        -> reconcile remote authority -> durable ledger checkpoint -> next day

accelerated bounded assignment -> parallel isolated day compute (no release writer lock)
                               -> authenticated transient partition bundle
                               -> short release-group single-writer publish
                               -> remote verification or authenticated no-op
```

The planner never treats the repository ledger, Actions artifacts, or caches as canonical authority.
It omits remote-durable partitions, resumes compatible partial uploads, rejects duplicate,
divergent, unexpected, or out-of-plan assets, and schedules each remaining UTC day once. Day
execution re-derives its unfinished asset subset from remote Releases, so arbitrary interruption
after any file or partition publication is restart-safe.

Execution is sequential within each day, transient retries are bounded, source and transformed
objects are capped, and each job has a six-hour limit. Temporary source bytes are deleted after the
single combined scan; staged result fragments are deleted immediately after verified publication.
The accelerated path preserves that per-job lifecycle while allowing different days to compute in
parallel. Its only cross-job staging contains completed canonical partition bytes, is isolated by
workflow run and day, and has one-day maximum retention. The release lock covers only remote
mutation and verification; it does not cover source acquisition or reconstruction.

PMXT filtering retains only exact official condition/token rows whose receive time is in the
market's one-hour causal warm-up through its 15-minute end. A market intersects at most two hourly
objects. Once the later object closes that market is reconstructed, classified, and resampled with
the same fail-closed code as the ordinary partition path. Accepted events/samples become compressed
per-market fragments; excluded raw events are deleted. Only the four future 15-minute windows per
asset can remain in the pending fragment set after an hourly source pass, so complete day-wide raw
or SQLite residency and a late duplicate condition index are not required.

One combined identity scan replaces the former seven asset-by-asset Parquet scans per hourly
object: a full 25-object day drops from at most 175 scans to exactly 25 without reacquisition.
Final assembly reads fragments in condition-id order and uses the pinned writer, preserving the
byte-level canonical result of the full-spool path. The capacity circuit breakers are derived in
`config/pmxt-capacity-evidence.json`; they are resource guards, not quality or exclusion rules.

Content-addressed assets are grouped in half-month Releases. A bucket contains at most 16 UTC days,
112 partitions, and 672 assets; Release-list and asset-list pagination are both finite and bounded.
