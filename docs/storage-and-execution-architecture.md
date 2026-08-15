# Storage and execution architecture

```text
remote production Releases -> unfinished asset/day plan
UTC day -> official discovery for unfinished assets
        -> acquire and parse each PMXT hourly object once
        -> filter into one shared condition-indexed spool
        -> build, verify, publish, and redownload each asset partition independently
        -> reconcile remote authority -> durable ledger checkpoint -> next day
```

The planner never treats the repository ledger, Actions artifacts, or caches as canonical authority.
It omits remote-durable partitions, resumes compatible partial uploads, rejects duplicate,
divergent, unexpected, or out-of-plan assets, and schedules each remaining UTC day once. Day
execution re-derives its unfinished asset subset from remote Releases, so arbitrary interruption
after any file or partition publication is restart-safe.

Execution is sequential (`max-parallel: 1`), transient retries are bounded, source and transformed
objects are capped, and each job has a six-hour limit. Temporary source bytes are deleted after the
shared spool or verified publication no longer needs them.

Content-addressed assets are grouped in half-month Releases. A bucket contains at most 16 UTC days,
112 partitions, and 672 assets; Release-list and asset-list pagination are both finite and bounded.
