# Chapter 2 pipeline operations

The production implementation is under `src/canonical_data`. It consumes one asset/day partition
at a time and enforces `config/pipeline.json`. No command implicitly expands a date range or starts
a backfill.

| Frozen responsibility | Implementation owner |
|---|---|
| Gamma slug discovery and official binding | `discovery.GammaClient`, `bind_gamma_market` |
| Hour/day inventories | `inventory` |
| Sequential source assembly and provenance | `sources.ProductionSourceLoader` |
| Bounded verified acquisition | `acquire.BoundedAcquirer` |
| PMXT range access and Parquet pruning | `rangeio`, `pmxt.read_pmxt_parquet` |
| PMXT decode/reconstruction | `pmxt.decode_rows`, `BookReconstructor` |
| Kacho Tier B ingestion | `kacho` |
| Binance reference normalization | `binance` |
| UTC nanoseconds and causal 200 ms grid | `timeutil`, `resample` |
| Tiers/exclusions | `quality` |
| Canonical Parquet/manifests/NOTICE/index | `parquetio`, `manifest` |
| Resume/no-op/conflict checkpoints | `state` |
| Draft publication and reverification | `release` |
| Daily orchestration and raw cleanup | `pipeline` |

PMXT remote reads use HEAD identity (`Content-Length`, ETag, `Accept-Ranges`) and conditional range
requests. Every fetched range records offset, length, and SHA-256; an ETag change, ignored range,
row-group cap, or filtered-row cap fails closed. Full-object fallback uses the bounded acquirer and
must remain sequential.

The pipeline accepts only already bound official markets. PMXT reconstruction failure may use
matching Kacho evidence only as Tier B; it cannot retain the malformed PMXT stream or use Kacho's
outcome. Kacho receive timestamps remain null. Binance rows always have `is_settlement=false`, and
HYPE is always `HYPEUSDT/usds_m_perpetual`.

Partition checkpoints progress `INVENTORIED → ACQUIRED → TRANSFORMED → VERIFIED → PUBLISHED` via
atomic replacement. Identity or manifest changes fail closed. Publication creates/uses a draft,
streams content-addressed assets, downloads every accepted asset, verifies length/SHA-256, and only
then advances the checkpoint and deletes declared raw inputs. The final release assembler writes a
canonical `index.json` and `NOTICE.json`; publishing a release is a separate explicit action.

Useful non-mutating commands:

```bash
dataset-pipeline inventory-pmxt --start 2026-04-13T19:00:00Z --end 2026-04-13T20:00:00Z
dataset-pipeline verify-partition PATH
dataset-pipeline verify-directory-release ROOT RELEASE TARGET
```

Chapter 3 supplies representative source objects, invokes these APIs for a bounded pilot, and
calibrates the already implemented caps. It must not add a missing pipeline stage.
