# Licensing and redistribution audit

This is an engineering audit, not legal advice. No combined dataset license is selected in Chapter
1 because upstream terms are heterogeneous and Polymarket has not supplied an explicit bulk-data
license in the reviewed documentation.

| Material | Evidence | Redistribution decision |
|---|---|---|
| PMXT v2 archive | Archive footer and [overview](https://archive.pmxt.dev/docs/v2-data-overview) state CC BY 4.0 | May transform/redistribute with attribution, license link, change indication and no implied endorsement. Every manifest preserves `pmxt_v2` provenance. |
| Kacho | [Dataset card](https://huggingface.co/datasets/kachoio/polymarket-5-minute-crypto-up-down-markets) declares CC0 1.0 | May use for Tier B/validation. Preserve provenance despite attribution not being required. |
| Binance public archive | Official [repository README and LICENSE](https://github.com/binance/binance-public-data) label the repository MIT | Archive-derived observations may be published only with source path/checksum and MIT notice. This audit does not claim trademark rights or that exchange data becomes settlement evidence. Recheck terms before Chapter 4 publication. |
| Polymarket API/page responses | Public unauthenticated access is documented; no reviewed page grants a dataset redistribution license | Store the minimum factual identity, rules digest/URL, retrieval evidence and official result needed for audit. Do not republish page presentation or bulk response dumps. Recheck Polymarket terms before the final public data release. |
| Chainlink values/UI | Rules name streams, but no free historical redistribution grant was established | Do not redistribute values or scrape history. Preserve only official market rules/source URL and outcome evidence. |
| Repository code/docs | Authored here | A repository license remains intentionally unset until project owner selection; generated releases carry a source-by-source NOTICE regardless. |

Publication is gated on a machine-readable NOTICE, per-file provenance, source URL, retrieval time,
upstream object digest where available, transformations, and license identifier. If a source's terms
change or cannot be confirmed, its records are quarantined rather than silently relicensed.
