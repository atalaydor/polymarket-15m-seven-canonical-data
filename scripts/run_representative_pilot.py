"""Reproduce the bounded Chapter 3 real-data pilot; never expands its fixed interval."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from canonical_data.audit import canonical_json_bytes
from canonical_data.binance import ingest_binance_zip
from canonical_data.discovery import GammaClient
from canonical_data.httpclient import USER_AGENT
from canonical_data.manifest import hash_file, verify_manifest
from canonical_data.models import Asset, Provenance
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.pmxt import read_pmxt_parquet
from canonical_data.release import DirectoryReleaseBackend, Publisher, download_and_verify_release
from canonical_data.sources import ProductionSourceLoader
from canonical_data.state import Checkpoint, Phase, StateStore

STARTS = (1_776_109_200, 1_776_109_500, 1_776_109_800)
DAY = "2026-04-13"
PMXT_URL = "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T19.parquet"
TOOL_COMMIT = "69bdcf60918a7b18ffc9e085318aa7186c79f3e5"


def _official_btc() -> list[dict[str, Any]]:
    result = []
    for start in STARTS[1:2]:
        slug = f"btc-updown-5m-{start}"
        url = "https://gamma-api.polymarket.com/events?" + urllib.parse.urlencode({"slug": slug})
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read(1_000_001)
        raw = json.loads(payload)[0]["markets"][0]
        result.append(
            {
                "slug": slug,
                "condition_id": raw["conditionId"],
                "tokens": json.loads(raw["clobTokenIds"]),
                "outcome_prices": json.loads(raw["outcomePrices"]),
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return result


def _expected_checksum(path: Path) -> str:
    return path.read_text().split()[0]


def _files(directory: Path) -> dict[str, str]:
    return {path.name: hash_file(path)[1] for path in sorted(directory.iterdir()) if path.is_file()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pmxt", type=Path, required=True)
    parser.add_argument("--kacho-root", type=Path, required=True)
    parser.add_argument("--binance-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    began = time.perf_counter()
    began_cpu = time.process_time()
    loader = ProductionSourceLoader(
        GammaClient(), time.time_ns(), args.output / "temporary-official-source"
    )
    btc = _official_btc()
    official: dict[Asset, Any] = {}
    discovery_seconds = 0.0
    for asset in Asset:
        stage = time.perf_counter()
        official[asset] = loader.discover(asset, list(STARTS))
        discovery_seconds += time.perf_counter() - stage
    all_markets = tuple(market for item in official.values() for market in item.markets)
    decode_started = time.perf_counter()
    events = read_pmxt_parquet(
        args.pmxt,
        {market.condition_id for market in all_markets},
        {token for market in all_markets for token in (market.token_up, market.token_down)},
        PMXT_URL,
    )
    decode_seconds = time.perf_counter() - decode_started
    btc_events = read_pmxt_parquet(
        args.pmxt,
        {item["condition_id"] for item in btc},
        {token for item in btc for token in item["tokens"]},
        PMXT_URL,
    )
    pmxt_bytes, pmxt_digest = hash_file(args.pmxt)
    pmxt_provenance = Provenance(
        "pmxt_v2",
        PMXT_URL,
        time.time_ns(),
        pmxt_bytes,
        pmxt_digest,
        "CC-BY-4.0",
        "ms",
        transformations=("bounded_verified_full_object", "condition_token_filter"),
    )
    per_asset: dict[str, Any] = {}
    built_partitions = []
    for asset in Asset:
        stage = time.perf_counter()
        discovered = official[asset]
        kacho_path = args.kacho_root / f"{asset.value.lower()}.parquet"
        kacho_rows, kacho_provenance = loader.load_kacho(kacho_path, discovered.markets)
        zip_path = args.binance_root / f"{asset.value.lower()}.zip"
        checksum_path = args.binance_root / f"{asset.value.lower()}.checksum"
        observations = ingest_binance_zip(
            zip_path,
            _expected_checksum(checksum_path),
            asset,
            "klines",
            f"binance/{asset.value}/{DAY}/klines-1m",
        )
        observations = [
            row
            for row in observations
            if STARTS[0] * 1_000_000_000 <= row.source_ts_ns < (STARTS[-1] + 300) * 1_000_000_000
        ]
        binance_bytes, binance_digest = hash_file(zip_path)
        provenance = (
            *discovered.provenance,
            pmxt_provenance,
            kacho_provenance,
            Provenance(
                "binance_public",
                f"https://data.binance.vision/{zip_path.name}",
                time.time_ns(),
                binance_bytes,
                binance_digest,
                "MIT_archive_repository_claim",
                "us" if asset is not Asset.HYPE else "ms",
                upstream_checksum=_expected_checksum(checksum_path),
                transformations=("checksum_verify", "pilot_window_filter"),
            ),
        )
        inputs = PartitionInputs(
            asset,
            DAY,
            discovered.markets,
            kacho_rows=tuple(kacho_rows),
            underlying=tuple(observations),
            provenance=provenance,
            decoded_pmxt_events=tuple(
                event
                for event in events
                if event.condition_id in {m.condition_id for m in discovered.markets}
            ),
        )
        root = args.output / "clean" / asset.value
        pipeline = Pipeline(
            root / "partitions", StateStore(root / "state"), TOOL_COMMIT, PipelineLimits()
        )
        built = pipeline.build(inputs, (STARTS[-1] + 300) * 1_000_000_000)
        verify_manifest(built.directory)
        clean_hashes = _files(built.directory)
        rerun_root = args.output / "rerun" / asset.value
        rerun = Pipeline(
            rerun_root / "partitions", StateStore(rerun_root / "state"), TOOL_COMMIT
        ).build(inputs, (STARTS[-1] + 300) * 1_000_000_000)
        if clean_hashes != _files(rerun.directory):
            raise RuntimeError("clean rerun is not byte-identical")
        interrupted_root = args.output / "interrupted" / asset.value
        interrupted_pipeline = Pipeline(
            interrupted_root / "partitions", StateStore(interrupted_root / "state"), TOOL_COMMIT
        )
        identity = interrupted_pipeline._identity_digest(inputs)
        interrupted_state = StateStore(interrupted_root / "state")
        if interrupted_state.load(f"{asset.value}/5m/{DAY}") is None:
            interrupted_state.advance(
                Checkpoint(f"{asset.value}/5m/{DAY}", Phase.ACQUIRED, identity)
            )
        resumed = interrupted_pipeline.build(inputs, (STARTS[-1] + 300) * 1_000_000_000)
        if clean_hashes != _files(resumed.directory):
            raise RuntimeError("resumed output is not byte-identical")
        publisher = Publisher(DirectoryReleaseBackend(args.output / "staged-publication"))
        assets = publisher.publish_partition(
            "chapter-3-pilot-draft", built.partition_id, built.directory
        )
        downloaded = download_and_verify_release(
            DirectoryReleaseBackend(args.output / "staged-publication"),
            "chapter-3-pilot-draft",
            args.output / "downloaded" / asset.value,
        )
        market_rows = pq.ParquetFile(built.directory / "markets.parquet").read().to_pylist()
        event_rows = pq.ParquetFile(built.directory / "book-events.parquet").metadata.num_rows
        sample_rows = pq.ParquetFile(built.directory / "book-200ms.parquet").metadata.num_rows
        per_asset[asset.value] = {
            "markets": len(discovered.markets),
            "completed": len(market_rows),
            "excluded": len(discovered.markets) - len(market_rows),
            "tiers": dict(Counter(row["quality_tier"] for row in market_rows)),
            "pmxt_events": sum(
                event.condition_id in {m.condition_id for m in discovered.markets}
                for event in events
            ),
            "native_event_rows": event_rows,
            "sample_200ms_rows": sample_rows,
            "kacho_rows": len(kacho_rows),
            "binance_rows": len(observations),
            "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
            "stage_seconds": time.perf_counter() - stage,
            "manifest_sha256": built.manifest_digest,
            "byte_identical_rerun": True,
            "byte_identical_resume": True,
            "publication_assets": len(assets),
            "downloaded_assets": len(downloaded),
        }
        built_partitions.append(built)
    report = {
        "schema_version": "1.0.0",
        "accessed_at": "2026-08-07",
        "interval": "2026-04-13T19:40:00Z/2026-04-13T19:55:00Z",
        "pmxt": {
            "object_bytes": pmxt_bytes,
            "sha256": pmxt_digest,
            "decoded_events": len(events),
            "event_types": dict(Counter(event.event_type.value for event in events)),
            "decode_seconds": decode_seconds,
        },
        "btc_control": {
            "markets": len(btc),
            "pmxt_events": len(btc_events),
            "official_outcome_prices": [item["outcome_prices"] for item in btc],
        },
        "assets": per_asset,
        "discovery_seconds": discovery_seconds,
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - began_cpu,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "publication_namespace": "chapter-3-pilot-draft",
        "historical_backfill": False,
    }
    args.report.write_bytes(canonical_json_bytes(report))


if __name__ == "__main__":
    main()
