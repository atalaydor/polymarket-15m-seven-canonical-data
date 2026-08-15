"""Sequential, restart-safe Polymarket 15m x 7 executor for Linux Actions."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from canonical_data.acquire import BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient
from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import (
    PMXT_MISSING_OBJECT_URLS,
    SourceObject,
    expected_15m_market_starts,
    pmxt_hourly_objects,
)
from canonical_data.models import Asset, BookEvent, Market, Provenance
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.planner import release_bucket
from canonical_data.release import GitHubReleaseBackend, Publisher
from canonical_data.sources import OfficialDiscovery, ProductionSourceLoader
from canonical_data.spool import EventSpool
from canonical_data.state import StateStore

REPOSITORY = "atalaydor/polymarket-15m-seven-canonical-data"
DATASET_RELEASE_PREFIX = "polymarket-15m-seven-v1"
RETRY_DELAYS = (2, 8, 32)
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
# Run 31902521041 measured 324,090 native rows for the busiest (BTC) market
# across its complete two-object reconstruction inventory. A 25% margin covers
# expected market variance; the interval geometry bounds one hourly object to
# eight 15m markets when one hour of causal warm-up is retained.
PMXT_MEASURED_ROWS_PER_MARKET = 324_090
PMXT_ROWS_PER_MARKET_WITH_MARGIN = (PMXT_MEASURED_ROWS_PER_MARKET * 5 + 3) // 4
PMXT_MARKETS_PER_ASSET_OBJECT = 8
PMXT_FILTERED_ROWS_PER_ASSET_OBJECT = (
    PMXT_ROWS_PER_MARKET_WITH_MARGIN * PMXT_MARKETS_PER_ASSET_OBJECT
)
PMXT_FILTERED_ROWS_PER_ASSET_DAY = PMXT_ROWS_PER_MARKET_WITH_MARGIN * 96
MAX_SOURCE_OBJECT_BYTES = 800_000_000
MINIMUM_FREE_DISK_BYTES = 8_000_000_000

# These immutable hourly objects span every Polymarket condition; their identities
# and observed absence are timeframe-neutral. The canary records fresh access.
PMXT_HTTP_404_GAPS = {
    url: {
        "accessed_at": "2026-08-15",
        "http_status": 404,
    }
    for url in PMXT_MISSING_OBJECT_URLS
}


def _peak_rss_kib() -> int:
    try:
        resource_module = cast(Any, importlib.import_module("resource"))
    except ModuleNotFoundError:
        return 0
    return int(resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss)


def enforce_shared_pmxt_asset_caps(
    events: tuple[BookEvent, ...],
    markets_by_asset: Mapping[Asset, tuple[Market, ...]],
    day_market_counts: dict[str, int] | None = None,
    day_asset_counts: dict[Asset, int] | None = None,
) -> dict[Asset, int]:
    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    counts = {asset: 0 for asset in markets_by_asset}
    market_counts: dict[str, int] = {}
    for event in events:
        asset = owner.get(event.condition_id)
        if asset is None:
            raise SourceError("shared PMXT event is outside the bound market inventory")
        counts[asset] += 1
        market_counts[event.condition_id] = market_counts.get(event.condition_id, 0) + 1
        if counts[asset] > PMXT_FILTERED_ROWS_PER_ASSET_OBJECT:
            raise ResourceLimitError(
                f"PMXT filtered output for {asset.value} exceeds per-object asset cap "
                f"({counts[asset]} > {PMXT_FILTERED_ROWS_PER_ASSET_OBJECT})"
            )
        if market_counts[event.condition_id] > PMXT_ROWS_PER_MARKET_WITH_MARGIN:
            raise ResourceLimitError(
                "PMXT filtered output exceeds measured per-market capacity bound"
            )
        if day_market_counts is not None:
            day_market_counts[event.condition_id] = (
                day_market_counts.get(event.condition_id, 0) + 1
            )
            if day_market_counts[event.condition_id] > PMXT_ROWS_PER_MARKET_WITH_MARGIN:
                raise ResourceLimitError(
                    "PMXT filtered output exceeds measured per-market daily capacity bound"
                )
        if day_asset_counts is not None:
            day_asset_counts[asset] = day_asset_counts.get(asset, 0) + 1
            if day_asset_counts[asset] > PMXT_FILTERED_ROWS_PER_ASSET_DAY:
                raise ResourceLimitError(
                    f"PMXT filtered output for {asset.value} exceeds per-day asset cap"
                )
    return counts


def _pmxt_source_window_ns(source: SourceObject) -> tuple[int, int]:
    stamp = source.url.rsplit("_", 1)[-1].removesuffix(".parquet")
    try:
        start = datetime.strptime(stamp, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    except ValueError as exc:
        raise SourceError("PMXT source URL lacks an authoritative hourly identity") from exc
    start_ns = int(start.timestamp()) * 1_000_000_000
    return start_ns, start_ns + 3_600_000_000_000


def _markets_relevant_to_source(
    markets: tuple[Market, ...], source: SourceObject
) -> tuple[Market, ...]:
    source_start_ns, source_end_ns = _pmxt_source_window_ns(source)
    relevant = tuple(
        market
        for market in markets
        if market.market_end_ns > source_start_ns
        and market.market_start_ns - 3_600_000_000_000 < source_end_ns
    )
    if len(relevant) > PMXT_MARKETS_PER_ASSET_OBJECT:
        raise ResourceLimitError("PMXT object intersects too many 15m market inventories")
    return relevant


def _restore_shared_pmxt_counts(
    spool: EventSpool, markets_by_asset: Mapping[Asset, tuple[Market, ...]]
) -> tuple[dict[str, int], dict[Asset, int]]:
    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    market_counts = spool.counts_by_condition()
    asset_counts = {asset: 0 for asset in markets_by_asset}
    for condition_id, count in market_counts.items():
        asset = owner.get(condition_id)
        if asset is None:
            raise SourceError("shared PMXT spool contains an event outside market inventory")
        if count > PMXT_ROWS_PER_MARKET_WITH_MARGIN:
            raise ResourceLimitError("resumed PMXT market exceeds daily capacity bound")
        asset_counts[asset] += count
    if any(count > PMXT_FILTERED_ROWS_PER_ASSET_DAY for count in asset_counts.values()):
        raise ResourceLimitError("resumed PMXT asset exceeds daily capacity bound")
    return market_counts, asset_counts


def _atomic_json(path: Path, value: Any) -> None:
    payload = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _tool_commit() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
    ).strip()


def _fetch_gamma(url: str, max_bytes: int) -> bytes:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = urllib.request.Request(
                url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                length = response.headers.get("Content-Length")
                if length is not None and int(length) > max_bytes:
                    raise SourceError("Gamma payload exceeds configured bound")
                payload = cast(bytes, response.read(max_bytes + 1))
            if len(payload) > max_bytes:
                raise SourceError("Gamma payload exceeds configured bound")
            return payload
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _acquire_with_retry(
    source: SourceObject, raw_dir: Path, max_object_bytes: int = MAX_SOURCE_OBJECT_BYTES
) -> Any:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            return BoundedAcquirer(raw_dir, max_object_bytes, 8_000_000_000).acquire(source)
        except ResourceLimitError:
            raise
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last = exc
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _provenance_from_json(value: dict[str, Any]) -> Provenance:
    return Provenance(
        source_id=str(value["source_id"]),
        source_url=str(value["source_url"]),
        retrieved_at_ns=int(value["retrieved_at_ns"]),
        byte_length=int(value["byte_length"]),
        sha256=str(value["sha256"]),
        license_id=str(value["license_id"]),
        source_precision=str(value["source_precision"]),
        etag=cast(str | None, value.get("etag")),
        upstream_checksum=cast(str | None, value.get("upstream_checksum")),
        transformations=tuple(str(item) for item in value["transformations"]),
    )


def _market_starts(
    day: date,
    coverage_start: datetime,
    cutoff: datetime,
    starts: tuple[int, ...] | None,
) -> list[int]:
    if starts is None:
        return expected_15m_market_starts(day, coverage_start, cutoff)
    midnight = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    end = midnight + 86_400
    if any(start < midnight or start >= end or start % 900 for start in starts):
        raise SourceError("explicit market starts must be aligned 15m timestamps in one UTC day")
    return sorted(set(starts))


def _validate_expected_market_identities(
    discoveries: dict[Asset, OfficialDiscovery],
    expected: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None,
) -> None:
    if expected is None:
        return
    if set(expected) != set(discoveries):
        raise SourceError("expected market identities do not cover the execution assets")
    for asset, discovery in discoveries.items():
        actual = {
            (market.condition_id, frozenset((market.token_up, market.token_down)))
            for market in discovery.markets
        }
        if actual != expected[asset]:
            raise SourceError("child discovery does not match source-qualified canary identity")


def _validate_expected_source_identity(
    source: SourceObject,
    byte_length: int,
    etag: str | None,
    expected: dict[str, tuple[int, str]] | None,
) -> None:
    if expected is None:
        return
    if expected.get(source.url) != (byte_length, etag):
        raise SourceError("acquired PMXT object does not match source-qualified identity")


def prepare_shared_day(
    day: date,
    work_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...] = tuple(Asset),
    starts: tuple[int, ...] | None = None,
    expected_market_identities: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None = None,
    expected_source_identities: dict[str, tuple[int, str]] | None = None,
) -> tuple[
    Path,
    dict[Asset, OfficialDiscovery],
    dict[Asset, tuple[Provenance, ...]],
    int,
]:
    selected_starts = _market_starts(day, coverage_start, cutoff, starts)
    shared = work_root / f"shared-{day.isoformat()}"
    state_path = shared / "state.json"
    state: dict[str, Any] = (
        json.loads(state_path.read_bytes())
        if state_path.exists()
        else {"completed_urls": {}, "source_bytes": 0, "source_gaps": {}}
    )
    discoveries: dict[Asset, OfficialDiscovery] = {}
    loaders: dict[Asset, ProductionSourceLoader] = {}
    for asset in assets:
        loader = ProductionSourceLoader(
            GammaClient(fetch=_fetch_gamma),
            time.time_ns(),
            work_root / f"{asset.value}-{day}" / "official",
        )
        loaders[asset] = loader
        discoveries[asset] = loader.discover(
            asset, selected_starts, allow_missing=True, allow_unresolved=True
        )
    _validate_expected_market_identities(discoveries, expected_market_identities)
    spool_path = shared / "events.sqlite"
    if not any(discovery.markets for discovery in discoveries.values()):
        with EventSpool(spool_path):
            pass
        return spool_path, discoveries, {asset: () for asset in assets}, 0

    markets_by_asset = {asset: discoveries[asset].markets for asset in assets}
    combined_markets = tuple(market for asset in assets for market in markets_by_asset[asset])
    first_start_ns = min(market.market_start_ns for market in combined_markets)
    last_end_ns = max(market.market_end_ns for market in combined_markets)
    inventory_start_ns = max(first_start_ns - 3_600_000_000_000, 0)
    source_objects = pmxt_hourly_objects(inventory_start_ns, last_end_ns)
    if expected_source_identities is not None and {source.url for source in source_objects} != set(
        expected_source_identities
    ):
        raise SourceError("qualified PMXT objects do not match the execution source set")
    with EventSpool(spool_path, create_index=False) as spool:
        spool.discard_uncommitted_sources(set(state["completed_urls"]))
        day_market_counts, day_asset_counts = _restore_shared_pmxt_counts(
            spool, markets_by_asset
        )
        spool.drop_index()
        for source in source_objects:
            if source.url in state["completed_urls"]:
                continue
            if source.url in PMXT_HTTP_404_GAPS:
                state["source_gaps"][source.url] = PMXT_HTTP_404_GAPS[source.url]
                _atomic_json(state_path, state)
                continue
            acquired = _acquire_with_retry(source, shared / "raw")
            _validate_expected_source_identity(
                source,
                acquired.byte_length,
                acquired.etag,
                expected_source_identities,
            )
            object_provenance: dict[str, dict[str, Any]] = {}
            for asset in assets:
                relevant_markets = _markets_relevant_to_source(markets_by_asset[asset], source)
                if not relevant_markets:
                    continue
                try:
                    loaded = loaders[asset].load_downloaded_pmxt(
                        acquired.path,
                        source.url,
                        relevant_markets,
                        acquired.etag,
                        max_filtered_rows=PMXT_FILTERED_ROWS_PER_ASSET_OBJECT,
                        verified_identity=(acquired.byte_length, acquired.sha256),
                    )
                except ResourceLimitError as exc:
                    raise ResourceLimitError(
                        f"{asset.value} capacity failure while filtering {source.url}: {exc}"
                    ) from exc
                enforce_shared_pmxt_asset_caps(
                    loaded.events,
                    {asset: relevant_markets},
                    day_market_counts,
                    day_asset_counts,
                )
                spool.append(loaded.events)
                object_provenance[asset.value] = asdict(loaded.provenance[0])
            if not object_provenance:
                raise SourceError("PMXT source object has no relevant authoritative market")
            shared_provenance = next(iter(object_provenance.values()))
            state["completed_urls"][source.url] = {
                asset.value: object_provenance.get(asset.value, shared_provenance)
                for asset in assets
            }
            state["source_bytes"] = int(state["source_bytes"]) + acquired.byte_length
            _atomic_json(state_path, state)
            acquired.path.unlink(missing_ok=True)
            if shutil.disk_usage(shared).free < MINIMUM_FREE_DISK_BYTES:
                raise ResourceLimitError("shared PMXT spool exhausted disk safety margin")
        spool.ensure_index()

    gap_provenance = tuple(
        Provenance(
            source_id="pmxt_v2",
            source_url=url,
            retrieved_at_ns=time.time_ns(),
            byte_length=0,
            sha256=hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            license_id="CC-BY-4.0",
            source_precision="http_status",
            transformations=("authoritative_http_404_absence",),
        )
        for url, evidence in sorted(state["source_gaps"].items())
    )
    provenance = {
        asset: (
            *tuple(
                _provenance_from_json(item[asset.value])
                for _, item in sorted(state["completed_urls"].items())
            ),
            *gap_provenance,
        )
        for asset in assets
    }
    return spool_path, discoveries, provenance, int(state["source_bytes"])


def run_partition(
    asset: Asset,
    day: date,
    work_root: Path,
    ledger_path: Path,
    cutoff: datetime,
    discovery: OfficialDiscovery,
    shared_spool: Path,
    shared_provenance: tuple[Provenance, ...],
    shared_source_bytes: int = 0,
    release_prefix: str = DATASET_RELEASE_PREFIX,
    coverage_start_ns: int | None = None,
) -> dict[str, Any]:
    partition_id = f"{asset.value}/15m/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    cpu_began = time.process_time()
    work = work_root / f"{asset.value}-{day.isoformat()}"
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    release_cutoff = min(day_start + timedelta(days=1), cutoff)
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovery.markets,
        provenance=(*discovery.provenance, *shared_provenance),
        event_spool_path=shared_spool,
        preexisting_exclusions=discovery.exclusions,
    )
    pipeline_config = json.loads(Path("config/pipeline.json").read_bytes())
    pipeline = Pipeline(
        work / "output",
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits.from_config(pipeline_config),
    )
    built = pipeline.build(
        inputs,
        int(release_cutoff.timestamp()) * 1_000_000_000,
        coverage_start_ns=coverage_start_ns,
    )
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    published = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend(REPOSITORY)),
        release_tag,
        (),
    )
    with EventSpool(shared_spool) as spool:
        pmxt_events = sum(
            spool.count_condition(market.condition_id) for market in discovery.markets
        )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovery.markets),
        "pmxt_events": pmxt_events,
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(published),
        "source_bytes": shared_source_bytes,
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": _peak_rss_kib(),
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def run_day(
    day: date,
    work_root: Path,
    ledger: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...],
    starts: tuple[int, ...] | None = None,
    release_prefix: str = DATASET_RELEASE_PREFIX,
    expected_market_identities: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None = None,
    expected_source_identities: dict[str, tuple[int, str]] | None = None,
) -> list[dict[str, Any]]:
    spool, discoveries, provenance, source_bytes = prepare_shared_day(
        day,
        work_root,
        coverage_start,
        cutoff,
        assets,
        starts,
        expected_market_identities,
        expected_source_identities,
    )
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    partition_coverage_start = (
        datetime.fromtimestamp(min(starts), UTC) if starts else max(day_start, coverage_start)
    )
    coverage_start_ns = int(partition_coverage_start.timestamp()) * 1_000_000_000
    results = []
    for index, asset in enumerate(assets):
        results.append(
            run_partition(
                asset,
                day,
                work_root,
                ledger,
                cutoff,
                discoveries[asset],
                spool,
                provenance[asset],
                source_bytes if index == 0 else 0,
                release_prefix,
                coverage_start_ns,
            )
        )
    shutil.rmtree(spool.parent)
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--coverage-start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--assets", default=",".join(asset.value for asset in Asset))
    parser.add_argument("--market-starts", default="")
    parser.add_argument("--release-prefix", default=DATASET_RELEASE_PREFIX)
    parser.add_argument("--expected-market-identities", type=Path)
    parser.add_argument("--expected-source-identities", type=Path)
    args = parser.parse_args()
    assets = tuple(Asset(value) for value in args.assets.split(","))
    if not assets or len(set(assets)) != len(assets):
        parser.error("--assets must contain a non-empty unique asset subset")
    if args.coverage_start.tzinfo is None or args.cutoff.tzinfo is None:
        parser.error("--coverage-start and --cutoff must be timezone-aware")
    starts = tuple(int(value) for value in args.market_starts.split(",") if value)
    expected_market_identities = None
    if args.expected_market_identities is not None:
        raw_expected = json.loads(args.expected_market_identities.read_bytes())
        expected_market_identities = {
            Asset(asset): frozenset(
                (
                    str(value["condition_id"]),
                    frozenset(str(token) for token in value["token_ids"]),
                )
                for value in values
            )
            for asset, values in raw_expected.items()
        }
    expected_source_identities = None
    if args.expected_source_identities is not None:
        raw_sources = json.loads(args.expected_source_identities.read_bytes())
        expected_source_identities = {
            str(url): (int(value["byte_length"]), str(value["etag"]))
            for url, value in raw_sources.items()
        }
    current = args.start
    while current <= args.end:
        for result in run_day(
            current,
            args.work_root,
            args.ledger,
            args.coverage_start,
            args.cutoff,
            assets,
            starts or None,
            args.release_prefix,
            expected_market_identities,
            expected_source_identities,
        ):
            print(json.dumps(result, sort_keys=True), flush=True)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
