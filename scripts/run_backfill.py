"""Sequential, restart-safe Chapter 4 executor."""

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

import pyarrow.parquet as pq

from canonical_data.acquire import BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.binance import ingest_binance_zip
from canonical_data.discovery import GammaClient
from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import SourceObject, expected_5m_market_starts, pmxt_hourly_objects
from canonical_data.manifest import hash_file
from canonical_data.models import Asset, BookEvent, Market, Provenance
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.release import GitHubReleaseBackend, Publisher
from canonical_data.sources import ProductionSourceLoader
from canonical_data.spool import EventSpool
from canonical_data.state import StateStore

RETRY_DELAYS = (2, 8, 32)
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
RELEASE_CUTOFF = datetime(2026, 8, 7, 15, tzinfo=UTC)
KACHO_FILES = {
    Asset.DOGE: (
        "ab8ddc3e1a143d5786b6817c04a2732e5be45da77b137e52275c3877fb5f00ef.source",
        "7fabd65d9af567ca39ac520f4e7ee749d059f8574aa3f53ea5d3db37a3707cee.source",
    ),
    Asset.BNB: (
        "85744b85e942f6d7348b5d9a87602fd523c2e42e33febeb519d5c2878d5c1efe.source",
        "1958c9643a15add7620703329f47d3f67d8ff4c5c8d6115696bc745fb866478e.source",
    ),
    Asset.HYPE: (
        "acfc39a2315f1340f41aec70b5e17e4a6275ad4614ffdf62cbfe3b23b035e022.source",
        "f99e833ba91bde2406d288b3419d502db396282749e5c07125c01ee4ebbe3dd6.source",
    ),
}

PMXT_FILTERED_ROWS_PER_ASSET_OBJECT = 500_000
MAX_SOURCE_OBJECT_BYTES = 800_000_000
PMXT_HTTP_404_GAPS = {
    f"https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T{hour}.parquet": {
        "accessed_at": "2026-08-14",
        "http_status": 404,
    }
    for hour in ("04", "05", "06")
}


def _peak_rss_kib() -> int:
    """Return Unix peak RSS while keeping metadata-only Windows imports usable."""
    try:
        resource_module = cast(Any, importlib.import_module("resource"))
    except ModuleNotFoundError:
        return 0
    return int(resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss)


def enforce_shared_pmxt_asset_caps(
    events: tuple[BookEvent, ...], markets_by_asset: Mapping[Asset, tuple[Market, ...]]
) -> None:
    """Preserve the frozen per-partition cap during one shared physical read."""
    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    counts = {asset: 0 for asset in markets_by_asset}
    for event in events:
        asset = owner.get(event.condition_id)
        if asset is None:
            raise SourceError("shared PMXT event is outside the bound market inventory")
        counts[asset] += 1
        if counts[asset] > PMXT_FILTERED_ROWS_PER_ASSET_OBJECT:
            raise ResourceLimitError("PMXT filtered output exceeds per-asset row cap")


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


def _fetch_text(url: str) -> str:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=60) as response:
                return cast(bytes, response.read(4096)).decode()
        except urllib.error.HTTPError as exc:
            if exc.code not in {408, 429, 500, 502, 503, 504}:
                raise
            last = exc
        except urllib.error.URLError as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _fetch_gamma(url: str, max_bytes: int) -> bytes:
    """Retry bounded official lookups without weakening identity validation."""
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = urllib.request.Request(
                url,
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
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


def _instrument(asset: Asset) -> tuple[str, str, str]:
    symbol = f"{asset.value}USDT"
    if asset is Asset.HYPE:
        root = "https://data.binance.vision/data/futures/um/daily"
        precision = "ms"
    else:
        root = "https://data.binance.vision/data/spot/daily"
        precision = "us"
    return symbol, root, precision


def _market_starts(metadata: Path, day: date) -> list[int]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    table = pq.read_table(
        metadata,
        columns=["market_start"],
        filters=[("market_start", ">=", start), ("market_start", "<", end)],
    )
    return sorted(int(value.timestamp()) for value in table["market_start"].to_pylist())


def _retry_pmxt(
    loader: ProductionSourceLoader, url: str, markets: tuple[Market, ...]
) -> Any:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            return loader.load_pmxt([url], markets)
        except ResourceLimitError:
            raise
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    if last is not None:
        raise last
    raise AssertionError("unreachable PMXT retry state")


def _load_pmxt_hour(
    loader: ProductionSourceLoader,
    source: SourceObject,
    markets: tuple[Market, ...],
    raw_dir: Path,
) -> Any:
    try:
        return _retry_pmxt(loader, source.url, markets)
    except ResourceLimitError as exc:
        if "selected row groups exceed scan-row cap" not in str(exc):
            raise
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            acquired = BoundedAcquirer(
                raw_dir, MAX_SOURCE_OBJECT_BYTES, 8_000_000_000
            ).acquire(source)
            loaded = loader.load_downloaded_pmxt(
                acquired.path, source.url, markets, acquired.etag
            )
            acquired.path.unlink(missing_ok=True)
            return loaded
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
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


def prepare_shared_day(
    day: date, kacho_root: Path, work_root: Path, assets: tuple[Asset, ...] = tuple(Asset)
) -> tuple[Path, dict[Asset, tuple[Provenance, ...]], int]:
    shared = work_root / f"shared-{day.isoformat()}"
    state_path = shared / "state.json"
    state: dict[str, Any] = (
        json.loads(state_path.read_bytes())
        if state_path.exists()
        else {"completed_urls": {}, "source_bytes": 0}
    )
    discoveries: dict[Asset, Any] = {}
    for asset in assets:
        starts = expected_5m_market_starts(day, RELEASE_CUTOFF)
        loader = ProductionSourceLoader(
            GammaClient(fetch=_fetch_gamma),
            time.time_ns(),
            work_root / f"{asset.value}-{day}" / "official",
        )
        discoveries[asset] = (
            loader,
            loader.discover(
                asset, starts, allow_missing=True, allow_unresolved=True
            ),
        )
    if not any(discoveries[asset][1].markets for asset in assets):
        spool_path = shared / "events.sqlite"
        with EventSpool(spool_path):
            pass
        return spool_path, {asset: () for asset in assets}, 0
    day_start_s = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    day_start_ns = day_start_s * 1_000_000_000
    day_end_ns = min(
        day_start_ns + 86_400_000_000_000,
        int(RELEASE_CUTOFF.timestamp()) * 1_000_000_000,
    )
    inventory_start_ns = max(day_start_ns - 3_600_000_000_000, 1_776_106_800_000_000_000)
    with EventSpool(shared / "events.sqlite", create_index=False) as spool:
        spool.drop_index()
        for source in pmxt_hourly_objects(inventory_start_ns, day_end_ns):
            if source.url in state["completed_urls"]:
                continue
            if source.url in PMXT_HTTP_404_GAPS:
                state.setdefault("source_gaps", {})[source.url] = PMXT_HTTP_404_GAPS[
                    source.url
                ]
                _atomic_json(state_path, state)
                continue
            acquired = _acquire_with_retry(source, shared / "raw")
            markets_by_asset = {asset: discoveries[asset][1].markets for asset in assets}
            combined_markets = tuple(
                market for markets in markets_by_asset.values() for market in markets
            )
            loaded = discoveries[assets[0]][0].load_downloaded_pmxt(
                acquired.path,
                source.url,
                combined_markets,
                acquired.etag,
                max_filtered_rows=(
                    PMXT_FILTERED_ROWS_PER_ASSET_OBJECT * len(markets_by_asset)
                ),
            )
            enforce_shared_pmxt_asset_caps(loaded.events, markets_by_asset)
            spool.append(loaded.events)
            by_asset = {asset.value: asdict(loaded.provenance[0]) for asset in assets}
            state["completed_urls"][source.url] = by_asset
            state["source_bytes"] = int(state["source_bytes"]) + acquired.byte_length
            _atomic_json(state_path, state)
            acquired.path.unlink(missing_ok=True)
        spool.ensure_index()
    gap_provenance = tuple(
        Provenance(
            source_id="pmxt_v2",
            source_url=url,
            retrieved_at_ns=1_786_665_600_000_000_000,
            byte_length=0,
            sha256=hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            license_id="CC-BY-4.0",
            source_precision="http_status",
            transformations=("authoritative_http_404_absence",),
        )
        for url, evidence in sorted(state.get("source_gaps", {}).items())
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
    return shared / "events.sqlite", provenance, int(state["source_bytes"])


def run_tier_b(
    asset: Asset,
    day: date,
    kacho_root: Path,
    work_root: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    partition_id = f"{asset.value}/5m/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    metadata_name, ticks_name = KACHO_FILES[asset]
    starts = _market_starts(kacho_root / metadata_name, day)
    if not starts:
        result = {
            "partition_id": partition_id,
            "quality": "EXCLUDED",
            "reason": "NO_MARKETS_IN_AUTHORITATIVE_PRODUCT_WINDOW",
            "markets": 0,
        }
        ledger["partitions"][partition_id] = result
        _atomic_json(ledger_path, ledger)
        return result
    work = work_root / f"{asset.value}-{day.isoformat()}"
    loader = ProductionSourceLoader(
        GammaClient(fetch=_fetch_gamma), time.time_ns(), work / "official"
    )
    discovered = loader.discover(asset, starts)
    kacho_rows, kacho_provenance = loader.load_kacho(kacho_root / ticks_name, discovered.markets)
    symbol, binance_root, precision = _instrument(asset)
    filename = f"{symbol}-1m-{day.isoformat()}.zip"
    url = f"{binance_root}/klines/{symbol}/1m/{filename}"
    checksum = _fetch_text(url + ".CHECKSUM").split()[0]
    acquired = _acquire_with_retry(
        SourceObject("binance_public", url, checksum), work / "raw", 10_000_000
    )
    observations = ingest_binance_zip(acquired.path, checksum, asset, "klines", url)
    binance_bytes, binance_digest = hash_file(acquired.path)
    binance_provenance = Provenance(
        "binance_public",
        url,
        time.time_ns(),
        binance_bytes,
        binance_digest,
        "MIT_archive_repository_claim",
        precision,
        upstream_checksum=checksum,
        transformations=("checksum_verify",),
    )
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovered.markets,
        kacho_rows=tuple(kacho_rows),
        underlying=tuple(observations),
        provenance=(*discovered.provenance, kacho_provenance, binance_provenance),
        temporary_raw_paths=(acquired.path,),
    )
    pipeline = Pipeline(
        work / "output",
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits(),
    )
    cutoff = int(
        (datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)).timestamp()
    )
    built = pipeline.build(inputs, cutoff * 1_000_000_000)
    release_tag = f"dataset-v1-{day:%Y-%m}"
    assets = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend("atalaydor/polymarket-doge-bnb-hype-canonical-data")),
        release_tag,
        inputs.temporary_raw_paths,
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovered.markets),
        "kacho_rows": len(kacho_rows),
        "underlying_rows": len(observations),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(assets),
        "source_bytes": binance_bytes,
        "wall_seconds": time.perf_counter() - began,
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def run_partition(
    asset: Asset,
    day: date,
    kacho_root: Path,
    work_root: Path,
    ledger_path: Path,
    shared_spool: Path | None = None,
    shared_provenance: tuple[Provenance, ...] = (),
    shared_source_bytes: int = 0,
) -> dict[str, Any]:
    if day <= date(2026, 4, 12):
        return run_tier_b(asset, day, kacho_root, work_root, ledger_path)
    partition_id = f"{asset.value}/5m/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    cpu_began = time.process_time()
    metadata_name, ticks_name = KACHO_FILES[asset]
    starts = expected_5m_market_starts(day, RELEASE_CUTOFF)
    work = work_root / f"{asset.value}-{day.isoformat()}"
    loader = ProductionSourceLoader(
        GammaClient(fetch=_fetch_gamma), time.time_ns(), work / "official"
    )
    discovered = loader.discover(
        asset, starts, allow_missing=True, allow_unresolved=True
    )
    if not discovered.markets:
        inputs = PartitionInputs(
            asset,
            day.isoformat(),
            (),
            provenance=discovered.provenance,
            preexisting_exclusions=discovered.exclusions,
        )
        pipeline = Pipeline(work / "output", StateStore(work / "state"), _tool_commit())
        partition_cutoff = min(
            datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1),
            RELEASE_CUTOFF,
        )
        built = pipeline.build(inputs, int(partition_cutoff.timestamp()) * 1_000_000_000)
        release_tag = f"dataset-v1-{day:%Y-%m}"
        assets = pipeline.publish(
            built,
            Publisher(GitHubReleaseBackend("atalaydor/polymarket-doge-bnb-hype-canonical-data")),
            release_tag,
            (),
        )
        result = {
            "partition_id": partition_id,
            "quality": built.tier.value,
            "markets": 0,
            "pmxt_events": 0,
            "kacho_rows": 0,
            "underlying_rows": 0,
            "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
            "manifest_sha256": built.manifest_digest,
            "release_tag": release_tag,
            "remote_assets": len(assets),
            "source_bytes": 0,
            "wall_seconds": time.perf_counter() - began,
            "cpu_seconds": time.process_time() - cpu_began,
            "peak_rss_kib": _peak_rss_kib(),
        }
        ledger["partitions"][partition_id] = result
        _atomic_json(ledger_path, ledger)
        shutil.rmtree(work)
        return result
    spool_path = shared_spool or work / "pmxt-events.sqlite"
    pmxt_provenance: list[Provenance] = list(shared_provenance)
    pmxt_events = 0
    day_start_ns = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1e9)
    inventory_start_ns = max(day_start_ns - 3_600_000_000_000, 1_776_106_800_000_000_000)
    day_end_ns = min(
        day_start_ns + 86_400_000_000_000,
        int(RELEASE_CUTOFF.timestamp()) * 1_000_000_000,
    )
    if shared_spool is None:
        with EventSpool(spool_path) as spool:
            for source in pmxt_hourly_objects(inventory_start_ns, day_end_ns):
                loaded = _load_pmxt_hour(loader, source, discovered.markets, work / "raw-pmxt")
                pmxt_events += spool.append(loaded.events)
                pmxt_provenance.extend(loaded.provenance)
    else:
        with EventSpool(spool_path) as spool:
            pmxt_events = sum(
                spool.count_condition(market.condition_id) for market in discovered.markets
            )
    kacho_rows: list[dict[str, object]] = []
    kacho_provenance: tuple[Provenance, ...] = ()
    if starts and day <= date(2026, 5, 18):
        rows, provenance = loader.load_kacho(kacho_root / ticks_name, discovered.markets)
        kacho_rows = rows
        kacho_provenance = (provenance,)
    symbol, binance_root, precision = _instrument(asset)
    filename = f"{symbol}-1m-{day.isoformat()}.zip"
    url = f"{binance_root}/klines/{symbol}/1m/{filename}"
    checksum = _fetch_text(url + ".CHECKSUM").split()[0]
    acquired = _acquire_with_retry(
        SourceObject("binance_public", url, checksum), work / "raw", 10_000_000
    )
    observations = ingest_binance_zip(acquired.path, checksum, asset, "klines", url)
    binance_bytes, binance_digest = hash_file(acquired.path)
    binance_provenance = Provenance(
        "binance_public",
        url,
        time.time_ns(),
        binance_bytes,
        binance_digest,
        "MIT_archive_repository_claim",
        precision,
        upstream_checksum=checksum,
        transformations=("checksum_verify",),
    )
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovered.markets,
        kacho_rows=tuple(kacho_rows),
        underlying=tuple(observations),
        provenance=(
            *discovered.provenance,
            *pmxt_provenance,
            *kacho_provenance,
            binance_provenance,
        ),
        temporary_raw_paths=(acquired.path,) if shared_spool else (acquired.path, spool_path),
        event_spool_path=spool_path,
        preexisting_exclusions=discovered.exclusions,
    )
    pipeline = Pipeline(work / "output", StateStore(work / "state"), _tool_commit())
    built = pipeline.build(inputs, day_end_ns)
    release_tag = f"dataset-v1-{day:%Y-%m}"
    assets = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend("atalaydor/polymarket-doge-bnb-hype-canonical-data")),
        release_tag,
        inputs.temporary_raw_paths,
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovered.markets),
        "pmxt_events": pmxt_events,
        "kacho_rows": len(kacho_rows),
        "underlying_rows": len(observations),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(assets),
        "source_bytes": binance_bytes
        + shared_source_bytes
        + (0 if shared_spool else sum(item.byte_length for item in pmxt_provenance)),
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": _peak_rss_kib(),
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kacho-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 4, 5))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 4, 12))
    parser.add_argument("--asset", choices=[asset.value for asset in Asset])
    parser.add_argument(
        "--assets",
        help="comma-separated asset subset; mutually exclusive with --asset",
    )
    args = parser.parse_args()
    if args.asset and args.assets:
        parser.error("--asset and --assets are mutually exclusive")
    requested_assets = (
        tuple(Asset(value) for value in args.assets.split(","))
        if args.assets
        else ((Asset(args.asset),) if args.asset else tuple(Asset))
    )
    if not requested_assets or len(set(requested_assets)) != len(requested_assets):
        parser.error("--assets must contain a non-empty unique asset subset")
    current = args.start
    while current <= args.end:
        selected_assets = requested_assets
        shared_spool: Path | None = None
        shared_provenance: dict[Asset, tuple[Provenance, ...]] = {}
        shared_source_bytes = 0
        if current >= date(2026, 4, 14):
            shared_spool, shared_provenance, shared_source_bytes = prepare_shared_day(
                current, args.kacho_root, args.work_root, selected_assets
            )
        source_bytes_owner = next(
            (asset for asset in selected_assets if shared_provenance.get(asset)),
            selected_assets[0],
        )
        for asset in selected_assets:
            print(
                json.dumps(
                    run_partition(
                        asset,
                        current,
                        args.kacho_root,
                        args.work_root,
                        args.ledger,
                        shared_spool,
                        shared_provenance.get(asset, ()),
                        shared_source_bytes if asset is source_bytes_owner else 0,
                    ),
                    sort_keys=True,
                ),
                flush=True,
            )
        if shared_spool is not None:
            shutil.rmtree(shared_spool.parent)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
