"""Bounded Actions control plane for the Polymarket 15m x 7 dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient
from canonical_data.errors import IdentityError, UnresolvedMarketError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import (
    PMXT_MISSING_OBJECT_URLS,
    PMXT_OBJECT_COVERAGE_CUTOFF,
    PMXT_VALIDATION_COVERAGE_START,
    SourceObject,
    pmxt_hourly_objects,
)
from canonical_data.models import Asset
from canonical_data.planner import build_backfill_plan, release_bucket
from canonical_data.quality import classify
from scripts.run_backfill import (
    DATASET_RELEASE_PREFIX,
    REPOSITORY,
    _fetch_gamma,
)

API = f"https://api.github.com/repos/{REPOSITORY}"
CANARY_RELEASE_PREFIX = "polymarket-15m-seven-canary-v1"
AUTHORITY_PATH = Path("config/production-plan.json")
CANARY_RECEIPT_PATH = Path("config/canary-receipt.json")
LEDGER_PATH = Path("config/backfill-ledger.json")
EXPECTED_FILES = {
    "book-200ms.parquet",
    "book-events.parquet",
    "exclusions.parquet",
    "manifest.json",
    "markets.parquet",
    "underlying.parquet",
}
ASSET_PATTERN = re.compile(
    r"^(BTC|ETH|SOL|XRP|DOGE|BNB|HYPE)--15m--"
    r"(\d{4}-\d{2}-\d{2})--([0-9a-f]{64})--(.+)$"
)
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
TRANSFER_RETRY_DELAYS = (2, 8, 32)


@dataclass(frozen=True)
class Authority:
    start: datetime
    cutoff: datetime
    assets: tuple[Asset, ...]
    canary_search_start: datetime
    canary_search_end: datetime
    canary_step_minutes: int


@dataclass(frozen=True)
class RemoteAsset:
    name: str
    size: int
    url: str
    digest: str
    filename: str
    state: str = "uploaded"
    asset_id: str | None = None
    release_tag: str | None = None


def _request(url: str, accept: str = "application/vnd.github+json") -> bytes:
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {
        "Accept": accept,
        "User-Agent": "polymarket-15m-seven-actions/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return cast(bytes, response.read())
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            break
    assert last_error is not None
    raise last_error


def _json(url: str) -> Any:
    return json.loads(_request(url))


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise RuntimeError("authority timestamps must be UTC")
    return parsed.astimezone(UTC)


def load_authority(path: Path = AUTHORITY_PATH) -> Authority:
    raw = json.loads(path.read_bytes())
    expected_assets = tuple(Asset)
    assets = tuple(Asset(item) for item in raw["partition"]["assets"])
    if raw.get("dataset_id") != "polymarket-15m-seven-v1":
        raise RuntimeError("production plan has the wrong dataset identity")
    if raw["partition"].get("timeframe") != "15m" or assets != expected_assets:
        raise RuntimeError("production plan is not exactly the frozen 15m x 7 scope")
    if raw["publication"].get("release_prefix") != DATASET_RELEASE_PREFIX:
        raise RuntimeError("production release namespace is not frozen")
    search = raw["canary"]["search"]
    authority = Authority(
        _parse_utc(raw["coverage_start"]),
        _parse_utc(raw["release_cutoff"]),
        assets,
        _parse_utc(search["start"]),
        _parse_utc(search["end"]),
        int(search["step_minutes"]),
    )
    if authority.cutoff <= authority.start:
        raise RuntimeError("release cutoff must follow release start")
    if (
        authority.start < PMXT_VALIDATION_COVERAGE_START
        or authority.cutoff > PMXT_OBJECT_COVERAGE_CUTOFF
    ):
        raise RuntimeError("production coverage exceeds authoritative PMXT validation coverage")
    if authority.canary_search_start < authority.canary_search_end:
        raise RuntimeError("canary search must run newest to oldest")
    if (
        authority.canary_search_end < authority.start
        or authority.canary_search_start + timedelta(minutes=15) > authority.cutoff
    ):
        raise RuntimeError("canary search exceeds production source coverage")
    if authority.canary_step_minutes not in {15, 30, 60}:
        raise RuntimeError("canary search step is outside the finite allowed set")
    return authority


def _full_plan(authority: Authority) -> list[dict[str, Any]]:
    final_day = (authority.cutoff - timedelta(microseconds=1)).date()
    return build_backfill_plan(authority.start.date(), final_day)


def _control_plane_digest() -> str:
    tracked = subprocess.check_output(["git", "ls-files", "-z"], encoding="utf-8").split("\0")
    excluded = {"config/backfill-ledger.json", "config/canary-receipt.json"}
    digest = hashlib.sha256()
    for name in sorted(item for item in tracked if item and item not in excluded):
        path = Path(name)
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _require_canary_receipt(authority: Authority) -> dict[str, Any]:
    if not CANARY_RECEIPT_PATH.exists():
        raise RuntimeError("full planning is locked until the one canary receipt is committed")
    receipt = json.loads(CANARY_RECEIPT_PATH.read_bytes())
    if (
        receipt.get("status") != "PASSED"
        or receipt.get("dataset_id") != "polymarket-15m-seven-v1"
        or receipt.get("timeframe") != "15m"
        or receipt.get("assets") != [asset.value for asset in authority.assets]
        or receipt.get("unexplained_failures") != 0
        or receipt.get("authenticated_no_op_partitions") != len(authority.assets)
        or receipt.get("settlement_bindings") != len(authority.assets)
        or receipt.get("legitimate_exclusion_contract_checks") != len(authority.assets)
        or receipt.get("shared_source_transfer_owners") != 1
        or int(receipt.get("shared_pmxt_objects", 0)) < 1
        or int(receipt.get("source_transfer_bytes", 0)) < 1
        or int(receipt.get("canonical_bytes", 0)) < 1
        or receipt.get("isolated_from_production") is not True
        or float(receipt.get("timeout_margin_seconds", 0)) <= 3_600
        or int(receipt.get("peak_rss_kib", 0)) < 1
        or int(receipt.get("minimum_free_disk_bytes", 0)) < 8_000_000_000
        or receipt.get("control_plane_sha256") != _control_plane_digest()
    ):
        raise RuntimeError("canary receipt does not authorize the frozen full plan")
    return cast(dict[str, Any], receipt)


def remote_inventory(
    release_prefix: str = DATASET_RELEASE_PREFIX,
    exact_tags: set[str] | None = None,
) -> dict[str, list[RemoteAsset]]:
    result: dict[str, list[RemoteAsset]] = {}
    releases = []
    for page in range(1, 11):
        batch = _json(f"{API}/releases?per_page=100&page={page}")
        releases.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise RuntimeError("release inventory exceeded bounded pagination")
    for release in releases:
        release_tag = str(release.get("tag_name", ""))
        if exact_tags is not None:
            selected = release_tag in exact_tags
        else:
            selected = release_tag.startswith(f"{release_prefix}-")
        if not selected:
            continue
        release_id = int(release["id"])
        for page in range(1, 11):
            assets = _json(f"{API}/releases/{release_id}/assets?per_page=100&page={page}")
            for item in assets:
                match = ASSET_PATTERN.fullmatch(str(item["name"]))
                if match is None:
                    raise RuntimeError(
                        f"15m release contains a noncanonical asset name: {item['name']}"
                    )
                partition = f"{match[1]}/15m/{match[2]}"
                expected_tag = f"{release_prefix}-{release_bucket(date.fromisoformat(match[2]))}"
                if exact_tags is None and release_tag != expected_tag:
                    raise RuntimeError(f"partition is published in the wrong release: {partition}")
                result.setdefault(partition, []).append(
                    RemoteAsset(
                        str(item["name"]),
                        int(item["size"]),
                        str(item["url"]),
                        match[3],
                        match[4],
                        str(item.get("state", "")),
                        str(item["id"]),
                        release_tag,
                    )
                )
            if len(assets) < 100:
                break
        else:
            raise RuntimeError("release asset inventory exceeded bounded pagination")
    return result


def verified_partitions(inventory: dict[str, list[RemoteAsset]]) -> set[str]:
    verified: set[str] = set()
    for partition, assets in inventory.items():
        filenames = [asset.filename for asset in assets]
        if (
            len(assets) == len(EXPECTED_FILES)
            and set(filenames) == EXPECTED_FILES
            and all(asset.state == "uploaded" for asset in assets)
        ):
            verified.add(partition)
    return verified


def unfinished_plan(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> list[dict[str, Any]]:
    selected = authority or load_authority()
    complete = verified_partitions(inventory)
    return [item for item in _full_plan(selected) if item["partition_id"] not in complete]


def inventory_anomalies(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> dict[str, list[str]]:
    selected = authority or load_authority()
    plan_ids = {str(item["partition_id"]) for item in _full_plan(selected)}
    verified = verified_partitions(inventory)
    partial = sorted(
        partition
        for partition, assets in inventory.items()
        if partition in plan_ids and partition not in verified and assets
    )
    divergent = sorted(
        f"{partition}/{filename}"
        for partition, assets in inventory.items()
        for filename in {asset.filename for asset in assets}
        if len({asset.digest for asset in assets if asset.filename == filename}) > 1
    )
    duplicate = sorted(
        f"{partition}/{filename}"
        for partition, assets in inventory.items()
        for filename in {asset.filename for asset in assets}
        if sum(asset.filename == filename for asset in assets) > 1
    )
    unexpected_files = sorted(
        f"{partition}/{asset.filename}"
        for partition, assets in inventory.items()
        for asset in assets
        if asset.filename not in EXPECTED_FILES
    )
    return {
        "partial": partial,
        "divergent": divergent,
        "duplicate": duplicate,
        "unexpected_files": unexpected_files,
        "out_of_plan": sorted(set(inventory) - plan_ids),
    }


def _fatal_inventory_anomalies(anomalies: dict[str, list[str]]) -> bool:
    return any(values for name, values in anomalies.items() if name != "partial")


def day_plan(plan: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {"day": day} for day in sorted({str(item["partition_id"]).split("/")[2] for item in plan})
    ]


def _download_verify(asset: RemoteAsset, directory: Path) -> Path:
    target = directory / asset.filename
    target.write_bytes(_request(asset.url, "application/octet-stream"))
    payload = target.read_bytes()
    if len(payload) != asset.size or hashlib.sha256(payload).hexdigest() != asset.digest:
        raise RuntimeError(f"remote digest verification failed: {asset.name}")
    return target


def verify_remote_partition(
    partition: str, inventory: dict[str, list[RemoteAsset]]
) -> dict[str, Any]:
    expected_asset = partition.split("/", 1)[0]
    assets = inventory.get(partition, [])
    if partition not in verified_partitions(inventory):
        raise RuntimeError(f"partition is not durably complete: {partition}")
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        for asset in sorted(assets, key=lambda item: item.filename):
            _download_verify(asset, directory)
        payload = (directory / "manifest.json").read_bytes()
        manifest = json.loads(payload)
        if canonical_json_bytes(manifest) != payload:
            raise RuntimeError("remote manifest is not canonical JSON")
        if (
            manifest.get("dataset_id") != "polymarket-15m-seven-v1"
            or manifest.get("partition_id") != partition
            or manifest.get("timeframe") != "15m"
        ):
            raise RuntimeError("remote manifest identity mismatch")
        market_rows = pq.read_table(directory / "markets.parquet").to_pylist()
        for row in market_rows:
            if (
                row["asset"] != expected_asset
                or row["timeframe"] != "15m"
                or row["market_end_ns"] - row["market_start_ns"] != 900_000_000_000
                or row["official_outcome"] not in {"UP", "DOWN", "SPLIT"}
                or not str(row["resolution_source_url"]).startswith(
                    "https://data.chain.link/streams/"
                )
            ):
                raise RuntimeError("remote market lacks frozen 15m settlement semantics")
    return {
        "partition_id": partition,
        "result": "VERIFIED_NO_OP",
        "assets": len(assets),
        "bytes": sum(asset.size for asset in assets),
        "markets": len(market_rows),
        "quality": manifest["quality_tier"],
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "pmxt_urls": sorted(
            item["source_url"] for item in manifest["provenance"] if item["source_id"] == "pmxt_v2"
        ),
        "authenticated_redownload": bool(os.environ.get("GITHUB_TOKEN")),
    }


def _atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_bytes(canonical_json_bytes(value))
    os.replace(temporary, path)


def reconcile_ledger(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> dict[str, Any]:
    selected = authority or load_authority()
    anomalies = inventory_anomalies(inventory, selected)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    complete = verified_partitions(inventory)
    partitions: dict[str, Any] = {}
    for partition in sorted(complete):
        manifest_asset = next(
            asset for asset in inventory[partition] if asset.filename == "manifest.json"
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = _download_verify(manifest_asset, Path(temporary))
            manifest = json.loads(path.read_bytes())
        partitions[partition] = {
            "manifest_sha256": manifest_asset.digest,
            "quality": manifest["quality_tier"],
            "release_tag": manifest_asset.release_tag,
        }
    plan = _full_plan(selected)
    unfinished = [
        str(item["partition_id"]) for item in plan if item["partition_id"] not in complete
    ]
    return {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-15m-seven-v1",
        "planned": len(plan),
        "completed": len(complete),
        "unfinished": len(unfinished),
        "continuation_partition": unfinished[0] if unfinished else None,
        "partitions": partitions,
        "durable_identity": "remote content-addressed assets plus embedded manifests",
    }


def _write_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def command_plan() -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    unfinished = unfinished_plan(inventory, authority)
    days = day_plan(unfinished)
    if len(days) > 256:
        raise RuntimeError("finite plan exceeds the single bounded Actions matrix")
    matrix = json.dumps({"include": days}, separators=(",", ":"))
    _write_output("matrix", matrix)
    print(
        json.dumps(
            {
                "planned_partitions": len(_full_plan(authority)),
                "durable_partitions": len(verified_partitions(inventory)),
                "unfinished_partitions": len(unfinished),
                "utc_days": len(days),
                "matrix": json.loads(matrix),
            },
            sort_keys=True,
        )
    )


def _raise_child_failure(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.returncode == 0:
        return
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    raise RuntimeError(f"15m executor failed with exit code {completed.returncode}")


def command_execute_day(day_text: str) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    day = date.fromisoformat(day_text)
    plan_for_day = [item for item in _full_plan(authority) if item["day"] == day_text]
    if not plan_for_day:
        raise RuntimeError("requested UTC day is outside the frozen plan")
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    complete = verified_partitions(inventory)
    assets = tuple(
        Asset(str(item["asset"])) for item in plan_for_day if item["partition_id"] not in complete
    )
    if not assets:
        print(json.dumps({"day": day_text, "result": "AUTHENTICATED_NO_OP"}))
        return
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_backfill",
                "--work-root",
                str(root / "work"),
                "--ledger",
                str(root / "ledger.json"),
                "--start",
                day.isoformat(),
                "--end",
                day.isoformat(),
                "--coverage-start",
                authority.start.isoformat(),
                "--cutoff",
                authority.cutoff.isoformat(),
                "--assets",
                ",".join(asset.value for asset in assets),
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        _raise_child_failure(completed)
        sys.stdout.write(completed.stdout)
    refreshed = remote_inventory()
    for asset in assets:
        verify_remote_partition(f"{asset.value}/15m/{day_text}", refreshed)
    _atomic_json(LEDGER_PATH, reconcile_ledger(refreshed, authority))


def _candidate_starts(authority: Authority) -> list[int]:
    current = authority.canary_search_start
    result = []
    while current >= authority.canary_search_end:
        timestamp = int(current.timestamp())
        if timestamp % 900:
            raise RuntimeError("canary search boundaries must be 15m aligned")
        result.append(timestamp)
        current -= timedelta(minutes=authority.canary_step_minutes)
    if not result or len(result) > 192:
        raise RuntimeError("canary candidate search is empty or exceeds its finite cap")
    return result


def _pmxt_source_exists(source: SourceObject) -> bool:
    request = urllib.request.Request(source.url, method="HEAD", headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = int(response.status)
                return 200 <= status < 300
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            break
    assert last_error is not None
    raise last_error


def find_canary_start(
    authority: Authority,
    source_exists: Callable[[SourceObject], bool] = _pmxt_source_exists,
) -> tuple[int, dict[Asset, Any], int]:
    gamma = GammaClient(fetch=_fetch_gamma)
    attempts = 0
    for start in _candidate_starts(authority):
        source_objects = pmxt_hourly_objects(
            (start - 3_600) * 1_000_000_000,
            (start + 900) * 1_000_000_000,
        )
        if any(source.url in PMXT_MISSING_OBJECT_URLS for source in source_objects):
            continue
        for source in source_objects:
            if not source_exists(source):
                raise RuntimeError(
                    f"catalog-listed PMXT canary source is missing: {source.url}"
                )
        markets: dict[Asset, Any] = {}
        rejected = False
        for asset in authority.assets:
            attempts += 1
            try:
                market, _, _ = gamma.fetch_market(asset, start)
            except (IdentityError, UnresolvedMarketError):
                rejected = True
                break
            if (
                market.timeframe != "15m"
                or market.market_start_ns != start * 1_000_000_000
                or market.market_end_ns - market.market_start_ns != 900_000_000_000
            ):
                raise RuntimeError("Gamma candidate violates exact 15m identity")
            markets[asset] = market
        if not rejected and len(markets) == len(authority.assets):
            return start, markets, attempts
    raise RuntimeError("bounded Gamma search found no resolved common 15m window for all assets")


def command_canary() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        raise RuntimeError("canary requires authenticated GitHub remote authority")
    authority = load_authority()
    began = time.monotonic()
    disk_before = shutil.disk_usage(Path.cwd()).free
    production_before = remote_inventory()
    start, markets, gamma_attempts = find_canary_start(authority)
    day = datetime.fromtimestamp(start, UTC).date()
    cutoff = datetime.fromtimestamp(start + 900, UTC)
    release_prefix = f"{CANARY_RELEASE_PREFIX}-{start}"
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        ledger_path = root / "ledger.json"
        minimum_free_disk = disk_before
        stop_sampling = threading.Event()

        def sample_disk() -> None:
            nonlocal minimum_free_disk
            while not stop_sampling.wait(1):
                minimum_free_disk = min(minimum_free_disk, shutil.disk_usage(root).free)

        sampler = threading.Thread(target=sample_disk, daemon=True)
        sampler.start()
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "scripts.run_backfill",
                    "--work-root",
                    str(root / "work"),
                    "--ledger",
                    str(ledger_path),
                    "--start",
                    day.isoformat(),
                    "--end",
                    day.isoformat(),
                    "--coverage-start",
                    datetime.fromtimestamp(start, UTC).isoformat(),
                    "--cutoff",
                    cutoff.isoformat(),
                    "--market-starts",
                    str(start),
                    "--release-prefix",
                    release_prefix,
                ],
                text=True,
                capture_output=True,
                check=False,
            )
        finally:
            stop_sampling.set()
            sampler.join()
        _raise_child_failure(completed)
        sys.stdout.write(completed.stdout)
        ledger = json.loads(ledger_path.read_bytes())
    if len(ledger["partitions"]) != len(authority.assets):
        raise RuntimeError("canary did not execute exactly seven isolated partitions")
    if any(item["markets"] != 1 for item in ledger["partitions"].values()):
        raise RuntimeError("canary did not bind one actual market for every asset")
    if any(item["quality"] != "TIER_A" for item in ledger["partitions"].values()):
        raise RuntimeError("canary requires usable PMXT evidence for every asset")

    canary_inventory = remote_inventory(exact_tags={release_tag})
    proofs = []
    for asset in authority.assets:
        partition = f"{asset.value}/15m/{day.isoformat()}"
        proof = verify_remote_partition(partition, canary_inventory)
        if proof["markets"] != 1 or not proof["authenticated_redownload"]:
            raise RuntimeError("canary publication proof is incomplete")
        proofs.append(proof)
        verify_remote_partition(partition, canary_inventory)
    common_pmxt_urls = set(proofs[0]["pmxt_urls"])
    if not common_pmxt_urls or any(set(proof["pmxt_urls"]) != common_pmxt_urls for proof in proofs):
        raise RuntimeError("seven assets do not share one PMXT acquisition inventory")
    if remote_inventory() != production_before:
        raise RuntimeError("isolated canary publication changed production authority")

    exclusion_checks = 0
    for market in markets.values():
        tier, exclusion = classify(market, False, [(market.market_start_ns, market.market_end_ns)])
        if tier.value != "EXCLUDED" or exclusion is None or not exclusion.evidence:
            raise RuntimeError("fail-closed exclusion contract lacks actual-market evidence")
        exclusion_checks += 1
    wall_seconds = time.monotonic() - began
    disk_after = shutil.disk_usage(Path.cwd()).free
    source_bytes = sum(int(item["source_bytes"]) for item in ledger["partitions"].values())
    source_owners = sum(int(item["source_bytes"]) > 0 for item in ledger["partitions"].values())
    if source_owners != 1:
        raise RuntimeError("shared PMXT transfer was not charged exactly once")
    receipt: dict[str, Any] = {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-15m-seven-v1",
        "status": "PASSED",
        "timeframe": "15m",
        "assets": [asset.value for asset in authority.assets],
        "market_start": datetime.fromtimestamp(start, UTC).isoformat(),
        "market_end": cutoff.isoformat(),
        "release_tag": release_tag,
        "isolated_from_production": True,
        "gamma_requests": gamma_attempts,
        "settlement_bindings": len(markets),
        "shared_pmxt_objects": len(common_pmxt_urls),
        "shared_source_transfer_owners": source_owners,
        "source_transfer_bytes": source_bytes,
        "canonical_bytes": sum(int(proof["bytes"]) for proof in proofs),
        "authenticated_no_op_partitions": len(proofs),
        "legitimate_exclusion_contract_checks": exclusion_checks,
        "unexplained_failures": 0,
        "wall_seconds": wall_seconds,
        "timeout_margin_seconds": 21_600 - wall_seconds,
        "peak_rss_kib": max(int(item["peak_rss_kib"]) for item in ledger["partitions"].values()),
        "disk_free_before_bytes": disk_before,
        "disk_free_after_bytes": disk_after,
        "minimum_free_disk_bytes": minimum_free_disk,
        "tool_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
        ).strip(),
        "control_plane_sha256": _control_plane_digest(),
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    if receipt["timeout_margin_seconds"] <= 3_600 or minimum_free_disk < 8_000_000_000:
        raise RuntimeError("canary lacks six-hour timeout or disk safety margin")
    _atomic_json(CANARY_RECEIPT_PATH, receipt)
    print(json.dumps(receipt, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    commands.add_parser("canary")
    commands.add_parser("reconcile")
    day = commands.add_parser("execute-day")
    day.add_argument("--day", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        command_plan()
    elif args.command == "canary":
        command_canary()
    elif args.command == "reconcile":
        authority = load_authority()
        _require_canary_receipt(authority)
        print(json.dumps(reconcile_ledger(remote_inventory(), authority), sort_keys=True))
    else:
        command_execute_day(args.day)


if __name__ == "__main__":
    main()
