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
from itertools import combinations
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
from canonical_data.models import Asset, Market
from canonical_data.planner import build_backfill_plan, release_bucket
from canonical_data.quality import classify
from scripts.run_backfill import (
    DATASET_RELEASE_PREFIX,
    REPOSITORY,
    _fetch_gamma,
)

API = f"https://api.github.com/repos/{REPOSITORY}"
CANARY_RELEASE_PREFIX = "polymarket-15m-seven-canary-v3"
AUTHORITY_PATH = Path("config/production-plan.json")
CANARY_RECEIPT_PATH = Path("config/canary-receipt.json")
LEDGER_PATH = Path("config/backfill-ledger.json")
CANARY_MAX_CANDIDATES = 8
CANARY_MAX_GAMMA_REQUESTS = CANARY_MAX_CANDIDATES * len(tuple(Asset))
CANARY_MAX_SOURCE_OBJECTS = 3
CANARY_MAX_SOURCE_BYTES = 2_400_000_000
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
class QualifiedCandidate:
    start: int
    markets: tuple[tuple[Asset, Market], ...]
    payloads: tuple[tuple[Asset, bytes, str], ...]


@dataclass(frozen=True)
class CanaryQualification:
    candidates: tuple[QualifiedCandidate, ...]
    source_objects: tuple[tuple[str, int, str], ...]
    gamma_requests: int
    source_requests: int


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
    if raw["publication"].get("canary_release_prefix") != CANARY_RELEASE_PREFIX:
        raise RuntimeError("canary release namespace is not frozen")
    canary = raw["canary"]
    search = canary["search"]
    limits = canary["limits"]
    if limits != {
        "max_candidates": CANARY_MAX_CANDIDATES,
        "max_gamma_requests": CANARY_MAX_GAMMA_REQUESTS,
        "max_source_objects": CANARY_MAX_SOURCE_OBJECTS,
        "max_source_transfer_bytes": CANARY_MAX_SOURCE_BYTES,
    }:
        raise RuntimeError("canary discovery limits are not frozen")
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
    candidates = _candidate_starts(authority)
    if len(candidates) != CANARY_MAX_CANDIDATES:
        raise RuntimeError("canary search does not use its exact bounded candidate budget")
    if len({datetime.fromtimestamp(start, UTC).date() for start in candidates}) != 1:
        raise RuntimeError("canary search must fit one UTC source-reuse day")
    source_bundle = pmxt_hourly_objects(
        (min(candidates) - 3_600) * 1_000_000_000,
        (max(candidates) + 900) * 1_000_000_000,
    )
    if len(source_bundle) != CANARY_MAX_SOURCE_OBJECTS:
        raise RuntimeError("canary search does not fit its source-object budget")
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


def _validate_receipt_coverage(
    receipt: dict[str, Any], authority: Authority, qualified_starts: list[int]
) -> None:
    usable_raw = receipt.get("usable_market_starts_by_asset")
    proofs_raw = receipt.get("remote_proofs")
    selected = receipt.get("selected_market_starts")
    asset_selection = receipt.get("asset_market_starts")
    expected_assets = {asset.value for asset in authority.assets}
    if (
        not isinstance(usable_raw, dict)
        or not isinstance(proofs_raw, dict)
        or not isinstance(selected, list)
        or not isinstance(asset_selection, dict)
        or set(usable_raw) != expected_assets
        or set(proofs_raw) != expected_assets
        or set(asset_selection) != expected_assets
    ):
        raise RuntimeError("canary receipt has malformed authenticated coverage")
    usable_by_start = {start: set[Asset]() for start in qualified_starts}
    for asset in authority.assets:
        starts = usable_raw[asset.value]
        proof = proofs_raw[asset.value]
        if (
            not isinstance(starts, list)
            or not starts
            or len(starts) != len(set(starts))
            or not set(starts).issubset(set(qualified_starts))
            or not isinstance(proof, dict)
            or proof.get("accepted_market_starts") != starts
            or proof.get("quality") != "TIER_A"
            or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("manifest_sha256", ""))) is None
        ):
            raise RuntimeError("canary receipt usable evidence is not proof-bound")
        for start in starts:
            usable_by_start[int(start)].add(asset)
    computed = minimum_canary_cover(
        {start: frozenset(assets) for start, assets in usable_by_start.items()},
        authority.assets,
    )
    if (
        selected != list(computed)
        or len(selected) != len(set(selected))
        or any(
            asset_selection[asset.value] not in usable_raw[asset.value]
            or asset_selection[asset.value] not in selected
            for asset in authority.assets
        )
    ):
        raise RuntimeError("canary receipt does not contain the exact usable minimum cover")


def _require_canary_receipt(authority: Authority) -> dict[str, Any]:
    if not CANARY_RECEIPT_PATH.exists():
        raise RuntimeError("full planning is locked until the one canary receipt is committed")
    receipt = json.loads(CANARY_RECEIPT_PATH.read_bytes())
    qualified_starts = receipt.get("qualified_market_starts", [])
    selected_starts = receipt.get("selected_market_starts", [])
    asset_starts = receipt.get("asset_market_starts", {})
    if (
        not isinstance(qualified_starts, list)
        or not isinstance(selected_starts, list)
        or not isinstance(asset_starts, dict)
    ):
        raise RuntimeError("canary receipt has malformed coverage selection")
    allowed_starts = set(_candidate_starts(authority))
    if (
        receipt.get("status") != "PASSED"
        or receipt.get("dataset_id") != "polymarket-15m-seven-v1"
        or receipt.get("timeframe") != "15m"
        or receipt.get("assets") != [asset.value for asset in authority.assets]
        or receipt.get("unexplained_failures") != 0
        or receipt.get("authenticated_no_op_partitions") != len(authority.assets)
        or receipt.get("settlement_bindings") != len(authority.assets)
        or receipt.get("usable_market_bindings") != len(authority.assets)
        or receipt.get("legitimate_exclusion_contract_checks") != len(authority.assets)
        or receipt.get("canary_release_prefix") != CANARY_RELEASE_PREFIX
        or not str(receipt.get("release_tag", "")).startswith(f"{CANARY_RELEASE_PREFIX}-")
        or receipt.get("candidate_limit") != CANARY_MAX_CANDIDATES
        or not 1 <= int(receipt.get("qualified_candidates", 0)) <= CANARY_MAX_CANDIDATES
        or len(qualified_starts) != int(receipt.get("qualified_candidates", 0))
        or len(qualified_starts) != len(set(qualified_starts))
        or not set(qualified_starts).issubset(allowed_starts)
        or not 1 <= len(selected_starts) <= CANARY_MAX_CANDIDATES
        or not set(selected_starts).issubset(set(qualified_starts))
        or receipt.get("common_window") != (len(selected_starts) == 1)
        or set(asset_starts) != {asset.value for asset in authority.assets}
        or not set(asset_starts.values()).issubset(set(selected_starts))
        or not len(authority.assets)
        <= int(receipt.get("gamma_requests", 0))
        <= CANARY_MAX_GAMMA_REQUESTS
        or not 1 <= int(receipt.get("source_head_requests", 0)) <= CANARY_MAX_SOURCE_OBJECTS
        or receipt.get("shared_source_transfer_owners") != 1
        or not 1 <= int(receipt.get("shared_pmxt_objects", 0)) <= CANARY_MAX_SOURCE_OBJECTS
        or receipt.get("shared_pmxt_objects") != receipt.get("source_head_requests")
        or not 1 <= int(receipt.get("source_transfer_bytes", 0)) <= CANARY_MAX_SOURCE_BYTES
        or int(receipt.get("canonical_bytes", 0)) < 1
        or receipt.get("isolated_from_production") is not True
        or float(receipt.get("timeout_margin_seconds", 0)) <= 3_600
        or int(receipt.get("peak_rss_kib", 0)) < 1
        or int(receipt.get("minimum_free_disk_bytes", 0)) < 8_000_000_000
        or receipt.get("control_plane_sha256") != _control_plane_digest()
    ):
        raise RuntimeError("canary receipt does not authorize the frozen full plan")
    _validate_receipt_coverage(receipt, authority, qualified_starts)
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


def _verify_canary_dispositions(
    market_rows: list[dict[str, Any]],
    exclusion_rows: list[dict[str, Any]],
    expected_candidates: dict[str, Market],
) -> list[int]:
    accepted_ids = [str(row["market_id"]) for row in market_rows]
    excluded_ids = [str(row["market_id"]) for row in exclusion_rows]
    if (
        len(accepted_ids) != len(set(accepted_ids))
        or len(excluded_ids) != len(set(excluded_ids))
        or set(accepted_ids) & set(excluded_ids)
        or set(accepted_ids) | set(excluded_ids) != set(expected_candidates)
    ):
        raise RuntimeError("remote canary candidate disposition is incomplete")
    for row in market_rows:
        expected = expected_candidates[str(row["market_id"])]
        actual = (
            str(row["condition_id"]),
            frozenset((str(row["token_up"]), str(row["token_down"]))),
            int(row["market_start_ns"]),
        )
        wanted = (
            expected.condition_id,
            frozenset((expected.token_up, expected.token_down)),
            expected.market_start_ns,
        )
        if actual != wanted or row["quality_tier"] != "TIER_A":
            raise RuntimeError("remote usable canary market changed identity or quality")
    for row in exclusion_rows:
        expected = expected_candidates[str(row["market_id"])]
        evidence = json.loads(str(row["evidence_json"]))
        if evidence.get("condition_id") != expected.condition_id:
            raise RuntimeError("remote canary exclusion lost its condition binding")
    return sorted(int(row["market_start_ns"]) // 1_000_000_000 for row in market_rows)


def verify_remote_partition(
    partition: str,
    inventory: dict[str, list[RemoteAsset]],
    expected_market: tuple[str, frozenset[str]] | None = None,
    expected_sources: frozenset[tuple[str, int, str]] | None = None,
    expected_candidates: dict[str, Market] | None = None,
    expected_gamma: frozenset[tuple[str, int, str]] | None = None,
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
        exclusion_rows = pq.read_table(directory / "exclusions.parquet").to_pylist()
        if expected_market is not None:
            published = {
                (row["condition_id"], frozenset((row["token_up"], row["token_down"])))
                for row in market_rows
            }
            if published != {expected_market}:
                raise RuntimeError("remote canary market does not match source-qualified identity")
        if expected_candidates is not None:
            _verify_canary_dispositions(market_rows, exclusion_rows, expected_candidates)
        if expected_sources is not None:
            published_sources = frozenset(
                (item["source_url"], int(item["byte_length"]), str(item.get("etag", "")))
                for item in manifest["provenance"]
                if item["source_id"] == "pmxt_v2"
            )
            if published_sources != expected_sources:
                raise RuntimeError("remote canary PMXT provenance changed")
        if expected_gamma is not None:
            published_gamma = frozenset(
                (
                    str(item["source_url"]),
                    int(item["byte_length"]),
                    str(item["sha256"]),
                )
                for item in manifest["provenance"]
                if item["source_id"] == "polymarket_gamma_clob"
            )
            if published_gamma != expected_gamma:
                raise RuntimeError("remote canary Gamma provenance changed")
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
        "accepted_market_starts": sorted(
            int(row["market_start_ns"]) // 1_000_000_000 for row in market_rows
        ),
        "exclusions": len(exclusion_rows),
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
    if not result or len(result) > CANARY_MAX_CANDIDATES:
        raise RuntimeError("canary candidate search is empty or exceeds its finite cap")
    return result


def _pmxt_source_identity(source: SourceObject) -> tuple[int, str]:
    request = urllib.request.Request(source.url, method="HEAD", headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = int(response.status)
                if not 200 <= status < 300:
                    raise RuntimeError(f"PMXT canary HEAD returned HTTP {status}")
                length = int(response.headers.get("Content-Length", "0"))
                etag = response.headers.get("ETag", "")
                if length <= 0 or not etag:
                    raise RuntimeError("PMXT canary HEAD lacks object identity")
                return length, etag
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise RuntimeError(
                    f"catalog-listed PMXT canary source is missing: {source.url}"
                ) from exc
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            break
    assert last_error is not None
    raise last_error


def qualify_canary_candidates(
    authority: Authority,
    source_identity: Callable[[SourceObject], tuple[int, str]] = _pmxt_source_identity,
) -> CanaryQualification:
    gamma = GammaClient(fetch=_fetch_gamma)
    gamma_requests = 0
    source_requests = 0
    candidates: list[QualifiedCandidate] = []
    source_identities: dict[str, tuple[int, str]] = {}
    seen_market_ids: set[str] = set()
    seen_conditions: set[str] = set()
    for start in _candidate_starts(authority):
        markets: list[tuple[Asset, Market]] = []
        payloads: list[tuple[Asset, bytes, str]] = []
        rejected = False
        for asset in authority.assets:
            gamma_requests += 1
            if gamma_requests > CANARY_MAX_GAMMA_REQUESTS:
                raise RuntimeError("canary exceeded its Gamma request budget")
            try:
                market, payload, url = gamma.fetch_market(asset, start)
            except (IdentityError, UnresolvedMarketError):
                rejected = True
                break
            if (
                market.asset is not asset
                or market.timeframe != "15m"
                or market.market_start_ns != start * 1_000_000_000
                or market.market_end_ns - market.market_start_ns != 900_000_000_000
            ):
                raise RuntimeError("Gamma candidate violates exact 15m identity")
            markets.append((asset, market))
            payloads.append((asset, payload, url))
        if rejected:
            continue
        market_ids = {market.market_id for _, market in markets}
        conditions = {market.condition_id for _, market in markets}
        if (
            len(market_ids) != len(authority.assets)
            or len(conditions) != len(authority.assets)
            or market_ids & seen_market_ids
            or conditions & seen_conditions
        ):
            raise RuntimeError("Gamma reused an identity across canary assets or windows")
        seen_market_ids.update(market_ids)
        seen_conditions.update(conditions)
        source_objects = pmxt_hourly_objects(
            (start - 3_600) * 1_000_000_000,
            (start + 900) * 1_000_000_000,
        )
        if any(source.url in PMXT_MISSING_OBJECT_URLS for source in source_objects):
            continue
        for source in source_objects:
            if source.url in source_identities:
                continue
            source_requests += 1
            if source_requests > CANARY_MAX_SOURCE_OBJECTS:
                raise RuntimeError("canary exceeded its PMXT source-object budget")
            source_identities[source.url] = source_identity(source)
            if sum(length for length, _ in source_identities.values()) > CANARY_MAX_SOURCE_BYTES:
                raise RuntimeError("canary exceeded its PMXT source-transfer budget")
        candidates.append(QualifiedCandidate(start, tuple(markets), tuple(payloads)))
    if not candidates:
        raise RuntimeError("bounded Actions discovery found no authoritative 15m candidates")
    starts = [candidate.start for candidate in candidates]
    if len({datetime.fromtimestamp(start, UTC).date() for start in starts}) != 1:
        raise RuntimeError("canary candidates must share one UTC source-reuse day")
    required_sources = pmxt_hourly_objects(
        (min(starts) - 3_600) * 1_000_000_000,
        (max(starts) + 900) * 1_000_000_000,
    )
    if any(source.url in PMXT_MISSING_OBJECT_URLS for source in required_sources):
        raise RuntimeError("canary source bundle crosses canonical PMXT absence")
    for source in required_sources:
        if source.url not in source_identities:
            source_requests += 1
            if source_requests > CANARY_MAX_SOURCE_OBJECTS:
                raise RuntimeError("canary exceeded its PMXT source-object budget")
            source_identities[source.url] = source_identity(source)
    if sum(length for length, _ in source_identities.values()) > CANARY_MAX_SOURCE_BYTES:
        raise RuntimeError("canary exceeded its PMXT source-transfer budget")
    return CanaryQualification(
        tuple(candidates),
        tuple((url, length, etag) for url, (length, etag) in sorted(source_identities.items())),
        gamma_requests,
        source_requests,
    )


def minimum_canary_cover(
    usable_by_start: dict[int, frozenset[Asset]],
    required_assets: tuple[Asset, ...] = tuple(Asset),
) -> tuple[int, ...]:
    required = frozenset(required_assets)
    starts = sorted(usable_by_start, reverse=True)
    for size in range(1, len(starts) + 1):
        for selected in combinations(starts, size):
            covered = frozenset().union(*(usable_by_start[start] for start in selected))
            if covered == required:
                return selected
    raise RuntimeError("bounded canary candidates provide no usable evidence cover")


def command_canary() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        raise RuntimeError("canary requires authenticated GitHub remote authority")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not run_id.isdigit():
        raise RuntimeError("canary requires its numeric GitHub Actions run identity")
    authority = load_authority()
    began = time.monotonic()
    disk_before = shutil.disk_usage(Path.cwd()).free
    production_before = remote_inventory()
    qualification = qualify_canary_candidates(authority)
    starts = tuple(candidate.start for candidate in qualification.candidates)
    day = datetime.fromtimestamp(starts[0], UTC).date()
    coverage_start = datetime.fromtimestamp(min(starts), UTC)
    cutoff = datetime.fromtimestamp(max(starts) + 900, UTC)
    release_prefix = f"{CANARY_RELEASE_PREFIX}-{run_id}-{max(starts)}-{min(starts)}"
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    markets_by_asset: dict[Asset, dict[str, Market]] = {asset: {} for asset in authority.assets}
    gamma_by_asset: dict[Asset, set[tuple[str, int, str]]] = {
        asset: set() for asset in authority.assets
    }
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        work_root = root / "work"
        ledger_path = root / "ledger.json"
        expected_markets_path = root / "expected-markets.json"
        expected_sources_path = root / "expected-sources.json"
        for candidate in qualification.candidates:
            candidate_markets = dict(candidate.markets)
            for asset, payload, url in candidate.payloads:
                market = candidate_markets[asset]
                markets_by_asset[asset][market.market_id] = market
                gamma_by_asset[asset].add((url, len(payload), hashlib.sha256(payload).hexdigest()))
                slug = f"{asset.value.lower()}-updown-15m-{candidate.start}"
                cache = work_root / f"{asset.value}-{day.isoformat()}" / "official"
                cache.mkdir(parents=True, exist_ok=True)
                (cache / f"{slug}.json").write_bytes(payload)
        _atomic_json(
            expected_markets_path,
            {
                asset.value: [
                    {
                        "condition_id": market.condition_id,
                        "token_ids": sorted((market.token_up, market.token_down)),
                    }
                    for market in sorted(
                        markets_by_asset[asset].values(), key=lambda item: item.market_start_ns
                    )
                ]
                for asset in authority.assets
            },
        )
        _atomic_json(
            expected_sources_path,
            {
                url: {"byte_length": byte_length, "etag": etag}
                for url, byte_length, etag in qualification.source_objects
            },
        )
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
                    str(work_root),
                    "--ledger",
                    str(ledger_path),
                    "--start",
                    day.isoformat(),
                    "--end",
                    day.isoformat(),
                    "--coverage-start",
                    coverage_start.isoformat(),
                    "--cutoff",
                    cutoff.isoformat(),
                    "--market-starts",
                    ",".join(str(start) for start in starts),
                    "--release-prefix",
                    release_prefix,
                    "--expected-market-identities",
                    str(expected_markets_path),
                    "--expected-source-identities",
                    str(expected_sources_path),
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
    if any(
        item["markets"] != len(qualification.candidates) for item in ledger["partitions"].values()
    ):
        raise RuntimeError("canary execution lost an authoritative candidate identity")

    canary_inventory = remote_inventory(exact_tags={release_tag})
    proofs: list[dict[str, Any]] = []
    proofs_by_asset: dict[Asset, dict[str, Any]] = {}
    expected_sources = frozenset(qualification.source_objects)
    usable_by_start = {start: set[Asset]() for start in starts}
    for asset in authority.assets:
        partition = f"{asset.value}/15m/{day.isoformat()}"
        proof = verify_remote_partition(
            partition,
            canary_inventory,
            expected_sources=expected_sources,
            expected_candidates=markets_by_asset[asset],
            expected_gamma=frozenset(gamma_by_asset[asset]),
        )
        if not proof["authenticated_redownload"]:
            raise RuntimeError("canary publication proof is incomplete")
        for start in proof["accepted_market_starts"]:
            usable_by_start[int(start)].add(asset)
        proofs.append(proof)
        proofs_by_asset[asset] = proof
        verify_remote_partition(
            partition,
            canary_inventory,
            expected_sources=expected_sources,
            expected_candidates=markets_by_asset[asset],
            expected_gamma=frozenset(gamma_by_asset[asset]),
        )
    selected_starts = minimum_canary_cover(
        {start: frozenset(assets) for start, assets in usable_by_start.items()},
        authority.assets,
    )
    asset_market_starts = {
        asset.value: next(start for start in selected_starts if asset in usable_by_start[start])
        for asset in authority.assets
    }
    usable_market_starts_by_asset = {
        asset.value: sorted(
            start for start, usable_assets in usable_by_start.items() if asset in usable_assets
        )
        for asset in authority.assets
    }
    common_pmxt_urls = set(proofs[0]["pmxt_urls"])
    if not common_pmxt_urls or any(set(proof["pmxt_urls"]) != common_pmxt_urls for proof in proofs):
        raise RuntimeError("seven assets do not share one PMXT acquisition inventory")
    if remote_inventory() != production_before:
        raise RuntimeError("isolated canary publication changed production authority")

    exclusion_checks = 0
    for _, market in qualification.candidates[0].markets:
        tier, exclusion = classify(market, False, [(market.market_start_ns, market.market_end_ns)])
        if tier.value != "EXCLUDED" or exclusion is None or not exclusion.evidence:
            raise RuntimeError("fail-closed exclusion contract lacks actual-market evidence")
        exclusion_checks += 1
    wall_seconds = time.monotonic() - began
    disk_after = shutil.disk_usage(Path.cwd()).free
    source_bytes = sum(int(item["source_bytes"]) for item in ledger["partitions"].values())
    source_owners = sum(int(item["source_bytes"]) > 0 for item in ledger["partitions"].values())
    if source_owners != 1 or not 0 < source_bytes <= CANARY_MAX_SOURCE_BYTES:
        raise RuntimeError("shared PMXT transfer was not charged exactly once")
    receipt: dict[str, Any] = {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-15m-seven-v1",
        "status": "PASSED",
        "timeframe": "15m",
        "assets": [asset.value for asset in authority.assets],
        "qualified_market_starts": list(starts),
        "selected_market_starts": list(selected_starts),
        "asset_market_starts": asset_market_starts,
        "usable_market_starts_by_asset": usable_market_starts_by_asset,
        "remote_proofs": {
            asset.value: {
                "accepted_market_starts": usable_market_starts_by_asset[asset.value],
                "manifest_sha256": proofs_by_asset[asset]["manifest_sha256"],
                "quality": proofs_by_asset[asset]["quality"],
            }
            for asset in authority.assets
        },
        "common_window": len(selected_starts) == 1,
        "release_tag": release_tag,
        "canary_release_prefix": CANARY_RELEASE_PREFIX,
        "isolated_from_production": True,
        "candidate_limit": CANARY_MAX_CANDIDATES,
        "qualified_candidates": len(qualification.candidates),
        "gamma_requests": qualification.gamma_requests,
        "source_head_requests": qualification.source_requests,
        "settlement_bindings": len(authority.assets),
        "usable_market_bindings": len(asset_market_starts),
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
