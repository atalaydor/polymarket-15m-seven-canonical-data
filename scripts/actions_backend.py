"""Bounded GitHub Actions adapter for the finite Chapter 4 executor."""

from __future__ import annotations

import argparse
import concurrent.futures
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
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from canonical_data.audit import canonical_json_bytes
from canonical_data.planner import build_backfill_plan

REPOSITORY = "atalaydor/polymarket-doge-bnb-hype-canonical-data"
API = f"https://api.github.com/repos/{REPOSITORY}"
PLAN_START = date(2026, 4, 5)
PLAN_END = date(2026, 8, 7)
EXPECTED_FILES = {
    "book-200ms.parquet",
    "book-events.parquet",
    "exclusions.parquet",
    "manifest.json",
    "markets.parquet",
    "underlying.parquet",
}
CANARY_PARTITIONS = (
    "HYPE/5m/2026-05-23",  # durable normal-production no-op
    "DOGE/5m/2026-05-25",  # durable GitHub TLS-retry/no-op class
    "HYPE/5m/2026-05-30",  # checkout-TLS job never entered production
    "BNB/5m/2026-06-01",  # durable explicit EXCLUDED no-op
    "DOGE/5m/2026-06-03",
    "BNB/5m/2026-06-03",
    "HYPE/5m/2026-06-03",
    "DOGE/5m/2026-06-04",
    "BNB/5m/2026-06-04",
    "HYPE/5m/2026-06-04",
    "DOGE/5m/2026-06-05",
    "BNB/5m/2026-06-05",
    "HYPE/5m/2026-06-05",
)
ASSET_PATTERN = re.compile(
    r"^(DOGE|BNB|HYPE)--5m--(\d{4}-\d{2}-\d{2})--([0-9a-f]{64})--(.+)$"
)
KACHO_REVISION = "f646aa62c6ffe3873fefb9f4a9b4ef7df0f5b4e5"
KACHO_TICKS = {
    "DOGE": (
        "doge_ticks.parquet",
        "7fabd65d9af567ca39ac520f4e7ee749d059f8574aa3f53ea5d3db37a3707cee.source",
        "7fabd65d9af567ca39ac520f4e7ee749d059f8574aa3f53ea5d3db37a3707cee",
        80_060_676,
    ),
    "BNB": (
        "bnb_ticks.parquet",
        "1958c9643a15add7620703329f47d3f67d8ff4c5c8d6115696bc745fb866478e.source",
        "1958c9643a15add7620703329f47d3f67d8ff4c5c8d6115696bc745fb866478e",
        76_690_643,
    ),
    "HYPE": (
        "hype_ticks.parquet",
        "f99e833ba91bde2406d288b3419d502db396282749e5c07125c01ee4ebbe3dd6.source",
        "f99e833ba91bde2406d288b3419d502db396282749e5c07125c01ee4ebbe3dd6",
        74_977_804,
    ),
}
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
TRANSFER_RETRY_DELAYS = (2, 8, 32)


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
        "User-Agent": "chapter-4-actions-backend/1.0",
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


def remote_inventory() -> dict[str, list[RemoteAsset]]:
    releases = _json(f"{API}/releases?per_page=100")
    result: dict[str, list[RemoteAsset]] = {}
    for release in releases:
        release_tag = str(release.get("tag_name", ""))
        if not release_tag.startswith("dataset-v1-"):
            continue
        release_id = int(release["id"])
        for page in range(1, 11):
            assets = _json(f"{API}/releases/{release_id}/assets?per_page=100&page={page}")
            for item in assets:
                match = ASSET_PATTERN.fullmatch(str(item["name"]))
                if match is None:
                    raise RuntimeError(
                        f"dataset release contains a noncanonical asset name: {item['name']}"
                    )
                partition = f"{match[1]}/5m/{match[2]}"
                if release_tag != f"dataset-v1-{match[2][:7]}":
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


def select_proof_partition(
    ledger: dict[str, Any], inventory: dict[str, list[RemoteAsset]]
) -> str:
    plan = build_backfill_plan(PLAN_START, PLAN_END)
    completed = int(ledger["completed"])
    if completed < 1 or completed > len(plan):
        raise RuntimeError("ledger completed count is outside the finite plan")
    complete = verified_partitions(inventory)
    if completed != len(complete):
        raise RuntimeError("ledger completed count does not match durable remote evidence")
    requested = str(ledger.get("last_partition", ""))
    if requested not in {str(item["partition_id"]) for item in plan}:
        raise RuntimeError("ledger proof partition is outside the finite plan")
    if requested not in complete:
        raise RuntimeError(
            f"ledger proof partition lacks durable remote evidence: {requested}"
        )
    return requested


def unfinished_plan(inventory: dict[str, list[RemoteAsset]]) -> list[dict[str, Any]]:
    complete = verified_partitions(inventory)
    return [
        item
        for item in build_backfill_plan(PLAN_START, PLAN_END)
        if item["partition_id"] not in complete
    ]


def inventory_anomalies(inventory: dict[str, list[RemoteAsset]]) -> dict[str, list[str]]:
    plan_ids = {
        str(item["partition_id"]) for item in build_backfill_plan(PLAN_START, PLAN_END)
    }
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
    return {
        "partial": partial,
        "divergent": divergent,
        "out_of_plan": sorted(set(inventory) - plan_ids),
    }


def day_plan(plan: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {"day": day}
        for day in sorted({str(item["partition_id"]).split("/")[2] for item in plan})
    ]


def matrix_stages(plan: list[dict[str, Any]]) -> list[dict[str, list[dict[str, str]]]]:
    stages = []
    days = day_plan(plan)
    for offset in range(0, len(days), 256):
        include = days[offset : offset + 256]
        stages.append({"include": include})
    return stages or [{"include": []}]


def _download_verify(asset: RemoteAsset, directory: Path) -> None:
    target = directory / asset.filename
    target.write_bytes(_request(asset.url, "application/octet-stream"))
    payload = target.read_bytes()
    if len(payload) != asset.size or hashlib.sha256(payload).hexdigest() != asset.digest:
        raise RuntimeError(f"remote digest verification failed: {asset.name}")


def verify_remote_partition(
    partition: str, inventory: dict[str, list[RemoteAsset]]
) -> dict[str, Any]:
    assets = inventory.get(partition, [])
    if partition not in verified_partitions(inventory):
        raise RuntimeError(f"partition is not durably complete: {partition}")
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        for asset in sorted(assets, key=lambda item: item.filename):
            _download_verify(asset, directory)
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if manifest["partition_id"] != partition:
            raise RuntimeError("remote manifest partition identity mismatch")
    return {
        "partition_id": partition,
        "result": "VERIFIED_NO_OP",
        "assets": len(assets),
        "bytes": sum(asset.size for asset in assets),
        "authenticated_redownload": bool(os.environ.get("GITHUB_TOKEN")),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


def kacho_required(day: str) -> bool:
    return date.fromisoformat(day) <= date(2026, 5, 18)


def _raise_child_failure(completed: subprocess.CompletedProcess[str]) -> None:
    if not completed.returncode:
        return
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    raise RuntimeError(f"one-partition executor failed with exit code {completed.returncode}")


def acquire_kacho(asset: str, root: Path) -> int:
    source_name, target_name, expected_digest, expected_bytes = KACHO_TICKS[asset]
    root.mkdir(parents=True, exist_ok=True)
    target = root / target_name
    url = (
        "https://huggingface.co/datasets/kachoio/"
        "polymarket-5-minute-crypto-up-down-markets/resolve/"
        f"{KACHO_REVISION}/{source_name}"
    )
    request = urllib.request.Request(url, headers={"User-Agent": "chapter-4-actions-backend/1.0"})
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=300) as response, target.open(
                "wb"
            ) as handle:
                shutil.copyfileobj(response, handle, 1_048_576)
                status = int(response.status)
                content_type = response.headers.get_content_type()
                content_length = response.headers.get("Content-Length")
                etag = response.headers.get("ETag")
                response_host = urlsplit(response.geturl()).hostname
            break
        except urllib.error.HTTPError as exc:
            target.unlink(missing_ok=True)
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            target.unlink(missing_ok=True)
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            assert last_error is not None
            raise last_error
    actual_bytes = target.stat().st_size
    actual_digest = _sha256(target)
    if actual_bytes != expected_bytes or actual_digest != expected_digest:
        diagnostics = {
            "source": source_name,
            "revision": KACHO_REVISION,
            "status": status,
            "response_host": response_host,
            "content_type": content_type,
            "content_length": content_length,
            "etag": etag,
            "expected_bytes": expected_bytes,
            "actual_bytes": actual_bytes,
            "expected_sha256": expected_digest,
            "actual_sha256": actual_digest,
        }
        raise RuntimeError(
            f"Kacho source integrity mismatch: {json.dumps(diagnostics, sort_keys=True)}"
        )
    return actual_bytes


def _atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_bytes(canonical_json_bytes(value))
    os.replace(temporary, path)


def remote_manifest(partition: str, inventory: dict[str, list[RemoteAsset]]) -> dict[str, Any]:
    assets = inventory.get(partition, [])
    manifests = [asset for asset in assets if asset.filename == "manifest.json"]
    if len(manifests) != 1 or partition not in verified_partitions(inventory):
        raise RuntimeError(f"partition lacks one durable complete manifest: {partition}")
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        _download_verify(manifests[0], directory)
        manifest = json.loads((directory / "manifest.json").read_bytes())
    if manifest.get("partition_id") != partition:
        raise RuntimeError("remote manifest partition identity mismatch")
    return cast(dict[str, Any], manifest)


def reconcile_ledger(
    inventory: dict[str, list[RemoteAsset]], last_partition: str | None = None
) -> dict[str, Any]:
    path = Path("config/backfill-ledger.json")
    ledger = json.loads(path.read_bytes())
    complete = verified_partitions(inventory)
    plan_ids = {str(item["partition_id"]) for item in build_backfill_plan(PLAN_START, PLAN_END)}
    if not complete <= plan_ids:
        raise RuntimeError("durable inventory contains an out-of-plan partition")
    summaries = cast(dict[str, dict[str, Any]], ledger.setdefault("partitions", {}))
    for partition in tuple(summaries):
        if partition not in complete:
            del summaries[partition]
    for partition in complete & summaries.keys():
        remote_digest = next(
            asset.digest
            for asset in inventory[partition]
            if asset.filename == "manifest.json"
        )
        if summaries[partition].get("manifest_sha256") != remote_digest:
            raise RuntimeError(
                f"ledger manifest digest diverges from remote authority: {partition}"
            )
    missing = sorted(complete - summaries.keys())
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        downloaded = dict(
            zip(
                missing,
                pool.map(lambda item: remote_manifest(item, inventory), missing),
                strict=True,
            )
        )
    for partition, manifest in downloaded.items():
        summaries[partition] = {
            "quality": str(manifest["quality_tier"]),
            "manifest_sha256": next(
                asset.digest
                for asset in inventory[partition]
                if asset.filename == "manifest.json"
            ),
        }
    qualities = [str(summaries[partition]["quality"]) for partition in complete]
    ledger["completed"] = len(complete)
    ledger["tier_a"] = qualities.count("TIER_A")
    ledger["tier_b"] = qualities.count("TIER_B")
    ledger["excluded"] = qualities.count("EXCLUDED")
    if sum(ledger[key] for key in ("tier_a", "tier_b", "excluded")) != len(complete):
        raise RuntimeError("remote manifest contains an unsupported quality tier")
    if last_partition is not None:
        if last_partition not in complete:
            raise RuntimeError("last partition lacks durable remote authority")
        ledger["last_partition"] = last_partition
    elif ledger.get("last_partition") not in complete:
        ledger["last_partition"] = sorted(complete)[-1] if complete else None
    remaining = unfinished_plan(inventory)
    ledger["continuation_partition"] = (
        remaining[0]["partition_id"] if remaining else None
    )
    ledger["measured_canonical_bytes"] = sum(
        asset.size for partition in complete for asset in inventory[partition]
    )
    ledger["remote_assets"] = sum(len(items) for items in inventory.values())
    _atomic_json(path, ledger)
    return cast(dict[str, Any], ledger)


def _write_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def command_plan() -> None:
    inventory = remote_inventory()
    complete = verified_partitions(inventory)
    plan = unfinished_plan(inventory)
    stages = matrix_stages(plan)
    report = {
        "planned": 375,
        "verified": len(complete),
        "unfinished": len(plan),
        "verified_in_plan": sum(item["partition_id"] in complete for item in plan),
        "unique_unfinished": len({item["partition_id"] for item in plan}),
        "stages": [len(stage["include"]) for stage in stages],
        "production_unit": "same-day unfinished asset batch",
        "days": len(day_plan(plan)),
        "max_matrix": max(len(stage["include"]) for stage in stages),
        "max_parallel": 1,
        "fail_fast": False,
        "gaps": [str(item["partition_id"]) for item in plan],
        "anomalies": inventory_anomalies(inventory),
    }
    print(json.dumps(report, sort_keys=True))
    _write_output("matrix_0", json.dumps(stages[0], separators=(",", ":")))
    _write_output(
        "matrix_1",
        json.dumps(stages[1] if len(stages) > 1 else {"include": []}, separators=(",", ":")),
    )
    _write_output("unfinished", str(len(plan)))


def command_proof() -> None:
    began = time.monotonic()
    inventory = remote_inventory()
    ledger = json.loads(Path("config/backfill-ledger.json").read_bytes())
    partition = select_proof_partition(ledger, inventory)
    report = verify_remote_partition(partition, inventory)
    report["wall_seconds"] = time.monotonic() - began
    print(json.dumps(report, sort_keys=True))


def command_execute_day(
    day: str, requested_assets: tuple[str, ...] | None = None
) -> list[dict[str, Any]]:
    before = remote_inventory()
    date.fromisoformat(day)
    valid = {str(item["partition_id"]) for item in unfinished_plan(before)}
    candidates = requested_assets or ("DOGE", "BNB", "HYPE")
    assets = [asset for asset in candidates if f"{asset}/5m/{day}" in valid]
    if not assets:
        proofs = [
            verify_remote_partition(f"{asset}/5m/{day}", before)
            for asset in candidates
            if f"{asset}/5m/{day}" in verified_partitions(before)
        ]
        for proof in proofs:
            print(json.dumps(proof, sort_keys=True))
        return proofs
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        kacho_bytes = sum(
            acquire_kacho(asset, root / "kacho") for asset in assets
        ) if kacho_required(day) else 0
        detailed = root / "partition-ledger.json"
        command = [
            "python",
            "scripts/run_backfill.py",
            "--kacho-root",
            str(root / "kacho"),
            "--work-root",
            str(root / "work"),
            "--ledger",
            str(detailed),
            "--start",
            day,
            "--end",
            day,
            "--assets",
            ",".join(assets),
        ]
        samples = {"peak_work_bytes": 0, "minimum_free_disk_bytes": shutil.disk_usage(root).free}
        stopped = threading.Event()

        def sample_disk() -> None:
            while not stopped.wait(1):
                used = 0
                for path in root.rglob("*"):
                    try:
                        if path.is_file():
                            used += path.stat().st_size
                    except FileNotFoundError:
                        # The child removes verified temporary inputs concurrently.
                        continue
                samples["peak_work_bytes"] = max(samples["peak_work_bytes"], used)
                samples["minimum_free_disk_bytes"] = min(
                    samples["minimum_free_disk_bytes"], shutil.disk_usage(root).free
                )

        sampler = threading.Thread(target=sample_disk, daemon=True)
        sampler.start()
        began = time.monotonic()
        try:
            completed = subprocess.run(command, check=False, text=True, capture_output=True)
        finally:
            stopped.set()
            sampler.join()
        wall = time.monotonic() - began
        if completed.returncode:
            # A preceding asset in the same process may already be durably published.
            reconcile_ledger(remote_inventory())
        _raise_child_failure(completed)
        lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
        if len(lines) != len(assets):
            raise RuntimeError("day executor did not return exactly one result per asset")
        results = [json.loads(line) for line in lines]
        for result in results:
            result["kacho_transfer_bytes"] = kacho_bytes
            result["actions_day_wall_seconds"] = wall
            result.update(samples)
            result["timeout_margin_seconds"] = 21_600 - wall
            print(json.dumps(result, sort_keys=True))
    after = remote_inventory()
    for result in results:
        proof = verify_remote_partition(str(result["partition_id"]), after)
        print(json.dumps(proof, sort_keys=True))
    reconcile_ledger(after, str(results[-1]["partition_id"]))
    return results


def command_canary() -> None:
    began = time.monotonic()
    for day in sorted({partition.split("/")[2] for partition in CANARY_PARTITIONS}):
        requested = tuple(
            partition.split("/")[0]
            for partition in CANARY_PARTITIONS
            if partition.endswith(f"/{day}")
        )
        command_execute_day(day, requested)
    inventory = remote_inventory()
    outcomes = []
    for partition in CANARY_PARTITIONS:
        proof = verify_remote_partition(partition, inventory)
        quality = str(remote_manifest(partition, inventory)["quality_tier"])
        outcomes.append({"partition_id": partition, "quality": quality, **proof})
        # A second authenticated verification is the required no-op rerun proof.
        verify_remote_partition(partition, inventory)
    report = {
        "canary_partitions": len(outcomes),
        "assets": sorted({item.split("/")[0] for item in CANARY_PARTITIONS}),
        "dates": len({item.split("/")[2] for item in CANARY_PARTITIONS}),
        "success": sum(item["quality"] != "EXCLUDED" for item in outcomes),
        "excluded": sum(item["quality"] == "EXCLUDED" for item in outcomes),
        "unexplained_failures": 0,
        "authenticated_no_op_reruns": len(outcomes),
        "wall_seconds": time.monotonic() - began,
    }
    print(json.dumps(report, sort_keys=True))


def command_certify() -> None:
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory)
    complete = verified_partitions(inventory)
    if len(complete) != 375 or any(anomalies.values()):
        raise RuntimeError(
            "Chapter 4 certification requires exactly 375 durable partitions and no anomalies"
        )
    ledger = reconcile_ledger(inventory)
    if int(ledger["completed"]) != 375 or ledger["continuation_partition"] is not None:
        raise RuntimeError("reconciled ledger is not complete")
    report = {
        "schema_version": "1.0.0",
        "chapter": 4,
        "status": "CERTIFIED",
        "certified_at": datetime.now(UTC).isoformat(),
        "release_cutoff": "2026-08-07T15:00:00Z",
        "planned": 375,
        "durable": 375,
        "tier_a": int(ledger["tier_a"]),
        "tier_b": int(ledger["tier_b"]),
        "excluded": int(ledger["excluded"]),
        "unfinished": 0,
        "remote_assets": sum(len(items) for items in inventory.values()),
        "release_tags": sorted(
            {
                asset.release_tag
                for assets in inventory.values()
                for asset in assets
                if asset.release_tag is not None
            }
        ),
        "anomalies": anomalies,
        "durable_identity": "content-addressed asset names and embedded partition manifests",
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
    }
    _atomic_json(Path("docs/chapter-4-certification.json"), report)
    print(json.dumps(report, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    commands.add_parser("proof")
    commands.add_parser("canary")
    commands.add_parser("reconcile")
    commands.add_parser("certify")
    execute = commands.add_parser("execute")
    execute.add_argument("--partition")
    day = commands.add_parser("execute-day")
    day.add_argument("--day", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        command_plan()
    elif args.command == "proof":
        command_proof()
    elif args.command == "canary":
        command_canary()
    elif args.command == "reconcile":
        print(json.dumps(reconcile_ledger(remote_inventory()), sort_keys=True))
    elif args.command == "certify":
        command_certify()
    elif args.command == "execute-day":
        command_execute_day(args.day)
    else:
        if not args.partition:
            raise RuntimeError("--partition is required")
        _, timeframe, requested_day = args.partition.split("/")
        if timeframe != "5m":
            raise RuntimeError("unsupported timeframe")
        requested_asset = args.partition.split("/")[0]
        command_execute_day(requested_day, (requested_asset,))


if __name__ == "__main__":
    main()
