from __future__ import annotations

import hashlib
import io
import json
import subprocess
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.inventory import SourceObject
from canonical_data.models import Asset, Exclusion, ExclusionReason, Provenance
from scripts.actions_backend import (
    KACHO_REVISION,
    KACHO_TICKS,
    RemoteAsset,
    _raise_child_failure,
    _request,
    acquire_kacho,
    inventory_anomalies,
    kacho_required,
    matrix_stages,
    select_proof_partition,
    unfinished_plan,
    verified_partitions,
)
from scripts.run_backfill import (
    MAX_SOURCE_OBJECT_BYTES,
    PMXT_HTTP_404_GAPS,
    _acquire_with_retry,
    _fetch_gamma,
    prepare_shared_day,
    run_partition,
)


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/octet-stream"
        self.headers["Content-Length"] = str(len(payload))
        self.headers["ETag"] = '"object-etag"'

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        return "https://cdn-lfs.example.test/object?credential=secret"


class ActionsBackendTests(unittest.TestCase):
    def test_github_request_retries_tls_transport_failure(self) -> None:
        tls_failure = urllib.error.URLError("certificate verify failed")
        with (
            patch(
                "scripts.actions_backend.urllib.request.urlopen",
                side_effect=(tls_failure, FakeResponse(b"{}")),
            ) as urlopen,
            patch("scripts.actions_backend.time.sleep") as sleep,
        ):
            self.assertEqual(_request("https://api.github.test/releases"), b"{}")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_child_failure_surfaces_captured_diagnostics(self) -> None:
        completed = subprocess.CompletedProcess(
            ["executor"], 7, stdout="child-out\n", stderr="child-error\n"
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            self.assertRaisesRegex(RuntimeError, "exit code 7"),
        ):
            _raise_child_failure(completed)
        self.assertEqual(stdout.getvalue(), "child-out\n")
        self.assertEqual(stderr.getvalue(), "child-error\n")

    def test_gamma_retries_transient_status_and_preserves_payload_bound(self) -> None:
        throttled = urllib.error.HTTPError(
            "https://example.test/gamma", 429, "Too Many Requests", Message(), None
        )
        with (
            patch(
                "scripts.run_backfill.urllib.request.urlopen",
                side_effect=(throttled, FakeResponse(b"[]")),
            ) as urlopen,
            patch("scripts.run_backfill.time.sleep") as sleep,
        ):
            self.assertEqual(_fetch_gamma("https://example.test/gamma", 2), b"[]")
            self.assertEqual(urlopen.call_count, 2)
            sleep.assert_called_once_with(2)
        with patch(
            "scripts.run_backfill.urllib.request.urlopen",
            return_value=FakeResponse(b"oversized"),
        ):
            with self.assertRaisesRegex(SourceError, "configured bound"):
                _fetch_gamma("https://example.test/gamma", 2)

    def test_acquisition_does_not_retry_limits_or_unexplained_404(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        missing = urllib.error.HTTPError(source.url, 404, "Not Found", Message(), None)
        for failure in (ResourceLimitError("cap"), missing):
            with (
                patch(
                    "scripts.run_backfill.BoundedAcquirer.acquire", side_effect=failure
                ) as acquire,
                patch("scripts.run_backfill.time.sleep") as sleep,
                TemporaryDirectory() as temporary,
            ):
                with self.assertRaises(type(failure)):
                    _acquire_with_retry(source, Path(temporary))
            acquire.assert_called_once()
            sleep.assert_not_called()

    def test_recovery_source_bound_and_exact_declared_gaps_are_finite(self) -> None:
        self.assertEqual(MAX_SOURCE_OBJECT_BYTES, 800_000_000)
        self.assertGreater(MAX_SOURCE_OBJECT_BYTES, 764_905_077)
        self.assertEqual(
            sorted(PMXT_HTTP_404_GAPS),
            [
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T04.parquet",
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T05.parquet",
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T06.parquet",
            ],
        )

    def test_all_missing_official_inventory_skips_pmxt_acquisition(self) -> None:
        class MissingLoader:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def discover(self, *args: object, **kwargs: object) -> object:
                from types import SimpleNamespace

                return SimpleNamespace(markets=(), provenance=(), exclusions=(object(),))

        with TemporaryDirectory() as temporary:
            with (
                patch("scripts.run_backfill.ProductionSourceLoader", MissingLoader),
                patch("scripts.run_backfill.pmxt_hourly_objects") as inventory,
            ):
                spool, provenance, source_bytes = prepare_shared_day(
                    date(2026, 6, 1),
                    Path(temporary) / "kacho",
                    Path(temporary) / "work",
                    (Asset.HYPE,),
                )
            inventory.assert_not_called()
            self.assertTrue(spool.exists())
            self.assertEqual(provenance, {Asset.HYPE: ()})
            self.assertEqual(source_bytes, 0)

    def test_all_missing_partition_skips_binance_and_writes_excluded_ledger(self) -> None:
        official = Provenance(
            "polymarket_gamma_clob",
            "https://gamma-api.polymarket.com/events?slug=missing",
            1,
            2,
            "a" * 64,
            "ungranted_for_bulk_redistribution",
            "official_api",
        )
        exclusion = Exclusion(
            "hype-updown-5m-missing",
            ExclusionReason.SOURCE_GAP,
            "official Gamma slug returned no market",
            {"payload_sha256": "b" * 64},
        )

        class MissingLoader:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def discover(self, *args: object, **kwargs: object) -> object:
                from types import SimpleNamespace

                return SimpleNamespace(
                    markets=(), provenance=(official,), exclusions=(exclusion,)
                )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = root / "ledger.json"
            with (
                patch("scripts.run_backfill.ProductionSourceLoader", MissingLoader),
                patch("scripts.run_backfill._fetch_text") as fetch_binance,
                patch("scripts.run_backfill._tool_commit", return_value="d" * 40),
                patch("scripts.run_backfill.GitHubReleaseBackend"),
                patch("scripts.run_backfill.Pipeline.publish", return_value=[object()] * 6),
            ):
                result = run_partition(
                    Asset.HYPE,
                    date(2026, 6, 1),
                    root / "kacho",
                    root / "work",
                    ledger,
                )
            fetch_binance.assert_not_called()
            self.assertEqual(result["quality"], "EXCLUDED")
            self.assertEqual(result["source_bytes"], 0)
            self.assertEqual(result["remote_assets"], 6)
            stored = json.loads(ledger.read_bytes())
            self.assertEqual(stored["partitions"]["HYPE/5m/2026-06-01"], result)

    def test_kacho_transfer_is_limited_to_the_frozen_fallback_window(self) -> None:
        self.assertTrue(kacho_required("2026-05-18"))
        self.assertFalse(kacho_required("2026-05-19"))
        self.assertFalse(kacho_required("2026-08-07"))

    def test_kacho_retries_transient_throttle_without_retaining_partial_bytes(self) -> None:
        payload = b"pinned-parquet-object"
        digest = hashlib.sha256(payload).hexdigest()
        throttled = urllib.error.HTTPError(
            "https://example.test/object", 429, "Too Many Requests", Message(), None
        )
        with TemporaryDirectory() as temporary:
            with (
                patch.dict(
                    KACHO_TICKS,
                    {"DOGE": ("doge_ticks.parquet", f"{digest}.source", digest, len(payload))},
                ),
                patch(
                    "scripts.actions_backend.urllib.request.urlopen",
                    side_effect=(throttled, FakeResponse(payload)),
                ) as urlopen,
                patch("scripts.actions_backend.time.sleep") as sleep,
            ):
                self.assertEqual(acquire_kacho("DOGE", Path(temporary)), len(payload))
                self.assertEqual(urlopen.call_count, 2)
                sleep.assert_called_once_with(2)
                self.assertEqual(
                    (Path(temporary) / f"{digest}.source").read_bytes(), payload
                )

    def test_kacho_accepts_only_pinned_object_at_immutable_revision(self) -> None:
        payload = b"pinned-parquet-object"
        digest = hashlib.sha256(payload).hexdigest()
        response = FakeResponse(payload)
        with TemporaryDirectory() as temporary:
            with (
                patch.dict(
                    KACHO_TICKS,
                    {"DOGE": ("doge_ticks.parquet", f"{digest}.source", digest, len(payload))},
                ),
                patch(
                    "scripts.actions_backend.urllib.request.urlopen", return_value=response
                ) as urlopen,
            ):
                self.assertEqual(acquire_kacho("DOGE", Path(temporary)), len(payload))
                requested = str(urlopen.call_args.args[0].full_url)
                self.assertIn(f"/resolve/{KACHO_REVISION}/doge_ticks.parquet", requested)
                self.assertEqual((Path(temporary) / f"{digest}.source").read_bytes(), payload)

    def test_kacho_rejects_wrong_object_with_bounded_safe_diagnostics(self) -> None:
        with TemporaryDirectory() as temporary:
            with patch(
                "scripts.actions_backend.urllib.request.urlopen",
                return_value=FakeResponse(b"not-the-pinned-parquet"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    r'"actual_bytes": 22.*"actual_sha256":.*'
                    r'"response_host": "cdn-lfs.example.test"',
                ) as raised:
                    acquire_kacho("DOGE", Path(temporary))
        self.assertNotIn("credential", str(raised.exception))
        self.assertNotIn("not-the-pinned-parquet", str(raised.exception))

    @staticmethod
    def _assets(partition: str) -> list[RemoteAsset]:
        asset, _, day = partition.split("/")
        return [
            RemoteAsset(
                f"{asset}--5m--{day}--{'a' * 64}--{filename}",
                1,
                "https://example.test/asset",
                "a" * 64,
                filename,
            )
            for filename in (
                "book-200ms.parquet",
                "book-events.parquet",
                "exclusions.parquet",
                "manifest.json",
                "markets.parquet",
                "underlying.parquet",
            )
        ]

    def test_complete_remote_partition_is_omitted_and_partial_fails_closed(self) -> None:
        partition = "DOGE/5m/2026-04-05"
        assets = [
            RemoteAsset(
                f"DOGE--5m--2026-04-05--{'a' * 64}--{filename}",
                1,
                "https://example.test/asset",
                "a" * 64,
                filename,
            )
            for filename in (
                "book-200ms.parquet",
                "book-events.parquet",
                "exclusions.parquet",
                "manifest.json",
                "markets.parquet",
                "underlying.parquet",
            )
        ]
        inventory = {partition: assets}
        self.assertEqual(verified_partitions(inventory), {partition})
        self.assertNotIn(partition, {item["partition_id"] for item in unfinished_plan(inventory)})
        self.assertEqual(verified_partitions({partition: assets[:-1]}), set())
        starter = [*assets]
        starter[0] = RemoteAsset(
            starter[0].name,
            starter[0].size,
            starter[0].url,
            starter[0].digest,
            starter[0].filename,
            "starter",
            "513096757",
        )
        starter_inventory = {partition: starter}
        self.assertEqual(verified_partitions(starter_inventory), set())
        self.assertEqual(inventory_anomalies(starter_inventory)["partial"], [partition])

    def test_proof_partition_allows_out_of_order_recovery_with_remote_evidence(self) -> None:
        partition = "HYPE/5m/2026-05-27"
        other = "DOGE/5m/2026-04-05"
        ledger = {"completed": 2, "last_partition": partition}
        inventory = {
            partition: self._assets(partition),
            other: self._assets(other),
        }
        self.assertEqual(select_proof_partition(ledger, inventory), partition)

    def test_proof_partition_rejects_inconsistent_or_unsupported_ledger_claim(self) -> None:
        partition = "HYPE/5m/2026-04-05"
        with self.assertRaisesRegex(RuntimeError, "count does not match"):
            select_proof_partition(
                {"completed": 3, "last_partition": "DOGE/5m/2026-04-05"},
                {partition: self._assets(partition)},
            )
        with self.assertRaisesRegex(RuntimeError, "lacks durable remote evidence"):
            inventory = {
                item: self._assets(item)
                for item in (
                    "DOGE/5m/2026-04-05",
                    "BNB/5m/2026-04-05",
                    "DOGE/5m/2026-04-06",
                )
            }
            select_proof_partition(
                {"completed": 3, "last_partition": partition}, inventory
            )

    def test_matrices_are_finite_unique_and_bounded(self) -> None:
        plan = unfinished_plan({})
        stages = matrix_stages(plan)
        flattened = [item["day"] for stage in stages for item in stage["include"]]
        self.assertEqual(len(flattened), 125)
        self.assertEqual(len(set(flattened)), 125)
        self.assertTrue(all(len(stage["include"]) <= 256 for stage in stages))
        self.assertEqual([len(stage["include"]) for stage in stages], [125])
