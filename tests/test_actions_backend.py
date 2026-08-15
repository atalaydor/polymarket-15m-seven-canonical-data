from __future__ import annotations

import io
import subprocess
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from pathlib import Path
from unittest.mock import patch

from helpers import START_S, market

from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.inventory import SourceObject
from canonical_data.models import Asset
from scripts.actions_backend import (
    Authority,
    RemoteAsset,
    _candidate_starts,
    _raise_child_failure,
    _request,
    day_plan,
    find_canary_start,
    inventory_anomalies,
    load_authority,
    unfinished_plan,
    verified_partitions,
)
from scripts.run_backfill import _acquire_with_retry, _market_starts


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/octet-stream"
        self.headers["Content-Length"] = str(len(payload))

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


class ActionsBackendTests(unittest.TestCase):
    @staticmethod
    def authority(start: date = date(2026, 4, 5), end: date = date(2026, 4, 6)) -> Authority:
        return Authority(
            datetime(start.year, start.month, start.day, tzinfo=UTC),
            datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1),
            tuple(Asset),
            datetime.fromtimestamp(START_S, UTC),
            datetime.fromtimestamp(START_S, UTC),
            15,
        )

    @staticmethod
    def assets(partition: str) -> list[RemoteAsset]:
        asset, _, day = partition.split("/")
        return [
            RemoteAsset(
                f"{asset}--15m--{day}--{'a' * 64}--{filename}",
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

    def test_repository_authority_is_exact_15m_x7_and_finite(self) -> None:
        authority = load_authority()
        self.assertEqual(authority.assets, tuple(Asset))
        self.assertEqual(
            [asset.value for asset in authority.assets],
            ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"],
        )
        self.assertEqual(len(_candidate_starts(authority)), 48)

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

    def test_acquisition_does_not_retry_limits_or_unexplained_404(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        missing = urllib.error.HTTPError(source.url, 404, "Not Found", Message(), None)
        for failure in (ResourceLimitError("cap"), missing):
            with (
                patch(
                    "scripts.run_backfill.BoundedAcquirer.acquire", side_effect=failure
                ) as acquire,
                patch("scripts.run_backfill.time.sleep") as sleep,
                tempfile.TemporaryDirectory() as temporary,
            ):
                with self.assertRaises(type(failure)):
                    _acquire_with_retry(source, Path(temporary))
            acquire.assert_called_once()
            sleep.assert_not_called()

    def test_explicit_market_starts_must_be_one_day_and_15m_aligned(self) -> None:
        day = datetime.fromtimestamp(START_S, UTC).date()
        cutoff = datetime.fromtimestamp(START_S + 900, UTC)
        coverage_start = datetime.fromtimestamp(START_S, UTC)
        self.assertEqual(_market_starts(day, coverage_start, cutoff, (START_S,)), [START_S])
        with self.assertRaisesRegex(SourceError, "aligned"):
            _market_starts(day, coverage_start, cutoff, (START_S + 1,))

    def test_remote_durable_partitions_are_zero_times_and_unfinished_once(self) -> None:
        authority = self.authority()
        durable = "BTC/15m/2026-04-05"
        inventory = {durable: self.assets(durable)}
        plan = unfinished_plan(inventory, authority)
        ids = [str(item["partition_id"]) for item in plan]
        self.assertNotIn(durable, ids)
        self.assertEqual(len(ids), 13)
        self.assertEqual(len(ids), len(set(ids)))
        days = day_plan(plan)
        self.assertEqual(days, [{"day": "2026-04-05"}, {"day": "2026-04-06"}])

    def test_partial_resumes_while_unsafe_remote_state_fails_closed(self) -> None:
        authority = self.authority()
        partition = "BTC/15m/2026-04-05"
        assets = self.assets(partition)
        self.assertEqual(verified_partitions({partition: assets}), {partition})
        partial_inventory = {partition: assets[:-1]}
        self.assertEqual(inventory_anomalies(partial_inventory, authority)["partial"], [partition])
        self.assertIn(
            partition,
            {str(item["partition_id"]) for item in unfinished_plan(partial_inventory, authority)},
        )
        divergent = [*assets, replace(assets[0], digest="b" * 64)]
        self.assertTrue(inventory_anomalies({partition: divergent}, authority)["divergent"])
        duplicate = [*assets, assets[0]]
        self.assertTrue(inventory_anomalies({partition: duplicate}, authority)["duplicate"])
        outside = "BTC/15m/2026-04-07"
        self.assertEqual(
            inventory_anomalies({outside: self.assets(outside)}, authority)["out_of_plan"],
            [outside],
        )

    def test_common_canary_candidate_requires_exact_market_for_all_assets(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        with patch("scripts.actions_backend.GammaClient", FakeGamma):
            start, markets, attempts = find_canary_start(authority)
        self.assertEqual(start, START_S)
        self.assertEqual(set(markets), set(Asset))
        self.assertEqual(attempts, 7)

    def test_canary_candidate_search_is_bounded(self) -> None:
        authority = replace(
            self.authority(),
            canary_search_start=datetime(2026, 4, 6, tzinfo=UTC),
            canary_search_end=datetime(2026, 4, 4, tzinfo=UTC),
            canary_step_minutes=15,
        )
        with self.assertRaisesRegex(RuntimeError, "finite cap"):
            _candidate_starts(authority)


if __name__ == "__main__":
    unittest.main()
