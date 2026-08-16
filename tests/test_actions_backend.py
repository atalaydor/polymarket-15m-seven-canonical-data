from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from pathlib import Path
from unittest.mock import Mock, patch

from helpers import START_S, market

from canonical_data.errors import ResourceLimitError, SourceError, UnresolvedMarketError
from canonical_data.inventory import SourceObject, pmxt_hourly_objects
from canonical_data.models import Asset
from canonical_data.sources import OfficialDiscovery
from scripts.actions_backend import (
    CANARY_MAX_CANDIDATES,
    CANARY_MAX_CANDIDATES_TOTAL,
    CANARY_MAX_GAMMA_REQUESTS,
    CANARY_MAX_ROUNDS,
    CANARY_MAX_SOURCE_OBJECTS,
    Authority,
    CanaryQualification,
    QualifiedCandidate,
    RemoteAsset,
    _adaptive_round_authorities,
    _candidate_starts,
    _execute_canary_round,
    _pmxt_source_identity,
    _raise_child_failure,
    _request,
    _validate_receipt_coverage,
    _verify_canary_dispositions,
    _verify_prior_canary_evidence,
    day_plan,
    inventory_anomalies,
    load_authority,
    minimum_canary_cover,
    qualify_canary_candidates,
    unfinished_plan,
    verified_partitions,
)
from scripts.run_backfill import (
    _acquire_with_retry,
    _market_starts,
    _validate_expected_market_identities,
    _validate_expected_source_identity,
    run_day,
)


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
        self.assertEqual(authority.start, datetime(2026, 4, 13, 20, tzinfo=UTC))
        self.assertEqual(authority.cutoff, datetime(2026, 8, 10, 1, tzinfo=UTC))
        self.assertEqual(authority.canary_search_start, datetime(2026, 7, 17, 19, 45, tzinfo=UTC))
        candidates = _candidate_starts(authority)
        self.assertEqual(len(candidates), CANARY_MAX_CANDIDATES)
        self.assertEqual(len(_adaptive_round_authorities(authority)), CANARY_MAX_ROUNDS)
        all_candidates = [
            start
            for selected in _adaptive_round_authorities(authority)
            for start in _candidate_starts(selected)
        ]
        self.assertEqual(len(all_candidates), CANARY_MAX_CANDIDATES_TOTAL)
        self.assertEqual(len(all_candidates), len(set(all_candidates)))

    def test_production_entrypoints_use_import_safe_module_execution(self) -> None:
        root = Path(__file__).resolve().parents[1]
        workflows = sorted((root / ".github/workflows").glob("*.y*ml"))
        sources = sorted((root / "scripts").glob("*.py"))
        for path in workflows:
            self.assertNotIn("python scripts/", path.read_text(), str(path))
        direct_script = re.compile(r'["\']scripts[/\\][^"\']+\.py["\']')
        for path in sources:
            self.assertNotRegex(path.read_text(), direct_script, str(path))
        workflow = (root / ".github/workflows/polymarket-15m-seven.yml").read_text()
        for command in ("canary", "plan", "execute-day"):
            self.assertIn(f"python -m scripts.actions_backend {command}", workflow)
        for module in ("scripts.actions_backend", "scripts.run_backfill"):
            completed = subprocess.run(
                [sys.executable, "-m", module, "--help"],
                cwd=root,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

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

    def test_actions_discovery_is_gamma_first_bounded_and_reuses_source_probes(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        newest = START_S + 4_500
        oldest = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(newest, UTC),
            canary_search_end=datetime.fromtimestamp(oldest, UTC),
        )
        events: list[str] = []

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                events.append(f"gamma:{start}:{asset.value}")
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", f"https://example.test/{asset.value}/{start}"

        def source_identity(source: SourceObject) -> tuple[int, str]:
            events.append(f"source:{source.url}")
            return 100, '"stable"'

        with patch("scripts.actions_backend.GammaClient", FakeGamma):
            result = qualify_canary_candidates(authority, source_identity)
        self.assertEqual([item.start for item in result.candidates], [newest, oldest])
        self.assertEqual(result.gamma_requests, 14)
        self.assertLessEqual(result.gamma_requests, CANARY_MAX_GAMMA_REQUESTS)
        self.assertEqual(result.source_requests, 2)
        self.assertLessEqual(result.source_requests, CANARY_MAX_SOURCE_OBJECTS)
        self.assertTrue(all(item.startswith("gamma:") for item in events[:7]))
        self.assertEqual(
            [item.removeprefix("source:") for item in events if item.startswith("source:")],
            [
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T19.parquet",
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T20.parquet",
            ],
        )

    def test_adaptive_discovery_queries_only_uncovered_assets(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(start, UTC),
            canary_search_end=datetime.fromtimestamp(start, UTC),
        )
        requested: list[Asset] = []

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, candidate: int) -> tuple[object, bytes, str]:
                requested.append(asset)
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{candidate}",
                    condition_id=f"0x{candidate + tuple(Asset).index(asset):064x}",
                    market_start_ns=candidate * 1_000_000_000,
                    market_end_ns=(candidate + 900) * 1_000_000_000,
                )
                return fixture, b"payload", f"https://example.test/{asset.value}/{candidate}"

        with patch("scripts.actions_backend.GammaClient", FakeGamma):
            result = qualify_canary_candidates(
                authority,
                Mock(return_value=(100, '"stable"')),
                assets=(Asset.ETH, Asset.HYPE),
            )
        self.assertEqual(requested, [Asset.ETH, Asset.HYPE])
        self.assertEqual(result.gamma_requests, 2)
        self.assertEqual([asset for asset, _ in result.candidates[0].markets], requested)

    def test_prior_btc_proof_is_reused_only_after_authoritative_revalidation(self) -> None:
        starts = [1_786_312_800 + offset * 900 for offset in range(8)]
        payloads = {start: f"gamma-{start}".encode() for start in starts}
        qualified = [
            {
                "start": start,
                "gamma_sha256": hashlib.sha256(payloads[start]).hexdigest(),
            }
            for start in starts
        ]
        raw = {
            "schema_version": "1.0.0",
            "accessed_at": "2026-08-16",
            "asset": "BTC",
            "timeframe": "15m",
            "run_url": "https://github.com/atalaydor/polymarket-15m-seven-canonical-data/actions/runs/31914715144",
            "partition_id": "BTC/15m/2026-08-09",
            "release_tag": "polymarket-15m-seven-canary-v3-proof",
            "manifest_sha256": "a" * 64,
            "tool_commit": "b" * 40,
            "qualified_markets": qualified,
            "accepted_market_starts": starts[:2],
            "source_objects": [
                {
                    "url": source.url,
                    "byte_length": 1,
                    "etag": '"x"',
                }
                for source in pmxt_hourly_objects(
                    (min(starts) - 3_600) * 1_000_000_000,
                    (max(starts) + 900) * 1_000_000_000,
                )
            ],
        }
        authority = Authority(
            datetime.fromtimestamp(min(starts) - 900, UTC),
            datetime.fromtimestamp(max(starts) + 1_800, UTC),
            tuple(Asset),
            datetime.fromtimestamp(starts[-1], UTC),
            datetime.fromtimestamp(starts[0], UTC),
            15,
        )

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_id=f"BTC-{start}",
                    condition_id=f"0x{start:064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, payloads[start], f"https://example.test/gamma/{start}"

        proof = {
            "quality": "TIER_A",
            "manifest_sha256": "a" * 64,
            "tool_commit": "b" * 40,
            "accepted_market_starts": starts[:2],
            "authenticated_redownload": True,
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "prior.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with (
                patch("scripts.actions_backend.GammaClient", FakeGamma),
                patch("scripts.actions_backend.remote_inventory", return_value={}),
                patch("scripts.actions_backend.verify_remote_partition", return_value=proof),
            ):
                reused, candidates, requests, tag = _verify_prior_canary_evidence(
                    authority, path
                )
        self.assertIs(reused, proof)
        self.assertEqual(len(candidates), 8)
        self.assertEqual(requests, 8)
        self.assertEqual(tag, raw["release_tag"])

    def test_multi_window_execution_acquires_one_shared_source_bundle(self) -> None:
        day = datetime.fromtimestamp(START_S, UTC).date()
        starts = (START_S, START_S + 900)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spool = root / "shared" / "events.sqlite"
            spool.parent.mkdir()
            spool.touch()
            discoveries = {
                asset: OfficialDiscovery((market(asset),), ()) for asset in tuple(Asset)[:2]
            }
            provenance = {asset: () for asset in discoveries}
            with (
                patch(
                    "scripts.run_backfill.prepare_shared_day",
                    return_value=(spool, discoveries, provenance, 123),
                ) as prepare,
                patch("scripts.run_backfill.run_partition", return_value={}) as partition,
            ):
                run_day(
                    day,
                    root / "work",
                    root / "ledger.json",
                    datetime.fromtimestamp(START_S, UTC),
                    datetime.fromtimestamp(START_S + 1_800, UTC),
                    tuple(discoveries),
                    starts,
                )
        prepare.assert_called_once()
        self.assertEqual(prepare.call_args.args[5], starts)
        self.assertEqual([call.args[8] for call in partition.call_args_list], [123, 0])

    def test_actions_discovery_fails_closed_on_unresolved_gamma(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class UnresolvedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                raise UnresolvedMarketError(f"{asset.value}-{start}", "market", "condition")

        source_probe = Mock(return_value=(100, '"stable"'))
        with (
            patch("scripts.actions_backend.GammaClient", UnresolvedGamma),
            self.assertRaisesRegex(RuntimeError, "no authoritative 15m candidates"),
        ):
            qualify_canary_candidates(authority, source_probe)
        source_probe.assert_not_called()

    def test_actions_discovery_fails_closed_on_unexplained_source_absence(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class FakeGamma:
            calls = 0

            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                self.__class__.calls += 1
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        def missing(source: SourceObject) -> tuple[int, str]:
            raise RuntimeError(f"catalog-listed PMXT canary source is missing: {source.url}")

        with (
            patch("scripts.actions_backend.GammaClient", FakeGamma),
            self.assertRaisesRegex(RuntimeError, "catalog-listed PMXT canary source is missing"),
        ):
            qualify_canary_candidates(authority, missing)
        self.assertEqual(FakeGamma.calls, 7)

    def test_actions_discovery_rejects_mismatched_or_reused_asset_identity(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class MismatchedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(Asset.BTC),
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        class ReusedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        for gamma, message in (
            (MismatchedGamma, "violates exact 15m identity"),
            (ReusedGamma, "reused an identity"),
        ):
            source_probe = Mock(return_value=(100, '"stable"'))
            with (
                self.subTest(gamma=gamma.__name__),
                patch("scripts.actions_backend.GammaClient", gamma),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                qualify_canary_candidates(authority, source_probe)
            source_probe.assert_not_called()

        class CrossRoundGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_id="prior-market",
                    condition_id="0x" + "f" * 64,
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        source_probe = Mock(return_value=(100, '"stable"'))
        with (
            patch("scripts.actions_backend.GammaClient", CrossRoundGamma),
            self.assertRaisesRegex(RuntimeError, "reused an identity"),
        ):
            qualify_canary_candidates(
                authority,
                source_probe,
                assets=(Asset.BTC,),
                prior_market_ids=frozenset(("prior-market",)),
                prior_conditions=frozenset(("0x" + "f" * 64,)),
            )
        source_probe.assert_not_called()

    def test_canary_source_probe_captures_object_identity(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        response = FakeResponse(b"")
        response.headers.replace_header("Content-Length", "123")
        response.headers["ETag"] = '"stable"'
        with patch("scripts.actions_backend.urllib.request.urlopen", return_value=response):
            self.assertEqual(_pmxt_source_identity(source), (123, '"stable"'))
        changed = FakeResponse(b"")
        changed.headers.replace_header("Content-Length", "0")
        with (
            patch("scripts.actions_backend.urllib.request.urlopen", return_value=changed),
            self.assertRaisesRegex(RuntimeError, "lacks object identity"),
        ):
            _pmxt_source_identity(source)

    def test_child_acquisition_must_match_source_qualified_object(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        expected = {source.url: (123, '"stable"')}
        _validate_expected_source_identity(source, 123, '"stable"', expected)
        with self.assertRaisesRegex(SourceError, "source-qualified identity"):
            _validate_expected_source_identity(source, 124, '"changed"', expected)

    def test_child_discovery_must_match_source_qualified_identity(self) -> None:
        discovered = market(Asset.BTC)
        discoveries = {Asset.BTC: OfficialDiscovery((discovered,), ())}
        expected = {
            Asset.BTC: frozenset(
                (
                    (
                        discovered.condition_id,
                        frozenset((discovered.token_up, discovered.token_down)),
                    ),
                )
            )
        }
        _validate_expected_market_identities(discoveries, expected)
        expected[Asset.BTC] = frozenset(
            (("0x" + "b" * 64, frozenset((discovered.token_up, discovered.token_down))),)
        )
        with self.assertRaisesRegex(SourceError, "source-qualified canary identity"):
            _validate_expected_market_identities(discoveries, expected)

    def test_canary_candidate_outside_catalog_is_rejected_before_probe(self) -> None:
        authority = replace(
            self.authority(),
            canary_search_start=datetime(2026, 8, 14, 23, tzinfo=UTC),
            canary_search_end=datetime(2026, 8, 14, 23, tzinfo=UTC),
        )
        source_probe = Mock(return_value=True)

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 900) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        with (
            patch("scripts.actions_backend.GammaClient", FakeGamma),
            self.assertRaisesRegex(SourceError, "authoritative catalog"),
        ):
            qualify_canary_candidates(authority, source_probe)
        source_probe.assert_not_called()

    def test_canary_cover_prefers_one_common_window(self) -> None:
        assets = frozenset(Asset)
        self.assertEqual(
            minimum_canary_cover({3: assets, 2: frozenset((Asset.BTC,)), 1: assets}),
            (3,),
        )

    def test_canary_cover_uses_minimum_windows_and_ignores_excluded_assets(self) -> None:
        first = frozenset(tuple(Asset)[:4])
        second = frozenset(tuple(Asset)[4:])
        self.assertEqual(minimum_canary_cover({3: first, 2: second, 1: first}), (3, 2))
        with self.assertRaisesRegex(RuntimeError, "no usable evidence cover"):
            minimum_canary_cover({3: frozenset(tuple(Asset)[:-1])})

    def test_remote_exclusion_is_a_disposition_but_not_usable_coverage(self) -> None:
        accepted = replace(market(Asset.BTC), market_id="accepted")
        excluded = replace(
            market(Asset.BTC),
            market_id="excluded",
            market_start_ns=accepted.market_start_ns + 900_000_000_000,
            market_end_ns=accepted.market_end_ns + 900_000_000_000,
        )
        accepted_rows = [
            {
                "market_id": accepted.market_id,
                "condition_id": accepted.condition_id,
                "token_up": accepted.token_up,
                "token_down": accepted.token_down,
                "market_start_ns": accepted.market_start_ns,
                "quality_tier": "TIER_A",
            }
        ]
        excluded_rows = [
            {
                "market_id": excluded.market_id,
                "evidence_json": json.dumps({"condition_id": excluded.condition_id}),
            }
        ]
        self.assertEqual(
            _verify_canary_dispositions(
                accepted_rows,
                excluded_rows,
                {accepted.market_id: accepted, excluded.market_id: excluded},
            ),
            [accepted.market_start_ns // 1_000_000_000],
        )

    def test_receipt_recomputes_proof_bound_minimum_cover(self) -> None:
        authority = self.authority()
        first_assets = tuple(Asset)[:4]
        usable = {asset.value: [3] if asset in first_assets else [2] for asset in authority.assets}
        receipt = {
            "release_tags": ["canary-proof"],
            "usable_market_starts_by_asset": usable,
            "remote_proofs": {
                asset.value: {
                    "accepted_market_starts": usable[asset.value],
                    "manifest_sha256": "a" * 64,
                    "quality": "TIER_A",
                    "release_tag": "canary-proof",
                    "accepted_market_bindings": [
                        {
                            "market_id": f"{asset.value}-{usable[asset.value][0]}",
                            "condition_id": "0x" + f"{tuple(Asset).index(asset):064x}",
                            "token_ids": [
                                str(tuple(Asset).index(asset) * 2 + 1),
                                str(tuple(Asset).index(asset) * 2 + 2),
                            ],
                            "market_start": usable[asset.value][0],
                            "official_outcome": "UP",
                            "resolution_source_url": "https://data.chain.link/streams/example",
                        }
                    ],
                }
                for asset in authority.assets
            },
            "selected_market_starts": [3, 2],
            "asset_market_starts": {
                asset.value: usable[asset.value][0] for asset in authority.assets
            },
        }
        _validate_receipt_coverage(receipt, authority, [3, 2, 1])
        altered = json.loads(json.dumps(receipt))
        altered["remote_proofs"]["BTC"]["accepted_market_bindings"][0][
            "condition_id"
        ] = "not-a-condition"
        with self.assertRaisesRegex(RuntimeError, "market identity binding"):
            _validate_receipt_coverage(altered, authority, [3, 2, 1])
        reused = json.loads(json.dumps(receipt))
        reused["remote_proofs"]["ETH"]["accepted_market_bindings"][0][
            "condition_id"
        ] = reused["remote_proofs"]["BTC"]["accepted_market_bindings"][0][
            "condition_id"
        ]
        with self.assertRaisesRegex(RuntimeError, "market identity binding"):
            _validate_receipt_coverage(reused, authority, [3, 2, 1])
        receipt["selected_market_starts"] = [3, 2, 1]
        with self.assertRaisesRegex(RuntimeError, "exact usable minimum cover"):
            _validate_receipt_coverage(receipt, authority, [3, 2, 1])

    def test_canary_candidate_search_is_bounded(self) -> None:
        authority = replace(
            self.authority(),
            canary_search_start=datetime(2026, 4, 6, tzinfo=UTC),
            canary_search_end=datetime(2026, 4, 4, tzinfo=UTC),
            canary_step_minutes=15,
        )
        with self.assertRaisesRegex(RuntimeError, "finite cap"):
            _candidate_starts(authority)

    def test_adaptive_round_refuses_work_after_wall_deadline(self) -> None:
        start = 1_786_132_800
        fixture = replace(
            market(Asset.BTC),
            market_id=f"BTC-{start}",
            condition_id=f"0x{start:064x}",
            market_start_ns=start * 1_000_000_000,
            market_end_ns=(start + 900) * 1_000_000_000,
        )
        qualification = CanaryQualification(
            (
                QualifiedCandidate(
                    start,
                    ((Asset.BTC, fixture),),
                    ((Asset.BTC, b"gamma", "https://example.test/gamma"),),
                ),
            ),
            (("https://r2v2.pmxt.dev/hour.parquet", 1, '"etag"'),),
            1,
            1,
        )
        with (
            patch("scripts.actions_backend.subprocess.run") as run,
            self.assertRaisesRegex(RuntimeError, "five-hour execution bound"),
        ):
            _execute_canary_round(
                self.authority(),
                qualification,
                (Asset.BTC,),
                "123",
                1,
                10_000_000_000,
                time.monotonic() - 1,
            )
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
