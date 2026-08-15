from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from helpers import CONDITION, START_NS, gamma_payload, market

from canonical_data.acquire import BoundedAcquirer, copy_bounded
from canonical_data.binance import ingest_binance_zip
from canonical_data.discovery import GammaClient
from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.inventory import (
    SourceObject,
    binance_daily_objects,
    expected_5m_market_starts,
    pmxt_hourly_objects,
)
from canonical_data.kacho import ingest_kacho_ticks, kacho_native_events, read_kacho_parquet
from canonical_data.models import (
    Asset,
    BookEvent,
    EventType,
    ExclusionReason,
    Outcome,
    QualityTier,
)
from canonical_data.planner import build_backfill_plan
from canonical_data.quality import RELEASE_START, classify
from canonical_data.rangeio import BoundedRangeReader
from canonical_data.resample import resample_200ms
from canonical_data.sources import ProductionSourceLoader
from scripts.run_backfill import enforce_shared_pmxt_asset_caps


class InventoryAndAcquisitionTests(unittest.TestCase):
    def test_shared_pmxt_cap_is_enforced_per_asset_not_combined_read(self) -> None:
        doge = market(Asset.DOGE)
        bnb = replace(market(Asset.BNB), condition_id="0x" + "b" * 64)

        def event(condition_id: str, source_row: int) -> BookEvent:
            return BookEvent(
                condition_id=condition_id,
                token_id="1",
                source_ts_ns=START_NS,
                receive_ts_ns=START_NS,
                source_object="fixture",
                source_row=source_row,
                sequence=0,
                event_type=EventType.PRICE_CHANGE,
            )

        inventories = {Asset.DOGE: (doge,), Asset.BNB: (bnb,)}
        accepted = (
            event(doge.condition_id, 0),
            event(doge.condition_id, 1),
            event(bnb.condition_id, 2),
            event(bnb.condition_id, 3),
        )
        with mock.patch("scripts.run_backfill.PMXT_FILTERED_ROWS_PER_ASSET_OBJECT", 2):
            enforce_shared_pmxt_asset_caps(accepted, inventories)
            with self.assertRaises(ResourceLimitError):
                enforce_shared_pmxt_asset_caps(
                    (*accepted, event(doge.condition_id, 4)), inventories
                )

    def test_pmxt_era_inventory_does_not_depend_on_degraded_kacho_metadata(self) -> None:
        cutoff = datetime(2026, 8, 7, 15, tzinfo=UTC)
        self.assertEqual(len(expected_5m_market_starts(date(2026, 4, 15), cutoff)), 288)
        self.assertEqual(len(expected_5m_market_starts(date(2026, 8, 7), cutoff)), 180)

    def test_finite_backfill_plan_is_ordered_and_month_grouped(self) -> None:
        plan = build_backfill_plan(date(2026, 4, 5), date(2026, 8, 7))
        self.assertEqual(len(plan), 375)
        self.assertEqual(plan[0]["partition_id"], "DOGE/5m/2026-04-05")
        self.assertEqual(plan[-1]["partition_id"], "HYPE/5m/2026-08-07")
        groups = {row["release_group"] for row in plan}
        self.assertEqual(len(groups), 5)
        self.assertLessEqual(
            max(sum(row["release_group"] == group for row in plan) for group in groups), 93
        )

    def test_official_discovery_cache_pins_mutable_gamma_payload_for_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            calls: list[str] = []

            def fetch(url: str, _limit: int) -> bytes:
                calls.append(url)
                return gamma_payload()

            cache = Path(temp) / "official"
            first = ProductionSourceLoader(GammaClient(fetch), 1, cache).discover(
                Asset.DOGE, [START_NS // 1_000_000_000]
            )
            second = ProductionSourceLoader(
                GammaClient(lambda _url, _limit: (_ for _ in ()).throw(AssertionError())),
                2,
                cache,
            ).discover(Asset.DOGE, [START_NS // 1_000_000_000])
            self.assertEqual(len(calls), 1)
            self.assertEqual(first.markets, second.markets)
            self.assertEqual(first.provenance[0].sha256, second.provenance[0].sha256)

    def test_official_missing_slot_is_evidence_backed_source_gap(self) -> None:
        result = ProductionSourceLoader(
            GammaClient(lambda _url, _limit: b"[]"), 7
        ).discover(Asset.DOGE, [START_NS // 1_000_000_000], allow_missing=True)
        self.assertEqual(result.markets, ())
        self.assertEqual(result.exclusions[0].reason_code, ExclusionReason.SOURCE_GAP)
        self.assertEqual(len(result.provenance), 1)

    def test_official_unresolved_slot_is_only_excluded_after_full_binding(self) -> None:
        raw = json.loads(gamma_payload())
        raw[0]["markets"][0]["closed"] = False
        result = ProductionSourceLoader(
            GammaClient(lambda _url, _limit: json.dumps(raw).encode()), 7
        ).discover(
            Asset.DOGE,
            [START_NS // 1_000_000_000],
            allow_unresolved=True,
        )
        self.assertEqual(result.markets, ())
        self.assertEqual(result.exclusions[0].reason_code, ExclusionReason.UNRESOLVED_MARKET)
        self.assertEqual(result.exclusions[0].evidence["condition_id"], CONDITION)
        self.assertEqual(len(result.provenance), 1)

    def test_seekable_range_reader_ledgers_and_enforces_cap(self) -> None:
        payload = b"0123456789"
        reader = BoundedRangeReader(
            len(payload), lambda offset, length: payload[offset : offset + length], 6
        )
        self.assertEqual(reader.read(3), b"012")
        reader.seek(5)
        self.assertEqual(reader.read(3), b"567")
        self.assertEqual(sum(item.byte_length for item in reader.ledger), 6)
        with self.assertRaises(ResourceLimitError):
            reader.read(1)

    def test_hour_inventory_is_exact_and_bounded(self) -> None:
        items = pmxt_hourly_objects(START_NS, START_NS + 3_600_000_000_001)
        self.assertEqual(len(items), 2)
        self.assertTrue(items[0].url.endswith("2026-04-13T19.parquet"))
        exact_hour = pmxt_hourly_objects(START_NS, START_NS + 3_600_000_000_000)
        self.assertEqual(len(exact_hour), 1)
        hype = binance_daily_objects(Asset.HYPE, "2026-04-13", ("trades", "markPriceKlines"))
        self.assertTrue(all("futures/um" in item.url for item in hype))
        doge = binance_daily_objects(Asset.DOGE, "2026-04-13", ("klines",))
        self.assertIn("/spot/", doge[0].url)

    def test_acquisition_checksum_idempotency_and_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source_path = root / "input"
            source_path.write_bytes(b"bounded-source")
            digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
            source = SourceObject(
                "fixture", source_path.as_uri(), digest, len(source_path.read_bytes())
            )
            acquired = BoundedAcquirer(root / "work", 100, 0).acquire(source)
            again = BoundedAcquirer(root / "work", 100, 0).acquire(source)
            self.assertEqual(acquired.sha256, again.sha256)
            conflicting = SourceObject(
                "fixture", source_path.as_uri(), "0" * 64, len(source_path.read_bytes())
            )
            with self.assertRaises(SourceError):
                BoundedAcquirer(root / "work", 100, 0).acquire(conflicting)

    def test_transfer_bound_fails_without_partial_acceptance(self) -> None:
        with self.assertRaises(ResourceLimitError):
            copy_bounded(io.BytesIO(b"12345"), io.BytesIO(), 4)


class KachoTests(unittest.TestCase):
    def test_bounded_parquet_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "kacho.parquet"
            table = pa.Table.from_pylist(
                [
                    {"condition_id": CONDITION, "t": START_NS // 1_000_000_000},
                    {"condition_id": CONDITION, "t": START_NS // 1_000_000_000 + 600},
                    {"condition_id": "other", "t": START_NS // 1_000_000_000},
                ]
            )
            pq.write_table(table, path)
            rows = read_kacho_parquet(
                path,
                {CONDITION},
                max_rows=1,
                start_s=START_NS // 1_000_000_000,
                end_s=START_NS // 1_000_000_000 + 300,
            )
            self.assertEqual([row["condition_id"] for row in rows], [CONDITION])

    def test_tier_b_semantics_and_inferred_outcome_ignored(self) -> None:
        row = {
            "condition_id": CONDITION,
            "t": START_NS // 1_000_000_000,
            "bu": "0.4",
            "au": "0.6",
            "su": "10",
            "sau": "11",
            "bd": "0.3",
            "ad": "0.7",
            "sd": "8",
            "sad": "9",
            "outcome": "Down",
        }
        states = ingest_kacho_ticks(market(), [row])
        self.assertTrue(all(item.quality_tier is QualityTier.TIER_B for item in states))
        self.assertTrue(all(item.native_interval_ms == 1000 for item in states))
        self.assertTrue(all(item.receive_ts_ns is None for item in states))
        native = kacho_native_events(states)
        self.assertTrue(all(item.source_id == "kacho_5m" for item in native))
        self.assertTrue(all(item.receive_ts_ns is None for item in native))
        samples, _ = resample_200ms(market(), states)
        self.assertTrue(all(item.quality_tier is QualityTier.TIER_B for item in samples))
        self.assertTrue(all(item.asof_receive_ts_ns is None for item in samples))
        self.assertEqual(market().official_outcome, Outcome.UP)

    def test_duplicate_or_crossed_tick_rejected(self) -> None:
        row = {
            "condition_id": CONDITION,
            "t": START_NS // 1_000_000_000,
            "bu": "0.7",
            "au": "0.6",
            "su": "1",
            "sau": "1",
        }
        with self.assertRaises(SourceError):
            ingest_kacho_ticks(market(), [row])


class BinanceTests(unittest.TestCase):
    def _zip(self, root: Path, name: str, content: str) -> tuple[Path, str]:
        path = root / f"{name}.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(name, content)
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def test_spot_and_hype_perpetual_normalization(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            spot, spot_hash = self._zip(root, "DOGE.csv", "1,0.1,2,0.2,1735689600000000,False\n")
            doge = ingest_binance_zip(spot, spot_hash, Asset.DOGE, "trades", "spot/path")
            self.assertEqual(doge[0].instrument_type, "spot")
            self.assertFalse(doge[0].is_settlement)
            hype, hype_hash = self._zip(
                root, "HYPE.csv", "open_time,open,high,low,close\n1748601000000,33,34,32,33.5\n"
            )
            observations = ingest_binance_zip(hype, hype_hash, Asset.HYPE, "klines", "um/path")
            self.assertEqual(observations[0].instrument_type, "usds_m_perpetual")
            self.assertEqual(observations[0].symbol, "HYPEUSDT")
            self.assertFalse(observations[0].is_settlement)

    def test_corruption_and_zip_bomb_bound_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path, digest = self._zip(root, "x.csv", "1,1,1,1,1748601000000,False\n")
            with self.assertRaises(SourceError):
                ingest_binance_zip(path, "0" * 64, Asset.HYPE, "trades", "path")
            with self.assertRaises(ResourceLimitError):
                ingest_binance_zip(
                    path, digest, Asset.HYPE, "trades", "path", max_uncompressed_bytes=1
                )


class QualityTests(unittest.TestCase):
    def test_tiers_and_exclusions_are_fail_closed(self) -> None:
        tier, exclusion = classify(market(), True, False, [])
        self.assertEqual((tier, exclusion), (QualityTier.TIER_A, None))
        old = replace(market(), market_start_ns=RELEASE_START - 1)
        tier, exclusion = classify(old, False, False, [])
        self.assertEqual(tier, QualityTier.EXCLUDED)
        assert exclusion is not None
        self.assertEqual(exclusion.reason_code, ExclusionReason.OUT_OF_SCOPE_DATE)
        tier, exclusion = classify(market(), True, False, [(START_NS, START_NS + 1)])
        self.assertEqual(tier, QualityTier.EXCLUDED)
