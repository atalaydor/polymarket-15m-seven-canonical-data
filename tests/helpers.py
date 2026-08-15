from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Any

from canonical_data.models import Asset, Market, Outcome, Provenance, QualityTier

START_S = 1_776_106_800
START_NS = START_S * 1_000_000_000
CONDITION = "0x" + "a" * 64


def gamma_payload(asset: Asset = Asset.DOGE, outcome_prices: list[str] | None = None) -> bytes:
    symbol = asset.value.lower()
    display = "Hyperliquid" if asset is Asset.HYPE else asset.value
    stream = f"https://data.chain.link/streams/{symbol}-usd"
    event = {
        "id": "event-1",
        "markets": [
            {
                "id": "market-1",
                "slug": f"{symbol}-updown-15m-{START_S}",
                "conditionId": CONDITION,
                "outcomes": json.dumps(["Up", "Down"]),
                "clobTokenIds": json.dumps(["1", "2"]),
                "closed": True,
                "outcomePrices": json.dumps(outcome_prices or ["1", "0"]),
                "resolutionSource": stream,
                "description": (
                    f"This market resolves Up if the {display} price is greater than or equal "
                    f"to its start value; otherwise Down. Source: {stream}"
                ),
            }
        ],
    }
    return json.dumps([event], sort_keys=True).encode()


def market(asset: Asset = Asset.DOGE, tier: QualityTier = QualityTier.TIER_A) -> Market:
    payload = gamma_payload(asset)
    return Market(
        asset=asset,
        event_id="event-1",
        market_id="market-1",
        condition_id=CONDITION,
        token_up="1",
        token_down="2",
        market_start_ns=START_NS,
        market_end_ns=START_NS + 900_000_000_000,
        rules_text_sha256="b" * 64,
        resolution_source_url=f"https://data.chain.link/streams/{asset.value.lower()}-usd",
        official_outcome=Outcome.UP,
        official_resolution_ts_ns=START_NS + 301_000_000_000,
        quality_tier=tier,
        evidence_sha256=hashlib.sha256(payload).hexdigest(),
    )


def provenance(source_id: str = "pmxt_v2") -> Provenance:
    return Provenance(
        source_id=source_id,
        source_url="https://example.test/source",
        retrieved_at_ns=START_NS,
        byte_length=10,
        sha256="c" * 64,
        license_id="CC-BY-4.0",
        source_precision="ms",
        transformations=("decode",),
    )


def pmxt_rows(update: bool = True) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for token in ("1", "2"):
        rows.append(
            {
                "timestamp_received": START_S * 1000,
                "timestamp": START_S * 1000,
                "market": CONDITION,
                "event_type": "book",
                "asset_id": token,
                "bids": '[["0.40","10.000000"]]',
                "asks": '[["0.60","12.000000"]]',
            }
        )
    if update:
        rows.extend(
            [
                {
                    "timestamp_received": START_S * 1000 + 200,
                    "timestamp": START_S * 1000 + 190,
                    "market": CONDITION,
                    "event_type": "price_change",
                    "asset_id": "1",
                    "side": "BUY",
                    "price": "0.45",
                    "size": "5.000000",
                    "best_bid": "0.45",
                    "best_ask": "0.60",
                },
                {
                    "timestamp_received": START_S * 1000 + 300,
                    "timestamp": START_S * 1000 + 290,
                    "market": CONDITION,
                    "event_type": "tick_size_change",
                    "asset_id": "1",
                    "old_tick_size": "0.01",
                    "new_tick_size": "0.001",
                },
                {
                    "timestamp_received": START_S * 1000 + 400,
                    "timestamp": START_S * 1000 + 390,
                    "market": CONDITION,
                    "event_type": "last_trade_price",
                    "asset_id": "1",
                    "side": "BUY",
                    "price": "0.46",
                    "size": "1.000000",
                    "fee_rate_bps": 100,
                    "transaction_hash": "0x123",
                },
            ]
        )
    return rows


def d(value: str) -> Decimal:
    return Decimal(value)
