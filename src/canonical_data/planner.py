"""Deterministic finite Chapter 4 partition planner."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from canonical_data.models import Asset


def build_backfill_plan(start: date, end: date) -> list[dict[str, Any]]:
    if end < start:
        raise ValueError("backfill end precedes start")
    result: list[dict[str, Any]] = []
    current = start
    while current <= end:
        day = current.isoformat()
        release_group = f"dataset-v1-{current:%Y-%m}"
        for asset in Asset:
            result.append(
                {
                    "partition_id": f"{asset.value}/5m/{day}",
                    "asset": asset.value,
                    "timeframe": "5m",
                    "day": day,
                    "release_group": release_group,
                }
            )
        current += timedelta(days=1)
    return result
