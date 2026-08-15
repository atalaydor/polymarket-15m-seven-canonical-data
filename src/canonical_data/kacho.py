"""Kacho ingestion with explicit Tier B degradation and no settlement authority."""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from canonical_data.errors import SourceError
from canonical_data.models import BookEvent, BookState, EventType, Level, Market, QualityTier
from canonical_data.timeutil import epoch_to_ns


def read_kacho_parquet(
    path: Path,
    condition_ids: set[str],
    max_file_bytes: int = 250_000_000,
    max_rows: int = 500_000,
    start_s: int | None = None,
    end_s: int | None = None,
) -> list[dict[str, Any]]:
    if path.stat().st_size > max_file_bytes:
        raise SourceError("Kacho file exceeds configured bound")
    filters: list[tuple[str, str, object]] = [("condition_id", "in", sorted(condition_ids))]
    if start_s is not None:
        filters.append(("t", ">=", start_s))
    if end_s is not None:
        filters.append(("t", "<", end_s))
    table = pq.read_table(path, filters=filters)
    if table.num_rows > max_rows:
        raise SourceError("Kacho filtered rows exceed configured bound")
    if "condition_id" not in table.column_names:
        raise SourceError("Kacho condition_id column missing")
    return cast(list[dict[str, Any]], table.to_pylist())


def _optional_level(price: Any, size: Any) -> tuple[Level, ...]:
    if price is None or size is None:
        return ()
    parsed_price = Decimal(str(price))
    parsed_size = Decimal(str(size))
    if not parsed_price.is_finite() or not parsed_size.is_finite() or parsed_size < 0:
        raise SourceError("invalid Kacho level")
    return () if parsed_size == 0 else (Level(parsed_price, parsed_size),)


def ingest_kacho_ticks(market: Market, rows: Iterable[dict[str, Any]]) -> list[BookState]:
    """Read only observed quotes; intentionally ignores every input outcome field."""
    states: list[BookState] = []
    previous_t: int | None = None
    for sequence, row in enumerate(rows):
        if str(row.get("condition_id")) != market.condition_id:
            raise SourceError("Kacho condition mismatch")
        t = int(row["t"])
        if previous_t is not None and t <= previous_t:
            raise SourceError("Kacho ticks must be unique and increasing")
        previous_t = t
        timestamp = epoch_to_ns(t, "s")
        if not market.market_start_ns <= timestamp < market.market_end_ns:
            raise SourceError("Kacho tick outside market window")
        for token, bid, ask, bid_size, ask_size in (
            (market.token_up, row.get("bu"), row.get("au"), row.get("su"), row.get("sau")),
            (market.token_down, row.get("bd"), row.get("ad"), row.get("sd"), row.get("sad")),
        ):
            bids = _optional_level(bid, bid_size)
            asks = _optional_level(ask, ask_size)
            if bids and asks and bids[0].price >= asks[0].price:
                raise SourceError("Kacho top of book is crossed")
            if not bids and not asks:
                continue
            states.append(
                BookState(
                    condition_id=market.condition_id,
                    token_id=token,
                    source_ts_ns=timestamp,
                    receive_ts_ns=None,
                    sequence=sequence * 2 + (token == market.token_down),
                    bids=bids,
                    asks=asks,
                    tick_size=None,
                    native_interval_ms=1000,
                    quality_tier=QualityTier.TIER_B,
                )
            )
    if not states:
        raise SourceError("Kacho market has no usable ticks")
    return states


def kacho_native_events(states: Iterable[BookState]) -> list[BookEvent]:
    return [
        BookEvent(
            condition_id=state.condition_id,
            token_id=state.token_id,
            source_ts_ns=state.source_ts_ns,
            receive_ts_ns=None,
            source_object="kacho_5m",
            source_row=index,
            sequence=state.sequence,
            event_type=EventType.BOOK,
            source_id="kacho_5m",
            bids=state.bids,
            asks=state.asks,
        )
        for index, state in enumerate(states)
    ]
