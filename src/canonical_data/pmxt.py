"""PMXT v2 decoding, deterministic ordering, deduplication and reconstruction."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from canonical_data.audit import canonical_json_bytes, sha256_bytes
from canonical_data.errors import (
    MissingInitialSnapshotError,
    ReconstructionError,
    ResourceLimitError,
    SourceError,
)
from canonical_data.models import BookEvent, BookState, EventType, Level, QualityTier
from canonical_data.timeutil import epoch_to_ns

EVENT_MAP = {
    "book": EventType.BOOK,
    "price_change": EventType.PRICE_CHANGE,
    "last_trade_price": EventType.LAST_TRADE_PRICE,
    "tick_size_change": EventType.TICK_SIZE_CHANGE,
}

PMXT_COLUMNS = [
    "timestamp_received",
    "timestamp",
    "market",
    "event_type",
    "asset_id",
    "bids",
    "asks",
    "price",
    "size",
    "side",
    "best_bid",
    "best_ask",
    "fee_rate_bps",
    "transaction_hash",
    "old_tick_size",
    "new_tick_size",
]


def read_pmxt_parquet(
    source: str | Any,
    condition_ids: set[str],
    token_ids: set[str],
    source_object: str,
    max_scanned_rows: int = 25_000_000,
    max_output_rows: int = 500_000,
    receive_bounds_by_condition: Mapping[str, tuple[int, int]] | None = None,
) -> list[BookEvent]:
    if not condition_ids or not token_ids:
        return []
    parquet = pq.ParquetFile(source)
    try:
        market_index = parquet.schema_arrow.get_field_index("market")
        selected: list[int] = []
        scanned = 0
        encoded = {value.encode("ascii") for value in condition_ids}
        for index in range(parquet.metadata.num_row_groups):
            group = parquet.metadata.row_group(index)
            stats = group.column(market_index).statistics
            include = True
            if stats is not None and stats.has_min_max:
                minimum = stats.min if isinstance(stats.min, bytes) else str(stats.min).encode()
                maximum = stats.max if isinstance(stats.max, bytes) else str(stats.max).encode()
                include = any(minimum <= value <= maximum for value in encoded)
            if include:
                scanned += group.num_rows
                if scanned > max_scanned_rows:
                    raise ResourceLimitError("PMXT selected row groups exceed scan-row cap")
                selected.append(index)
        if not selected:
            return []
        market_values = pa.array(sorted(encoded), type=pa.binary(66))
        token_values = pa.array(sorted(token_ids))
        events: list[BookEvent] = []
        row_offset = 0
        for index in selected:
            table = parquet.read_row_group(index, columns=PMXT_COLUMNS)
            table = table.filter(pc.is_in(table["market"], value_set=market_values))
            table = table.filter(pc.is_in(table["asset_id"], value_set=token_values))
            if receive_bounds_by_condition is not None and table.num_rows:
                if table["timestamp_received"].null_count:
                    raise SourceError("PMXT receive timestamp is required")
                received_ms = pc.cast(table["timestamp_received"], pa.int64())
                window_mask: pa.Array | pa.ChunkedArray | None = None
                for condition_id, (lower_ns, upper_ns) in receive_bounds_by_condition.items():
                    lower_ms = (lower_ns + 999_999) // 1_000_000
                    upper_ms = (upper_ns + 999_999) // 1_000_000
                    condition_mask = pc.equal(
                        table["market"],
                        pa.scalar(condition_id.encode("ascii"), type=table["market"].type),
                    )
                    time_mask = pc.and_(
                        pc.greater_equal(received_ms, lower_ms),
                        pc.less(received_ms, upper_ms),
                    )
                    bounded = pc.and_(condition_mask, time_mask)
                    window_mask = bounded if window_mask is None else pc.or_(window_mask, bounded)
                table = table.slice(0, 0) if window_mask is None else table.filter(window_mask)
            if len(events) + table.num_rows > max_output_rows:
                raise ResourceLimitError(
                    "PMXT filtered output exceeds row cap "
                    f"({len(events) + table.num_rows} > {max_output_rows})"
                )
            decoded = decode_rows(table.to_pylist(), source_object, row_offset)
            if receive_bounds_by_condition is not None:
                decoded = [
                    event
                    for event in decoded
                    if (
                        event.condition_id in receive_bounds_by_condition
                        and event.receive_ts_ns is not None
                        and receive_bounds_by_condition[event.condition_id][0]
                        <= event.receive_ts_ns
                        < receive_bounds_by_condition[event.condition_id][1]
                    )
                ]
            events.extend(decoded)
            row_offset += parquet.metadata.row_group(index).num_rows
        return order_and_deduplicate(events)
    finally:
        parquet.close()


def _decimal(value: Any, name: str, nullable: bool = True) -> Decimal | None:
    if value is None and nullable:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SourceError(f"invalid decimal {name}") from exc
    if not result.is_finite():
        raise SourceError(f"non-finite decimal {name}")
    return result


def _timestamp_ns(value: Any) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise SourceError("PMXT datetime must be timezone-aware")
        return int(value.timestamp() * 1_000_000_000)
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    if isinstance(value, int):
        return epoch_to_ns(value, "ms")
    raise SourceError("invalid PMXT timestamp")


def _levels(value: Any, side: str) -> tuple[Level, ...]:
    if value is None:
        raise SourceError(f"book {side} missing")
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise SourceError(f"book {side} must be a list")
    levels: list[Level] = []
    for raw in parsed:
        if isinstance(raw, dict):
            price, size = raw.get("price"), raw.get("size")
        elif isinstance(raw, list) and len(raw) == 2:
            price, size = raw
        else:
            raise SourceError(f"invalid {side} level")
        parsed_price = _decimal(price, "price", nullable=False)
        parsed_size = _decimal(size, "size", nullable=False)
        assert parsed_price is not None and parsed_size is not None
        level = Level(parsed_price, parsed_size)
        if level.size == 0:
            raise SourceError("snapshot contains zero-size level")
        levels.append(level)
    reverse = side == "bids"
    ordered = tuple(sorted(levels, key=lambda item: item.price, reverse=reverse))
    if len({level.price for level in ordered}) != len(ordered):
        raise SourceError("snapshot contains duplicate price")
    return ordered


def decode_rows(
    rows: Iterable[dict[str, Any]], source_object: str, source_row_offset: int = 0
) -> list[BookEvent]:
    decoded: list[BookEvent] = []
    for source_row, raw in enumerate(rows):
        try:
            event_type = EVENT_MAP[str(raw["event_type"])]
            condition_id = raw["market"]
            if isinstance(condition_id, bytes):
                condition_id = condition_id.decode("ascii")
            token_id = str(raw["asset_id"])
            source_ts = _timestamp_ns(raw["timestamp"])
            receive_ts = _timestamp_ns(raw["timestamp_received"])
        except (KeyError, UnicodeDecodeError, ValueError) as exc:
            raise SourceError(f"malformed PMXT row {source_row}") from exc
        if (
            not isinstance(condition_id, str)
            or not condition_id.startswith("0x")
            or not token_id.isdigit()
        ):
            raise SourceError(f"invalid PMXT identity at row {source_row}")
        bids = _levels(raw.get("bids"), "bids") if event_type is EventType.BOOK else None
        asks = _levels(raw.get("asks"), "asks") if event_type is EventType.BOOK else None
        event = BookEvent(
            condition_id=condition_id,
            token_id=token_id,
            source_ts_ns=source_ts,
            receive_ts_ns=receive_ts,
            source_object=source_object,
            source_row=source_row + source_row_offset,
            sequence=0,
            event_type=event_type,
            bids=bids,
            asks=asks,
            side=str(raw["side"]) if raw.get("side") is not None else None,
            price=_decimal(raw.get("price"), "price"),
            size=_decimal(raw.get("size"), "size"),
            best_bid=_decimal(raw.get("best_bid"), "best_bid"),
            best_ask=_decimal(raw.get("best_ask"), "best_ask"),
            old_tick_size=_decimal(raw.get("old_tick_size"), "old_tick_size"),
            new_tick_size=_decimal(raw.get("new_tick_size"), "new_tick_size"),
            fee_rate_bps=int(raw["fee_rate_bps"]) if raw.get("fee_rate_bps") is not None else None,
            transaction_hash=str(raw["transaction_hash"]) if raw.get("transaction_hash") else None,
        )
        _validate_event_shape(event)
        decoded.append(event)
    return order_and_deduplicate(decoded)


def _validate_event_shape(event: BookEvent) -> None:
    if event.receive_ts_ns is None:
        raise SourceError("PMXT receive timestamp is required")
    if event.receive_ts_ns < 0 or event.source_ts_ns < 0:
        raise SourceError("negative timestamp")
    if event.event_type is EventType.BOOK and (event.bids is None or event.asks is None):
        raise SourceError("BOOK lacks depth")
    if event.event_type is EventType.PRICE_CHANGE:
        if event.side not in {"BUY", "SELL"} or event.price is None or event.size is None:
            raise SourceError("PRICE_CHANGE lacks side/price/size")
        if event.size < 0:
            raise SourceError("negative update size")
    if event.event_type is EventType.LAST_TRADE_PRICE and (
        event.price is None or event.size is None
    ):
        raise SourceError("LAST_TRADE_PRICE lacks price/size")
    if event.event_type is EventType.TICK_SIZE_CHANGE:
        if event.old_tick_size is None or event.new_tick_size is None:
            raise SourceError("TICK_SIZE_CHANGE lacks sizes")
        if event.old_tick_size <= 0 or event.new_tick_size <= 0:
            raise SourceError("tick sizes must be positive")


def _event_fingerprint(event: BookEvent) -> str:
    raw = asdict(event)
    raw.pop("source_object")
    raw.pop("source_row")
    raw.pop("sequence")
    return sha256_bytes(canonical_json_bytes(_stringify(raw)))


def _stringify(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _stringify(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_stringify(item) for item in value]
    return value


def order_and_deduplicate(events: Iterable[BookEvent]) -> list[BookEvent]:
    ordered = sorted(
        events, key=lambda event: (event.condition_id, event.token_id, event.order_key)
    )
    fingerprints: set[str] = set()
    result: list[BookEvent] = []
    for event in ordered:
        fingerprint = _event_fingerprint(event)
        if fingerprint in fingerprints:
            continue
        fingerprints.add(fingerprint)
        result.append(event)
    return [
        BookEvent(**{**event.__dict__, "sequence": sequence})
        for sequence, event in enumerate(result)
    ]


class BookReconstructor:
    """Reconstruct each token causally from its first full snapshot."""

    def reconstruct(self, events: Iterable[BookEvent]) -> list[BookState]:
        ordered = order_and_deduplicate(events)
        grouped: dict[tuple[str, str], list[BookEvent]] = {}
        for event in ordered:
            grouped.setdefault((event.condition_id, event.token_id), []).append(event)
        states: list[BookState] = []
        for key in sorted(grouped):
            states.extend(self._token_states(grouped[key]))
        return states

    def _token_states(self, events: list[BookEvent]) -> list[BookState]:
        bids: dict[Decimal, Decimal] | None = None
        asks: dict[Decimal, Decimal] | None = None
        tick_size: Decimal | None = None
        states: list[BookState] = []
        for event_index, event in enumerate(events):
            event_context = (
                f"receive_ts_ns={event.receive_ts_ns}, source_ts_ns={event.source_ts_ns}, "
                f"token_id={event.token_id}, source_object={event.source_object}, "
                f"source_row={event.source_row}"
            )
            if event.event_type is EventType.BOOK:
                if event.receive_ts_ns is None:
                    raise ReconstructionError("PMXT event lacks receive timestamp")
                assert event.bids is not None and event.asks is not None
                bids = {level.price: level.size for level in event.bids}
                asks = {level.price: level.size for level in event.asks}
            elif event.event_type is EventType.PRICE_CHANGE:
                if bids is None or asks is None:
                    continue
                assert event.price is not None and event.size is not None and event.side is not None
                levels = bids if event.side == "BUY" else asks
                if event.size == 0:
                    levels.pop(event.price, None)
                else:
                    levels[event.price] = event.size
            elif event.event_type is EventType.TICK_SIZE_CHANGE:
                if bids is None or asks is None:
                    continue
                if tick_size is not None and event.old_tick_size != tick_size:
                    raise ReconstructionError(
                        f"tick-size chain is inconsistent: {event_context}"
                    )
                tick_size = event.new_tick_size
            elif event.event_type is EventType.LAST_TRADE_PRICE:
                continue
            if bids is None or asks is None:
                continue
            next_event = events[event_index + 1] if event_index + 1 < len(events) else None
            batch_end = next_event is None or next_event.receive_ts_ns != event.receive_ts_ns
            if batch_end and event.event_type is EventType.PRICE_CHANGE:
                if event.best_bid is not None:
                    bids = (
                        {}
                        if event.best_bid == 0
                        else {
                            price: size for price, size in bids.items() if price <= event.best_bid
                        }
                    )
                if event.best_ask is not None:
                    asks = (
                        {}
                        if event.best_ask == 1
                        else {
                            price: size for price, size in asks.items() if price >= event.best_ask
                        }
                    )
            bid_levels = tuple(
                Level(price, size) for price, size in sorted(bids.items(), reverse=True)
            )
            ask_levels = tuple(Level(price, size) for price, size in sorted(asks.items()))
            if batch_end:
                if bid_levels and ask_levels and bid_levels[0].price >= ask_levels[0].price:
                    raise ReconstructionError(f"crossed or locked book: {event_context}")
                if event.best_bid is not None:
                    actual_bid = bid_levels[0].price if bid_levels else None
                    claimed_bid = None if event.best_bid == 0 else event.best_bid
                    if actual_bid != claimed_bid:
                        detail = f"reconstructed={actual_bid}, claimed={event.best_bid}"
                        raise ReconstructionError(
                            f"best bid disagrees: {detail}; {event_context}"
                        )
                if event.best_ask is not None:
                    actual_ask = ask_levels[0].price if ask_levels else None
                    claimed_ask = None if event.best_ask == 1 else event.best_ask
                    if actual_ask != claimed_ask:
                        detail = f"reconstructed={actual_ask}, claimed={event.best_ask}"
                        raise ReconstructionError(
                            f"best ask disagrees: {detail}; {event_context}"
                        )
            states.append(
                BookState(
                    condition_id=event.condition_id,
                    token_id=event.token_id,
                    source_ts_ns=event.source_ts_ns,
                    receive_ts_ns=event.receive_ts_ns,
                    sequence=event.sequence,
                    bids=bid_levels,
                    asks=ask_levels,
                    tick_size=tick_size,
                    quality_tier=QualityTier.TIER_A,
                )
            )
        if not states:
            raise MissingInitialSnapshotError("no usable initial snapshot")
        return states
