"""Bounded temporary PMXT event spool keyed by condition."""

from __future__ import annotations

import pickle
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from canonical_data.errors import ConflictError
from canonical_data.models import BookEvent
from canonical_data.pmxt import order_and_deduplicate


class EventSpool:
    def __init__(self, path: Path, create_index: bool = True):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "condition_id TEXT NOT NULL, source_object TEXT NOT NULL, payload BLOB NOT NULL)"
        )
        if create_index:
            self.ensure_index()

    def ensure_index(self) -> None:
        with self.connection:
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS events_condition ON events(condition_id)"
            )

    def drop_index(self) -> None:
        with self.connection:
            self.connection.execute("DROP INDEX IF EXISTS events_condition")

    def append(self, events: Iterable[BookEvent]) -> int:
        count = 0

        def rows() -> Iterable[tuple[str, str, sqlite3.Binary]]:
            nonlocal count
            for event in events:
                count += 1
                yield (
                    event.condition_id,
                    event.source_object,
                    sqlite3.Binary(pickle.dumps(event, protocol=5)),
                )

        with self.connection:
            self.connection.executemany(
                "INSERT INTO events(condition_id,source_object,payload) VALUES (?,?,?)", rows()
            )
        return count

    def discard_uncommitted_sources(self, completed_sources: set[str]) -> int:
        sources = {
            str(row[0])
            for row in self.connection.execute("SELECT DISTINCT source_object FROM events")
        }
        uncommitted = sources - completed_sources
        removed = 0
        with self.connection:
            for source in sorted(uncommitted):
                cursor = self.connection.execute(
                    "DELETE FROM events WHERE source_object=?", (source,)
                )
                removed += cursor.rowcount
        return removed

    def load(self, condition_id: str) -> list[BookEvent]:
        rows = self.connection.execute(
            "SELECT payload FROM events WHERE condition_id=?", (condition_id,)
        ).fetchall()
        events = []
        for (payload,) in rows:
            event = pickle.loads(payload)
            if not isinstance(event, BookEvent):
                raise ConflictError("temporary event spool contains an invalid record")
            events.append(event)
        return order_and_deduplicate(events)

    def count(self) -> int:
        value = self.connection.execute("SELECT COUNT(*) FROM events").fetchone()
        assert value is not None
        return int(value[0])

    def count_condition(self, condition_id: str) -> int:
        value = self.connection.execute(
            "SELECT COUNT(*) FROM events WHERE condition_id=?", (condition_id,)
        ).fetchone()
        assert value is not None
        return int(value[0])

    def counts_by_condition(self) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT condition_id, COUNT(*) FROM events GROUP BY condition_id"
        ).fetchall()
        return {str(condition_id): int(count) for condition_id, count in rows}

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> EventSpool:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
