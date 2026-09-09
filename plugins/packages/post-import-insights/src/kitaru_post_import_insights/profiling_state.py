#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Bounded accumulators for full-import deterministic profiling."""

import sqlite3
import uuid
from bisect import bisect_right, insort
from collections.abc import Iterator
from dataclasses import dataclass, field


class CountsStore:
    """Count exact names, spilling high cardinality imports to temporary SQLite."""

    def __init__(self, memory_limit: int = 1024) -> None:
        """Initialize a small counter with a shared cardinality threshold."""
        self.memory_limit = memory_limit
        self._counts: dict[tuple[str, str], int] = {}
        self._connection: sqlite3.Connection | None = None

    def increment(self, namespace: str, label: str) -> int:
        """Increment a count and return its new value."""
        key = (namespace, label)
        if self._connection is None:
            if key in self._counts or len(self._counts) < self.memory_limit:
                value = self._counts.get(key, 0) + 1
                self._counts[key] = value
                return value
            connection = sqlite3.connect("")
            self._connection = connection
            # SQLite deletes its unnamed temporary database on close. Bound its
            # page cache and force large ORDER BY operations to spill to disk.
            connection.execute("PRAGMA cache_size = -256")
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute(
                "CREATE TABLE counts (namespace TEXT, label TEXT, count INTEGER, "
                "PRIMARY KEY (namespace, label)) WITHOUT ROWID"
            )
            connection.executemany(
                "INSERT INTO counts VALUES (?, ?, ?)",
                ((ns, name, count) for (ns, name), count in self._counts.items()),
            )
            self._counts.clear()
        row = self._connection.execute(
            "INSERT INTO counts VALUES (?, ?, 1) "
            "ON CONFLICT(namespace, label) DO UPDATE SET count = count + 1 "
            "RETURNING count",
            (namespace, label),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def top(self, namespace: str, limit: int) -> list[tuple[str, int]]:
        """Return the exact leading counts in stable count and label order."""
        if self._connection is None:
            return sorted(
                (
                    (label, count)
                    for (ns, label), count in self._counts.items()
                    if ns == namespace
                ),
                key=lambda item: (-item[1], item[0]),
            )[:limit]
        return self._connection.execute(
            "SELECT label, count FROM counts WHERE namespace = ? "
            "ORDER BY count DESC, label ASC LIMIT ?",
            (namespace, limit),
        ).fetchall()

    def contains(self, namespace: str, label: str) -> bool:
        """Check exact membership without retaining all names in memory."""
        if self._connection is None:
            return (namespace, label) in self._counts
        return (
            self._connection.execute(
                "SELECT 1 FROM counts WHERE namespace = ? AND label = ?",
                (namespace, label),
            ).fetchone()
            is not None
        )

    def close(self) -> None:
        """Release temporary storage and the in-memory counts."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._counts.clear()


@dataclass
class LabelCounts:
    """Exact label totals with bounded chart projection."""

    store: CountsStore
    namespace: str
    distinct: int = 0
    total: int = 0

    def add(self, label: str) -> None:
        """Count one label occurrence."""
        if self.store.increment(self.namespace, label) == 1:
            self.distinct += 1
        self.total += 1

    def __len__(self) -> int:
        """Return the exact number of distinct labels."""
        return self.distinct

    def top(self, limit: int = 20) -> list[tuple[str, int]]:
        """Return top labels plus an exact combined remainder when needed."""
        values = self.store.top(self.namespace, limit)
        remainder = self.total - sum(count for _, count in values)
        if remainder:
            label = "Other categories (combined)"
            while self.store.contains(self.namespace, label):
                label = "_" + label
            values.append((label, remainder))
        return values


@dataclass
class SessionReferences:
    """Count unique, consecutive session contributions and retain smallest IDs."""

    limit: int
    count: int = 0
    retained: list[uuid.UUID] = field(default_factory=list)
    _last: uuid.UUID | None = None

    def add(self, session_id: uuid.UUID) -> None:
        """Add a contribution; all occurrences for a session must be consecutive."""
        if session_id == self._last:
            return
        self._last = session_id
        self.count += 1
        if len(self.retained) < self.limit or session_id < self.retained[-1]:
            insort(self.retained, session_id)
            del self.retained[self.limit :]

    def __len__(self) -> int:
        """Return the exact number of contributing sessions."""
        return self.count

    def __iter__(self) -> Iterator[uuid.UUID]:
        """Iterate over bounded, canonically ordered retained IDs."""
        return iter(self.retained)


@dataclass
class HighestValueSessions:
    """Retain bounded session values, descending by value then ascending by ID."""

    limit: int
    retained: list[tuple[float, uuid.UUID]] = field(default_factory=list)

    def add(self, session_id: uuid.UUID, value: float) -> None:
        """Record one eligible observation per session."""
        insort(self.retained, (-value, session_id))
        del self.retained[self.limit :]

    def get_entries(self) -> list[tuple[uuid.UUID, float]]:
        """Return session IDs and values in retained rank order."""
        return [
            (session_id, -negative_value)
            for negative_value, session_id in self.retained
        ]


@dataclass
class Histogram:
    """Count observations in fixed bins without retaining individual values."""

    bounds: tuple[float, ...]
    count: int = 0
    minimum: float = float("inf")
    maximum: float = float("-inf")
    bins: list[int] = field(init=False)

    def __post_init__(self) -> None:
        """Allocate fixed bins for the configured boundaries."""
        self.bins = [0] * (len(self.bounds) + 1)

    def add(self, value: float) -> None:
        """Record one observation and update exact extrema."""
        self.count += 1
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        self.bins[self.get_bin_index(value)] += 1

    def get_bin_index(self, value: float) -> int:
        """Locate a value using inclusive lower and exclusive upper bounds."""
        return bisect_right(self.bounds, value)

    def get_highest_occupied_bin(self) -> int | None:
        """Return the highest occupied bin index, or None for no observations."""
        return next(
            (index for index in reversed(range(len(self.bins))) if self.bins[index]),
            None,
        )

    def __len__(self) -> int:
        """Return the total number of observations."""
        return self.count
