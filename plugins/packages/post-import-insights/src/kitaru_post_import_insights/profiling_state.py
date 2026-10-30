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
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

# SQLite stores signed 64-bit integers.
MAX_OBSERVATION_VALUE = 2**63 - 1


def _open_spill_database() -> sqlite3.Connection:
    """Open an unnamed temporary database with a bounded page cache."""
    connection = sqlite3.connect("")
    # SQLite deletes its unnamed temporary database on close. Bound its page
    # cache and force large ORDER BY operations to spill to disk.
    connection.execute("PRAGMA cache_size = -256")
    connection.execute("PRAGMA temp_store = FILE")
    return connection


class CountsStore:
    """Count exact names, spilling high cardinality imports to temporary SQLite."""

    def __init__(self, memory_limit: int = 1024) -> None:
        """Initialize a small counter with a shared cardinality threshold."""
        self.memory_limit = memory_limit
        self._counts: dict[tuple[str, str], int] = {}
        self._connection: sqlite3.Connection | None = None

    def increment(self, namespace: str, label: str, amount: int = 1) -> int:
        """Add a positive amount to a count and return its new value."""
        if amount < 1:
            raise ValueError("count increments must be positive")
        key = (namespace, label)
        if self._connection is None:
            if key in self._counts or len(self._counts) < self.memory_limit:
                value = self._counts.get(key, 0) + amount
                self._counts[key] = value
                return value
            connection = _open_spill_database()
            self._connection = connection
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
            "INSERT INTO counts VALUES (?, ?, ?) "
            "ON CONFLICT(namespace, label) "
            "DO UPDATE SET count = count + excluded.count "
            "RETURNING count",
            (namespace, label, amount),
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

    def get(self, namespace: str, label: str) -> int:
        """Return one exact count, or zero for a label never counted."""
        if self._connection is None:
            return self._counts.get((namespace, label), 0)
        row = self._connection.execute(
            "SELECT count FROM counts WHERE namespace = ? AND label = ?",
            (namespace, label),
        ).fetchone()
        return 0 if row is None else int(row[0])

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

    def add(self, label: str, amount: int = 1) -> None:
        """Add a positive amount, such as one occurrence, to a label's total."""
        # Amounts are positive, so only a label's first amount equals its total.
        if self.store.increment(self.namespace, label, amount) == amount:
            self.distinct += 1
        self.total += amount

    def get(self, label: str) -> int:
        """Return one label's exact total."""
        return self.store.get(self.namespace, label)

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


@dataclass
class HighestValueNodes:
    """Retain bounded node values, descending by value then ascending by IDs."""

    limit: int
    retained: list[tuple[float, uuid.UUID, uuid.UUID]] = field(default_factory=list)

    def add(self, session_id: uuid.UUID, node_id: uuid.UUID, value: float) -> None:
        """Record one eligible node observation."""
        insort(self.retained, (-value, session_id, node_id))
        del self.retained[self.limit :]

    def get_nodes(self) -> list[tuple[uuid.UUID, uuid.UUID]]:
        """Return session and node IDs in retained rank order."""
        return [(session_id, node_id) for _, session_id, node_id in self.retained]


@dataclass(frozen=True)
class Observation:
    """One exact integer observation and the session, node, and key it describes."""

    value: int
    session: str
    node: str = ""
    key: str = ""


class ObservationStore:
    """Keep exact integer observations, spilling large imports to temporary SQLite.

    Ranked reads order by value descending, then session and node ascending, so
    results never depend on the order in which observations arrived.
    """

    def __init__(self, memory_limit: int = 20_000) -> None:
        """Initialize an empty store that spills after ``memory_limit`` rows."""
        self.memory_limit = memory_limit
        self._rows: defaultdict[str, list[Observation]] = defaultdict(list)
        self._size = 0
        self._connection: sqlite3.Connection | None = None
        self._indexed = False

    def add(self, namespace: str, observation: Observation) -> None:
        """Record one observation whose value fits the SQLite integer range."""
        if not 0 <= observation.value <= MAX_OBSERVATION_VALUE:
            raise ValueError("observation value is outside the supported range")
        if self._connection is None and self._size < self.memory_limit:
            self._rows[namespace].append(observation)
            self._size += 1
            return
        self._get_connection().execute(
            "INSERT INTO observations VALUES (?, ?, ?, ?, ?)",
            (
                namespace,
                observation.value,
                observation.session,
                observation.node,
                observation.key,
            ),
        )

    def count(self, namespace: str, *, minimum: int = 0) -> int:
        """Count observations whose value is at least ``minimum``."""
        if self._connection is None:
            return sum(row.value >= minimum for row in self._rows[namespace])
        row = self._query(
            "SELECT COUNT(*) FROM observations WHERE namespace = ? AND value >= ?",
            (namespace, minimum),
        ).fetchone()
        return int(row[0])

    def count_sessions(self, namespace: str, *, minimum: int = 0) -> int:
        """Count distinct sessions with an observation of at least ``minimum``."""
        if self._connection is None:
            return len(
                {row.session for row in self._rows[namespace] if row.value >= minimum}
            )
        row = self._query(
            "SELECT COUNT(DISTINCT session) FROM observations "
            "WHERE namespace = ? AND value >= ?",
            (namespace, minimum),
        ).fetchone()
        return int(row[0])

    def get_median(self, namespace: str) -> int:
        """Return the lower median value, the lower middle one for an even count."""
        index = (self.count(namespace) - 1) // 2
        if self._connection is None:
            return sorted(row.value for row in self._rows[namespace])[index]
        row = self._query(
            "SELECT value FROM observations WHERE namespace = ? "
            "ORDER BY value LIMIT 1 OFFSET ?",
            (namespace, index),
        ).fetchone()
        if row is None:
            raise IndexError("no observations to take a median of")
        return int(row[0])

    def iter_highest(
        self, namespace: str, *, key: str | None = None, minimum: int = 0
    ) -> Iterator[Observation]:
        """Yield matching observations, highest value first."""
        if self._connection is None:
            rows = [
                row
                for row in self._rows[namespace]
                if row.value >= minimum and (key is None or row.key == key)
            ]
            rows.sort(key=lambda row: (-row.value, row.session, row.node))
            yield from rows
            return
        cursor = self._query(
            "SELECT value, session, node, key FROM observations "
            "WHERE namespace = ? AND value >= ? AND (? IS NULL OR key = ?) "
            "ORDER BY value DESC, session ASC, node ASC",
            (namespace, minimum, key, key),
        )
        for value, session, node, row_key in cursor:
            yield Observation(value=value, session=session, node=node, key=row_key)

    def get_unique_key_sessions(
        self, namespace: str, limit: int
    ) -> tuple[int, list[str]]:
        """Return how many keys occur once and the smallest of their sessions."""
        if self._connection is None:
            rows = self._rows[namespace]
            counts = Counter(row.key for row in rows)
            sessions = sorted(row.session for row in rows if counts[row.key] == 1)
            return len(sessions), sessions[:limit]
        unique = (
            "SELECT MIN(session) AS session FROM observations WHERE namespace = ? "
            "GROUP BY key HAVING COUNT(*) = 1"
        )
        total = self._query(f"SELECT COUNT(*) FROM ({unique})", (namespace,)).fetchone()
        sessions = self._query(
            f"SELECT session FROM ({unique}) ORDER BY session LIMIT ?",
            (namespace, limit),
        ).fetchall()
        return int(total[0]), [str(row[0]) for row in sessions]

    def close(self) -> None:
        """Release temporary storage and the in-memory rows."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._rows.clear()
        self._size = 0
        self._indexed = False

    def _query(self, sql: str, parameters: Sequence[Any]) -> sqlite3.Cursor:
        """Run a read against the spill database, indexing it before the first."""
        connection = self._get_connection()
        # Rows only arrive before reads, and indexing them all at once is far
        # cheaper than keeping an index current through every insert.
        if not self._indexed:
            connection.execute(
                "CREATE INDEX observations_by_value ON observations (namespace, value)"
            )
            self._indexed = True
        return connection.execute(sql, parameters)

    def _get_connection(self) -> sqlite3.Connection:
        """Return the spill database, moving in-memory rows into it once."""
        if self._connection is not None:
            return self._connection
        connection = _open_spill_database()
        self._connection = connection
        connection.execute(
            "CREATE TABLE observations (namespace TEXT, value INTEGER, "
            "session TEXT, node TEXT, key TEXT)"
        )
        connection.executemany(
            "INSERT INTO observations VALUES (?, ?, ?, ?, ?)",
            (
                (namespace, row.value, row.session, row.node, row.key)
                for namespace, rows in self._rows.items()
                for row in rows
            ),
        )
        self._rows.clear()
        return connection
