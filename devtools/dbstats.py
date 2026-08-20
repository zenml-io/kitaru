"""Postgres statistics collection and reporting for local Kitaru databases."""

import argparse
import asyncio
import contextlib
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
from stack import (
    DB_CONTAINER,
    DB_HOST,
    DB_PORT,
    DB_PWD,
    DB_USER,
    REPO_ROOT,
    RUN_DIR,
    database_exists,
    ensure_postgres,
    resolve_db_name,
)

_NOISE_PREFIXES = (
    "SET",
    "SHOW",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "CREATE EXTENSION",
    "ALTER SYSTEM",
    "pg_stat",
    "EXPLAIN",
)


def build_dsn(db_name: str) -> str:
    """Build a DSN for a database on the local test Postgres."""
    return f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{db_name}"


def _collapse_query(query: str, max_len: int = 300) -> str:
    """Collapse whitespace in a query string and truncate it."""
    collapsed = re.sub(r"\s+", " ", query).strip()
    if len(collapsed) > max_len:
        return collapsed[:max_len] + "..."
    return collapsed


def _is_noise(query: str) -> bool:
    """Whether a query is utility noise or this tool's own stats query."""
    stripped = query.strip().upper()
    return any(stripped.startswith(prefix.upper()) for prefix in _NOISE_PREFIXES)


def _hit_ratio(hit: int, read: int) -> float:
    """Return a cache hit ratio in [0, 1], treating no accesses as a full hit."""
    total = hit + read
    if total == 0:
        return 1.0
    return hit / total


async def _column_exists(conn: asyncpg.Connection, table: str, column: str) -> bool:
    """Whether a column exists on a table in the connected database."""
    return bool(
        await conn.fetchval(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = $1 AND column_name = $2",
            table,
            column,
        )
    )


async def _extension_available(conn: asyncpg.Connection, name: str) -> bool:
    """Whether an extension is installed in the connected database."""
    return bool(
        await conn.fetchval("SELECT 1 FROM pg_extension WHERE extname = $1", name)
    )


def _stat_statement_row(record: asyncpg.Record, has_max: bool) -> dict[str, Any]:
    """Shape one pg_stat_statements row into a report entry."""
    hit = record["shared_blks_hit"]
    read = record["shared_blks_read"]
    row = {
        "query": _collapse_query(record["query"]),
        "calls": record["calls"],
        "total_exec_time_ms": round(record["total_exec_time"], 2),
        "mean_exec_time_ms": round(record["mean_exec_time"], 2),
        "stddev_exec_time_ms": round(record["stddev_exec_time"], 2),
        "rows": record["rows"],
        "shared_blks_hit": hit,
        "shared_blks_read": read,
        "hit_ratio": round(_hit_ratio(hit, read), 4),
    }
    if has_max:
        row["max_exec_time_ms"] = round(record["max_exec_time"], 2)
    return row


async def _pg_stat_statements(conn: asyncpg.Connection) -> dict[str, Any]:
    """Collect the four pg_stat_statements views, or an unavailable marker."""
    if not await _extension_available(conn, "pg_stat_statements"):
        unavailable = {
            "unavailable": (
                "pg_stat_statements extension not installed, "
                "run dbstats.py enable first"
            )
        }
        return {
            "top_queries_by_total_time": unavailable,
            "top_queries_by_mean_time": unavailable,
            "top_queries_by_rows": unavailable,
            "top_queries_by_io": unavailable,
        }
    has_max = await _column_exists(conn, "pg_stat_statements", "max_exec_time")
    max_col = "s.max_exec_time," if has_max else ""
    records = await conn.fetch(
        f"""
        SELECT s.query, s.calls, s.total_exec_time, s.mean_exec_time,
               s.stddev_exec_time, {max_col} s.rows,
               s.shared_blks_hit, s.shared_blks_read
        FROM pg_stat_statements s
        JOIN pg_database d ON d.oid = s.dbid
        WHERE d.datname = current_database()
        """
    )
    rows = [
        _stat_statement_row(record, has_max)
        for record in records
        if not _is_noise(record["query"])
    ]
    by_total = sorted(rows, key=lambda r: r["total_exec_time_ms"], reverse=True)[:25]
    by_mean = sorted(
        (r for r in rows if r["calls"] >= 5),
        key=lambda r: r["mean_exec_time_ms"],
        reverse=True,
    )[:25]
    by_rows = sorted(rows, key=lambda r: r["rows"], reverse=True)[:10]
    by_io = sorted(rows, key=lambda r: r["shared_blks_read"], reverse=True)[:10]
    return {
        "top_queries_by_total_time": by_total,
        "top_queries_by_mean_time": by_mean,
        "top_queries_by_rows": by_rows,
        "top_queries_by_io": by_io,
    }


async def _table_stats(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Collect per-table size, scan, and cache hit statistics."""
    records = await conn.fetch(
        """
        SELECT
            t.relname,
            t.n_live_tup,
            t.n_dead_tup,
            t.seq_scan,
            t.seq_tup_read,
            t.idx_scan,
            t.idx_tup_fetch,
            pg_table_size(t.relid) AS table_size,
            pg_indexes_size(t.relid) AS index_size,
            t.last_autovacuum,
            t.last_analyze,
            io.heap_blks_hit,
            io.heap_blks_read
        FROM pg_stat_user_tables t
        LEFT JOIN pg_statio_user_tables io ON io.relid = t.relid
        ORDER BY t.n_live_tup DESC
        """
    )
    return [
        {
            "table": record["relname"],
            "n_live_tup": record["n_live_tup"],
            "n_dead_tup": record["n_dead_tup"],
            "seq_scan": record["seq_scan"],
            "seq_tup_read": record["seq_tup_read"],
            "idx_scan": record["idx_scan"],
            "idx_tup_fetch": record["idx_tup_fetch"],
            "table_size_bytes": record["table_size"],
            "index_size_bytes": record["index_size"],
            "last_autovacuum": (
                record["last_autovacuum"].isoformat()
                if record["last_autovacuum"]
                else None
            ),
            "last_analyze": (
                record["last_analyze"].isoformat() if record["last_analyze"] else None
            ),
            "cache_hit_ratio": round(
                _hit_ratio(record["heap_blks_hit"] or 0, record["heap_blks_read"] or 0),
                4,
            ),
        }
        for record in records
    ]


async def _missing_index_candidates(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Find heavily sequentially scanned tables that look like missing indexes."""
    records = await conn.fetch(
        """
        SELECT relname, seq_scan, seq_tup_read, idx_scan, n_live_tup
        FROM pg_stat_user_tables
        WHERE seq_scan > 50 AND n_live_tup > 1000
        ORDER BY seq_tup_read DESC
        """
    )
    return [
        {
            "table": record["relname"],
            "seq_scan": record["seq_scan"],
            "seq_tup_read": record["seq_tup_read"],
            "idx_scan": record["idx_scan"],
            "n_live_tup": record["n_live_tup"],
            "avg_rows_per_seq_scan": round(
                record["seq_tup_read"] / record["seq_scan"], 1
            ),
            "seq_scan_to_idx_scan_ratio": round(
                record["seq_scan"] / (record["idx_scan"] or 1), 2
            ),
        }
        for record in records
    ]


async def _unused_indexes(
    conn: asyncpg.Connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Find zero-scan and near-zero-scan indexes, excluding constraint-backing ones."""
    records = await conn.fetch(
        """
        SELECT i.indexrelname AS index_name, i.relname AS table_name,
               i.idx_scan, pg_relation_size(i.indexrelid) AS size
        FROM pg_stat_user_indexes i
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid
        )
        ORDER BY i.idx_scan ASC, size DESC
        """
    )
    unused = [
        {
            "index": record["index_name"],
            "table": record["table_name"],
            "idx_scan": record["idx_scan"],
            "size_bytes": record["size"],
        }
        for record in records
        if record["idx_scan"] == 0
    ]
    rarely_used = [
        {
            "index": record["index_name"],
            "table": record["table_name"],
            "idx_scan": record["idx_scan"],
            "size_bytes": record["size"],
        }
        for record in records
        if 0 < record["idx_scan"] < 10
    ]
    return unused, rarely_used


async def _duplicate_indexes(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """Find index pairs on the same table where one's columns prefix the other's."""
    records = await conn.fetch(
        """
        SELECT
            t.relname AS table_name,
            i.indexrelid,
            ic.relname AS index_name,
            i.indkey::text AS indkey
        FROM pg_index i
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND i.indpred IS NULL
        """
    )
    by_table: dict[str, list[asyncpg.Record]] = {}
    for record in records:
        by_table.setdefault(record["table_name"], []).append(record)

    duplicates: list[dict[str, Any]] = []
    for table_name, indexes in by_table.items():
        for i, left in enumerate(indexes):
            left_cols = left["indkey"].split()
            for right in indexes[i + 1 :]:
                right_cols = right["indkey"].split()
                shorter, longer = sorted((left_cols, right_cols), key=len)
                if shorter and longer[: len(shorter)] == shorter:
                    shorter_name = (
                        left["index_name"]
                        if left_cols == shorter
                        else right["index_name"]
                    )
                    longer_name = (
                        right["index_name"]
                        if left_cols == shorter
                        else left["index_name"]
                    )
                    duplicates.append(
                        {
                            "table": table_name,
                            "prefix_index": shorter_name,
                            "superset_index": longer_name,
                        }
                    )
    return duplicates


async def _database_stats(conn: asyncpg.Connection) -> dict[str, Any]:
    """Collect database-wide activity counters for the connected database."""
    record = await conn.fetchrow(
        """
        SELECT xact_commit, xact_rollback, blks_hit, blks_read,
               tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
               temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time
        FROM pg_stat_database
        WHERE datname = current_database()
        """
    )
    return {
        "xact_commit": record["xact_commit"],
        "xact_rollback": record["xact_rollback"],
        "blks_hit": record["blks_hit"],
        "blks_read": record["blks_read"],
        "hit_ratio": round(_hit_ratio(record["blks_hit"], record["blks_read"]), 4),
        "tup_returned": record["tup_returned"],
        "tup_fetched": record["tup_fetched"],
        "tup_inserted": record["tup_inserted"],
        "tup_updated": record["tup_updated"],
        "tup_deleted": record["tup_deleted"],
        "temp_files": record["temp_files"],
        "temp_bytes": record["temp_bytes"],
        "deadlocks": record["deadlocks"],
        "blk_read_time_ms": round(record["blk_read_time"], 2),
        "blk_write_time_ms": round(record["blk_write_time"], 2),
    }


async def _connections(conn: asyncpg.Connection) -> dict[str, Any]:
    """Collect current connection counts by state and the max_connections setting."""
    records = await conn.fetch(
        """
        SELECT state, count(*) AS count
        FROM pg_stat_activity
        WHERE datname = current_database()
        GROUP BY state
        """
    )
    max_connections = await conn.fetchval("SHOW max_connections")
    return {
        "by_state": {(r["state"] or "unknown"): r["count"] for r in records},
        "max_connections": int(max_connections),
    }


async def _index_definitions(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    """List every user index with its definition and scan count."""
    records = await conn.fetch(
        """
        SELECT s.relname AS table_name, s.indexrelname AS index_name,
               s.idx_scan, pg_get_indexdef(s.indexrelid) AS definition
        FROM pg_stat_user_indexes s
        ORDER BY s.relname, s.indexrelname
        """
    )
    return [
        {
            "table": record["table_name"],
            "index": record["index_name"],
            "idx_scan": record["idx_scan"],
            "definition": record["definition"],
        }
        for record in records
    ]


async def reset_stats(dsn: str) -> None:
    """Reset pg_stat_statements and the connected database's built-in stats."""
    conn = await asyncpg.connect(dsn)
    try:
        if await _extension_available(conn, "pg_stat_statements"):
            await conn.execute("SELECT pg_stat_statements_reset()")
        else:
            print(
                "warning: pg_stat_statements extension not installed, "
                "skipping query stats reset",
                file=sys.stderr,
            )
        await conn.execute("SELECT pg_stat_reset()")
    finally:
        await conn.close()


async def collect_stats(dsn: str) -> dict[str, Any]:
    """Collect the full statistics report for the connected database."""
    conn = await asyncpg.connect(dsn)
    try:
        stats: dict[str, Any] = {}
        stats["database_stats"] = await _database_stats(conn)
        stats["connections"] = await _connections(conn)
        stats["table_stats"] = await _table_stats(conn)
        stats["missing_index_candidates"] = await _missing_index_candidates(conn)
        unused, rarely_used = await _unused_indexes(conn)
        stats["unused_indexes"] = unused
        stats["rarely_used_indexes"] = rarely_used
        stats["duplicate_indexes"] = await _duplicate_indexes(conn)
        stats["index_definitions"] = await _index_definitions(conn)
        stats.update(await _pg_stat_statements(conn))
        return stats
    finally:
        await conn.close()


def _restart_database() -> None:
    """Restart the test Postgres container."""
    # The container may belong to another worktree's compose project, where
    # `docker compose restart db` matches nothing and still exits zero.
    restarted = subprocess.run(["docker", "restart", DB_CONTAINER], capture_output=True)
    if restarted.returncode != 0:
        subprocess.run(
            ["docker", "compose", "restart", "db"], cwd=REPO_ROOT, check=True
        )


async def enable_statements(dsn: str) -> None:
    """Enable pg_stat_statements, restarting the compose db when needed."""
    conn = await asyncpg.connect(dsn)
    try:
        preloaded = "pg_stat_statements" in (
            await conn.fetchval("SHOW shared_preload_libraries") or ""
        )
        if not preloaded:
            await conn.execute(
                "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements'"
            )
            await conn.execute("ALTER SYSTEM SET track_io_timing = on")
    finally:
        await conn.close()
    if not preloaded:
        print(f"Restarting {DB_CONTAINER} to preload pg_stat_statements ...")
        _restart_database()
        deadline = asyncio.get_event_loop().time() + 60.0
        while True:
            try:
                conn = await asyncpg.connect(dsn)
                break
            except (OSError, asyncpg.PostgresError):
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                await asyncio.sleep(0.5)
        loaded = "pg_stat_statements" in (
            await conn.fetchval("SHOW shared_preload_libraries") or ""
        )
        if not loaded:
            await conn.close()
            raise RuntimeError(
                f"{DB_CONTAINER} came back without pg_stat_statements preloaded, "
                "so the restart did not apply the setting."
            )
    else:
        conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    finally:
        await conn.close()
    print("pg_stat_statements enabled.")


def _humanize_bytes(size: float) -> str:
    """Format a byte count as a human-readable size."""
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if abs(size) < 1024.0:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}PB"


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    """Render a list of dicts as a markdown table restricted to the given columns."""
    if not rows:
        return "_no rows_\n"
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, separator]
    for row in rows:
        values = [str(row.get(column, "")) for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines) + "\n"


def _query_table_section(title: str, data: Any, columns: list[str]) -> str:
    """Render one query-stats section as a markdown heading plus table."""
    lines = [f"## {title}\n"]
    if isinstance(data, dict) and "unavailable" in data:
        lines.append(f"_unavailable: {data['unavailable']}_\n")
    else:
        lines.append(_markdown_table(data, columns))
    return "\n".join(lines)


_QUERY_COLUMNS = [
    "query",
    "calls",
    "total_exec_time_ms",
    "mean_exec_time_ms",
    "max_exec_time_ms",
    "stddev_exec_time_ms",
    "rows",
    "hit_ratio",
]


def render_markdown(stats: dict[str, Any]) -> str:
    """Render a collected stats dict as a markdown report."""
    lines: list[str] = ["# Kitaru DB Report\n"]

    db = stats.get("database_stats", {})
    tables = stats.get("table_stats", [])
    total_size = (
        sum(t.get("table_size_bytes", 0) for t in tables)
        if isinstance(tables, list)
        else 0
    )
    mean_queries = stats.get("top_queries_by_mean_time")
    worst_query = ""
    if isinstance(mean_queries, list) and mean_queries:
        worst = mean_queries[0]
        worst_query = f"{worst['mean_exec_time_ms']}ms mean: `{worst['query'][:80]}`"
    missing = stats.get("missing_index_candidates", [])
    unused = stats.get("unused_indexes", [])
    total_time = stats.get("top_queries_by_total_time")
    tracked_count = len(total_time) if isinstance(total_time, list) else 0

    missing_count = len(missing) if isinstance(missing, list) else 0
    unused_count = len(unused) if isinstance(unused, list) else 0

    lines.append("## Summary\n")
    lines.append(f"- Total table size: {_humanize_bytes(total_size)}")
    lines.append(f"- Cache hit ratio: {db.get('hit_ratio', 'n/a')}")
    lines.append(f"- Distinct queries tracked: {tracked_count}")
    lines.append(f"- Worst mean-time query: {worst_query or 'n/a'}")
    lines.append(f"- Missing index candidates: {missing_count}")
    lines.append(f"- Unused indexes: {unused_count}\n")

    lines.append(
        _query_table_section(
            "Top queries by total time",
            stats.get("top_queries_by_total_time"),
            _QUERY_COLUMNS,
        )
    )
    lines.append(
        _query_table_section(
            "Top queries by mean time (calls >= 5)",
            stats.get("top_queries_by_mean_time"),
            _QUERY_COLUMNS,
        )
    )
    lines.append(
        _query_table_section(
            "Top queries by rows returned",
            stats.get("top_queries_by_rows"),
            _QUERY_COLUMNS,
        )
    )
    lines.append(
        _query_table_section(
            "Top queries by shared blocks read",
            stats.get("top_queries_by_io"),
            _QUERY_COLUMNS,
        )
    )

    lines.append("## Table stats\n")
    rendered = [
        {
            **t,
            "table_size": _humanize_bytes(t["table_size_bytes"]),
            "index_size": _humanize_bytes(t["index_size_bytes"]),
        }
        for t in (tables if isinstance(tables, list) else [])
    ]
    lines.append(
        _markdown_table(
            rendered,
            [
                "table",
                "n_live_tup",
                "n_dead_tup",
                "seq_scan",
                "seq_tup_read",
                "idx_scan",
                "idx_tup_fetch",
                "table_size",
                "index_size",
                "cache_hit_ratio",
                "last_autovacuum",
                "last_analyze",
            ],
        )
    )

    lines.append("## Missing index candidates\n")
    lines.append(
        _markdown_table(
            missing if isinstance(missing, list) else [],
            [
                "table",
                "seq_scan",
                "seq_tup_read",
                "idx_scan",
                "n_live_tup",
                "avg_rows_per_seq_scan",
                "seq_scan_to_idx_scan_ratio",
            ],
        )
    )

    lines.append("## Unused indexes\n")
    unused_rendered = [
        {**u, "size": _humanize_bytes(u["size_bytes"])}
        for u in (unused if isinstance(unused, list) else [])
    ]
    lines.append(
        _markdown_table(unused_rendered, ["index", "table", "idx_scan", "size"])
    )

    rarely_used = stats.get("rarely_used_indexes", [])
    lines.append("## Rarely used indexes (fewer than 10 scans)\n")
    rarely_rendered = [
        {**u, "size": _humanize_bytes(u["size_bytes"])}
        for u in (rarely_used if isinstance(rarely_used, list) else [])
    ]
    lines.append(
        _markdown_table(rarely_rendered, ["index", "table", "idx_scan", "size"])
    )

    lines.append("## Duplicate indexes\n")
    duplicate_indexes = stats.get("duplicate_indexes", [])
    lines.append(
        _markdown_table(
            duplicate_indexes if isinstance(duplicate_indexes, list) else [],
            ["table", "prefix_index", "superset_index"],
        )
    )

    lines.append("## Database stats\n")
    lines.append(_markdown_table([db], list(db.keys())) if db else "_no data_\n")

    connections = stats.get("connections", {})
    lines.append("## Connections\n")
    if connections:
        by_state = connections.get("by_state", {})
        conn_rows = [
            {"state": state, "count": count} for state, count in by_state.items()
        ]
        lines.append(_markdown_table(conn_rows, ["state", "count"]))
        lines.append(
            f"\nmax_connections: {connections.get('max_connections', 'n/a')}\n"
        )

    lock_waits = stats.get("extra", {}).get("lock_waits")
    if lock_waits:
        lines.append("## Lock waits (sampled during the workload)\n")
        lines.append(
            f"samples: {lock_waits.get('samples', 0)}, "
            f"peak concurrent waiters: {lock_waits.get('peak_waiters', 0)}, "
            f"peak active backends: {lock_waits.get('peak_active', 0)}, "
            f"peak idle-in-transaction: {lock_waits.get('peak_idle_in_tx', 0)}\n"
        )
        lines.append(
            _markdown_table(
                lock_waits.get("pairs", []),
                [
                    "blocked_query",
                    "blocking_query",
                    "wait_event",
                    "samples_seen",
                    "max_waiters",
                ],
            )
        )

    lines.append("## Index definitions\n")
    index_defs = stats.get("index_definitions", [])
    lines.append(
        _markdown_table(
            index_defs if isinstance(index_defs, list) else [],
            ["table", "index", "idx_scan", "definition"],
        )
    )

    return "\n".join(lines) + "\n"


class LockSampler:
    """Sample lock waits and backend states on an interval until stopped."""

    def __init__(self, dsn: str, interval: float = 1.0) -> None:
        """Initialize the sampler."""
        self.dsn = dsn
        self.interval = interval
        self.samples = 0
        self.peak_waiters = 0
        self.peak_active = 0
        self.peak_idle_in_tx = 0
        self._pairs: dict[tuple[str, str, str], dict[str, int]] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def _sample(self, conn: asyncpg.Connection) -> None:
        """Take one sample of blocked backends and state counts."""
        blocked = await conn.fetch(
            """
            SELECT left(a.query, 160) AS blocked_query,
                   coalesce(a.wait_event_type || ':' || a.wait_event, '')
                       AS wait_event,
                   (SELECT left(b.query, 160) FROM pg_stat_activity b
                    WHERE b.pid = ANY(pg_blocking_pids(a.pid)) LIMIT 1)
                       AS blocking_query
            FROM pg_stat_activity a
            WHERE a.datname = current_database()
              AND cardinality(pg_blocking_pids(a.pid)) > 0
            """
        )
        states = await conn.fetch(
            """
            SELECT state, count(*) AS n FROM pg_stat_activity
            WHERE datname = current_database() GROUP BY state
            """
        )
        self.samples += 1
        self.peak_waiters = max(self.peak_waiters, len(blocked))
        for record in states:
            if record["state"] == "active":
                self.peak_active = max(self.peak_active, record["n"])
            elif record["state"] == "idle in transaction":
                self.peak_idle_in_tx = max(self.peak_idle_in_tx, record["n"])
        by_pair: dict[tuple[str, str, str], int] = {}
        for record in blocked:
            key = (
                _collapse_query(record["blocked_query"] or "", 160),
                _collapse_query(record["blocking_query"] or "", 160),
                record["wait_event"],
            )
            by_pair[key] = by_pair.get(key, 0) + 1
        for key, waiters in by_pair.items():
            entry = self._pairs.setdefault(key, {"samples_seen": 0, "max_waiters": 0})
            entry["samples_seen"] += 1
            entry["max_waiters"] = max(entry["max_waiters"], waiters)

    async def _run(self) -> None:
        """Sample on the interval, reconnecting on errors."""
        conn: asyncpg.Connection | None = None
        try:
            while not self._stop.is_set():
                try:
                    if conn is None or conn.is_closed():
                        conn = await asyncpg.connect(dsn=self.dsn)
                    await self._sample(conn)
                except (OSError, asyncpg.PostgresError):
                    if conn is not None and not conn.is_closed():
                        await conn.close()
                    conn = None
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
        finally:
            if conn is not None and not conn.is_closed():
                await conn.close()

    def start(self) -> None:
        """Start sampling in a background task."""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> dict[str, Any]:
        """Stop sampling and return the aggregated summary."""
        self._stop.set()
        if self._task is not None:
            await self._task
        pairs = [
            {
                "blocked_query": blocked,
                "blocking_query": blocking,
                "wait_event": wait_event,
                "samples_seen": entry["samples_seen"],
                "max_waiters": entry["max_waiters"],
            }
            for (blocked, blocking, wait_event), entry in sorted(
                self._pairs.items(),
                key=lambda item: item[1]["samples_seen"],
                reverse=True,
            )
        ]
        return {
            "samples": self.samples,
            "peak_waiters": self.peak_waiters,
            "peak_active": self.peak_active,
            "peak_idle_in_tx": self.peak_idle_in_tx,
            "pairs": pairs[:20],
        }


async def write_report(
    dsn: str, out_dir: Path, extra: dict[str, Any] | None = None
) -> Path:
    """Collect stats, write stats.json and db_report.md, and return the md path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = await collect_stats(dsn)
    if extra:
        stats["extra"] = extra
    json_path = out_dir / "stats.json"
    json_path.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")
    md_path = out_dir / "db_report.md"
    md_path.write_text(render_markdown(stats), encoding="utf-8")
    return md_path


async def _run_sample(dsn: str, seconds: float, interval: float) -> None:
    """Sample lock waits for a fixed window and print the summary."""
    sampler = LockSampler(dsn, interval=interval)
    sampler.start()
    print(f"Sampling lock waits for {seconds:.0f}s ...")
    await asyncio.sleep(seconds)
    summary = await sampler.stop()
    print(json.dumps(summary, indent=2))


async def _check_database(db_name: str) -> None:
    """Exit with a readable message when the target database is missing."""
    await ensure_postgres()
    if not await database_exists(db_name):
        raise SystemExit(
            f"Database {db_name} does not exist on {DB_HOST}:{DB_PORT}. "
            "Start a stack or pass --db-name."
        )


def main() -> int:
    """Run the DB stats CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn", default=None, help="Postgres DSN, falls back to KITARU_DB_DSN."
    )
    parser.add_argument(
        "--db-name",
        default=None,
        help=(
            "Database on the local test Postgres, ignored when --dsn is set. "
            "Defaults to the running stack's database."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "enable", help="Enable pg_stat_statements, restarting the compose db."
    )
    commands.add_parser("reset", help="Reset pg_stat_statements and pg_stat counters.")
    report = commands.add_parser("report", help="Collect and write a stats report.")
    report.add_argument("--out", default=None, help="Output directory.")
    sample = commands.add_parser("sample", help="Sample lock waits for a window.")
    sample.add_argument("--seconds", type=float, default=30.0)
    sample.add_argument("--interval", type=float, default=1.0)

    args = parser.parse_args()
    dsn = args.dsn or os.environ.get("KITARU_DB_DSN")
    if dsn is None:
        db_name = args.db_name or resolve_db_name()
        asyncio.run(_check_database(db_name))
        dsn = build_dsn(db_name)

    if args.command == "enable":
        asyncio.run(enable_statements(dsn))
    elif args.command == "reset":
        asyncio.run(reset_stats(dsn))
        print(f"Stats reset on {dsn}")
    elif args.command == "report":
        if args.out:
            out_dir = Path(args.out)
        else:
            timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            out_dir = RUN_DIR / "reports" / timestamp
        md_path = asyncio.run(write_report(dsn, out_dir))
        print(f"Report written to {md_path}")
        print(f"Raw stats written to {out_dir / 'stats.json'}")
    elif args.command == "sample":
        asyncio.run(_run_sample(dsn, args.seconds, args.interval))
    return 0


if __name__ == "__main__":
    sys.exit(main())
