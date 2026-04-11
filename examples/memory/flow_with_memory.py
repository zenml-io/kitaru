"""Memory Walkthrough — Kitaru's durable memory across all three scopes.

This example demonstrates memory evolving **checkpoint by checkpoint**:

- **namespace scope**: seeded before the flow, read and updated during it
- **execution scope**: tracking per-run progress within the flow body
- **flow scope**: accumulating cross-run summaries

The flow interleaves memory writes between checkpoints so that each
checkpoint boundary has a different memory state visible — ideal for UI
panels that show "what memory was available at this checkpoint."

Also covers:
- detached post-run writes into an execution scope via ``KitaruClient``
- post-run memory maintenance: compact, purge, and compaction audit log

Run it directly::

    uv run examples/memory/flow_with_memory.py
"""

import argparse
import json
import time
from typing import Any, Protocol, cast, runtime_checkable

from kitaru import KitaruClient, checkpoint, flow, memory
from kitaru.memory import MemoryEntry, MemoryScopeInfo

FLOW_SCOPE = "repo_memory_demo"
"""The human-readable flow name, derived from the ``@flow`` function name below."""

# ---------------------------------------------------------------------------
# Checkpoints — the durable work units
# ---------------------------------------------------------------------------


@checkpoint
def capture_initial_state(
    topic: str,
    test_runner: Any,
    python_defaults: Any,
    topic_count: Any,
) -> dict[str, Any]:
    """Snapshot the initial namespace memory state."""
    if not isinstance(test_runner, dict):
        raise RuntimeError("Expected test_runner memory to contain a dictionary.")
    if not isinstance(python_defaults, dict):
        raise RuntimeError("Expected python_defaults memory to contain a dictionary.")
    if not isinstance(topic_count, int):
        raise RuntimeError("Expected topic_count memory to contain an integer.")
    return {
        "topic": topic,
        "test_runner_command": test_runner["command"],
        "python_runner": python_defaults["runner"],
        "topic_count": topic_count,
    }


@checkpoint
def increment_topic_count(current: Any) -> int:
    """Compute the next topic counter from the seeded namespace value."""
    if not isinstance(current, int):
        raise RuntimeError("Expected topic_count memory to contain an integer.")
    return current + 1


@checkpoint
def run_analysis(topic: str, topic_count: int) -> str:
    """Produce an analysis result using the current topic state."""
    return f"Analyzed {topic!r} (topic #{topic_count})"


@checkpoint
def build_summary(topic: str, analysis: str, topic_count: int) -> str:
    """Build a final summary from the analysis and state."""
    return f"Summary: {analysis} — {topic_count} topics tracked"


@checkpoint
def finalize(summary: str) -> str:
    """Mark the workflow complete with the final summary."""
    return f"Complete: {summary}"


# ---------------------------------------------------------------------------
# Flow — memory writes interleaved between checkpoints
# ---------------------------------------------------------------------------


@flow
def repo_memory_demo(namespace_scope: str, topic: str) -> None:
    """Demonstrate memory evolving across checkpoints in all three scopes.

    Each memory write happens between checkpoints so that the UI can show
    "what memory was available at this checkpoint" using creation timestamps.

    Timeline for UI visibility:

    - Before checkpoint 1: seeded namespace entries visible
    - Before checkpoint 3: updated namespace + new execution memory visible
    - Before checkpoint 5: updated execution + new flow memory visible
    """
    # ── Phase 1: Read seeded namespace memory ──────────────────
    memory.configure(scope=namespace_scope, scope_type="namespace")
    test_runner = memory.get("conventions/test_runner")
    python_defaults = memory.get("conventions/python")
    topic_count_before = memory.get("sessions/topic_count")

    capture_initial_state(topic, test_runner, python_defaults, topic_count_before)

    # ── Phase 2: Update namespace + create execution-scope tracking ──
    next_topic_count = increment_topic_count(topic_count_before)
    memory.set("sessions/topic_count", next_topic_count)
    memory.set("sessions/last_topic", topic)
    memory.delete("scratch/obsolete")

    memory.configure(scope_type="execution")
    memory.set("progress/phase", "analysis")
    memory.set("progress/items_processed", 0)

    analysis = run_analysis(topic, next_topic_count)

    # ── Phase 3: Update execution progress + write flow summary ──
    memory.set("progress/phase", "synthesis")
    memory.set("progress/items_processed", 3)

    memory.configure(scope_type="flow")
    summary = build_summary(topic, analysis, next_topic_count)
    memory.set("summaries/latest", summary)

    finalize(summary)


# ---------------------------------------------------------------------------
# Outside-flow seeding — detached provenance, no execution link
# ---------------------------------------------------------------------------


def seed_namespace_memory(namespace_scope: str) -> dict[str, Any]:
    """Seed detached namespace memory before the flow starts."""
    memory.configure(scope=namespace_scope, scope_type="namespace")

    memory.set(
        "conventions/test_runner",
        {"command": "just test", "notes": "Start targeted, then run the full suite."},
    )
    memory.set(
        "conventions/test_runner",
        {
            "command": "just test",
            "notes": "After code changes, rerun the tests so the save state is fresh.",
        },
    )
    memory.set(
        "conventions/python",
        {"style": "typed functions", "runner": "uv run", "version": "3.12"},
    )
    memory.set("sessions/topic_count", 2)
    memory.set("scratch/outdated", {"status": "old snapshot"})
    memory.delete("scratch/outdated")
    memory.set("scratch/obsolete", {"status": "delete inside flow"})

    listed = memory.list()
    topic_count_history = memory.history("sessions/topic_count")
    outdated_history = memory.history("scratch/outdated")

    return {
        "namespace_scope": namespace_scope,
        "active_keys": _keys(listed),
        "topic_count": cast(int, memory.get("sessions/topic_count")),
        "topic_count_history_versions": _versions(topic_count_history),
        "deleted_key_hidden": memory.get("scratch/outdated") is None,
        "deleted_history_contains_tombstone": any(
            entry.is_deleted for entry in outdated_history
        ),
    }


# ---------------------------------------------------------------------------
# Snapshot helpers — stable serialization for tests and JSON output
# ---------------------------------------------------------------------------


@runtime_checkable
class _MemoryEntryLike(Protocol):
    """Structural protocol for memory entries crossing runtime boundaries."""

    key: str
    version: int

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]: ...


def _entry_snapshot(entry: Any) -> dict[str, Any]:
    """Convert one ``MemoryEntry`` into a JSON-friendly payload."""
    if isinstance(entry, dict):
        return entry
    if isinstance(entry, _MemoryEntryLike):
        return entry.model_dump(mode="json")
    raise RuntimeError(f"Expected a memory entry, got {type(entry)!r}.")


def _entries_snapshot(entries: list[MemoryEntry]) -> list[dict[str, Any]]:
    """Convert memory entry lists into JSON-friendly payloads."""
    return [_entry_snapshot(entry) for entry in entries]


def _scope_snapshot(scope: MemoryScopeInfo) -> dict[str, Any]:
    """Convert one ``MemoryScopeInfo`` into a JSON-friendly payload."""
    return scope.model_dump(mode="json")


def _entry_key(entry: Any) -> str:
    """Read a memory-entry key from either a model or a plain dictionary."""
    if isinstance(entry, _MemoryEntryLike):
        return entry.key
    if isinstance(entry, dict):
        return str(entry["key"])
    raise RuntimeError(f"Expected a memory entry with a key, got {type(entry)!r}.")


def _entry_version(entry: Any) -> int:
    """Read a memory-entry version from either a model or a plain dictionary."""
    if isinstance(entry, _MemoryEntryLike):
        return entry.version
    if isinstance(entry, dict):
        return int(entry["version"])
    raise RuntimeError(f"Expected a memory entry with a version, got {type(entry)!r}.")


def _keys(entries: list[Any]) -> list[str]:
    """Return stable sorted memory keys from memory-entry-like objects."""
    return [_entry_key(entry) for entry in entries]


def _versions(entries: list[Any]) -> list[int]:
    """Return memory version numbers from memory-entry-like objects."""
    return [_entry_version(entry) for entry in entries]


def _optional_entry_snapshot(value: Any) -> dict[str, Any] | None:
    """Normalize an optional memory entry into a JSON-friendly payload."""
    if value is None:
        return None
    if isinstance(value, _MemoryEntryLike):
        return _entry_snapshot(value)
    if isinstance(value, dict):
        return value
    raise RuntimeError(f"Expected a MemoryEntry-compatible value, got {type(value)!r}.")


def _load_value(client: KitaruClient, entry: MemoryEntry | None) -> Any:
    """Load the artifact value for a memory entry, or return None."""
    if entry is None:
        return None
    return client.artifacts.get(entry.artifact_id).load()


# ---------------------------------------------------------------------------
# Orchestration — run the full walkthrough and collect a structured snapshot
# ---------------------------------------------------------------------------


def run_workflow(
    topic: str = "release_notes",
    namespace_scope: str = "repo_docs",
    *,
    include_maintenance: bool = True,
    compact_model: str = "default",
) -> dict[str, Any]:
    """Run the full memory walkthrough and collect a final inspection snapshot.

    Args:
        topic: The topic string passed into the flow.
        namespace_scope: Namespace scope name for seeding and flow usage.
        include_maintenance: When False, skip compact/purge/audit log.
        compact_model: Model alias or identifier for LLM compaction.

    Returns:
        Structured snapshot dict consumed by tests and the text renderer.
    """
    seed_snapshot = seed_namespace_memory(namespace_scope)

    handle = repo_memory_demo.run(namespace_scope, topic)
    while True:
        status = handle.status
        if status.is_finished:
            if not status.is_successful:
                raise RuntimeError(
                    f"repo_memory_demo execution {handle.exec_id} finished with "
                    f"status {status.value!r}."
                )
            break
        time.sleep(1)

    client = KitaruClient()
    execution_scope = handle.exec_id
    execution = client.executions.get(handle.exec_id)
    flow_scope_id = execution.flow_id
    if flow_scope_id is None:
        raise RuntimeError("Expected execution to expose a flow_id for flow memory.")

    # --- Post-flow detached execution-scope writes (annotation pattern) ---
    client.memories.set(
        "execution/notes",
        {"topic": topic, "stage": "draft"},
        scope=execution_scope,
        scope_type="execution",
    )
    client.memories.set(
        "execution/notes",
        {"topic": topic, "stage": "final"},
        scope=execution_scope,
        scope_type="execution",
    )
    client.memories.set(
        "execution/transient",
        {"topic": topic, "status": "temporary"},
        scope=execution_scope,
        scope_type="execution",
    )
    client.memories.delete(
        "execution/transient",
        scope=execution_scope,
        scope_type="execution",
    )

    # --- Client inspection of all scopes ---
    namespace_entries = client.memories.list(
        scope=namespace_scope,
        scope_type="namespace",
    )
    flow_entries = client.memories.list(scope=flow_scope_id, scope_type="flow")
    execution_entries = client.memories.list(
        scope=execution_scope,
        scope_type="execution",
    )

    namespace_topic_count = client.memories.get(
        "sessions/topic_count",
        scope=namespace_scope,
        scope_type="namespace",
    )
    namespace_topic_count_value = _load_value(client, namespace_topic_count)
    namespace_topic_count_history = client.memories.history(
        "sessions/topic_count",
        scope=namespace_scope,
        scope_type="namespace",
    )
    namespace_last_topic = client.memories.get(
        "sessions/last_topic",
        scope=namespace_scope,
        scope_type="namespace",
    )
    namespace_last_topic_value = _load_value(client, namespace_last_topic)
    namespace_obsolete = client.memories.get(
        "scratch/obsolete",
        scope=namespace_scope,
        scope_type="namespace",
    )
    namespace_obsolete_history = client.memories.history(
        "scratch/obsolete",
        scope=namespace_scope,
        scope_type="namespace",
    )

    flow_summary_entry = client.memories.get(
        "summaries/latest",
        scope=flow_scope_id,
        scope_type="flow",
    )
    flow_summary_value = _load_value(client, flow_summary_entry)
    flow_summary_history = client.memories.history(
        "summaries/latest",
        scope=flow_scope_id,
        scope_type="flow",
    )

    # In-flow execution memory (written during the flow, not post-flow)
    execution_phase = client.memories.get(
        "progress/phase",
        scope=execution_scope,
        scope_type="execution",
    )
    execution_phase_value = _load_value(client, execution_phase)
    execution_phase_history = client.memories.history(
        "progress/phase",
        scope=execution_scope,
        scope_type="execution",
    )
    execution_items = client.memories.get(
        "progress/items_processed",
        scope=execution_scope,
        scope_type="execution",
    )
    execution_items_value = _load_value(client, execution_items)

    # Post-flow execution memory
    execution_notes_history = client.memories.history(
        "execution/notes",
        scope=execution_scope,
        scope_type="execution",
    )
    execution_transient = client.memories.get(
        "execution/transient",
        scope=execution_scope,
        scope_type="execution",
    )

    scopes = client.memories.scopes()

    flow_snapshot = {
        "topic_count_before": (
            cast(int, namespace_topic_count_value) - 1
            if isinstance(namespace_topic_count_value, int)
            else None
        ),
        "topic_count_after": namespace_topic_count_value,
        "topic_count_history_versions_after_write": _versions(
            namespace_topic_count_history
        ),
        "last_topic": namespace_last_topic_value,
        "obsolete_hidden_after_delete": namespace_obsolete is None,
        "obsolete_history_versions": _versions(namespace_obsolete_history),
        "obsolete_deleted_entry": _optional_entry_snapshot(
            namespace_obsolete_history[0] if namespace_obsolete_history else None
        ),
        "flow_summary": flow_summary_value,
        "flow_keys": _keys(flow_entries),
        "flow_summary_history_versions": _versions(flow_summary_history),
    }

    execution_snapshot = {
        "in_flow_phase": execution_phase_value,
        "in_flow_phase_history_versions": _versions(execution_phase_history),
        "in_flow_items_processed": execution_items_value,
        "post_flow_notes_history_versions": _versions(execution_notes_history),
        "post_flow_transient_hidden": execution_transient is None,
    }

    # --- Memory maintenance (admin surface) ---
    maintenance_snapshot: dict[str, Any] | None = None

    if include_maintenance:
        compact_result = client.memories.compact(
            scope=namespace_scope,
            scope_type="namespace",
            keys=["conventions/test_runner", "conventions/python"],
            target_key="summaries/conventions",
            instruction="Summarize these repo conventions in 2-3 concise bullets.",
            model=compact_model,
        )

        summary_value = client.artifacts.get(compact_result.entry.artifact_id).load()

        purge_result = client.memories.purge(
            "conventions/test_runner",
            scope=namespace_scope,
            scope_type="namespace",
            keep=1,
        )

        test_runner_history_after = client.memories.history(
            "conventions/test_runner",
            scope=namespace_scope,
            scope_type="namespace",
        )

        compaction_log = client.memories.compaction_log(
            scope=namespace_scope,
            scope_type="namespace",
        )

        maintenance_snapshot = {
            "compact_result": compact_result.model_dump(mode="json"),
            "summary_value": summary_value,
            "purge_result": purge_result.model_dump(mode="json"),
            "test_runner_history_versions_after_purge": _versions(
                test_runner_history_after
            ),
            "compaction_log": [
                record.model_dump(mode="json") for record in compaction_log
            ],
        }

    return {
        "execution_id": execution_scope,
        "namespace_scope": namespace_scope,
        "flow_scope": flow_scope_id,
        "flow_name": execution.flow_name or FLOW_SCOPE,
        "seed_snapshot": seed_snapshot,
        "flow_snapshot": flow_snapshot,
        "execution_snapshot": execution_snapshot,
        "maintenance_snapshot": maintenance_snapshot,
        "client_snapshot": {
            "namespace_keys": _keys(namespace_entries),
            "flow_keys": _keys(flow_entries),
            "execution_keys": _keys(execution_entries),
            "namespace_entries": _entries_snapshot(namespace_entries),
            "flow_entries": _entries_snapshot(flow_entries),
            "execution_entries": _entries_snapshot(execution_entries),
            "namespace_topic_count_entry": _optional_entry_snapshot(
                namespace_topic_count
            ),
            "namespace_topic_count_value": namespace_topic_count_value,
            "namespace_topic_count_history_versions": _versions(
                namespace_topic_count_history
            ),
            "namespace_last_topic_entry": _optional_entry_snapshot(
                namespace_last_topic
            ),
            "namespace_last_topic_value": namespace_last_topic_value,
            "namespace_obsolete_hidden": namespace_obsolete is None,
            "namespace_obsolete_history_versions": _versions(
                namespace_obsolete_history
            ),
            "flow_summary_entry": _optional_entry_snapshot(flow_summary_entry),
            "flow_summary_value": flow_summary_value,
            "flow_summary_history_versions": _versions(flow_summary_history),
            "execution_phase_entry": _optional_entry_snapshot(execution_phase),
            "execution_phase_value": execution_phase_value,
            "execution_items_entry": _optional_entry_snapshot(execution_items),
            "execution_items_value": execution_items_value,
            "execution_notes_history_versions": _versions(execution_notes_history),
            "execution_transient_hidden": execution_transient is None,
            "scopes": [_scope_snapshot(scope) for scope in scopes],
        },
    }


# ---------------------------------------------------------------------------
# Text renderer — narrated output for demos and first-time runs
# ---------------------------------------------------------------------------


def _render_text(
    snapshot: dict[str, Any],
    *,
    include_maintenance: bool,
    model_available: bool = True,
) -> str:
    """Convert a structured snapshot into concise narrated terminal output."""
    lines: list[str] = []

    lines.append("=== Memory Walkthrough ===")
    lines.append("")
    lines.append(f"Namespace scope: {snapshot['namespace_scope']}")
    lines.append(f"Flow name:       {snapshot['flow_name']}")
    lines.append(f"Flow scope ID:   {snapshot['flow_scope']}")
    lines.append(f"Execution:       {snapshot['execution_id']}")

    # --- Seeding ---
    seed = snapshot["seed_snapshot"]
    lines.append("")
    lines.append("--- Seeding (namespace scope, detached) ---")
    n_keys = len(seed["active_keys"])
    lines.append(f"Seeded {n_keys} keys in {snapshot['namespace_scope']} namespace")
    if seed["deleted_key_hidden"]:
        lines.append(
            "Soft-deleted scratch/outdated (hidden from get, preserved in history)"
        )

    # --- Flow execution ---
    fs = snapshot["flow_snapshot"]
    es = snapshot["execution_snapshot"]
    lines.append("")
    lines.append("--- Flow execution (all three scopes evolving) ---")
    lines.append(
        f"Namespace: topic count {fs['topic_count_before']} -> "
        f"{fs['topic_count_after']}, last_topic = {fs['last_topic']!r}"
    )
    if fs["obsolete_hidden_after_delete"]:
        lines.append("Namespace: soft-deleted scratch/obsolete in flow body")
    lines.append(
        f"Execution: progress/phase evolved to {es['in_flow_phase']!r} "
        f"({len(es['in_flow_phase_history_versions'])} versions)"
    )
    lines.append(
        f"Execution: progress/items_processed = {es['in_flow_items_processed']}"
    )
    if fs.get("flow_summary"):
        lines.append(f"Flow: summaries/latest = {fs['flow_summary']!r}")

    # --- Post-flow execution-scope annotation ---
    lines.append("")
    lines.append("--- Post-flow annotation (detached execution-scope writes) ---")
    lines.append(
        f"execution/notes: {len(es['post_flow_notes_history_versions'])} versions "
        "(draft -> final)"
    )
    if es["post_flow_transient_hidden"]:
        lines.append("execution/transient: written then soft-deleted")

    # --- Maintenance ---
    lines.append("")
    lines.append("--- Maintenance ---")
    maint = snapshot["maintenance_snapshot"]
    if maint is not None:
        compact = maint["compact_result"]
        record = compact.get("compaction_record", {})
        source_keys = record.get("source_keys", [])
        target = compact["entry"]["key"]
        sources_read = compact["sources_read"]
        source_mode = record.get("source_mode", "current")
        keys_label = " + ".join(source_keys) if source_keys else f"{sources_read} keys"
        lines.append(f"Compacted {keys_label} -> {target}")
        lines.append(f"  Source mode: {source_mode} | Sources read: {sources_read}")

        purge = maint["purge_result"]
        lines.append(
            f"Purged {purge['scope']}: "
            f"{purge['versions_deleted']} version(s) deleted (kept newest)"
        )

        log_records = maint["compaction_log"]
        ops = [r["operation"] for r in log_records]
        lines.append(f"Audit log: {len(log_records)} records ({', '.join(ops)})")
    elif not model_available:
        lines.append("Skipped (no model configured).")
        lines.append("To see compaction, purge, and audit log:")
        lines.append("  kitaru model register default --model openai/gpt-5-nano")
        lines.append("  uv run examples/memory/flow_with_memory.py")
    elif not include_maintenance:
        lines.append("Skipped (--skip-maintenance flag).")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entrypoint — auto-detects model availability
# ---------------------------------------------------------------------------


def _probe_model(model: str) -> bool:
    """Return True if the given model alias/identifier can be resolved."""
    try:
        from kitaru.config import resolve_model_selection

        resolve_model_selection(model)
    except Exception:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Memory Walkthrough — Kitaru's durable memory demo.",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--skip-maintenance",
        action="store_true",
        help="Skip compact/purge/audit even when a model is available.",
    )
    parser.add_argument(
        "--namespace-scope",
        default="repo_docs",
        help="Namespace scope name (default: repo_docs).",
    )
    parser.add_argument(
        "--topic",
        default="release_notes",
        help="Topic string for the flow (default: release_notes).",
    )
    parser.add_argument(
        "--model",
        default="default",
        help="Model alias or identifier for compaction (default: default).",
    )
    args = parser.parse_args()

    model_available = _probe_model(args.model)
    include_maintenance = model_available and not args.skip_maintenance

    snapshot = run_workflow(
        topic=args.topic,
        namespace_scope=args.namespace_scope,
        include_maintenance=include_maintenance,
        compact_model=args.model,
    )

    if args.output == "json":
        print(json.dumps(snapshot, indent=2, sort_keys=True, default=str))
    else:
        print(
            _render_text(
                snapshot,
                include_maintenance=include_maintenance,
                model_available=model_available,
            )
        )


if __name__ == "__main__":
    main()
