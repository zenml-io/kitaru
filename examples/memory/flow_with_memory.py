"""Repo Memory Walkthrough — Kitaru's durable memory surface end to end.

This example demonstrates:

- seeding namespace memory outside a flow
- reading, listing, updating, deleting, and inspecting history inside a flow
- switching from namespace scope to flow scope with ``memory.configure(...)``
- inspecting namespace, flow, and execution scopes via ``KitaruClient.memories``
- detached post-run writes into an execution scope, and how that differs from
  execution provenance
- post-run memory maintenance: compact, purge, and compaction audit log

Run it directly::

    uv run examples/memory/flow_with_memory.py

If a default model is configured (``kitaru model register default --model ...``),
the walkthrough includes LLM-powered compaction, purge, and audit log inspection.
Without a model, those sections are skipped with guidance on how to enable them.
"""

import argparse
import json
import time
from typing import Any, Protocol, cast, runtime_checkable

from kitaru import KitaruClient, checkpoint, flow, memory
from kitaru.memory import MemoryEntry, MemoryScopeInfo

FLOW_SCOPE = "repo_memory_demo"
"""The flow scope name, derived from the ``@flow`` function name below."""

# ---------------------------------------------------------------------------
# Checkpoints — the durable work units
# ---------------------------------------------------------------------------


@checkpoint
def build_topic_summary(
    topic: str,
    test_runner: Any,
    python_defaults: Any,
    previous_topic_count: Any,
) -> str:
    """Build a summary using memory values that were read in the flow body."""
    if not isinstance(test_runner, dict):
        raise RuntimeError("Expected test_runner memory to contain a dictionary.")
    if not isinstance(python_defaults, dict):
        raise RuntimeError("Expected python_defaults memory to contain a dictionary.")
    if not isinstance(previous_topic_count, int):
        raise RuntimeError(
            "Expected previous_topic_count memory to contain an integer."
        )
    return (
        f"Prepared {topic!r} using {test_runner['command']} with "
        f"{python_defaults['runner']} and {previous_topic_count} previous topics."
    )


@checkpoint
def summarize_namespace_reads(
    topic: str,
    test_runner: Any,
    python_defaults: Any,
    topic_count: Any,
    namespace_entries: list[Any],
    topic_count_history: list[Any],
) -> dict[str, Any]:
    """Turn resolved namespace memory reads into a stable summary payload."""
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
        "topic_count_before": topic_count,
        "namespace_keys_before_write": _keys(namespace_entries),
        "topic_count_history_versions_before_write": _versions(topic_count_history),
    }


@checkpoint
def increment_topic_count(topic_count: Any) -> int:
    """Compute the next topic counter from the seeded namespace value."""
    if not isinstance(topic_count, int):
        raise RuntimeError("Expected topic_count memory to contain an integer.")
    return topic_count + 1


@checkpoint
def summarize_flow_reads(
    flow_entries: list[Any],
    flow_history: list[Any],
) -> dict[str, Any]:
    """Summarize flow-scoped reads that happened before writing new data."""
    return {
        "flow_keys_before_write": _keys(flow_entries),
        "flow_history_versions_before_write": _versions(flow_history),
    }


# ---------------------------------------------------------------------------
# Flow — the "read → checkpoint → write" pattern front and center
# ---------------------------------------------------------------------------


@flow
def repo_memory_demo(namespace_scope: str, topic: str) -> None:
    """Run a flow that reads and mutates memory in multiple scopes."""
    memory.configure(scope=namespace_scope, scope_type="namespace")

    test_runner = memory.get("conventions/test_runner")
    python_defaults = memory.get("conventions/python")
    topic_count_before = memory.get("sessions/topic_count")
    namespace_before = memory.list()
    topic_count_history_before = memory.history("sessions/topic_count")

    summarize_namespace_reads(
        topic,
        test_runner=test_runner,
        python_defaults=python_defaults,
        topic_count=topic_count_before,
        namespace_entries=namespace_before,
        topic_count_history=topic_count_history_before,
    )
    summary = build_topic_summary(
        topic,
        test_runner,
        python_defaults,
        topic_count_before,
    )
    next_topic_count = increment_topic_count(topic_count_before)

    memory.set("sessions/topic_count", next_topic_count)
    memory.set("sessions/last_topic", topic)
    memory.delete("scratch/obsolete")

    memory.configure(scope_type="flow")
    flow_entries_before = memory.list()
    flow_history_before = memory.history("summaries/latest")
    summarize_flow_reads(flow_entries_before, flow_history_before)
    memory.set("summaries/latest", summary)
    return None


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
        "entries": _entries_snapshot(listed),
        "topic_count": cast(int, memory.get("sessions/topic_count")),
        "topic_count_history_versions": _versions(topic_count_history),
        "deleted_key_hidden": memory.get("scratch/outdated") is None,
        "deleted_history_versions": _versions(outdated_history),
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
    # Poll instead of handle.wait() because wait() tries to extract the flow
    # result, which fails when the flow ends with multiple terminal synthetic
    # memory steps (ambiguous output).
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
    client.memories.delete("execution/transient", scope=execution_scope)

    namespace_entries = client.memories.list(scope=namespace_scope)
    flow_entries = client.memories.list(scope=FLOW_SCOPE)
    execution_entries = client.memories.list(scope=execution_scope)
    execution_history = client.memories.history(
        "execution/notes",
        scope=execution_scope,
    )
    namespace_topic_count = client.memories.get(
        "sessions/topic_count",
        scope=namespace_scope,
    )
    namespace_topic_count_history = client.memories.history(
        "sessions/topic_count",
        scope=namespace_scope,
    )
    namespace_topic_count_value = (
        client.artifacts.get(namespace_topic_count.artifact_id).load()
        if namespace_topic_count is not None
        else None
    )
    namespace_last_topic = client.memories.get(
        "sessions/last_topic",
        scope=namespace_scope,
    )
    namespace_last_topic_value = (
        client.artifacts.get(namespace_last_topic.artifact_id).load()
        if namespace_last_topic is not None
        else None
    )
    namespace_obsolete = client.memories.get(
        "scratch/obsolete",
        scope=namespace_scope,
    )
    namespace_obsolete_history = client.memories.history(
        "scratch/obsolete",
        scope=namespace_scope,
    )
    flow_summary_entry = client.memories.get("summaries/latest", scope=FLOW_SCOPE)
    flow_summary_value = (
        client.artifacts.get(flow_summary_entry.artifact_id).load()
        if flow_summary_entry is not None
        else None
    )
    flow_summary_history = client.memories.history(
        "summaries/latest",
        scope=FLOW_SCOPE,
    )
    execution_transient = client.memories.get(
        "execution/transient",
        scope=execution_scope,
    )
    scopes = client.memories.scopes()

    flow_snapshot = {
        "planned_summary": flow_summary_value,
        "topic_count_before": (
            cast(int, namespace_topic_count_value) - 1
            if isinstance(namespace_topic_count_value, int)
            else None
        ),
        "topic_count_after": namespace_topic_count_value,
        "topic_count_history_versions_after_write": _versions(
            namespace_topic_count_history
        ),
        "obsolete_hidden_after_delete": namespace_obsolete is None,
        "obsolete_history_versions": _versions(namespace_obsolete_history),
        "obsolete_deleted_entry": _optional_entry_snapshot(
            namespace_obsolete_history[0] if namespace_obsolete_history else None
        ),
        "flow_keys": _keys(flow_entries),
        "flow_summary_history_versions": _versions(flow_summary_history),
    }

    # --- Memory maintenance (admin surface) ---
    maintenance_snapshot: dict[str, Any] | None = None

    if include_maintenance:
        # Compact: summarize two convention keys into one summary key.
        # This sends the current values to an LLM and writes the summary as a
        # new memory version.  Source keys are left untouched.
        compact_result = client.memories.compact(
            scope=namespace_scope,
            keys=["conventions/test_runner", "conventions/python"],
            target_key="summaries/conventions",
            instruction="Summarize these repo conventions in 2-3 concise bullets.",
            model=compact_model,
        )

        # The compact result carries the written entry — load via artifact_id.
        summary_value = client.artifacts.get(compact_result.entry.artifact_id).load()

        # Purge: keep only the newest version of test_runner, delete the rest.
        purge_result = client.memories.purge(
            "conventions/test_runner",
            scope=namespace_scope,
            keep=1,
        )

        # Check that history was actually trimmed.
        test_runner_history_after = client.memories.history(
            "conventions/test_runner", scope=namespace_scope
        )

        # Audit log: read back the compact + purge records.
        compaction_log = client.memories.compaction_log(scope=namespace_scope)

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
        "flow_scope": FLOW_SCOPE,
        "seed_snapshot": seed_snapshot,
        "flow_snapshot": flow_snapshot,
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
            "execution_history_versions": _versions(execution_history),
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

    lines.append("=== Repo Memory Walkthrough ===")
    lines.append("")
    lines.append(f"Namespace scope: {snapshot['namespace_scope']}")
    lines.append(f"Flow scope:      {snapshot['flow_scope']}")
    lines.append(f"Execution:       {snapshot['execution_id']}")

    # --- Seeding ---
    seed = snapshot["seed_snapshot"]
    lines.append("")
    lines.append("--- Seeding ---")
    n_keys = len(seed["active_keys"])
    lines.append(f"Seeded {n_keys} keys in {snapshot['namespace_scope']} namespace")
    if seed["deleted_key_hidden"]:
        lines.append(
            "Soft-deleted scratch/outdated (hidden from get, preserved in history)"
        )

    # --- Flow execution ---
    fs = snapshot["flow_snapshot"]
    lines.append("")
    lines.append("--- Flow execution ---")
    lines.append(
        f"Topic count: {fs['topic_count_before']} -> {fs['topic_count_after']}"
    )
    if fs.get("planned_summary"):
        lines.append("Flow summary written to summaries/latest")
    if fs["obsolete_hidden_after_delete"]:
        lines.append("Soft-deleted scratch/obsolete in flow body")

    # --- Execution-scope inspection ---
    client_snapshot = snapshot["client_snapshot"]
    execution_notes_entry = next(
        (
            entry
            for entry in client_snapshot["execution_entries"]
            if entry.get("key") == "execution/notes"
        ),
        None,
    )
    if execution_notes_entry is not None:
        producer = (
            execution_notes_entry.get("execution_id")
            or execution_notes_entry.get("producer_pipeline_run_id")
            or "detached write (execution_id=None)"
        )
        flow_label = execution_notes_entry.get("flow_name") or "not indexed"
        lines.append("")
        lines.append("--- Execution-scope inspection ---")
        lines.append(
            "Post-run updated execution/notes in execution scope "
            f"{execution_notes_entry.get('scope', snapshot['execution_id'])}"
        )
        lines.append(
            "Membership: "
            f"scope={execution_notes_entry.get('scope', snapshot['execution_id'])} "
            f"({execution_notes_entry.get('scope_type', 'execution')})"
        )
        lines.append(f"Producer: {producer}")
        lines.append(f"Flow context: belongs to flow {flow_label} for discovery")

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
        description="Repo Memory Walkthrough — Kitaru's durable memory demo.",
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
