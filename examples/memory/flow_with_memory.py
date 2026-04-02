"""Showcase Kitaru memory across outside-flow, in-flow, and client surfaces.

This example demonstrates:
- seeding namespace memory outside a flow
- reading, listing, updating, deleting, and inspecting history inside a flow
- switching from namespace scope to flow scope with ``memory.configure(...)``
- inspecting namespace, flow, and execution scopes via ``KitaruClient.memories``
"""

import json
import time
from typing import Any, Protocol, cast, runtime_checkable

from kitaru import KitaruClient, checkpoint, flow, memory
from kitaru.memory import MemoryEntry, MemoryScopeInfo


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


@flow
def memory_showcase(namespace_scope: str, topic: str) -> None:
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


def run_workflow(
    topic: str = "memory",
    namespace_scope: str = "repo_memory_demo",
) -> dict[str, Any]:
    """Run the full memory showcase and collect a final inspection snapshot."""
    seed_snapshot = seed_namespace_memory(namespace_scope)

    handle = memory_showcase.run(namespace_scope, topic)
    while True:
        status = handle.status
        if status.is_finished:
            if not status.is_successful:
                raise RuntimeError(
                    f"memory_showcase execution {handle.exec_id} finished with "
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
    flow_entries = client.memories.list(scope="memory_showcase")
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
    flow_summary_entry = client.memories.get(
        "summaries/latest", scope="memory_showcase"
    )
    flow_summary_value = (
        client.artifacts.get(flow_summary_entry.artifact_id).load()
        if flow_summary_entry is not None
        else None
    )
    flow_summary_history = client.memories.history(
        "summaries/latest",
        scope="memory_showcase",
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
        "flow_scope": "memory_showcase",
        "flow_keys": _keys(flow_entries),
        "flow_summary_history_versions": _versions(flow_summary_history),
    }

    return {
        "execution_id": execution_scope,
        "namespace_scope": namespace_scope,
        "flow_scope": "memory_showcase",
        "execution_scope": execution_scope,
        "seed_snapshot": seed_snapshot,
        "flow_snapshot": flow_snapshot,
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
            "execution_transient_hidden": client.memories.get(
                "execution/transient",
                scope=execution_scope,
            )
            is None,
            "scopes": [_scope_snapshot(scope) for scope in scopes],
        },
    }


def main() -> None:
    """Run the example as a script."""
    print(json.dumps(run_workflow(), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
