"""Integration test for the memory example workflow."""

from __future__ import annotations

from typing import Any, cast

from examples.memory.flow_with_memory import run_workflow

from kitaru import KitaruClient


def _scope_map(scopes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index scope snapshots by scope name for clearer assertions."""
    return {scope["scope"]: scope for scope in scopes}


def test_phase20_memory_example_runs_end_to_end(primed_zenml) -> None:
    """Verify namespace, flow, and execution memory behavior end to end."""
    namespace_scope = "repo_memory_demo"
    snapshot = run_workflow(topic="memory-browser", namespace_scope=namespace_scope)

    execution_id = cast(str, snapshot["execution_id"])
    flow_snapshot = cast(dict[str, Any], snapshot["flow_snapshot"])
    client_snapshot = cast(dict[str, Any], snapshot["client_snapshot"])
    seed_snapshot = cast(dict[str, Any], snapshot["seed_snapshot"])

    assert execution_id
    assert snapshot["namespace_scope"] == namespace_scope
    assert snapshot["flow_scope"] == "memory_showcase"

    assert "conventions/test_runner" in seed_snapshot["active_keys"]
    assert seed_snapshot["topic_count"] == 2
    assert seed_snapshot["deleted_key_hidden"] is True
    assert seed_snapshot["deleted_history_contains_tombstone"] is True
    assert flow_snapshot["topic_count_before"] == 2
    assert flow_snapshot["topic_count_after"] == 3
    assert flow_snapshot["topic_count_history_versions_after_write"] == [2, 1]
    assert flow_snapshot["obsolete_hidden_after_delete"] is True
    assert flow_snapshot["obsolete_history_versions"] == [2, 1]
    assert (
        cast(dict[str, Any], flow_snapshot["obsolete_deleted_entry"])["is_deleted"]
        is True
    )

    client = KitaruClient()

    namespace_entries = client.memories.list(scope=namespace_scope)
    namespace_keys = [entry.key for entry in namespace_entries]
    assert "conventions/test_runner" in namespace_keys
    assert "conventions/python" in namespace_keys
    assert "sessions/topic_count" in namespace_keys
    assert "sessions/last_topic" in namespace_keys
    assert client.memories.get("scratch/obsolete", scope=namespace_scope) is None

    topic_history = client.memories.history(
        "sessions/topic_count", scope=namespace_scope
    )
    assert [entry.version for entry in topic_history] == [2, 1]
    assert topic_history[0].execution_id == execution_id
    assert topic_history[1].execution_id is None

    flow_entry = client.memories.get("summaries/latest", scope="memory_showcase")
    assert flow_entry is not None
    flow_summary = client.artifacts.get(flow_entry.artifact_id).load()
    assert flow_summary == flow_snapshot["planned_summary"]
    assert [
        entry.version
        for entry in client.memories.history(
            "summaries/latest", scope="memory_showcase"
        )
    ] == [1]

    execution_entries = client.memories.list(scope=execution_id)
    assert [entry.key for entry in execution_entries] == ["execution/notes"]
    execution_history = client.memories.history("execution/notes", scope=execution_id)
    assert [entry.version for entry in execution_history] == [2, 1]
    assert client.memories.get("execution/transient", scope=execution_id) is None

    scopes = _scope_map(cast(list[dict[str, Any]], client_snapshot["scopes"]))
    assert scopes[namespace_scope]["scope_type"] == "namespace"
    assert scopes["memory_showcase"]["scope_type"] == "flow"
    assert scopes[execution_id]["scope_type"] == "execution"
