"""Integration test for the memory example workflow."""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import patch

from examples.memory.flow_with_memory import FLOW_SCOPE, run_workflow

from kitaru import KitaruClient
from kitaru.config import ResolvedModelSelection
from kitaru.llm import _LLMUsage, _ProviderCallResult


def _scope_map(scopes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index scope snapshots by scope name for clearer assertions."""
    return {scope["scope"]: scope for scope in scopes}


def _stub_resolve_model_selection(model: str | None) -> ResolvedModelSelection:
    """Return a deterministic model selection for compaction tests."""
    return ResolvedModelSelection(
        requested_model=model,
        alias=None,
        resolved_model="test-model",
        secret=None,
    )


def _stub_dispatch_provider_call(**_kwargs: Any) -> _ProviderCallResult:
    """Return a deterministic LLM response for compaction tests."""
    return _ProviderCallResult(
        response_text="Stubbed conventions summary",
        usage=_LLMUsage(
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
        ),
    )


def test_phase20_memory_example_runs_end_to_end(primed_zenml) -> None:
    """Verify namespace, flow, and execution memory behavior end to end."""
    namespace_scope = "repo_memory_test"

    with (
        patch(
            "kitaru.llm.resolve_model_selection",
            side_effect=_stub_resolve_model_selection,
        ),
        patch(
            "kitaru.llm._dispatch_provider_call",
            side_effect=_stub_dispatch_provider_call,
        ),
    ):
        snapshot = run_workflow(topic="memory-browser", namespace_scope=namespace_scope)

    execution_id = cast(str, snapshot["execution_id"])
    flow_snapshot = cast(dict[str, Any], snapshot["flow_snapshot"])
    execution_snapshot = cast(dict[str, Any], snapshot["execution_snapshot"])
    client_snapshot = cast(dict[str, Any], snapshot["client_snapshot"])
    seed_snapshot = cast(dict[str, Any], snapshot["seed_snapshot"])
    maintenance = cast(dict[str, Any], snapshot["maintenance_snapshot"])

    assert execution_id
    assert snapshot["namespace_scope"] == namespace_scope
    assert snapshot["flow_scope"] == FLOW_SCOPE

    # --- Seed phase assertions ---
    assert "conventions/test_runner" in seed_snapshot["active_keys"]
    assert seed_snapshot["topic_count"] == 2
    assert seed_snapshot["deleted_key_hidden"] is True
    assert seed_snapshot["deleted_history_contains_tombstone"] is True

    # --- Flow snapshot assertions (namespace mutations) ---
    assert flow_snapshot["topic_count_before"] == 2
    assert flow_snapshot["topic_count_after"] == 3
    assert flow_snapshot["topic_count_history_versions_after_write"] == [2, 1]
    assert flow_snapshot["last_topic"] == "memory-browser"
    assert flow_snapshot["obsolete_hidden_after_delete"] is True
    assert flow_snapshot["obsolete_history_versions"] == [2, 1]
    assert (
        cast(dict[str, Any], flow_snapshot["obsolete_deleted_entry"])["is_deleted"]
        is True
    )
    assert flow_snapshot["flow_summary"] is not None
    assert "memory-browser" in cast(str, flow_snapshot["flow_summary"])
    assert flow_snapshot["flow_summary_history_versions"] == [1]

    # --- Execution snapshot assertions (in-flow + post-flow) ---
    assert execution_snapshot["in_flow_phase"] == "synthesis"
    assert execution_snapshot["in_flow_phase_history_versions"] == [2, 1]
    assert execution_snapshot["in_flow_items_processed"] == 3
    assert execution_snapshot["post_flow_notes_history_versions"] == [2, 1]
    assert execution_snapshot["post_flow_transient_hidden"] is True

    # --- Client inspection assertions ---
    client = KitaruClient()

    # Namespace scope
    namespace_entries = client.memories.list(
        scope=namespace_scope,
        scope_type="namespace",
    )
    namespace_keys = [entry.key for entry in namespace_entries]
    assert "conventions/test_runner" in namespace_keys
    assert "conventions/python" in namespace_keys
    assert "sessions/topic_count" in namespace_keys
    assert "sessions/last_topic" in namespace_keys
    assert (
        client.memories.get(
            "scratch/obsolete",
            scope=namespace_scope,
            scope_type="namespace",
        )
        is None
    )

    topic_history = client.memories.history(
        "sessions/topic_count",
        scope=namespace_scope,
        scope_type="namespace",
    )
    assert [entry.version for entry in topic_history] == [2, 1]
    assert topic_history[0].execution_id == execution_id
    assert topic_history[1].execution_id is None

    # Flow scope
    flow_entry = client.memories.get(
        "summaries/latest",
        scope=FLOW_SCOPE,
        scope_type="flow",
    )
    assert flow_entry is not None
    flow_summary = client.artifacts.get(flow_entry.artifact_id).load()
    assert flow_summary == flow_snapshot["flow_summary"]
    assert [
        entry.version
        for entry in client.memories.history(
            "summaries/latest",
            scope=FLOW_SCOPE,
            scope_type="flow",
        )
    ] == [1]

    # Execution scope — in-flow entries
    execution_entries = client.memories.list(
        scope=execution_id,
        scope_type="execution",
    )
    execution_keys = [entry.key for entry in execution_entries]
    assert "progress/phase" in execution_keys
    assert "progress/items_processed" in execution_keys
    assert "execution/notes" in execution_keys

    phase_entry = client.memories.get(
        "progress/phase",
        scope=execution_id,
        scope_type="execution",
    )
    assert phase_entry is not None
    assert phase_entry.scope == execution_id
    assert phase_entry.scope_type == "execution"
    phase_value = client.artifacts.get(phase_entry.artifact_id).load()
    assert phase_value == "synthesis"

    phase_history = client.memories.history(
        "progress/phase",
        scope=execution_id,
        scope_type="execution",
    )
    assert [entry.version for entry in phase_history] == [2, 1]
    first_phase = client.artifacts.get(phase_history[1].artifact_id).load()
    assert first_phase == "analysis"

    items_entry = client.memories.get(
        "progress/items_processed",
        scope=execution_id,
        scope_type="execution",
    )
    assert items_entry is not None
    items_value = client.artifacts.get(items_entry.artifact_id).load()
    assert items_value == 3

    # Execution scope — post-flow detached entries
    notes_entry = next(e for e in execution_entries if e.key == "execution/notes")
    assert notes_entry.scope == execution_id
    assert notes_entry.scope_type == "execution"
    assert notes_entry.execution_id is None
    assert notes_entry.flow_id
    assert notes_entry.flow_name == FLOW_SCOPE

    execution_history = client.memories.history(
        "execution/notes",
        scope=execution_id,
        scope_type="execution",
    )
    assert [entry.version for entry in execution_history] == [2, 1]
    assert all(entry.scope == execution_id for entry in execution_history)
    assert all(entry.scope_type == "execution" for entry in execution_history)
    assert all(entry.execution_id is None for entry in execution_history)
    assert all(entry.flow_id for entry in execution_history)
    assert {entry.flow_name for entry in execution_history} == {FLOW_SCOPE}
    assert (
        client.memories.get(
            "execution/transient",
            scope=execution_id,
            scope_type="execution",
        )
        is None
    )

    # Scopes
    scopes = _scope_map(cast(list[dict[str, Any]], client_snapshot["scopes"]))
    assert scopes[namespace_scope]["scope_type"] == "namespace"
    assert scopes[FLOW_SCOPE]["scope_type"] == "flow"
    assert scopes[execution_id]["scope_type"] == "execution"

    # --- Maintenance phase assertions ---
    compact_result = maintenance["compact_result"]
    assert compact_result["entry"]["key"] == "summaries/conventions"
    assert compact_result["entry"]["scope"] == namespace_scope
    assert compact_result["sources_read"] == 2
    assert compact_result["scope"] == namespace_scope
    assert compact_result["compaction_record"]["source_mode"] == "current"

    assert maintenance["summary_value"] == "Stubbed conventions summary"

    purge_result = maintenance["purge_result"]
    assert purge_result["versions_deleted"] == 1
    assert purge_result["keys_affected"] == 1
    assert purge_result["scope"] == namespace_scope

    assert maintenance["test_runner_history_versions_after_purge"] == [2]

    log_records = maintenance["compaction_log"]
    assert len(log_records) == 2
    assert log_records[0]["operation"] == "purge"
    assert log_records[0]["keep"] == 1
    assert log_records[0]["versions_deleted"] == 1
    assert log_records[0]["source_mode"] is None
    assert log_records[1]["operation"] == "compact"
    assert log_records[1]["target_key"] == "summaries/conventions"
    assert log_records[1]["target_version"] is not None
    assert log_records[1]["source_mode"] == "current"


def test_phase20_memory_without_maintenance(primed_zenml) -> None:
    """Running without maintenance skips LLM and returns None maintenance snapshot."""
    snapshot = run_workflow(
        topic="runtime-test",
        namespace_scope="repo_memory_runtime",
        include_maintenance=False,
    )

    assert snapshot["maintenance_snapshot"] is None
    assert snapshot["flow_scope"] == FLOW_SCOPE

    # Core runtime claims still hold.
    seed = snapshot["seed_snapshot"]
    assert seed["topic_count"] == 2
    assert seed["deleted_key_hidden"] is True

    flow_snap = snapshot["flow_snapshot"]
    assert flow_snap["topic_count_before"] == 2
    assert flow_snap["topic_count_after"] == 3
    assert flow_snap["obsolete_hidden_after_delete"] is True

    # Execution-scope memory was written during the flow.
    exec_snap = snapshot["execution_snapshot"]
    assert exec_snap["in_flow_phase"] == "synthesis"
    assert exec_snap["in_flow_items_processed"] == 3
