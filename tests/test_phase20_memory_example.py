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
    assert flow_snapshot["topic_count_before"] == 2
    assert flow_snapshot["topic_count_after"] == 3
    assert flow_snapshot["topic_count_history_versions_after_write"] == [2, 1]
    assert flow_snapshot["obsolete_hidden_after_delete"] is True
    assert flow_snapshot["obsolete_history_versions"] == [2, 1]
    assert (
        cast(dict[str, Any], flow_snapshot["obsolete_deleted_entry"])["is_deleted"]
        is True
    )

    # --- Client inspection assertions ---
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

    flow_entry = client.memories.get("summaries/latest", scope=FLOW_SCOPE)
    assert flow_entry is not None
    flow_summary = client.artifacts.get(flow_entry.artifact_id).load()
    assert flow_summary == flow_snapshot["planned_summary"]
    assert [
        entry.version
        for entry in client.memories.history("summaries/latest", scope=FLOW_SCOPE)
    ] == [1]

    execution_entries = client.memories.list(scope=execution_id)
    assert [entry.key for entry in execution_entries] == ["execution/notes"]
    execution_history = client.memories.history("execution/notes", scope=execution_id)
    assert [entry.version for entry in execution_history] == [2, 1]
    assert client.memories.get("execution/transient", scope=execution_id) is None

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

    # After purge with keep=1, only the newest version remains.
    assert maintenance["test_runner_history_versions_after_purge"] == [2]

    # Compaction log should have 2 records: purge (newest) then compact.
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
