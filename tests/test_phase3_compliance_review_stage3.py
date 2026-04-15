"""Guarded tests for the compliance review Stage 3 memory-backed flow."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

from kitaru import KitaruClient
from tests.compliance_review_fakes import (
    clear_compliance_review_modules,
    fake_claude_response,
    install_fake_claude_agent_sdk,
)


@pytest.fixture
def stage3_module(monkeypatch):
    """Import Stage 3 with a fake Claude SDK module."""
    install_fake_claude_agent_sdk(monkeypatch)
    clear_compliance_review_modules(
        "examples.compliance_review.stage_3_memory",
    )
    return importlib.import_module("examples.compliance_review.stage_3_memory")


def _load_memory_value(client: KitaruClient, artifact_id: str) -> Any:
    """Load the stored value for a memory metadata entry."""
    return client.artifacts.get(artifact_id).load()


def test_stage3_flow_preserves_memory_across_repeated_runs(
    monkeypatch,
    primed_zenml,
    stage3_module,
) -> None:
    """The real Stage 3 flow should read/write flow memory across runs."""
    del primed_zenml
    calls: list[dict[str, Any]] = []

    async def fake_run_agent_turn(
        prompt: str,
        *,
        allowed_tools: list[str],
        cwd: Path,
    ) -> dict[str, Any]:
        call_index = len(calls)
        calls.append({"prompt": prompt, "allowed_tools": allowed_tools, "cwd": cwd})

        if stage3_module.IT_BASE_PROMPT in prompt:
            run_number = 2 if "Previous audit memory" in prompt else 1
            result = f"IT run {run_number}: SOC 2 data retention gap remains open."
        elif stage3_module.HR_BASE_PROMPT in prompt:
            run_number = 2 if "Previous audit memory" in prompt else 1
            result = f"HR run {run_number}: parental leave policy gap remains open."
        else:
            result = f"Change report run {2 if call_index >= 5 else 1}."

        return fake_claude_response(
            prompt=prompt,
            cwd=cwd,
            session_id=f"stage-3-test-session-{call_index}",
            result=result,
        )

    monkeypatch.setattr(stage3_module, "run_agent_turn", fake_run_agent_turn)

    first_handle = stage3_module.audit_with_memory.run()
    first_result = first_handle.wait()
    second_handle = stage3_module.audit_with_memory.run()
    second_result = second_handle.wait()

    assert isinstance(first_result, stage3_module.ClaudeAgentResult)
    assert isinstance(second_result, stage3_module.ClaudeAgentResult)
    assert first_result.result == "Change report run 1."
    assert second_result.result == "Change report run 2."

    assert len(calls) == 6
    first_it_prompt = calls[0]["prompt"]
    first_hr_prompt = calls[1]["prompt"]
    second_it_prompt = calls[3]["prompt"]
    second_hr_prompt = calls[4]["prompt"]

    assert "Memory has no previous finding for this domain" in first_it_prompt
    assert "Memory has no previous finding for this domain" in first_hr_prompt
    assert "Previous audit memory for this domain" in second_it_prompt
    assert "IT run 1: SOC 2 data retention gap remains open." in second_it_prompt
    assert "Previous audit memory for this domain" in second_hr_prompt
    assert "HR run 1: parental leave policy gap remains open." in second_hr_prompt
    assert {tuple(call["allowed_tools"]) for call in calls} == {
        tuple(stage3_module.DEFAULT_ALLOWED_TOOLS)
    }
    assert {call["cwd"] for call in calls} == {stage3_module.EXAMPLE_DIR}

    client = KitaruClient()
    first_execution = client.executions.get(first_handle.exec_id)
    second_execution = client.executions.get(second_handle.exec_id)
    flow_scope = first_execution.flow_id
    assert flow_scope is not None
    assert second_execution.flow_id == flow_scope

    entries = client.memories.list(
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    keys = {entry.key for entry in entries}
    assert {
        stage3_module.IT_FINDING_KEY,
        stage3_module.HR_FINDING_KEY,
        stage3_module.LAST_RUN_KEY,
    } <= keys

    scopes = client.memories.scopes()
    assert any(
        scope.scope == flow_scope
        and scope.scope_type == stage3_module.MEMORY_SCOPE_TYPE
        and scope.entry_count >= 3
        for scope in scopes
    )

    it_entry = client.memories.get(
        stage3_module.IT_FINDING_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    hr_entry = client.memories.get(
        stage3_module.HR_FINDING_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    last_run_entry = client.memories.get(
        stage3_module.LAST_RUN_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    assert it_entry is not None
    assert hr_entry is not None
    assert last_run_entry is not None
    assert _load_memory_value(client, it_entry.artifact_id) == (
        "IT run 2: SOC 2 data retention gap remains open."
    )
    assert _load_memory_value(client, hr_entry.artifact_id) == (
        "HR run 2: parental leave policy gap remains open."
    )
    assert _load_memory_value(client, last_run_entry.artifact_id) == {
        "domains": ["it_security", "hr"],
        "artifact": stage3_module.CHANGE_REPORT_ARTIFACT_NAME,
    }

    it_history = client.memories.history(
        stage3_module.IT_FINDING_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    hr_history = client.memories.history(
        stage3_module.HR_FINDING_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    last_run_history = client.memories.history(
        stage3_module.LAST_RUN_KEY,
        scope=flow_scope,
        scope_type=stage3_module.MEMORY_SCOPE_TYPE,
    )
    assert [entry.version for entry in it_history] == [2, 1]
    assert [entry.version for entry in hr_history] == [2, 1]
    assert [entry.version for entry in last_run_history] == [2, 1]
    assert _load_memory_value(client, it_history[1].artifact_id) == (
        "IT run 1: SOC 2 data retention gap remains open."
    )
    assert _load_memory_value(client, hr_history[1].artifact_id) == (
        "HR run 1: parental leave policy gap remains open."
    )
