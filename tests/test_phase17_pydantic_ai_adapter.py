"""Integration tests for the PydanticAI adapter."""

import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import pytest
from zenml.client import Client

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import checkpoint, flow
from kitaru.adapters.pydantic_ai import KitaruAgent, _tracking


@dataclass(frozen=True)
class _FakeCheckpointScope:
    execution_id: str | None
    checkpoint_id: str | None
    name: str


def _make_wrapped_agent(
    *, name_prefix: str, granular: bool = False
) -> KitaruAgent[Any, str]:
    agent = Agent(
        TestModel(call_tools=["add"]),
        name=f"{name_prefix}_{uuid4().hex[:8]}",
        output_type=str,
    )

    @agent.tool_plain
    def add(a: int = 0, b: int = 0) -> int:
        return a + b

    return KitaruAgent(agent, granular_checkpoints=granular)


def _artifact_names(hydrated_run: Any) -> list[str]:
    names: list[str] = []
    for step in hydrated_run.steps.values():
        for artifacts in step.outputs.values():
            names.extend(artifact.name for artifact in artifacts)
    return names


def _input_names_by_step(hydrated_run: Any) -> list[set[str]]:
    return [set(step.inputs) for step in hydrated_run.steps.values()]


def _has_step_input(hydrated_run: Any, input_name: str) -> bool:
    return any(input_name in inputs for inputs in _input_names_by_step(hydrated_run))


def _wait_for_hydrated_run(exec_id: str) -> Any:
    client = Client()
    deadline = time.time() + 30
    while True:
        run = client.get_pipeline_run(exec_id, allow_name_prefix_match=False)
        if run.status.is_finished:
            assert run.status.is_successful
            return run.get_hydrated_version()
        if time.time() >= deadline:
            raise AssertionError(f"Pipeline run {exec_id} did not finish in time.")
        time.sleep(0.2)


def _metadata_dict_from_steps(hydrated_run: Any, key: str) -> dict[str, Any]:
    for step in hydrated_run.steps.values():
        if key in step.run_metadata:
            return step.run_metadata[key]
    raise AssertionError(f"No step metadata contained key {key!r}.")


def _events(event_map: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [event for events in event_map.values() for event in events]


def _event_kinds(event_map: dict[str, list[dict[str, Any]]]) -> set[str]:
    return {event["kind"] for event in _events(event_map)}


def _assert_event_artifacts_use_display_names(
    event_map: dict[str, list[dict[str, Any]]], artifact_names: list[str]
) -> None:
    for event in _events(event_map):
        event_id = event["event_id"]
        for artifact_name in event.get("artifacts", {}).values():
            assert artifact_name in artifact_names
            assert not artifact_name.startswith(f"{event_id}_")


def test_phase17_event_artifact_names_use_short_display_shape() -> None:
    """Display artifact names should omit internal agent/run event prefixes."""
    event_id = "agent_name_ab12cd34_llm_call_1"

    assert _tracking.artifact_name(event_id, "prompt") == "llm_call_1_prompt"
    assert _tracking.artifact_name(event_id, "response") == "llm_call_1_response"
    assert (
        _tracking.artifact_name(event_id, "stream_transcript")
        == "llm_call_1_stream_transcript"
    )
    assert (
        _tracking._namespaced_artifact_name(
            event_id,
            "stream_transcript",
            namespace="agent_2",
        )
        == "agent_2_llm_call_1_stream_transcript"
    )


def test_phase17_turn_mode_tracks_events_and_artifacts(primed_zenml) -> None:
    """Turn mode should persist tracker metadata and checkpoint artifacts."""
    durable_agent = _make_wrapped_agent(name_prefix="turn_agent")

    @flow
    def turn_flow(prompt: str) -> str:
        return durable_agent.run_sync(prompt).output

    handle = turn_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    summary_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_run_summaries")
    event_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_events")
    summary = next(iter(summary_map.values()))

    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)

    assert _has_step_input(hydrated_run, "user_prompt")

    artifact_names = _artifact_names(hydrated_run)
    assert "event_log" in artifact_names
    assert "run_summary" in artifact_names
    assert "llm_call_1_prompt" in artifact_names
    assert "llm_call_1_response" in artifact_names
    assert any(re.fullmatch(r"tool_call_\d+_args", name) for name in artifact_names)
    assert any(re.fullmatch(r"tool_call_\d+_result", name) for name in artifact_names)
    _assert_event_artifacts_use_display_names(event_map, artifact_names)
    assert not any(summary["run_label"] in name for name in artifact_names)


def test_phase17_turn_mode_tracks_effective_history_input_for_continuations(
    primed_zenml,
) -> None:
    """Continuation turns should expose message_history without a fake prompt input."""
    durable_agent = _make_wrapped_agent(name_prefix="history_agent")

    @flow
    def continuation_flow() -> str:
        first = durable_agent.run_sync("first turn")
        second = durable_agent.run_sync(
            user_prompt=None, message_history=first.all_messages()
        )
        return second.output

    handle = continuation_flow.run()
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    inputs_by_step = _input_names_by_step(hydrated_run)
    assert any("message_history" in inputs for inputs in inputs_by_step)
    assert any(
        "message_history" in inputs and "user_prompt" not in inputs
        for inputs in inputs_by_step
    )
    artifact_names = _artifact_names(hydrated_run)
    assert "llm_call_1_prompt" in artifact_names
    assert "llm_call_1_response" in artifact_names


def test_phase17_turn_mode_omits_absent_prompt_and_history_inputs(
    primed_zenml,
) -> None:
    """Instructions-only turns should not create prompt/history placeholders."""
    agent = Agent(
        TestModel(),
        name=f"empty_input_agent_{uuid4().hex[:8]}",
        output_type=str,
        instructions="Reply successfully.",
    )
    durable_agent = KitaruAgent(agent)

    @flow
    def instructions_only_flow() -> str:
        return durable_agent.run_sync(user_prompt=None).output

    handle = instructions_only_flow.run()
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    assert not _has_step_input(hydrated_run, "user_prompt")
    assert not _has_step_input(hydrated_run, "message_history")
    artifact_names = _artifact_names(hydrated_run)
    assert "llm_call_1_prompt" in artifact_names
    assert "llm_call_1_response" in artifact_names


def test_phase17_granular_mode_tracks_at_flow_scope(primed_zenml) -> None:
    """Granular mode should flush run metadata at flow scope."""
    durable_agent = _make_wrapped_agent(name_prefix="granular_agent", granular=True)

    @flow
    def granular_flow(prompt: str) -> str:
        return durable_agent.run_sync(prompt).output

    handle = granular_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    assert "pydantic_ai_run_summaries" in hydrated_run.run_metadata
    assert "pydantic_ai_events" in hydrated_run.run_metadata

    summary_map = hydrated_run.run_metadata["pydantic_ai_run_summaries"]
    event_map = hydrated_run.run_metadata["pydantic_ai_events"]
    summary = next(iter(summary_map.values()))

    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)
    artifact_names = _artifact_names(hydrated_run)
    assert len(hydrated_run.steps) >= 2
    assert "llm_call_1_prompt" in artifact_names
    assert "llm_call_1_response" in artifact_names
    _assert_event_artifacts_use_display_names(event_map, artifact_names)
    assert not any(name.endswith("_run_summary") for name in artifact_names)
    assert not any(summary["run_label"] in name for name in artifact_names)


_AUTO_FLOW_AGENT: KitaruAgent[Any, str] | None = None


def _invoke_shared_auto_flow_agent() -> str:
    assert _AUTO_FLOW_AGENT is not None
    return _AUTO_FLOW_AGENT.run_sync("use the add tool").output


def test_phase17_tracker_namespace_allocation_is_checkpoint_shared(monkeypatch) -> None:
    """Concurrent trackers in one checkpoint should not all get plain names."""
    checkpoint_scope = _FakeCheckpointScope(
        execution_id="exec-1",
        checkpoint_id="checkpoint-1",
        name="shared_checkpoint",
    )
    monkeypatch.setattr(_tracking, "get_current_checkpoint", lambda: checkpoint_scope)
    monkeypatch.setattr(
        _tracking, "get_current_checkpoint_name", lambda: checkpoint_scope.name
    )
    monkeypatch.setattr(
        _tracking, "get_current_execution_id", lambda: checkpoint_scope.execution_id
    )

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            namespaces = list(
                executor.map(
                    lambda index: (
                        _tracking.EventTracker(
                            agent_name=f"agent_{index}"
                        ).artifact_namespace
                    ),
                    range(4),
                )
            )

        suffixes = sorted(
            int(namespace.rsplit("_", 1)[1])
            for namespace in namespaces
            if namespace is not None
        )
        assert namespaces.count(None) == 1
        assert suffixes == [2, 3, 4]
    finally:
        _tracking._reset_artifact_namespace_state(checkpoint_scope)


def test_phase17_multiple_tracker_scopes_in_checkpoint_get_namespaces(
    primed_zenml,
) -> None:
    """Only later trackers in the same checkpoint should get a namespace."""
    first_agent = _make_wrapped_agent(name_prefix="first_agent")
    second_agent = _make_wrapped_agent(name_prefix="second_agent")

    @checkpoint
    def run_both_agents(prompt: str) -> str:
        first = first_agent.run_sync(prompt).output
        second = second_agent.run_sync(prompt).output
        return f"{first}\n{second}"

    @flow
    def multi_agent_flow(prompt: str) -> str:
        return run_both_agents(prompt)

    handle = multi_agent_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)
    event_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_events")
    artifact_names = _artifact_names(hydrated_run)

    assert "event_log" in artifact_names
    assert "run_summary" in artifact_names
    assert "llm_call_1_prompt" in artifact_names
    assert "llm_call_1_response" in artifact_names
    assert any(
        re.fullmatch(r"[a-zA-Z0-9_]+_2_event_log", name) for name in artifact_names
    )
    assert any(
        re.fullmatch(r"[a-zA-Z0-9_]+_2_run_summary", name) for name in artifact_names
    )
    assert any(
        re.fullmatch(r"[a-zA-Z0-9_]+_2_llm_call_1_prompt", name)
        for name in artifact_names
    )
    assert any(
        re.fullmatch(r"[a-zA-Z0-9_]+_2_llm_call_1_response", name)
        for name in artifact_names
    )
    _assert_event_artifacts_use_display_names(event_map, artifact_names)
    event_artifact_names = [
        stored_name
        for event in _events(event_map)
        for stored_name in event.get("artifacts", {}).values()
    ]
    assert any(
        re.fullmatch(r"[a-zA-Z0-9_]+_2_llm_call_1_prompt", name)
        for name in event_artifact_names
    )

    event_log_names = [name for name in artifact_names if name.endswith("event_log")]
    run_summary_names = [
        name for name in artifact_names if name.endswith("run_summary")
    ]
    assert len(event_log_names) == 2
    assert len(run_summary_names) == 2
    assert len(event_log_names) == len(set(event_log_names))
    assert len(run_summary_names) == len(set(run_summary_names))


def test_phase17_auto_flow_runs_end_to_end(primed_zenml) -> None:
    """`run_sync()` outside any flow auto-opens a flow and completes."""
    global _AUTO_FLOW_AGENT
    _AUTO_FLOW_AGENT = _make_wrapped_agent(name_prefix="auto_flow_agent")
    try:
        result = _invoke_shared_auto_flow_agent()
    finally:
        _AUTO_FLOW_AGENT = None
    assert isinstance(result, str)


def test_phase17_persist_message_history_extends_across_runs(primed_zenml) -> None:
    """Two successive `run_sync` calls on the same instance accumulate history."""
    durable_agent = _make_wrapped_agent(name_prefix="chat_agent")
    durable_agent._persist_message_history = True

    @flow
    def chat_flow() -> str:
        durable_agent.run_sync("first turn")
        second = durable_agent.run_sync("second turn")
        return second.output

    handle = chat_flow.run()
    _wait_for_hydrated_run(handle.exec_id)

    assert durable_agent._last_messages is not None
    assert len(durable_agent._last_messages) >= 4
