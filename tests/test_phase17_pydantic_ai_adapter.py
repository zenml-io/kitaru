"""Integration tests for the PydanticAI adapter."""

from __future__ import annotations

import time
from typing import Any
from uuid import uuid4

import pytest
from zenml.client import Client

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import flow
from kitaru.adapters.pydantic_ai import KitaruAgent


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


def _step_metadata_maps(hydrated_run: Any) -> list[dict[str, Any]]:
    return [step.run_metadata for step in hydrated_run.steps.values()]


def _artifact_names(hydrated_run: Any) -> list[str]:
    names: list[str] = []
    for step in hydrated_run.steps.values():
        for artifacts in step.outputs.values():
            names.extend(artifact.name for artifact in artifacts)
    return names


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
    for metadata in _step_metadata_maps(hydrated_run):
        if key in metadata:
            return metadata[key]
    raise AssertionError(f"No step metadata contained key {key!r}.")


def _event_kinds(event_map: dict[str, list[dict[str, Any]]]) -> set[str]:
    return {event["kind"] for events in event_map.values() for event in events}


def _as_text_output(value: Any) -> str:
    return value.output if hasattr(value, "output") else value


def test_phase17_turn_mode_tracks_events_and_artifacts(primed_zenml) -> None:
    """Turn mode should persist tracker metadata and checkpoint artifacts."""
    durable_agent = _make_wrapped_agent(name_prefix="turn_agent")

    @flow
    def turn_flow(prompt: str) -> str:
        return durable_agent.run_sync(prompt).output

    handle = turn_flow.run("use the add tool")
    result = handle.wait()

    assert isinstance(_as_text_output(result), str)

    hydrated_run = (
        Client()
        .get_pipeline_run(
            handle.exec_id,
            allow_name_prefix_match=False,
        )
        .get_hydrated_version()
    )

    summary_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_run_summaries")
    event_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_events")
    summary = next(iter(summary_map.values()))

    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)

    artifact_names = _artifact_names(hydrated_run)
    assert any(name.endswith("_event_log") for name in artifact_names)
    assert any(name.endswith("_run_summary") for name in artifact_names)


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
    assert len(hydrated_run.steps) >= 2
    assert not any(
        name.endswith("_run_summary") for name in _artifact_names(hydrated_run)
    )


_AUTO_FLOW_AGENT: KitaruAgent[Any, str] | None = None


def _invoke_shared_auto_flow_agent() -> str:
    assert _AUTO_FLOW_AGENT is not None
    return _AUTO_FLOW_AGENT.run_sync("use the add tool").output


def test_phase17_auto_flow_runs_end_to_end(primed_zenml) -> None:
    """`run_sync()` outside any flow should auto-open a flow and complete.

    Local stacks reuse the in-process auto-flow registry, so closures over
    unpicklable state (models with live HTTP clients, toolsets, etc.) still
    execute successfully — cloudpickle is best-effort and only needed for
    remote stacks.
    """
    global _AUTO_FLOW_AGENT
    assert _AUTO_FLOW_AGENT is None, (
        "Module-global leaked from an earlier test — indicates a teardown bug."
    )
    _AUTO_FLOW_AGENT = _make_wrapped_agent(name_prefix="auto_flow_agent")
    try:
        result = _invoke_shared_auto_flow_agent()
    finally:
        _AUTO_FLOW_AGENT = None
    assert isinstance(result, str)


def test_phase17_persist_message_history_extends_across_runs(primed_zenml) -> None:
    """Two successive `run_sync` calls on the same instance accumulate history.

    Exercises the cache-key fix: `_auto_checkpoint_sync` now derives the step
    cache key from the prompt + history, so different prompts produce distinct
    checkpoints (and `_remember_messages` runs on each).
    """
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
    # Two turns with a one-message TestModel response plus echoed user prompts
    # always exceed a single-turn transcript length.
    assert len(durable_agent._last_messages) >= 4
