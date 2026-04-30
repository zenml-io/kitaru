"""Integration tests for the PydanticAI adapter."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager, nullcontext
from typing import Any
from uuid import uuid4

import pytest
from zenml.client import Client

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.agent.abstract import AbstractAgent
from pydantic_ai.capabilities.hooks import Hooks
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
    for step in hydrated_run.steps.values():
        if key in step.run_metadata:
            return step.run_metadata[key]
    raise AssertionError(f"No step metadata contained key {key!r}.")


def _event_kinds(event_map: dict[str, list[dict[str, Any]]]) -> set[str]:
    return {event["kind"] for events in event_map.values() for event in events}


def test_phase17_capabilities_forwarded_to_pydantic_ai_run_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The adapter should mirror and forward Pydantic AI's per-run capabilities."""
    durable_agent = _make_wrapped_agent(name_prefix="capability_agent")
    capabilities = [Hooks()]
    captured: dict[str, object] = {}
    run_result = object()
    stream_result = object()
    iter_result = object()

    async def run_async_direct(body: Any, **_: Any) -> Any:
        return await body()

    def run_sync_direct(body: Any, **_: Any) -> Any:
        return body()

    async def fake_run(self: Any, *args: Any, **kwargs: Any) -> object:
        captured["run"] = kwargs["capabilities"]
        return run_result

    def fake_run_sync(self: Any, *args: Any, **kwargs: Any) -> object:
        captured["run_sync"] = kwargs["capabilities"]
        return run_result

    @asynccontextmanager
    async def fake_run_stream(self: Any, *args: Any, **kwargs: Any) -> Any:
        captured["run_stream"] = kwargs["capabilities"]
        yield stream_result

    @asynccontextmanager
    async def fake_iter(*args: Any, **kwargs: Any) -> Any:
        captured["iter"] = kwargs["capabilities"]
        yield iter_result

    monkeypatch.setattr(durable_agent, "_run_async", run_async_direct)
    monkeypatch.setattr(durable_agent, "_run_sync", run_sync_direct)
    monkeypatch.setattr(durable_agent, "_kitaru_overrides", nullcontext)
    monkeypatch.setattr(durable_agent, "_tracking_scope", nullcontext)
    monkeypatch.setattr(durable_agent, "_allow_internal_iter", nullcontext)
    monkeypatch.setattr(durable_agent, "_remember_messages", lambda _result: None)
    monkeypatch.setattr(
        durable_agent, "_require_explicit_checkpoint", lambda _method: None
    )
    monkeypatch.setattr(AbstractAgent, "run", fake_run)
    monkeypatch.setattr(AbstractAgent, "run_sync", fake_run_sync)
    monkeypatch.setattr(AbstractAgent, "run_stream", fake_run_stream)
    monkeypatch.setattr(durable_agent.wrapped, "iter", fake_iter)

    async def exercise_async_surfaces() -> None:
        result = await durable_agent.run("prompt", capabilities=capabilities)
        assert result is run_result
        async with durable_agent.run_stream(
            "prompt", capabilities=capabilities
        ) as streamed_result:
            assert streamed_result is stream_result
        async with durable_agent.iter("prompt", capabilities=capabilities) as agent_run:
            assert agent_run is iter_result

    asyncio.run(exercise_async_surfaces())
    assert durable_agent.run_sync("prompt", capabilities=capabilities) is run_result

    assert captured == {
        "run": capabilities,
        "run_stream": capabilities,
        "iter": capabilities,
        "run_sync": capabilities,
    }


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
