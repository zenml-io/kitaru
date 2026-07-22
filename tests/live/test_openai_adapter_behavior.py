# ruff: noqa: E402
"""Provider-extended live behavior checks for OpenAI-backed adapters.

These tests make real OpenAI-backed adapter calls and are intentionally marked
``provider_extended`` so the weekly provider-core run stays cheap.
"""

import os
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast
from uuid import uuid4

import pytest
from pydantic import BaseModel

from kitaru import checkpoint, flow
from kitaru._client._models import ExecutionStatus
from kitaru.client import KitaruClient

pytestmark = [
    pytest.mark.live_llm,
    pytest.mark.live_openai,
    pytest.mark.provider_extended,
]

agents = pytest.importorskip("agents")
pytest.importorskip("pydantic_ai")
pytest.importorskip("langchain")
pytest.importorskip("langchain_openai")
pytest.importorskip("langgraph")

from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
from pydantic_ai import Agent as PydanticAgent

from kitaru.adapters.langgraph import KitaruGraphRunner, LangGraphCapturePolicy
from kitaru.adapters.langgraph._constants import (
    LANGGRAPH_EVENTS_METADATA_KEY,
    LANGGRAPH_RUN_SUMMARIES_METADATA_KEY,
)
from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest
from kitaru.adapters.pydantic_ai import KitaruAgent

_OPENAI_MODEL = os.environ.get("KITARU_LIVE_OPENAI_MODEL", "gpt-4o-mini")
_LIVE_MARKER = "kitaru-openai-adapter-live-marker-7319"


class _PydanticLiveAnswer(BaseModel):
    """Small structured object returned by the live PydanticAI test."""

    marker: str
    status: str
    priority: int


class _ForceLangGraphToolChoice(AgentMiddleware):
    """Force the first LangGraph model call to choose our tiny tool."""

    def __init__(self, tool_name: str) -> None:
        self._tool_name = tool_name

    def wrap_model_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        if _has_langgraph_tool_result(getattr(request, "messages", [])):
            return handler(request)
        forced_request = request.override(
            tool_choice={
                "type": "function",
                "function": {"name": self._tool_name},
            },
            model_settings={"parallel_tool_calls": False},
        )
        return handler(forced_request)


def _has_langgraph_tool_result(messages: Sequence[Any]) -> bool:
    return any(type(message).__name__ == "ToolMessage" for message in messages)


def _wait_for_hydrated_run(exec_id: str, *, timeout_seconds: float = 60.0) -> Any:
    from zenml.client import Client

    client = Client()
    deadline = time.time() + timeout_seconds
    while True:
        run = client.get_pipeline_run(exec_id, allow_name_prefix_match=False)
        if run.status.is_finished:
            assert run.status.is_successful
            return run.get_hydrated_version()
        if time.time() >= deadline:
            raise AssertionError(f"Pipeline run {exec_id} did not finish in time.")
        time.sleep(0.25)


def _metadata_dict_from_steps(hydrated_run: Any, key: str) -> dict[str, Any]:
    for step in hydrated_run.steps.values():
        value = step.run_metadata.get(key)
        if isinstance(value, dict):
            return value
    raise AssertionError(f"No step metadata contained key {key!r}.")


def _events(event_map: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [event for events in event_map.values() for event in events]


def _event_kinds(event_map: Mapping[str, list[dict[str, Any]]]) -> set[str]:
    return {str(event.get("kind")) for event in _events(event_map)}


def _langgraph_events(
    event_map: Mapping[str, Any], *, exec_id: str
) -> list[dict[str, Any]]:
    """Collect LangGraph events from full or lightweight run metadata.

    When the tracker flushes inside a checkpoint it records only a lightweight
    pointer (`artifact_name`, `event_count`, ...) and persists the full event
    list as an artifact, so follow the pointer in that case.
    """
    client = KitaruClient()
    events: list[dict[str, Any]] = []
    for payload in event_map.values():
        if isinstance(payload, list):
            events.extend(cast(list[dict[str, Any]], payload))
            continue
        artifact_name = cast(str, payload["artifact_name"])
        refs = client.artifacts.list(exec_id, name=artifact_name, limit=1)
        assert refs, f"No artifact named {artifact_name!r} on execution {exec_id}."
        events.extend(cast(list[dict[str, Any]], refs[0].load()))
    return events


def _openai_tool_runner(tool_calls: list[str]) -> KitaruRunner:
    @agents.function_tool
    def lookup_live_marker(marker: str) -> str:
        """Return a deterministic marker for the live adapter behavior test."""
        tool_calls.append(marker)
        return f"live-tool-result:{marker}:ok"

    agent = agents.Agent(
        name=f"kitaru-live-openai-tool-{uuid4().hex[:8]}",
        instructions=(
            "Call lookup_live_marker exactly once with the marker from the user. "
            "Then answer in one short sentence that includes the tool result."
        ),
        model=_OPENAI_MODEL,
        tools=[lookup_live_marker],
        model_settings=agents.ModelSettings(tool_choice="required"),
    )
    return KitaruRunner(
        agent,
        checkpoint_strategy="calls",
        run_config_factory=lambda: agents.RunConfig(tracing_disabled=True),
    )


def test_openai_agents_live_tool_call_records_result_evidence(primed_zenml) -> None:
    """The OpenAI Agents adapter can run and record a real local tool call."""
    tool_calls: list[str] = []
    runner = _openai_tool_runner(tool_calls)

    result = runner.run_sync(
        OpenAIRunRequest.start(
            f"Use lookup_live_marker with marker {_LIVE_MARKER}.",
            max_turns=3,
        )
    )

    assert result.status == "completed"
    assert tool_calls == [_LIVE_MARKER]
    assert result.final_output
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None
    assert result.output_artifact_name is not None


def test_openai_agents_live_streaming_records_lifecycle_evidence(
    primed_zenml,
) -> None:
    """The OpenAI Agents streaming adapter exposes durable run evidence."""
    agent = agents.Agent(
        name=f"kitaru-live-openai-stream-{uuid4().hex[:8]}",
        instructions="Answer in one short sentence. Do not use tools or files.",
        model=_OPENAI_MODEL,
    )
    runner = KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: agents.RunConfig(tracing_disabled=True),
    )

    result = runner.run_stream_sync(
        OpenAIRunRequest.start(
            "Say that the OpenAI Agents streaming adapter completed.",
            max_turns=1,
        )
    )

    assert result.status == "completed"
    assert result.final_output
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None


def _make_pydantic_live_agent(
    tool_calls: list[str],
) -> KitaruAgent[Any, _PydanticLiveAnswer]:
    agent = PydanticAgent[Any, _PydanticLiveAnswer](
        f"openai:{_OPENAI_MODEL}",
        name=f"kitaru_live_pydantic_ai_{uuid4().hex[:8]}",
        output_type=_PydanticLiveAnswer,
        instructions=(
            "You are testing adapter behavior. Call lookup_ticket exactly once. "
            "Use its result to return marker, status, and priority."
        ),
    )

    @agent.tool_plain
    def lookup_ticket(ticket_id: str) -> str:
        """Return deterministic ticket details for a live provider tool call."""
        tool_calls.append(ticket_id)
        return f"ticket={ticket_id}; marker={_LIVE_MARKER}; status=green; priority=2"

    return KitaruAgent(agent)


@flow
def pydantic_ai_live_behavior_flow(nonce: str) -> dict[str, Any]:
    """Run one PydanticAI live tool + structured-output turn."""
    tool_calls: list[str] = []
    agent = _make_pydantic_live_agent(tool_calls)
    result = agent.run_sync(
        "Call lookup_ticket with ticket_id='LIVE-42'. Return the structured object. "
        f"Nonce: {nonce}."
    )
    return {"answer": result.output.model_dump(), "tool_calls": tool_calls}


def test_pydantic_ai_live_tool_and_structured_output_metadata(primed_zenml) -> None:
    """PydanticAI records real model/tool events and structured output."""
    handle = pydantic_ai_live_behavior_flow.run(uuid4().hex)
    hydrated = _wait_for_hydrated_run(handle.exec_id)

    payload = cast(dict[str, Any], handle.wait())
    answer = cast(dict[str, Any], payload["answer"])
    tool_calls = cast(list[str], payload["tool_calls"])
    summary_map = cast(
        dict[str, dict[str, Any]], hydrated.run_metadata["pydantic_ai_run_summaries"]
    )
    event_map = cast(
        dict[str, list[dict[str, Any]]], hydrated.run_metadata["pydantic_ai_events"]
    )
    summary = next(iter(summary_map.values()))

    assert tool_calls == ["LIVE-42"]
    assert answer["marker"] == _LIVE_MARKER
    assert answer["status"] == "green"
    assert answer["priority"] == 2
    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)


def add_one_live(value: int) -> str:
    """Add one to the provided value for the live LangGraph test."""
    return str(value + 1)


@checkpoint
def persist_langgraph_live_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Persist a small summary so the test can inspect the Kitaru execution."""
    return summary


@flow
def langgraph_live_calls_flow(nonce: str) -> dict[str, Any]:
    """Run one OpenAI-backed LangGraph calls-mode turn."""
    graph = create_agent(
        model=ChatOpenAI(model=_OPENAI_MODEL, temperature=0),
        tools=[add_one_live],
        middleware=[
            _ForceLangGraphToolChoice("add_one_live"),
            KitaruLangGraphMiddleware(graph_name="kitaru_live_langgraph_calls"),
        ],
        checkpointer=InMemorySaver(),
        name="kitaru_live_langgraph_calls",
        system_prompt=(
            "Call add_one_live exactly once with value=4. Then answer with the "
            "tool result and do not call any more tools."
        ),
    )
    runner = KitaruGraphRunner(
        graph,
        name="kitaru_live_langgraph_calls",
        checkpoint_strategy="calls",
        capture=LangGraphCapturePolicy(save_state_snapshot=False),
    )
    result = runner.invoke(
        {
            "messages": [
                {
                    "role": "user",
                    "content": f"Use the required tool now. Nonce: {nonce}",
                }
            ]
        },
        thread_id=f"kitaru-live-langgraph-{nonce}",
    )
    output = cast(dict[str, Any], result.output)
    messages = cast(list[Any], output["messages"])
    summary = {
        "status": result.status,
        "message_types": [type(message).__name__ for message in messages],
        "contents": [str(getattr(message, "content", "")) for message in messages],
        "tool_call_counts": [
            len(getattr(message, "tool_calls", None) or []) for message in messages
        ],
        "tool_call_ids": [
            [
                str(tool_call["id"])
                for tool_call in cast(
                    list[dict[str, Any]],
                    getattr(message, "tool_calls", None) or [],
                )
                if "id" in tool_call
            ]
            for message in messages
        ],
    }
    return persist_langgraph_live_summary(summary)


def test_langgraph_live_calls_mode_records_model_and_tool_evidence(
    primed_zenml,
) -> None:
    """LangGraph calls mode records real model/tool behavior through Kitaru."""
    nonce = uuid4().hex
    handle = langgraph_live_calls_flow.run(nonce)
    deadline = time.monotonic() + 60
    while not handle.status.is_finished:
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Execution {handle.exec_id} did not finish within 60 seconds."
            )
        time.sleep(0.25)

    assert handle.status.is_successful
    execution = KitaruClient().executions.get(handle.exec_id)
    assert execution.status == ExecutionStatus.COMPLETED

    summary = cast(dict[str, Any], handle.wait())
    assert summary["status"] == "completed"
    assert "ToolMessage" in summary["message_types"]
    assert any(count >= 1 for count in summary["tool_call_counts"])

    checkpoint_names = [checkpoint.name for checkpoint in execution.checkpoints]
    assert any(name.startswith("model_call__") for name in checkpoint_names)
    assert any(name.startswith("tool_call__add_one_live_") for name in checkpoint_names)

    hydrated = _wait_for_hydrated_run(handle.exec_id)
    event_map = _metadata_dict_from_steps(hydrated, LANGGRAPH_EVENTS_METADATA_KEY)
    summary_map = cast(
        dict[str, dict[str, Any]],
        _metadata_dict_from_steps(hydrated, LANGGRAPH_RUN_SUMMARIES_METADATA_KEY),
    )
    events = _langgraph_events(event_map, exec_id=handle.exec_id)
    event_kinds = {str(event.get("kind")) for event in events}
    run_summary = next(iter(summary_map.values()))

    assert {"model_call", "tool_call"} <= event_kinds
    assert run_summary["status"] == "completed"
