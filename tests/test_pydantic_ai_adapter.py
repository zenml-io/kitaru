"""Tests for the PydanticAI adapter."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from uuid import uuid4

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import (
    Agent,
    CallDeferred,
    FunctionToolset,
    ToolsetTool,
)
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ApprovalRequired
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets import AbstractToolset
from pydantic_ai.usage import RunUsage

from kitaru._safe_save import _safe_save
from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import _agent as adapter_agent
from kitaru.adapters.pydantic_ai import _model as adapter_model
from kitaru.adapters.pydantic_ai import _toolset as adapter_toolset
from kitaru.adapters.pydantic_ai._toolset import (
    KitaruFunctionToolset,
    KitaruMCPToolset,
    KitaruToolset,
    KitaruWrapperToolset,
)
from kitaru.adapters.pydantic_ai._tracking import (
    ChildEventTracker,
    normalize_agent_name,
    tracker_scope,
)
from kitaru.errors import KitaruUsageError
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _is_inside_checkpoint,
    _is_inside_flow,
)

MCPServer: type[Any] | None
try:
    from pydantic_ai.mcp import MCPServer as _MCPServer
except ImportError:  # pragma: no cover - depends on optional install extras
    MCPServer = None
else:
    MCPServer = _MCPServer

pytestmark = [pytest.mark.anyio]


class _DummyToolset(AbstractToolset[None]):
    """Minimal custom toolset for dispatch tests."""

    @property
    def id(self) -> str | None:
        return "dummy"

    async def get_tools(self, ctx: RunContext[None]) -> dict[str, ToolsetTool[None]]:
        return {}

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[None],
        tool: ToolsetTool[None],
    ) -> Any:
        raise KeyError(name)


if MCPServer is not None:

    class _DummyMCPServer(MCPServer):
        """Minimal MCP server stub for wrapper dispatch tests."""

        @asynccontextmanager
        async def client_streams(
            self,
        ) -> AsyncIterator[tuple[Any, Any]]:
            yield None, None


def _scope_ids() -> tuple[str, str]:
    """Return valid execution/checkpoint IDs for scope setup."""
    return str(uuid4()), str(uuid4())


def _collect_logged_dict(
    logged_payloads: list[dict[str, Any]],
    key: str,
) -> dict[str, dict[str, Any]]:
    """Merge keyed dict payloads from `kitaru.log()` calls."""
    merged: dict[str, dict[str, Any]] = {}
    for payload in logged_payloads:
        value = payload.get(key)
        if isinstance(value, dict):
            merged.update(cast(dict[str, dict[str, Any]], value))
    return merged


@pytest.fixture
def capture_adapter_io(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[tuple[str, str, Any]], list[dict[str, Any]]]:
    """Capture adapter artifact saves and metadata logs."""
    saved: list[tuple[str, str, Any]] = []
    logged: list[dict[str, Any]] = []

    def fake_save(name: str, value: Any, *, type: str = "output") -> None:
        saved.append((name, type, value))

    def fake_log(**kwargs: Any) -> None:
        logged.append(kwargs)

    monkeypatch.setattr(adapter_model, "save", fake_save)
    monkeypatch.setattr(adapter_toolset, "save", fake_save)
    monkeypatch.setattr(adapter_model, "log", fake_log)
    monkeypatch.setattr(adapter_toolset, "log", fake_log)
    monkeypatch.setattr(adapter_agent, "log", fake_log)

    return saved, logged


def test_wrap_returns_kitaru_agent() -> None:
    """wrap() should return an adapter wrapper with model/toolset interception."""
    wrapped = kp.wrap(Agent(TestModel(), name="researcher"))

    assert wrapped.name == "researcher"
    assert wrapped.model.__class__.__name__ == "KitaruModel"
    assert wrapped.toolsets


def test_agent_name_normalization_handles_edge_cases() -> None:
    """Agent names should normalize into stable metadata-safe identifiers."""
    assert normalize_agent_name("  Research Team  ") == "Research_Team"
    assert normalize_agent_name("***") == "agent"
    assert normalize_agent_name(None) == "agent"


def test_tracker_assigns_sequence_and_turn_with_explicit_fan_edges() -> None:
    """Tracker lineage fields should expose fan-out/fan-in relationships."""
    tracker = ChildEventTracker(agent_name="demo")

    first_llm_id, first_llm_ctx = tracker.start_model_event()
    tracker.complete_model_event(first_llm_id)

    tool_id, tool_ctx = tracker.start_tool_event()
    tracker.complete_tool_event(tool_id)

    second_llm_id, second_llm_ctx = tracker.start_model_event()
    tracker.complete_model_event(second_llm_id)

    assert first_llm_ctx.sequence_index == 1
    assert first_llm_ctx.turn_index == 1
    assert tool_ctx.sequence_index == 2
    assert tool_ctx.turn_index == 1
    assert tool_ctx.fan_out_from == first_llm_id
    assert second_llm_ctx.sequence_index == 3
    assert second_llm_ctx.turn_index == 2
    assert second_llm_ctx.fan_in_from == [tool_id]


def test_kitaru_wrapper_toolset_is_immutable() -> None:
    """visit_and_replace should return self once a toolset is Kitaru-wrapped."""
    wrapped = adapter_toolset.kitaruify_toolset(FunctionToolset())

    replacement = wrapped.visit_and_replace(lambda _: FunctionToolset())

    assert replacement is wrapped
    assert isinstance(wrapped, KitaruWrapperToolset)


def test_kitaruify_toolset_dispatches_function_and_generic_wrappers() -> None:
    """Dispatch should distinguish FunctionToolset from generic toolsets."""
    function_wrapped = adapter_toolset.kitaruify_toolset(FunctionToolset())
    generic_wrapped = adapter_toolset.kitaruify_toolset(_DummyToolset())

    assert isinstance(function_wrapped, KitaruFunctionToolset)
    assert isinstance(generic_wrapped, KitaruToolset)


@pytest.mark.skipif(MCPServer is None, reason="MCP optional dependency is unavailable")
def test_kitaruify_toolset_dispatches_mcp_wrappers() -> None:
    """Dispatch should detect MCP servers and wrap them with MCP-aware wrapper."""
    assert MCPServer is not None
    mcp_wrapped = adapter_toolset.kitaruify_toolset(_DummyMCPServer(id="dummy-mcp"))
    assert isinstance(mcp_wrapped, KitaruMCPToolset)


async def test_capture_config_metadata_only_tracks_without_artifacts(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """`metadata_only` should keep lineage metadata while skipping artifacts."""
    saved, logged = capture_adapter_io

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool
    def increment(value: int) -> int:
        return value + 1

    wrapped = adapter_toolset.kitaruify_toolset(
        toolset,
        tool_capture_config={"mode": "metadata_only"},
    )

    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await toolset.get_tools(ctx))["increment"]
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        tracker_scope("demo_agent"),
    ):
        result = await wrapped.call_tool("increment", {"value": 1}, ctx, tool)

    assert result == 2
    assert saved == []

    events = _collect_logged_dict(logged, "pydantic_ai_events")
    assert len(events) == 1
    payload = next(iter(events.values()))
    assert payload["capture_mode"] == "metadata_only"
    assert payload["artifacts"] == {}


async def test_capture_config_off_disables_tracking_for_selected_tool(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """Per-tool mode `off` should bypass adapter observability for that tool."""
    saved, logged = capture_adapter_io

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool
    def increment(value: int) -> int:
        return value + 1

    wrapped = adapter_toolset.kitaruify_toolset(
        toolset,
        tool_capture_config_by_name={"increment": {"mode": "off"}},
    )

    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await toolset.get_tools(ctx))["increment"]
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        tracker_scope("demo_agent"),
    ):
        result = await wrapped.call_tool("increment", {"value": 1}, ctx, tool)

    assert result == 2
    assert saved == []
    assert _collect_logged_dict(logged, "pydantic_ai_events") == {}


def test_safe_save_uses_blob_when_primary_save_fails() -> None:
    """Shared fallback save should store repr payloads when primary save fails."""
    attempts: list[tuple[str, str, Any]] = []
    value = object()

    def fake_save(name: str, value: Any, *, type: str = "output") -> None:
        attempts.append((name, type, value))
        if type != "blob":
            raise TypeError("cannot serialize")

    artifact_type = _safe_save(
        "tool_payload",
        value,
        artifact_type="input",
        save_func=fake_save,
    )

    assert artifact_type == "blob"
    assert attempts[0] == ("tool_payload", "input", value)
    assert attempts[1][0] == "tool_payload"
    assert attempts[1][1] == "blob"
    assert attempts[1][2] == {
        "repr": repr(value),
        "python_type": value.__class__.__name__,
    }


def test_wrap_tracks_model_and_tool_events_with_timing_and_ordering(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """Tracked events should include timing, ordering, and lineage fields."""
    saved, logged = capture_adapter_io

    def add(a: int, b: int) -> int:
        return a + b

    agent = kp.wrap(Agent(TestModel(), name="researcher", tools=[add]))
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        result = agent.run_sync("Add two numbers")

    assert result.output is not None

    events = _collect_logged_dict(logged, "pydantic_ai_events")
    assert events

    ordered_events = sorted(events.values(), key=lambda event: event["sequence_index"])
    assert [event["sequence_index"] for event in ordered_events] == list(
        range(1, len(ordered_events) + 1)
    )

    for event in events.values():
        assert isinstance(event["duration_ms"], float)
        assert event["turn_index"] >= 1

    tool_events = [event for event in events.values() if event["type"] == "tool_call"]
    llm_events = [event for event in events.values() if event["type"] == "llm_call"]
    assert tool_events
    assert llm_events
    assert all("fan_out_from" in event for event in tool_events)
    assert all("fan_in_from" in event for event in llm_events)
    assert all(isinstance(event["fan_in_from"], list) for event in llm_events)

    saved_types = {artifact_type for _, artifact_type, _ in saved}
    assert saved_types >= {"prompt", "response", "input", "output"}


def test_run_summary_is_logged_per_agent_run(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """Each run should emit a summary keyed by an isolated run label."""
    _, logged = capture_adapter_io
    agent = kp.wrap(Agent(TestModel(), name="reviewer"))
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        first = agent.run_sync("First question")
        second = agent.run_sync("Second question", message_history=first.new_messages())

    assert first.output is not None
    assert second.output is not None

    summaries = _collect_logged_dict(logged, "pydantic_ai_run_summaries")
    assert len(summaries) == 2
    summary_labels = set(summaries)
    assert len(summary_labels) == 2
    for summary in summaries.values():
        assert summary["status"] == "completed"
        assert summary["total_events"] >= 1
        assert summary["event_ids_in_order"]


def test_hitl_tool_translates_to_flow_level_wait(
    monkeypatch: pytest.MonkeyPatch,
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """HITL-marked tools should call wait() with checkpoint scope suspended."""
    _, logged = capture_adapter_io
    wait_calls: list[dict[str, Any]] = []

    def fake_wait(**kwargs: Any) -> bool:
        assert _is_inside_flow()
        assert not _is_inside_checkpoint()
        wait_calls.append(kwargs)
        return True

    monkeypatch.setattr(adapter_toolset, "wait", fake_wait)

    @kp.hitl_tool(question="Approve publish?", schema=bool)
    def approve(change_summary: str) -> bool:
        raise AssertionError("HITL tools should not execute their Python body.")

    agent = kp.wrap(Agent(TestModel(), name="reviewer", tools=[approve]))
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        result = agent.run_sync("Review this draft")
        assert _is_inside_checkpoint()

    assert result.output is not None
    assert len(wait_calls) == 1

    tool_events = [
        event
        for event in _collect_logged_dict(logged, "pydantic_ai_events").values()
        if event["type"] == "tool_call"
    ]
    assert tool_events
    assert tool_events[0]["hitl"] is True


async def test_deferred_tool_approval_required_raises_clear_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ApprovalRequired should surface adapter guidance for deferred tool flows."""
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool
    def plain_tool() -> str:
        return "ok"

    async def raise_approval_required(*args: Any, **kwargs: Any) -> Any:
        raise ApprovalRequired

    monkeypatch.setattr(toolset, "call_tool", raise_approval_required)
    monkeypatch.setattr(adapter_toolset, "save", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter_toolset, "log", lambda **kwargs: None)

    wrapped_toolset = KitaruToolset(toolset, toolset_kind="function")
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await toolset.get_tools(ctx))["plain_tool"]
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        tracker_scope("demo_agent"),
        pytest.raises(KitaruUsageError, match="deferred tool flows"),
    ):
        await wrapped_toolset.call_tool("plain_tool", {}, ctx, tool)


async def test_deferred_tool_call_deferred_raises_clear_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CallDeferred should surface adapter guidance for deferred tool flows."""
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool
    def plain_tool() -> str:
        return "ok"

    async def raise_call_deferred(*args: Any, **kwargs: Any) -> Any:
        raise CallDeferred

    monkeypatch.setattr(toolset, "call_tool", raise_call_deferred)
    monkeypatch.setattr(adapter_toolset, "save", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter_toolset, "log", lambda **kwargs: None)

    wrapped_toolset = KitaruToolset(toolset, toolset_kind="function")
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await toolset.get_tools(ctx))["plain_tool"]
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        tracker_scope("demo_agent"),
        pytest.raises(KitaruUsageError, match="deferred tool flows"),
    ):
        await wrapped_toolset.call_tool("plain_tool", {}, ctx, tool)


async def test_stream_transcript_artifact_is_saved(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """Streaming calls should emit a replay-friendly transcript artifact."""
    saved, _ = capture_adapter_io
    agent = kp.wrap(Agent(TestModel(), name="streamer"))
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        async with agent.run_stream("Stream this") as result:
            await result.get_output()

    transcript_artifacts = [
        (name, artifact_type, value)
        for name, artifact_type, value in saved
        if name.endswith("_stream_transcript")
    ]
    assert transcript_artifacts
    transcript_name, transcript_type, transcript_value = transcript_artifacts[0]
    assert transcript_type == "context"
    assert re.search(r"_stream_transcript$", transcript_name)
    assert isinstance(transcript_value, dict)
    assert isinstance(transcript_value.get("event_count"), int)
    assert isinstance(transcript_value.get("events"), list)


def test_event_stream_handler_invocations_are_tracked_inline(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """run_sync event handlers should emit inline handler metadata + summary stats."""
    _, logged = capture_adapter_io
    agent = kp.wrap(Agent(TestModel(), name="handler-agent"))
    execution_id, checkpoint_id = _scope_ids()

    async def event_stream_handler(run_context: Any, stream: Any) -> None:
        del run_context
        async for _ in stream:
            pass

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        result = agent.run_sync(
            "Track handler", event_stream_handler=event_stream_handler
        )

    assert result.output is not None

    handler_events = _collect_logged_dict(logged, "pydantic_ai_event_stream_handlers")
    assert handler_events
    handler_payload = next(iter(handler_events.values()))
    assert handler_payload["status"] == "completed"
    assert handler_payload["duration_ms"] >= 0

    summaries = _collect_logged_dict(logged, "pydantic_ai_run_summaries")
    assert summaries
    summary_payload = next(iter(summaries.values()))
    assert summary_payload["event_stream_handler"]["call_count"] >= 1


def test_run_sync_uses_synthetic_checkpoint_in_flow_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flow-scope run_sync should route through the synthetic checkpoint helper."""
    calls: list[tuple[str, str | None]] = []

    def fake_synthetic_checkpoint(run_id: str, *, id: str | None = None) -> str:
        calls.append((run_id, id))
        return "synthetic-result"

    monkeypatch.setattr(
        adapter_agent,
        "_synthetic_agent_run_checkpoint",
        fake_synthetic_checkpoint,
    )

    agent = kp.wrap(Agent(TestModel(), name="synthetic-agent"))

    with _flow_scope(name="demo_flow", execution_id=str(uuid4())):
        result = agent.run_sync("Flow scope call")

    assert result == "synthetic-result"
    assert len(calls) == 1
    run_id, call_id = calls[0]
    assert run_id == call_id
    assert run_id.startswith("synthetic_agent_agent_run_")
    assert adapter_agent._SYNTHETIC_RUN_CALLABLES == {}


def test_wrapped_agent_passthrough_outside_flow_without_tracking(
    capture_adapter_io: tuple[list[tuple[str, str, Any]], list[dict[str, Any]]],
) -> None:
    """Outside Kitaru runtime scopes, wrapped agents should execute without tracking."""
    saved, logged = capture_adapter_io
    agent = kp.wrap(Agent(TestModel(), name="plain-agent"))

    result = agent.run_sync("Hello")

    assert result.output is not None
    assert saved == []
    assert logged == []
