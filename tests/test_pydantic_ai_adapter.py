"""Tests for the PydanticAI adapter."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent, FunctionToolset
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ApprovalRequired
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import _model as adapter_model
from kitaru.adapters.pydantic_ai import _toolset as adapter_toolset
from kitaru.adapters.pydantic_ai._toolset import KitaruToolset
from kitaru.adapters.pydantic_ai._tracking import tracker_scope
from kitaru.errors import KitaruUsageError
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _is_inside_checkpoint,
    _is_inside_flow,
)

pytestmark = [pytest.mark.anyio]


def _scope_ids() -> tuple[str, str]:
    """Return valid execution/checkpoint IDs for scope setup."""
    return str(uuid4()), str(uuid4())


def _collect_events(logged_payloads: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Merge logged child-event payloads by event ID."""
    events: dict[str, dict[str, Any]] = {}
    for payload in logged_payloads:
        events.update(payload["pydantic_ai_events"])
    return events


def test_wrap_returns_kitaru_agent() -> None:
    """wrap() should return a wrapper with Kitaru model/toolset interception."""
    wrapped = kp.wrap(Agent(TestModel(), name="researcher"))

    assert wrapped.name == "researcher"
    assert wrapped.model.__class__.__name__ == "KitaruModel"
    assert wrapped.toolsets


def test_wrap_tracks_model_and_tool_child_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wrapped runs inside checkpoints should capture model/tool child events."""
    saved: list[tuple[str, str, Any]] = []
    logged: list[dict[str, Any]] = []

    def fake_save(name: str, value: Any, *, type: str = "output") -> None:
        saved.append((name, type, value))

    def fake_log(**kwargs: Any) -> None:
        logged.append(kwargs)

    monkeypatch.setattr(adapter_model, "save", fake_save)
    monkeypatch.setattr(adapter_model, "log", fake_log)
    monkeypatch.setattr(adapter_toolset, "save", fake_save)
    monkeypatch.setattr(adapter_toolset, "log", fake_log)

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
    events = _collect_events(logged)

    llm_events = [event for event in events.values() if event["type"] == "llm_call"]
    tool_events = [event for event in events.values() if event["type"] == "tool_call"]

    assert llm_events
    assert tool_events
    assert tool_events[0]["parent_event_ids"]
    assert {artifact_type for _, artifact_type, _ in saved} >= {
        "prompt",
        "response",
        "input",
        "output",
    }


def test_hitl_tool_translates_to_flow_level_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HITL-marked tools should call wait() with checkpoint scope suspended."""
    logged: list[dict[str, Any]] = []
    wait_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(adapter_model, "save", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter_toolset, "save", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter_model, "log", lambda **kwargs: logged.append(kwargs))
    monkeypatch.setattr(adapter_toolset, "log", lambda **kwargs: logged.append(kwargs))

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
        for event in _collect_events(logged).values()
        if event["type"] == "tool_call"
    ]
    assert tool_events
    assert tool_events[0]["hitl"] is True


async def test_deferred_tool_exceptions_raise_clear_adapter_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adapter should reject native deferred-tool flows with guidance."""
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool
    def plain_tool() -> str:
        return "ok"

    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tools = await toolset.get_tools(ctx)
    tool = tools["plain_tool"]

    async def raise_approval_required(*args: Any, **kwargs: Any) -> Any:
        raise ApprovalRequired

    monkeypatch.setattr(toolset, "call_tool", raise_approval_required)
    monkeypatch.setattr(adapter_toolset, "save", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter_toolset, "log", lambda **kwargs: None)

    wrapped_toolset = KitaruToolset(toolset)
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
