"""Replay runtime tool override tests."""

from __future__ import annotations

from kitaru.replay_context import (
    KITARU_REPLAY_CONTEXT_ENV,
    ReplayRuntimeContext,
    resolve_tool_override,
)


def test_resolve_tool_override_imports_registered_tool(
    monkeypatch,
) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        tool_overrides={"lookup_policy": "tests._replay_tool_stub.lookup_policy"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())

    override = resolve_tool_override("lookup_policy")
    assert override is not None
    assert override(topic="x") == {"source": "stub"}


def test_resolve_tool_override_matches_tool_checkpoint_suffix(
    monkeypatch,
) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        tool_overrides={"lookup_policy": "tests._replay_tool_stub.lookup_policy"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())

    override = resolve_tool_override("lookup_policy_tool")
    assert override is not None
    assert override() == {"source": "stub"}


def test_resolve_tool_override_returns_none_without_context(
    monkeypatch,
) -> None:
    monkeypatch.delenv(KITARU_REPLAY_CONTEXT_ENV, raising=False)
    from kitaru.replay_context import get_replay_runtime_context

    get_replay_runtime_context.cache_clear()
    assert resolve_tool_override("lookup_policy") is None
    get_replay_runtime_context.cache_clear()
