"""Replay runtime override tests."""

from __future__ import annotations

from kitaru.replay_context import (
    KITARU_REPLAY_CONTEXT_ENV,
    ReplayRuntimeContext,
    get_replay_runtime_context,
    resolve_model_override,
    resolve_tool_override,
)


def test_resolve_tool_override_imports_registered_target(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={"lookup_policy_tool": "tests._replay_tool_stub.lookup_policy"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    override = resolve_tool_override("lookup_policy", target="lookup_policy_tool")
    assert override is not None
    assert override(topic="x") == {"source": "stub"}
    get_replay_runtime_context.cache_clear()


def test_resolve_tool_override_matches_tool_checkpoint_suffix(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={"lookup_policy": "tests._replay_tool_stub.lookup_policy"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    override = resolve_tool_override("lookup_policy_tool")
    assert override is not None
    assert override() == {"source": "stub"}
    get_replay_runtime_context.cache_clear()


def test_resolve_model_override_matches_only_target(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        model_overrides={"support_copilot_model_request_2": "openai/gpt-5-nano"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    assert (
        resolve_model_override("support_copilot_model_request_2") == "openai/gpt-5-nano"
    )
    assert resolve_model_override("support_copilot_model_request_1") is None
    get_replay_runtime_context.cache_clear()


def test_resolve_tool_override_returns_none_without_context(monkeypatch) -> None:
    monkeypatch.delenv(KITARU_REPLAY_CONTEXT_ENV, raising=False)
    get_replay_runtime_context.cache_clear()
    assert resolve_tool_override("lookup_policy") is None
    get_replay_runtime_context.cache_clear()
