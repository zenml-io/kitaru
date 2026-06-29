"""Replay runtime override tests."""

from __future__ import annotations

import pytest

from kitaru.errors import KitaruRuntimeError
from kitaru.replay_context import (
    KITARU_REPLAY_CONTEXT_ENV,
    ReplayRuntimeContext,
    get_replay_runtime_context,
    is_replay,
    resolve_model_override,
    resolve_tool_override,
)


def test_is_replay_returns_false_without_context(monkeypatch) -> None:
    monkeypatch.delenv(KITARU_REPLAY_CONTEXT_ENV, raising=False)
    get_replay_runtime_context.cache_clear()

    assert is_replay() is False
    get_replay_runtime_context.cache_clear()


def test_is_replay_returns_true_with_valid_context(monkeypatch) -> None:
    context = ReplayRuntimeContext(at="lookup_policy_tool")
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    assert is_replay() is True
    get_replay_runtime_context.cache_clear()


def test_is_replay_returns_true_with_malformed_context(monkeypatch) -> None:
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, "not-json")
    get_replay_runtime_context.cache_clear()

    assert get_replay_runtime_context() is None
    assert is_replay() is True
    get_replay_runtime_context.cache_clear()


def test_is_replay_ignores_cached_parsed_context_after_env_removed(monkeypatch) -> None:
    context = ReplayRuntimeContext(at="lookup_policy_tool")
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()
    assert get_replay_runtime_context() is not None

    monkeypatch.delenv(KITARU_REPLAY_CONTEXT_ENV)

    assert is_replay() is False
    get_replay_runtime_context.cache_clear()


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


def test_resolve_tool_override_raises_for_missing_attribute(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={
            "lookup_policy_tool": "tests._replay_tool_stub.missing_lookup_policy"
        },
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    with pytest.raises(KitaruRuntimeError, match="does not exist"):
        resolve_tool_override("lookup_policy", target="lookup_policy_tool")
    get_replay_runtime_context.cache_clear()


def test_resolve_tool_override_raises_for_non_callable(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={"lookup_policy_tool": "tests._replay_tool_stub.not_callable"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    with pytest.raises(KitaruRuntimeError, match="is not callable"):
        resolve_tool_override("lookup_policy", target="lookup_policy_tool")
    get_replay_runtime_context.cache_clear()


def test_resolve_tool_override_raises_for_malformed_import_path(monkeypatch) -> None:
    context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={"lookup_policy_tool": "not_a_dotted_path"},
    )
    monkeypatch.setenv(KITARU_REPLAY_CONTEXT_ENV, context.to_json())
    get_replay_runtime_context.cache_clear()

    with pytest.raises(KitaruRuntimeError, match="dotted import path"):
        resolve_tool_override("lookup_policy", target="lookup_policy_tool")
    get_replay_runtime_context.cache_clear()
