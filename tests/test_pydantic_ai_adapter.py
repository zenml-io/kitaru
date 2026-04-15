"""Unit coverage for the PydanticAI adapter's granular-mode helpers.

Focuses on the logic that doesn't require spinning up a flow — the dispatcher
decision, the per-tool override resolution, and the isolated-runtime guard.
Full end-to-end agent runs are exercised by the example tests.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pydantic_ai")

from kitaru.adapters.pydantic_ai._utils import (
    CheckpointConfig,
    reject_isolated_runtime,
    resolve_tool_checkpoint_config,
    with_default_type,
)
from kitaru.errors import KitaruUsageError


class TestRejectIsolatedRuntime:
    def test_raises_on_isolated_runtime(self) -> None:
        with pytest.raises(KitaruUsageError, match=r"runtime='isolated'"):
            reject_isolated_runtime({"runtime": "isolated"})

    def test_accepts_inline_runtime(self) -> None:
        reject_isolated_runtime({"runtime": "inline"})

    def test_accepts_missing_runtime(self) -> None:
        reject_isolated_runtime({})


class TestResolveToolCheckpointConfig:
    def test_default_used_when_no_override(self) -> None:
        default: CheckpointConfig = {"retries": 2}
        assert (
            resolve_tool_checkpoint_config("foo", default=default, by_name=None)
            == default
        )

    def test_per_name_override_wins(self) -> None:
        default: CheckpointConfig = {"retries": 2}
        override: CheckpointConfig = {"retries": 5}
        resolved = resolve_tool_checkpoint_config(
            "foo",
            default=default,
            by_name={"foo": override},
        )
        assert resolved == override

    def test_false_opts_tool_out(self) -> None:
        default: CheckpointConfig = {"retries": 2}
        resolved = resolve_tool_checkpoint_config(
            "fetch_secret",
            default=default,
            by_name={"fetch_secret": False},
        )
        assert resolved is None

    def test_unknown_tool_falls_back_to_default(self) -> None:
        default: CheckpointConfig = {"retries": 2}
        resolved = resolve_tool_checkpoint_config(
            "other",
            default=default,
            by_name={"foo": False},
        )
        assert resolved == default


class TestWithDefaultType:
    def test_injects_default_when_missing(self) -> None:
        config: CheckpointConfig = {"retries": 1}
        assert with_default_type(config, "llm_call") == {
            "retries": 1,
            "type": "llm_call",
        }

    def test_preserves_explicit_type(self) -> None:
        config: CheckpointConfig = {"type": "custom"}
        assert with_default_type(config, "llm_call") == {"type": "custom"}

    def test_does_not_mutate_input(self) -> None:
        config: CheckpointConfig = {"retries": 1}
        with_default_type(config, "llm_call")
        assert config == {"retries": 1}


class TestUseGranular:
    """The streaming fallback: granular mode must not apply when an event stream
    handler is in play, because per-call checkpointing can't drain an async
    stream inside a sync ZenML step.
    """

    def _make_agent(self, *, granular: bool):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        inner = Agent(TestModel(), name="streamer")
        return KitaruAgent(inner, granular_checkpoints=granular)

    def test_granular_off_always_false(self) -> None:
        agent = self._make_agent(granular=False)
        assert agent._use_granular(force_turn_checkpoint=False) is False
        assert agent._use_granular(force_turn_checkpoint=True) is False

    def test_granular_on_without_stream_handler(self) -> None:
        agent = self._make_agent(granular=True)
        assert agent._use_granular(force_turn_checkpoint=False) is True

    def test_granular_on_with_stream_handler_falls_back(self) -> None:
        agent = self._make_agent(granular=True)
        assert agent._use_granular(force_turn_checkpoint=True) is False


class TestPersistMessageHistory:
    """Instance-level conversation memory: opt-in, one instance = one conversation."""

    def _make_agent(self, *, persist: bool):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        inner = Agent(TestModel(), name="chat")
        return KitaruAgent(inner, persist_message_history=persist)

    def test_effective_history_returns_explicit_when_provided(self) -> None:
        agent = self._make_agent(persist=True)
        explicit = ["m1"]  # type: ignore[list-item]
        assert agent._effective_message_history(explicit) is explicit

    def test_effective_history_none_when_disabled(self) -> None:
        agent = self._make_agent(persist=False)
        assert agent._effective_message_history(None) is None

    def test_effective_history_none_when_no_prior_run(self) -> None:
        agent = self._make_agent(persist=True)
        assert agent._effective_message_history(None) is None

    def test_effective_history_returns_remembered(self) -> None:
        agent = self._make_agent(persist=True)
        stored = ["m1", "m2"]
        agent._last_messages = stored  # type: ignore[assignment]
        recalled = agent._effective_message_history(None)
        assert recalled == stored
        assert recalled is not stored

    def test_remember_messages_noop_when_disabled(self) -> None:
        agent = self._make_agent(persist=False)
        agent._remember_messages(SimpleNamespace(all_messages=lambda: ["m"]))
        assert agent._last_messages is None

    def test_remember_messages_stores_all_messages(self) -> None:
        agent = self._make_agent(persist=True)
        agent._remember_messages(SimpleNamespace(all_messages=lambda: ["m1", "m2"]))
        assert agent._last_messages == ["m1", "m2"]
