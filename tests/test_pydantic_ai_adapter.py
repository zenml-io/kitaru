"""Unit coverage for the PydanticAI adapter's granular-mode helpers.

Focuses on the logic that doesn't require spinning up a flow — the dispatcher
decision, the per-tool override resolution, and the isolated-runtime guard.
Full end-to-end agent runs are exercised by the example tests.
"""

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

pytest.importorskip("pydantic_ai")

from kitaru.adapters.pydantic_ai._utils import (
    CheckpointConfig,
    checkpoint_cache_key,
    reject_isolated_runtime,
    resolve_tool_checkpoint_config,
    validate_checkpoint_config,
    with_default_type,
)
from kitaru.errors import KitaruContextError, KitaruRuntimeError, KitaruUsageError


def _with_tool_call_id(ctx: Any, tool_call_id: str = "call_123") -> Any:
    ctx.tool_call_id = tool_call_id
    return ctx


def _install_checkpoint_step_recorder(
    monkeypatch: pytest.MonkeyPatch,
    *,
    enter_checkpoint_scope: bool = False,
) -> list[str]:
    from collections.abc import Awaitable, Callable

    from kitaru.runtime import _checkpoint_scope

    checkpoint_steps: list[str] = []

    async def fake_checkpoint(
        *, step_name: str, body: Callable[[], Awaitable[Any]], **_kwargs: Any
    ) -> Any:
        checkpoint_steps.append(step_name)
        if not enter_checkpoint_scope:
            return await body()
        with _checkpoint_scope(name=step_name, checkpoint_type="tool_call"):
            return await body()

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fake_checkpoint,
    )
    return checkpoint_steps


class TestValidateCheckpointConfig:
    @pytest.mark.parametrize("cache", [True, False, None])
    def test_accepts_cache(self, cache: bool | None) -> None:
        assert validate_checkpoint_config(
            {"cache": cache},
            context="model_checkpoint_config",
        ) == {"cache": cache}

    def test_unknown_key_error_lists_cache_with_allowed_keys(self) -> None:
        with pytest.raises(KitaruUsageError) as exc_info:
            validate_checkpoint_config(
                cast(Any, {"surprise": True}),
                context="model_checkpoint_config",
            )

        assert "Unsupported keys in model_checkpoint_config: 'surprise'" in str(
            exc_info.value
        )
        assert "'cache'" in str(exc_info.value)


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


class TestWaitCallSuffix:
    """Per-call wait-name suffix is unique per call, stable on replay."""

    def test_sanitized_tool_call_id_is_used_when_present(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_abc/123.xyz") == "call_abc_123_xyz"

    def test_empty_tool_call_id_raises_usage_error(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        with pytest.raises(KitaruUsageError, match="stable `tool_call_id`"):
            _wait_call_suffix("")

    def test_none_tool_call_id_raises_usage_error(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        with pytest.raises(KitaruUsageError, match="stable `tool_call_id`"):
            _wait_call_suffix(None)

    def test_unsanitizable_tool_call_id_raises_usage_error(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        with pytest.raises(KitaruUsageError, match="replay-safe wait name"):
            _wait_call_suffix("///")

    def test_same_tool_call_id_produces_same_suffix(self) -> None:
        """Stable across replays: identical tool_call_id => identical suffix."""
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_xyz") == _wait_call_suffix("call_xyz")

    def test_different_tool_call_ids_produce_different_suffixes(self) -> None:
        """Unique per call: different tool_call_ids => different suffixes."""
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_a") != _wait_call_suffix("call_b")


class TestModelMessageCacheSerialization:
    def _messages(self, *, run_id: str, second: int) -> list[Any]:
        from pydantic_ai.messages import (
            ModelRequest,
            ModelResponse,
            TextPart,
            ToolCallPart,
            ToolReturnPart,
            UserPromptPart,
        )

        return [
            ModelRequest(
                parts=[
                    UserPromptPart(
                        content="same prompt",
                        timestamp=datetime(2026, 5, 1, 12, 0, second, tzinfo=UTC),
                    )
                ],
                run_id=run_id,
            ),
            ModelResponse(
                parts=[
                    TextPart(content="thinking about lookup"),
                    ToolCallPart(
                        tool_name="lookup",
                        args={
                            "timestamp": "user supplied timestamp",
                            "nested": {"run_id": "user supplied run id"},
                        },
                        tool_call_id="call_lookup",
                    ),
                ],
                timestamp=datetime(2026, 5, 1, 12, 1, second, tzinfo=UTC),
                run_id=run_id,
            ),
            ModelRequest(
                parts=[
                    ToolReturnPart(
                        tool_name="lookup",
                        content={"timestamp": "user supplied tool result"},
                        tool_call_id="call_lookup",
                        timestamp=datetime(2026, 5, 1, 12, 2, second, tzinfo=UTC),
                    )
                ],
                run_id=run_id,
            ),
        ]

    def test_cache_serialization_ignores_message_envelope_not_user_content(
        self,
    ) -> None:
        from pydantic_ai.models import ModelRequestParameters

        from kitaru.adapters.pydantic_ai._model import (
            _serialize_messages,
            _serialize_messages_for_cache,
        )

        messages_a = self._messages(run_id="run-a", second=1)
        messages_b = self._messages(run_id="run-b", second=2)

        serialized = _serialize_messages(messages_a)
        assert serialized[0]["run_id"] == "run-a"
        assert serialized[0]["parts"][0]["timestamp"] == "2026-05-01T12:00:01Z"

        stable = _serialize_messages_for_cache(messages_a)
        assert "run_id" not in stable[0]
        assert "timestamp" not in stable[0]["parts"][0]
        assert "run_id" not in stable[1]
        assert "timestamp" not in stable[1]
        assert "timestamp" not in stable[2]["parts"][0]
        assert stable[1]["parts"][1]["args"] == {
            "timestamp": "user supplied timestamp",
            "nested": {"run_id": "user supplied run id"},
        }
        assert stable[2]["parts"][0]["content"] == {
            "timestamp": "user supplied tool result"
        }

        cache_key_a = checkpoint_cache_key(
            {
                "messages": stable,
                "model_settings": None,
                "model_request_parameters": ModelRequestParameters(),
            }
        )
        cache_key_b = checkpoint_cache_key(
            {
                "messages": _serialize_messages_for_cache(messages_b),
                "model_settings": None,
                "model_request_parameters": ModelRequestParameters(),
            }
        )
        assert cache_key_a == cache_key_b

    def test_granular_model_checkpoint_uses_stable_cache_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from collections.abc import Awaitable, Callable

        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelResponse, TextPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from kitaru.adapters.pydantic_ai import KitaruAgent
        from kitaru.runtime import _flow_scope

        calls = {"model": 0}
        cached_responses: dict[str, Any] = {}

        def model_function(_messages: list[Any], _info: AgentInfo) -> ModelResponse:
            calls["model"] += 1
            return ModelResponse(parts=[TextPart(content="fixed answer")])

        async def fake_checkpoint(
            *,
            config: CheckpointConfig,
            step_name: str,
            body: Callable[[], Awaitable[Any]],
            cache_key: str | None = None,
            **_kwargs: Any,
        ) -> Any:
            assert step_name == "stable_cache_agent_model_request"
            assert config["cache"] is True
            assert cache_key is not None
            if cache_key in cached_responses:
                return cached_responses[cache_key]
            result = await body()
            cached_responses[cache_key] = result
            return result

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )
        agent = KitaruAgent(
            Agent(
                FunctionModel(model_function),
                name="stable_cache_agent",
                output_type=str,
            ),
            granular_checkpoints=True,
            model_checkpoint_config={"cache": True},
        )

        with _flow_scope(name="test_flow"):
            first = agent.run_sync("same prompt").output
        with _flow_scope(name="test_flow"):
            second = agent.run_sync("same prompt").output

        assert first == "fixed answer"
        assert second == "fixed answer"
        assert calls == {"model": 1}
        assert len(cached_responses) == 1


class TestWaitForInput:
    """Adapter-namespaced helper for calling wait from a tool body."""

    def test_wait_for_input_is_re_exported(self) -> None:
        from kitaru.adapters import pydantic_ai as kp

        assert callable(kp.wait_for_input)

    def test_wait_for_input_delegates_to_kitaru_wait(self, monkeypatch) -> None:
        import kitaru as kitaru_module
        from kitaru.adapters.pydantic_ai import wait_for_input

        captured: dict = {}

        def fake_wait(**kwargs):
            captured.update(kwargs)
            return "human response"

        monkeypatch.setattr(kitaru_module, "wait", fake_wait)

        result = wait_for_input(
            schema=str,
            question="What severity?",
            name="ask_user",
            metadata={"extra": 1},
        )

        assert result == "human response"
        assert captured["schema"] is str
        assert captured["question"] == "What severity?"
        assert captured["name"] == "ask_user"
        assert captured["metadata"] == {
            "adapter": "pydantic_ai",
            "source": "tool_body",
            "extra": 1,
        }

    def test_wait_for_input_adapter_metadata_wins_over_caller_metadata(
        self, monkeypatch
    ) -> None:
        import kitaru as kitaru_module
        from kitaru.adapters.pydantic_ai import wait_for_input

        captured: dict = {}

        def fake_wait(**kwargs):
            captured.update(kwargs)
            return None

        monkeypatch.setattr(kitaru_module, "wait", fake_wait)

        wait_for_input(
            schema=str,
            metadata={"adapter": "impostor", "source": "impostor"},
        )

        assert captured["metadata"]["adapter"] == "pydantic_ai"
        assert captured["metadata"]["source"] == "tool_body"


class TestResolveHitlQuestion:
    """Dynamic wait question picks up LLM-supplied tool args."""

    def _config(self, **kwargs):
        from kitaru.adapters.pydantic_ai._hitl import HitlConfig

        return HitlConfig(**kwargs)

    def test_dynamic_arg_overrides_static_question(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question="default fallback")
        resolved = resolve_hitl_question(
            config, {"question": "What severity should this be?"}
        )
        assert resolved == "What severity should this be?"

    def test_static_fallback_when_arg_missing(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question="default fallback")
        assert resolve_hitl_question(config, {}) == "default fallback"

    def test_blank_dynamic_arg_falls_back_to_static(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question="default fallback")
        assert resolve_hitl_question(config, {"question": "   "}) == "default fallback"

    def test_non_string_dynamic_arg_falls_back_to_static(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question="default fallback")
        assert resolve_hitl_question(config, {"question": 42}) == "default fallback"

    def test_custom_question_arg_name(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question=None, question_arg="prompt")
        resolved = resolve_hitl_question(config, {"prompt": "Pick a severity"})
        assert resolved == "Pick a severity"

    def test_disabled_question_arg_uses_static(self) -> None:
        from kitaru.adapters.pydantic_ai._hitl import resolve_hitl_question

        config = self._config(question="static only", question_arg=None)
        assert resolve_hitl_question(config, {"question": "ignored"}) == "static only"


class TestUseGranular:
    def _make_agent(self, *, granular_checkpoints: bool):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        inner = Agent(TestModel(), name="streamer")
        return KitaruAgent(inner, granular_checkpoints=granular_checkpoints)

    def test_granular_off_always_false(self) -> None:
        agent = self._make_agent(granular_checkpoints=False)
        assert agent._use_granular(force_turn_checkpoint=False) is False
        assert agent._use_granular(force_turn_checkpoint=True) is False

    def test_granular_on_without_stream_handler(self) -> None:
        agent = self._make_agent(granular_checkpoints=True)
        assert agent._use_granular(force_turn_checkpoint=False) is True

    def test_granular_on_with_stream_handler_falls_back(self) -> None:
        agent = self._make_agent(granular_checkpoints=True)
        assert agent._use_granular(force_turn_checkpoint=True) is False

    def test_granular_is_default_and_enables_per_call_checkpoint_configs(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        agent = KitaruAgent(Agent(TestModel(), name="streamer"))
        assert agent._granular_checkpoints is True
        assert agent._use_granular(force_turn_checkpoint=False) is True
        assert agent._model_checkpoint_config == {}
        assert agent._tool_checkpoint_config == {}
        assert agent._mcp_checkpoint_config == {}

    def test_all_checkpoint_configs_accept_cache(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        agent = KitaruAgent(
            Agent(TestModel(), name="cache_config_agent"),
            granular_checkpoints=True,
            turn_checkpoint_config={"cache": True},
            model_checkpoint_config={"cache": True},
            tool_checkpoint_config={"cache": False},
            tool_checkpoint_config_by_name={
                "lookup": {"cache": True},
                "send_email": False,
            },
            mcp_checkpoint_config={"cache": None},
        )

        assert agent._turn_checkpoint_config == {"cache": True}
        assert agent._model_checkpoint_config == {"cache": True}
        assert agent._tool_checkpoint_config == {"cache": False}
        assert agent._tool_checkpoint_config_by_name == {
            "lookup": {"cache": True},
            "send_email": False,
        }
        assert agent._mcp_checkpoint_config == {"cache": None}

    def test_per_call_configs_require_granular_mode(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        inner = Agent(TestModel(), name="streamer")
        with pytest.raises(KitaruUsageError, match="granular_checkpoints=True"):
            KitaruAgent(
                inner,
                granular_checkpoints=False,
                model_checkpoint_config={"retries": 1},
            )

    def test_wrap_compatibility_shim_translates_legacy_capture_modes(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters import pydantic_ai as kp

        with pytest.deprecated_call():
            wrapped = kp.wrap(
                Agent(TestModel(), name="legacy"),
                tool_capture_config={"mode": "metadata_only"},
                tool_capture_config_by_name={"secret": {"mode": "off"}},
            )

        assert wrapped.capture.tool_capture == "metadata"
        assert wrapped.capture.tool_capture_overrides["secret"] is None


class TestPersistMessageHistory:
    def _make_agent(self, *, persist: bool):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        inner = Agent(TestModel(), name="chat")
        return KitaruAgent(inner, persist_message_history=persist)

    def test_effective_history_returns_explicit_when_provided(self) -> None:
        agent = self._make_agent(persist=True)
        explicit = ["m1"]
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
        agent._last_messages = stored
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

    def test_cached_run_sync_result_refreshes_history(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(all_messages=lambda: ["cached-message"])

        def fake_run_sync(_body, **_kwargs):
            return cached_result

        monkeypatch.setattr(agent, "_run_sync", fake_run_sync)

        assert agent.run_sync("hello") is cached_result
        assert agent._last_messages == ["cached-message"]

    @pytest.mark.anyio
    async def test_cached_run_result_refreshes_history(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(all_messages=lambda: ["cached-message"])

        async def fake_run_async(_body, **_kwargs):
            return cached_result

        monkeypatch.setattr(agent, "_run_async", fake_run_async)

        assert await agent.run("hello") is cached_result
        assert agent._last_messages == ["cached-message"]

    def test_event_handler_cached_turn_checkpoint_refreshes_history_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["streamed-cached-message"]
        )

        def fake_auto_checkpoint_sync(_body, **_kwargs):
            return cached_result

        async def handler(_ctx: Any, _stream: Any) -> None:
            pass

        monkeypatch.setattr(agent, "_auto_checkpoint_sync", fake_auto_checkpoint_sync)

        with _flow_scope(name="demo_flow"):
            assert (
                agent.run_sync("hello", event_stream_handler=handler) is cached_result
            )

        assert agent._last_messages == ["streamed-cached-message"]

    @pytest.mark.anyio
    async def test_event_handler_cached_turn_checkpoint_refreshes_history_async(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["streamed-cached-message"]
        )

        async def fake_auto_checkpoint_async(_body, **_kwargs):
            return cached_result

        async def handler(_ctx: Any, _stream: Any) -> None:
            pass

        monkeypatch.setattr(agent, "_auto_checkpoint_async", fake_auto_checkpoint_async)

        with _flow_scope(name="demo_flow"):
            assert (
                await agent.run("hello", event_stream_handler=handler) is cached_result
            )

        assert agent._last_messages == ["streamed-cached-message"]

    def test_persist_history_requires_all_messages_on_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        agent = self._make_agent(persist=True)

        def fake_run_sync(_body, **_kwargs):
            return object()

        monkeypatch.setattr(agent, "_run_sync", fake_run_sync)

        with pytest.raises(KitaruRuntimeError, match="all_messages"):
            agent.run_sync("hello")

    def test_warns_once_when_persist_history_runs_inside_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _checkpoint_scope, _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(all_messages=lambda: ["cached-message"])

        def fake_run_sync(_body, **_kwargs):
            return cached_result

        monkeypatch.setattr(agent, "_run_sync", fake_run_sync)

        with (
            _flow_scope(name="demo_flow"),
            _checkpoint_scope(name="outer", checkpoint_type="custom"),
        ):
            with pytest.warns(UserWarning, match="persist_message_history"):
                agent.run_sync("hello")
            with warnings.catch_warnings(record=True) as records:
                warnings.simplefilter("always")
                agent.run_sync("hello again")

        assert records == []

    def test_flow_scope_does_not_warn_for_persist_history(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(all_messages=lambda: ["cached-message"])

        def fake_run_sync(_body, **_kwargs):
            return cached_result

        monkeypatch.setattr(agent, "_run_sync", fake_run_sync)

        with (
            _flow_scope(name="demo_flow"),
            warnings.catch_warnings(record=True) as records,
        ):
            warnings.simplefilter("always")
            agent.run_sync("hello")

        assert records == []

    @pytest.mark.anyio
    async def test_internal_run_sync_delegation_does_not_warn_inside_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.adapters.pydantic_ai._agent import _INTERNAL_RUN_SYNC_DELEGATION
        from kitaru.runtime import _checkpoint_scope, _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(all_messages=lambda: ["cached-message"])

        async def fake_run_async(_body, **_kwargs):
            return cached_result

        monkeypatch.setattr(agent, "_run_async", fake_run_async)

        with (
            _flow_scope(name="demo_flow"),
            _checkpoint_scope(name="adapter_owned", checkpoint_type="llm_call"),
            warnings.catch_warnings(record=True) as records,
        ):
            warnings.simplefilter("always")
            token = _INTERNAL_RUN_SYNC_DELEGATION.set(True)
            try:
                assert await agent.run("hello") is cached_result
            finally:
                _INTERNAL_RUN_SYNC_DELEGATION.reset(token)

        assert records == []


@pytest.mark.anyio
async def test_iter_requires_explicit_checkpoint() -> None:
    from pydantic_ai import Agent
    from pydantic_ai.exceptions import UserError
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import KitaruAgent

    agent = KitaruAgent(Agent(TestModel(), name="iterer"))
    with pytest.raises(UserError, match=r"explicit `@kitaru.checkpoint`"):
        async with agent.iter("hello"):
            pass


@pytest.mark.anyio
async def test_capture_off_still_routes_hitl(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy, hitl_tool
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    @hitl_tool(schema=bool, question="Approve?")
    def approve_release() -> str:
        return "never reached"

    wrapped = kitaruify_toolset(toolset, capture=CapturePolicy(tool_capture=None))
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["approve_release"]

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_: True,
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("approve_release", {}, ctx, tool)

    assert result is True


@pytest.mark.anyio
async def test_explicit_hitl_tool_bypasses_granular_tool_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy, hitl_tool
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    @hitl_tool(schema=str, question="Need human input")
    def ask_human() -> str:
        return "never reached"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["ask_human"]

    async def fail_checkpoint(**_kwargs: Any) -> Any:
        raise AssertionError("explicit HITL should not open a granular tool checkpoint")

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fail_checkpoint,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_: "approved by human",
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("ask_human", {}, ctx, tool)

    assert result == "approved by human"


@pytest.mark.anyio
async def test_explicit_hitl_tool_without_tool_call_id_raises_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy, hitl_tool
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    @hitl_tool(schema=str, question="Need human input")
    def ask_human() -> str:
        return "never reached"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["ask_human"]

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_: pytest.fail("wait should not run without stable tool_call_id"),
    )

    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(KitaruUsageError, match="stable `tool_call_id`"),
    ):
        await wrapped.call_tool("ask_human", {}, ctx, tool)


@pytest.mark.anyio
async def test_non_hitl_tool_still_uses_granular_tool_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def ordinary_tool() -> str:
        return "ordinary result"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["ordinary_tool"]
    checkpoint_steps = _install_checkpoint_step_recorder(monkeypatch)

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("ordinary_tool", {}, ctx, tool)

    assert result == "ordinary result"
    assert checkpoint_steps == ["ordinary_tool_tool"]


@pytest.mark.anyio
async def test_named_hitl_tool_uses_name_as_unique_wait_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy, hitl_tool
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    @hitl_tool(name="collect_bug_report", schema=str, question="Describe it")
    def collect_bug_report() -> str:
        return "never reached"

    wrapped = kitaruify_toolset(
        toolset, capture=CapturePolicy(correlate_otel_spans=False)
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["collect_bug_report"]
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset._wait_call_suffix",
        lambda _: "call_123",
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **kwargs: captured.update(kwargs) or "approved",
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("collect_bug_report", {}, ctx, tool)

    assert result == "approved"
    assert captured["name"] == "collect_bug_report_call_123"


@pytest.mark.anyio
async def test_run_sync_refuses_inside_running_event_loop() -> None:
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import KitaruAgent

    agent = KitaruAgent(Agent(TestModel(), name="loop_refuser"))
    with pytest.raises(KitaruUsageError, match="running event loop"):
        agent.run_sync("anything")


@pytest.mark.anyio
async def test_approval_required_routes_through_wait_when_tool_checkpoint_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import ApprovalRequired
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    call_count = {"value": 0}
    approve_state = {"approved": False}
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish() -> str:
        call_count["value"] += 1
        if not approve_state["approved"]:
            raise ApprovalRequired(metadata={"channel": "prod"})
        return "published"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name={"publish": False},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["publish"]

    captured: dict[str, Any] = {}
    checkpoint_steps = _install_checkpoint_step_recorder(monkeypatch)

    def _fake_wait(**kwargs: Any) -> bool:
        captured.update(kwargs.get("metadata") or {})
        approve_state["approved"] = True
        return True

    monkeypatch.setattr("kitaru.adapters.pydantic_ai._toolset.kitaru.wait", _fake_wait)

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("publish", {}, ctx, tool)

    assert result == "published"
    assert captured.get("exception_metadata") == {"channel": "prod"}
    assert call_count["value"] == 2
    assert checkpoint_steps == []


@pytest.mark.anyio
async def test_approval_required_without_tool_call_id_raises_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import ApprovalRequired
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish() -> str:
        raise ApprovalRequired(metadata={"channel": "prod"})

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config_by_name={"publish": False},
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["publish"]

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_: pytest.fail("wait should not run without stable tool_call_id"),
    )

    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(KitaruUsageError, match="stable `tool_call_id`"),
    ):
        await wrapped.call_tool("publish", {}, ctx, tool)


@pytest.mark.anyio
async def test_approval_required_inside_tool_checkpoint_fails_before_wait_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import ApprovalRequired
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish() -> str:
        raise ApprovalRequired(metadata={"channel": "prod"})

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["publish"]
    checkpoint_steps = _install_checkpoint_step_recorder(
        monkeypatch,
        enter_checkpoint_scope=True,
    )

    import importlib

    wait_module = importlib.import_module("kitaru.wait")
    monkeypatch.setattr(
        wait_module,
        "_resolve_zenml_wait",
        lambda: pytest.fail("checkpoint-contained wait should fail first"),
    )
    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(
            KitaruContextError,
            match="cannot run inside a @checkpoint",
        ),
    ):
        await wrapped.call_tool("publish", {}, ctx, tool)

    assert checkpoint_steps == ["publish_tool"]


@pytest.mark.anyio
async def test_call_deferred_routes_through_wait_when_tool_checkpoint_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import CallDeferred
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def defer_release() -> str:
        raise CallDeferred(metadata={"ticket": "REL-123"})

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name={"defer_release": False},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["defer_release"]
    checkpoint_steps = _install_checkpoint_step_recorder(monkeypatch)
    captured: dict[str, Any] = {}

    def _fake_wait(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "provided later"

    monkeypatch.setattr("kitaru.adapters.pydantic_ai._toolset.kitaru.wait", _fake_wait)

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("defer_release", {}, ctx, tool)

    assert result == "provided later"
    assert captured["schema"] is str
    assert captured["metadata"]["exception_metadata"] == {"ticket": "REL-123"}
    assert checkpoint_steps == []


@pytest.mark.anyio
async def test_call_deferred_without_schema_raises_usage_error() -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import CallDeferred
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def defer_release():
        raise CallDeferred()

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["defer_release"]

    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(KitaruUsageError, match="Cannot infer a wait schema"),
    ):
        await wrapped.call_tool("defer_release", {}, ctx, tool)
