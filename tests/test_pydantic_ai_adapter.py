"""Unit coverage for the PydanticAI adapter's granular-mode helpers.

Focuses on the logic that doesn't require spinning up a flow — the dispatcher
decision, the per-tool override resolution, and the isolated-runtime guard.
Full end-to-end agent runs are exercised by the example tests.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterable
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
    turn_cache_key,
    validate_checkpoint_config,
    with_default_type,
)
from kitaru.errors import KitaruRuntimeError, KitaruUsageError


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


class TestTurnCacheKeyCallSites:
    def _make_agent(self):
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        return KitaruAgent(Agent(TestModel(), name="cache_key_agent"))

    def test_run_sync_forwards_run_kwargs_to_cache_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        agent = self._make_agent()
        capabilities = [Hooks()]
        spec = {"profile": "sync"}
        captured: dict[str, Any] = {}
        sentinel = object()

        def fake_turn_cache_key(**kwargs: Any) -> str:
            captured.update(kwargs)
            return "cache-sync"

        def fake_run_sync(_body: Any, **kwargs: Any) -> object:
            captured["run_sync_cache_key"] = kwargs["cache_key"]
            return sentinel

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._agent.turn_cache_key",
            fake_turn_cache_key,
        )
        monkeypatch.setattr(agent, "_run_sync", fake_run_sync)

        result = agent.run_sync(
            "hello",
            capabilities=capabilities,
            spec=spec,
            conversation_id="conversation-sync",
            output_retries=2,
        )

        assert result is sentinel
        assert captured["capabilities"] is capabilities
        assert captured["spec"] is spec
        assert captured["conversation_id"] == "conversation-sync"
        assert captured["output_retries"] == 2
        assert captured["run_sync_cache_key"] == "cache-sync"

    @pytest.mark.anyio
    async def test_run_forwards_run_kwargs_to_cache_key(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        agent = self._make_agent()
        capabilities = [Hooks()]
        spec = {"profile": "async"}
        captured: dict[str, Any] = {}
        sentinel = object()

        def fake_turn_cache_key(**kwargs: Any) -> str:
            captured.update(kwargs)
            return "cache-async"

        async def fake_run_async(_body: Any, **kwargs: Any) -> object:
            captured["run_cache_key"] = kwargs["cache_key"]
            return sentinel

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._agent.turn_cache_key",
            fake_turn_cache_key,
        )
        monkeypatch.setattr(agent, "_run_async", fake_run_async)

        result = await agent.run(
            "hello",
            capabilities=capabilities,
            spec=spec,
            conversation_id="conversation-async",
            output_retries=3,
        )

        assert result is sentinel
        assert captured["capabilities"] is capabilities
        assert captured["spec"] is spec
        assert captured["conversation_id"] == "conversation-async"
        assert captured["output_retries"] == 3
        assert captured["run_cache_key"] == "cache-async"


class TestPydanticAIAutoFlowNaming:
    def _install_inline_model_checkpoint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_checkpoint(*, body: Any, **_kwargs: Any) -> Any:
            return await body()

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )

    def _install_fake_auto_flow(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> list[str]:
        from kitaru.adapters.pydantic_ai import _agent as agent_module
        from kitaru.runtime import _flow_scope

        flow_names: list[str] = []

        class FakeHandle:
            def __init__(self, result: Any) -> None:
                self._result = result

            def wait(self) -> Any:
                return self._result

        class FakeAutoFlow:
            def __init__(self, agent_name: str) -> None:
                self.flow_name = agent_module._auto_flow_name_for_agent(agent_name)

            def run(
                self,
                run_id: str,
                serialized_body_path: str | None = None,
            ) -> FakeHandle:
                flow_names.append(self.flow_name)
                with _flow_scope(name=self.flow_name):
                    result = agent_module._run_auto_flow_body(
                        run_id,
                        serialized_body_path,
                    )
                return FakeHandle(result)

        def fake_auto_flow_for_agent(agent_name: str) -> FakeAutoFlow:
            return FakeAutoFlow(agent_name)

        monkeypatch.setattr(
            agent_module,
            "_auto_flow_for_agent",
            fake_auto_flow_for_agent,
        )
        monkeypatch.setattr(
            agent_module,
            "_try_serialize_auto_flow_body",
            lambda _body: None,
        )
        return flow_names

    def test_auto_flow_name_helper_uses_flow_name_normalization(self) -> None:
        from kitaru.adapters.pydantic_ai._agent import _auto_flow_name_for_agent

        assert _auto_flow_name_for_agent("research_agent") == "research_agent_flow"
        assert _auto_flow_name_for_agent("research agent") == "research_agent_flow"
        assert _auto_flow_name_for_agent("123") == "flow_123_flow"

    def test_direct_run_sync_uses_wrapped_agent_name_when_wrapper_name_absent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        self._install_inline_model_checkpoint(monkeypatch)
        flow_names = self._install_fake_auto_flow(monkeypatch)

        agent = KitaruAgent(Agent(TestModel(), name="wrapped_agent"))
        agent.run_sync("hello")

        assert flow_names == ["wrapped_agent_flow"]

    def test_direct_run_sync_wrapper_name_wins_over_wrapped_agent_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        self._install_inline_model_checkpoint(monkeypatch)
        flow_names = self._install_fake_auto_flow(monkeypatch)

        agent = KitaruAgent(
            Agent(TestModel(), name="wrapped_agent"),
            name="wrapper_agent",
        )
        agent.run_sync("hello")

        assert flow_names == ["wrapper_agent_flow"]

    def test_direct_run_sync_accepts_equal_wrapper_and_wrapped_names(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        self._install_inline_model_checkpoint(monkeypatch)
        flow_names = self._install_fake_auto_flow(monkeypatch)

        agent = KitaruAgent(
            Agent(TestModel(), name="shared_agent"),
            name="shared_agent",
        )
        agent.run_sync("hello")

        assert flow_names == ["shared_agent_flow"]

    def test_direct_run_sync_uses_wrapper_name_when_wrapped_agent_is_unnamed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        self._install_inline_model_checkpoint(monkeypatch)
        flow_names = self._install_fake_auto_flow(monkeypatch)

        agent = KitaruAgent(Agent(TestModel()), name="wrapper_only")
        agent.run_sync("hello")

        assert flow_names == ["wrapper_only_flow"]

    def test_unnamed_wrapper_and_wrapped_agent_still_raise(self) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.exceptions import UserError
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent

        with pytest.raises(UserError, match="requires a stable `name`"):
            KitaruAgent(Agent(TestModel()))

    def test_inside_explicit_flow_does_not_open_auto_flow(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai import Agent
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai import KitaruAgent
        from kitaru.adapters.pydantic_ai import _agent as agent_module
        from kitaru.runtime import _flow_scope

        def fail_auto_flow(_agent_name: str) -> Any:
            raise AssertionError("explicit flow should not use auto-flow")

        self._install_inline_model_checkpoint(monkeypatch)
        monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fail_auto_flow)
        agent = KitaruAgent(Agent(TestModel(), name="explicit_agent"))

        with _flow_scope(name="explicit_flow"):
            agent.run_sync("hello")

    def test_real_auto_flow_factory_caches_and_registers_source_alias(self) -> None:
        from kitaru._source_aliases import build_pipeline_source_alias
        from kitaru.adapters.pydantic_ai import _agent as agent_module

        flow_name = agent_module._auto_flow_name_for_agent("source alias agent")
        source_alias = build_pipeline_source_alias(flow_name)

        first = agent_module._auto_flow_for_agent("source alias agent")
        second = agent_module._auto_flow_for_agent("source alias agent")

        assert first is second
        assert getattr(agent_module, flow_name).__name__ == flow_name
        assert getattr(agent_module, source_alias) is first._pipeline


class TestStreamingHookCapabilities:
    def test_detects_configured_streaming_hooks(self) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        from kitaru.adapters.pydantic_ai._agent import (
            _capabilities_imply_streaming_hooks,
        )

        def hook(*_args: Any, **_kwargs: Any) -> None:
            return None

        assert _capabilities_imply_streaming_hooks(None) is False
        assert _capabilities_imply_streaming_hooks([]) is False
        assert _capabilities_imply_streaming_hooks([Hooks()]) is False
        assert (
            _capabilities_imply_streaming_hooks([Hooks(event=cast(Any, hook))]) is True
        )
        assert (
            _capabilities_imply_streaming_hooks(
                [Hooks(run_event_stream=cast(Any, hook))]
            )
            is True
        )
        assert (
            _capabilities_imply_streaming_hooks(
                cast(Any, [SimpleNamespace(on_event=hook)])
            )
            is True
        )
        assert (
            _capabilities_imply_streaming_hooks(
                cast(Any, [SimpleNamespace(on_run_event_stream=hook)])
            )
            is True
        )

    def test_detects_overridden_stream_wrapper(self) -> None:
        from pydantic_ai.capabilities import AbstractCapability

        from kitaru.adapters.pydantic_ai._agent import (
            _capabilities_imply_streaming_hooks,
        )

        class PlainCapability(AbstractCapability[Any]):
            pass

        class StreamingCapability(AbstractCapability[Any]):
            async def wrap_run_event_stream(
                self,
                ctx: Any,
                *,
                stream: AsyncIterable[Any],
            ) -> AsyncIterable[Any]:
                del ctx
                async for event in stream:
                    yield event

        assert _capabilities_imply_streaming_hooks([PlainCapability()]) is False
        assert _capabilities_imply_streaming_hooks([StreamingCapability()]) is True


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


class TestModelToolCallReservations:
    def test_trackable_tool_call_ids_follow_model_response_order(self) -> None:
        from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models import ModelRequestParameters
        from pydantic_ai.tools import ToolDefinition

        from kitaru.adapters.pydantic_ai._model import _trackable_tool_call_ids
        from kitaru.adapters.pydantic_ai._policy import CapturePolicy

        response = ModelResponse(
            parts=[
                TextPart(content="I will call tools."),
                ToolCallPart(
                    tool_name="alpha",
                    args={},
                    tool_call_id="call_alpha_1",
                ),
                ToolCallPart(
                    tool_name="output_tool",
                    args={},
                    tool_call_id="call_output",
                ),
                ToolCallPart(
                    tool_name="skip",
                    args={},
                    tool_call_id="call_skip",
                ),
                ToolCallPart(
                    tool_name="alpha",
                    args={},
                    tool_call_id="call_alpha_2",
                ),
                ToolCallPart(tool_name="beta", args={}, tool_call_id=""),
            ]
        )

        assert _trackable_tool_call_ids(
            response,
            ModelRequestParameters(
                function_tools=[
                    ToolDefinition(name="alpha"),
                    ToolDefinition(name="skip"),
                ],
                output_tools=[ToolDefinition(name="output_tool")],
            ),
            CapturePolicy(
                tool_capture="metadata",
                tool_capture_overrides={"skip": None},
            ),
        ) == ["call_alpha_1", "call_alpha_2"]


class TestCachedGranularModelCheckpoints:
    def _tool_call_response(self) -> Any:
        from pydantic_ai.messages import ModelResponse, ToolCallPart

        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="alpha",
                    args={},
                    tool_call_id="call_alpha",
                ),
                ToolCallPart(
                    tool_name="beta",
                    args={},
                    tool_call_id="call_beta",
                ),
            ],
            model_name="cached-test-model",
        )

    def _model_request_parameters(self) -> Any:
        from pydantic_ai.models import ModelRequestParameters
        from pydantic_ai.tools import ToolDefinition

        return ModelRequestParameters(
            function_tools=[ToolDefinition(name="alpha"), ToolDefinition(name="beta")]
        )

    @pytest.mark.anyio
    async def test_cached_model_checkpoint_records_event_and_reserves_tool_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai._model import KitaruModel
        from kitaru.adapters.pydantic_ai._policy import CapturePolicy
        from kitaru.adapters.pydantic_ai._tracking import EventTracker
        from kitaru.runtime import _flow_scope

        cached_response = self._tool_call_response()
        tracker = EventTracker(agent_name="cached_agent", run_label="cached")
        checkpoint_called = False

        async def fake_checkpoint(**_kwargs: Any) -> Any:
            nonlocal checkpoint_called
            checkpoint_called = True
            return cached_response

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.get_current_tracker",
            lambda: tracker,
        )
        model = KitaruModel(
            TestModel(),
            capture=CapturePolicy(
                save_prompts=False,
                save_responses=False,
                correlate_otel_spans=False,
            ),
            agent_name="cached_agent",
            checkpoint_config={"cache": True},
        )

        with _flow_scope(name="cached_flow"):
            response = await model.request([], None, self._model_request_parameters())

        assert response is cached_response
        assert checkpoint_called is True
        model_events = [event for event in tracker.events if event.kind == "llm_call"]
        assert len(model_events) == 1
        assert model_events[0].model_name == "cached-test-model"
        assert model_events[0].artifacts == {}

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_context.sequence_index < beta_context.sequence_index
        assert alpha_id.endswith("_tool_call_2")
        assert beta_id.endswith("_tool_call_3")
        assert alpha_context.fan_out_from == model_events[0].event_id
        assert beta_context.fan_out_from == model_events[0].event_id

    @pytest.mark.anyio
    async def test_executed_model_checkpoint_does_not_duplicate_reservations(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.models.function import FunctionModel

        from kitaru.adapters.pydantic_ai._model import KitaruModel
        from kitaru.adapters.pydantic_ai._policy import CapturePolicy
        from kitaru.adapters.pydantic_ai._tracking import EventTracker
        from kitaru.runtime import _checkpoint_scope, _flow_scope

        response = self._tool_call_response()
        tracker = EventTracker(agent_name="body_agent", run_label="body")

        def model_function(_messages: list[Any], _info: Any) -> Any:
            return response

        async def fake_checkpoint(**kwargs: Any) -> Any:
            with _checkpoint_scope(
                name=kwargs["step_name"],
                checkpoint_type=kwargs["config"].get("type", "llm_call"),
            ):
                return await kwargs["body"]()

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.get_current_tracker",
            lambda: tracker,
        )
        model = KitaruModel(
            FunctionModel(model_function),
            capture=CapturePolicy(
                save_prompts=False,
                save_responses=False,
                correlate_otel_spans=False,
            ),
            agent_name="body_agent",
            checkpoint_config={"cache": True},
        )

        with _flow_scope(name="body_flow"):
            actual = await model.request([], None, self._model_request_parameters())

        assert actual is response
        model_events = [event for event in tracker.events if event.kind == "llm_call"]
        assert len(model_events) == 1
        assert tracker._counter == 3

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_context.sequence_index < beta_context.sequence_index
        assert alpha_id.endswith("_tool_call_2")
        assert beta_id.endswith("_tool_call_3")

    @pytest.mark.anyio
    async def test_granular_model_checkpoint_uses_structural_messages_and_output_refs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.messages import ModelResponse, TextPart
        from pydantic_ai.models.function import FunctionModel

        from kitaru.adapters.pydantic_ai._model import KitaruModel
        from kitaru.adapters.pydantic_ai._policy import CapturePolicy
        from kitaru.adapters.pydantic_ai._tracking import EventTracker
        from kitaru.runtime import _checkpoint_scope, _flow_scope

        response = ModelResponse(parts=[TextPart(content="fixed answer")])
        tracker = EventTracker(agent_name="structural_agent", run_label="structural")
        captured_checkpoint_inputs: dict[str, Any] = {}

        def model_function(_messages: list[Any], _info: Any) -> Any:
            return response

        async def fake_checkpoint(**kwargs: Any) -> Any:
            captured_checkpoint_inputs.update(kwargs.get("checkpoint_inputs") or {})
            with _checkpoint_scope(
                name=kwargs["step_name"],
                checkpoint_type=kwargs["config"].get("type", "llm_call"),
            ):
                return await kwargs["body"]()

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.get_current_tracker",
            lambda: tracker,
        )

        def fail_model_manual_save(*args: Any, **kwargs: Any) -> None:
            pytest.fail(
                "granular model checkpoint should not manually save "
                f"{args!r} {kwargs!r}"
            )

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.kitaru.save",
            fail_model_manual_save,
        )
        model = KitaruModel(
            FunctionModel(model_function),
            capture=CapturePolicy(correlate_otel_spans=False),
            agent_name="structural_agent",
            checkpoint_config={"cache": True},
        )

        with _flow_scope(name="structural_flow"):
            actual = await model.request([], None, self._model_request_parameters())

        assert actual is response
        assert captured_checkpoint_inputs == {"messages": []}
        model_events = [event for event in tracker.events if event.kind == "llm_call"]
        assert len(model_events) == 1
        model_event = cast(Any, model_events[0])
        assert model_event.artifacts == {
            "prompt": "messages",
            "response": "output",
        }

    @pytest.mark.anyio
    async def test_cached_granular_model_event_uses_canonical_refs_when_captured(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from pydantic_ai.models.test import TestModel

        from kitaru.adapters.pydantic_ai._model import KitaruModel
        from kitaru.adapters.pydantic_ai._policy import CapturePolicy
        from kitaru.adapters.pydantic_ai._tracking import EventTracker
        from kitaru.runtime import _flow_scope

        cached_response = self._tool_call_response()
        tracker = EventTracker(agent_name="cached_refs", run_label="cached_refs")

        async def fake_checkpoint(**_kwargs: Any) -> Any:
            return cached_response

        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.run_async_in_checkpoint",
            fake_checkpoint,
        )
        monkeypatch.setattr(
            "kitaru.adapters.pydantic_ai._model.get_current_tracker",
            lambda: tracker,
        )
        model = KitaruModel(
            TestModel(),
            capture=CapturePolicy(correlate_otel_spans=False),
            agent_name="cached_refs",
            checkpoint_config={"cache": True},
        )

        with _flow_scope(name="cached_refs_flow"):
            response = await model.request([], None, self._model_request_parameters())

        assert response is cached_response
        model_events = [event for event in tracker.events if event.kind == "llm_call"]
        assert len(model_events) == 1
        model_event = cast(Any, model_events[0])
        assert model_event.artifacts == {
            "prompt": "messages",
            "response": "output",
        }
        assert model_event.checkpoint_name == "cached_refs_model_request"


class TestEventTrackerToolCallOrdering:
    def _record_completed_model(self, tracker: Any) -> tuple[str, Any]:
        event_id, event_context = tracker.start_model_event()
        tracker.record_model_event(
            event_id,
            event_context,
            status="completed",
            duration_ms=1.0,
            artifacts={},
            model_name="test-model",
        )
        return event_id, event_context

    def _record_completed_tool(
        self,
        tracker: Any,
        event_id: str,
        event_context: Any,
        *,
        name: str,
    ) -> None:
        tracker.record_tool_event(
            event_id,
            event_context,
            status="completed",
            name=name,
            toolset_kind="function",
            capture_mode="metadata",
            duration_ms=1.0,
            hitl=False,
            artifacts={},
        )

    def test_reserved_tool_ids_follow_model_order_when_start_order_reverses(
        self,
    ) -> None:
        from kitaru.adapters.pydantic_ai._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(
            parent_model_event_id=model_id,
            turn_index=model_context.turn_index,
            tool_call_ids=["call_alpha", "call_beta"],
        )

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_context.sequence_index < beta_context.sequence_index
        assert alpha_id.endswith("_tool_call_2")
        assert beta_id.endswith("_tool_call_3")
        assert alpha_context.fan_out_from == model_id
        assert beta_context.fan_out_from == model_id

    def test_reverse_completion_order_still_sorts_events_and_fan_in(self) -> None:
        from kitaru.adapters.pydantic_ai._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(
            parent_model_event_id=model_id,
            turn_index=model_context.turn_index,
            tool_call_ids=["call_alpha", "call_beta"],
        )
        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        self._record_completed_tool(tracker, beta_id, beta_context, name="beta")
        self._record_completed_tool(tracker, alpha_id, alpha_context, name="alpha")

        assert [event.event_id for event in tracker.events] == [
            model_id,
            alpha_id,
            beta_id,
        ]
        next_model_id, next_model_context = tracker.start_model_event()
        assert next_model_context.fan_in_from == [alpha_id, beta_id]
        assert next_model_id.endswith("_llm_call_4")

    def test_persisted_events_and_summary_use_reserved_sequence_order(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from kitaru.adapters.pydantic_ai import _tracking

        tracker = _tracking.EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(
            parent_model_event_id=model_id,
            turn_index=model_context.turn_index,
            tool_call_ids=["call_alpha", "call_beta"],
        )
        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(tracker, beta_id, beta_context, name="beta")
        self._record_completed_tool(tracker, alpha_id, alpha_context, name="alpha")
        logged: dict[str, Any] = {}
        monkeypatch.setattr(
            _tracking.kitaru,
            "log",
            lambda **kwargs: logged.update(kwargs),
        )

        tracker.persist()

        events_dump = logged["pydantic_ai_events"][tracker.run_label]
        summary_dump = logged["pydantic_ai_run_summaries"][tracker.run_label]
        assert [event["event_id"] for event in events_dump] == [
            model_id,
            alpha_id,
            beta_id,
        ]
        assert summary_dump["event_ids_in_order"] == [model_id, alpha_id, beta_id]
        assert summary_dump["total_events"] == 3

    def test_missing_or_unreserved_tool_call_id_keeps_counter_fallback(self) -> None:
        from kitaru.adapters.pydantic_ai._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, _model_context = self._record_completed_model(tracker)

        event_id, event_context = tracker.start_tool_event(tool_call_id=None)

        assert event_id.endswith("_tool_call_2")
        assert event_context.sequence_index == 2
        assert event_context.fan_out_from == model_id

    def test_abandoned_reserved_tool_slot_does_not_count_as_recorded_event(
        self,
    ) -> None:
        from kitaru.adapters.pydantic_ai._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(
            parent_model_event_id=model_id,
            turn_index=model_context.turn_index,
            tool_call_ids=["call_alpha", "call_beta"],
        )
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(
            tracker,
            alpha_id,
            alpha_context,
            name="alpha",
        )

        summary = tracker.build_run_summary()

        assert summary.total_events == 2
        assert summary.event_ids_in_order == [model_id, alpha_id]

        abandoned_beta_id = f"{tracker.agent_name}_{tracker.run_label}_tool_call_3"
        _next_model_id, next_model_context = tracker.start_model_event()
        assert next_model_context.fan_in_from == [alpha_id]
        assert abandoned_beta_id not in tracker._event_sequence_by_id


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
                conversation_id=f"conversation-{run_id}",
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
                conversation_id=f"conversation-{run_id}",
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
        assert "conversation_id" not in stable[0]
        assert "timestamp" not in stable[0]["parts"][0]
        assert "run_id" not in stable[1]
        assert "timestamp" not in stable[1]
        assert "run_id" not in stable[2]
        assert "conversation_id" not in stable[2]
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

    def test_cache_serialization_preserves_inherited_conversation_id(
        self,
    ) -> None:
        from pydantic_ai.models import ModelRequestParameters

        from kitaru.adapters.pydantic_ai._model import (
            _serialize_messages_for_cache,
            model_cache_run_context,
        )

        messages_a = self._messages(run_id="run-a", second=1)
        messages_b = self._messages(run_id="run-b", second=2)

        with model_cache_run_context(
            conversation_id=None,
            message_history=messages_a,
        ):
            stable_a = _serialize_messages_for_cache(messages_a)

        with model_cache_run_context(
            conversation_id=None,
            message_history=messages_b,
        ):
            stable_b = _serialize_messages_for_cache(messages_b)

        assert stable_a[0]["conversation_id"] == "conversation-run-a"
        assert stable_a[2]["conversation_id"] == "conversation-run-a"
        assert stable_b[0]["conversation_id"] == "conversation-run-b"
        assert stable_b[2]["conversation_id"] == "conversation-run-b"

        cache_key_a = checkpoint_cache_key(
            {
                "messages": stable_a,
                "model_settings": None,
                "model_request_parameters": ModelRequestParameters(),
            }
        )
        cache_key_b = checkpoint_cache_key(
            {
                "messages": stable_b,
                "model_settings": None,
                "model_request_parameters": ModelRequestParameters(),
            }
        )
        assert cache_key_a != cache_key_b

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

    def test_granular_model_cache_key_includes_explicit_conversation_id(
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
            assert step_name == "conversation_cache_agent_model_request"
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
                name="conversation_cache_agent",
                output_type=str,
            ),
            granular_checkpoints=True,
            model_checkpoint_config={"cache": True},
        )

        with _flow_scope(name="test_flow"):
            first = agent.run_sync(
                "same prompt", conversation_id="conversation-a"
            ).output
        with _flow_scope(name="test_flow"):
            second = agent.run_sync(
                "same prompt", conversation_id="conversation-a"
            ).output
        with _flow_scope(name="test_flow"):
            third = agent.run_sync(
                "same prompt", conversation_id="conversation-b"
            ).output

        assert first == "fixed answer"
        assert second == "fixed answer"
        assert third == "fixed answer"
        assert calls == {"model": 2}
        assert len(cached_responses) == 2


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


class TestTurnCacheKey:
    def _base_kwargs(self) -> dict[str, Any]:
        return {
            "agent_name": "agent",
            "user_prompt": "prompt",
            "message_history": [{"role": "user", "content": "hi"}],
            "deferred_tool_results": None,
            "output_type": str,
            "instructions": "be helpful",
            "deps": {"tenant": "a"},
            "model_settings": {"temperature": 0},
            "usage_limits": {"request_limit": 5},
            "usage": {"requests": 1},
            "metadata": {"trace": "one"},
            "infer_name": True,
            "toolsets": ["tools-a"],
            "builtin_tools": ["builtin-a"],
            "event_stream_handler": None,
            "conversation_id": "conversation-a",
            "output_retries": 1,
            "capabilities": ["capability-a"],
            "spec": {"output": "plain"},
        }

    def test_same_inputs_produce_same_key(self) -> None:
        kwargs = self._base_kwargs()

        assert turn_cache_key(**kwargs) == turn_cache_key(**kwargs)

    def test_none_capabilities_and_spec_do_not_crash(self) -> None:
        kwargs = {**self._base_kwargs(), "capabilities": None, "spec": None}
        key = turn_cache_key(**cast(Any, kwargs))

        assert isinstance(key, str)
        assert key

    def test_real_hooks_capability_can_be_hashed(self) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        kwargs = {**self._base_kwargs(), "capabilities": [Hooks()]}
        key = turn_cache_key(**cast(Any, kwargs))

        assert isinstance(key, str)
        assert key

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("output_type", int),
            ("deps", {"tenant": "b"}),
            ("usage_limits", {"request_limit": 9}),
            ("usage", {"requests": 2}),
            ("metadata", {"trace": "two"}),
            ("infer_name", False),
            ("toolsets", ["tools-b"]),
            ("builtin_tools", ["builtin-b"]),
            ("event_stream_handler", lambda *_args: None),
            ("conversation_id", "conversation-b"),
            ("output_retries", 2),
            ("capabilities", ["capability-b"]),
            ("spec", {"output": "structured"}),
        ],
    )
    def test_behavior_affecting_inputs_change_key(self, field: str, value: Any) -> None:
        base = self._base_kwargs()
        changed = {**base, field: value}

        assert turn_cache_key(**base) != turn_cache_key(**changed)


class TestHitlConfigJsonSerialization:
    """``HitlConfig`` round-trips through Pydantic JSON dumps without leaking
    class objects.

    Regression for an end-to-end break under pydantic-ai-slim 1.86+: that
    release surfaces per-tool ``metadata`` through
    ``AgentRunResult._state.last_model_request_parameters``, which means
    Kitaru's auto-checkpoint output (an ``AgentRunResult``) is fully
    JSON-serialized by ZenML's ``DataclassMaterializer``. The walked tree
    includes every tool's ``metadata['kitaru_hitl_config']``, and a raw class
    object in ``HitlConfig.schema`` (e.g. ``schema=bool``) crashes the dump
    with ``PydanticSerializationError: Unable to serialize unknown type:
    <class 'type'>``.
    """

    def test_dump_python_json_mode_stringifies_class_schema(self) -> None:
        from pydantic import TypeAdapter

        from kitaru.adapters.pydantic_ai._hitl import HitlConfig

        config = HitlConfig(question="Approve?", schema=bool)
        dumped = TypeAdapter(HitlConfig).dump_python(config, mode="json")

        assert dumped["schema"] == "builtins.bool"
        assert dumped["question"] == "Approve?"

    def test_runtime_schema_is_unchanged_after_dump(self) -> None:
        from pydantic import TypeAdapter

        from kitaru.adapters.pydantic_ai._hitl import HitlConfig

        config = HitlConfig(schema=bool)
        TypeAdapter(HitlConfig).dump_python(config, mode="json")
        # Runtime field still holds the live class so downstream identity
        # checks (e.g. ``request.schema is bool``) and ``kitaru.wait(schema=...)``
        # validation continue to work.
        assert config.schema is bool

    def test_nested_in_tool_metadata_dict_serializes(self) -> None:
        """Mirror the failing path: HitlConfig nested inside a ``dict[str, Any]``."""
        from pydantic import TypeAdapter

        from kitaru.adapters.pydantic_ai._hitl import HitlConfig

        config = HitlConfig(schema=bool)
        metadata = {"kitaru_hitl_config": config, "other": "ok"}
        dumped = TypeAdapter(dict).dump_python(metadata, mode="json")

        assert dumped["kitaru_hitl_config"]["schema"] == "builtins.bool"
        assert dumped["other"] == "ok"

    def test_none_schema_passes_through(self) -> None:
        from pydantic import TypeAdapter

        from kitaru.adapters.pydantic_ai._hitl import HitlConfig

        dumped = TypeAdapter(HitlConfig).dump_python(HitlConfig(), mode="json")
        assert dumped["schema"] is None


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

    def test_event_handler_turn_checkpoint_disables_cache_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["streamed-cached-message"]
        )

        def fake_auto_checkpoint_sync(_body, **kwargs):
            assert kwargs["checkpoint_config"]["cache"] is False
            return cached_result

        async def handler(_ctx: Any, _stream: Any) -> None:
            pass

        monkeypatch.setattr(agent, "_auto_checkpoint_sync", fake_auto_checkpoint_sync)

        with _flow_scope(name="demo_flow"):
            assert (
                agent.run_sync("hello", event_stream_handler=handler) is cached_result
            )

        assert agent._last_messages == ["streamed-cached-message"]

    def test_streaming_hook_capability_turn_checkpoint_disables_cache_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["hook-streamed-cached-message"]
        )

        def fake_auto_checkpoint_sync(_body, **kwargs):
            assert kwargs["checkpoint_config"]["cache"] is False
            return cached_result

        def hook(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(agent, "_auto_checkpoint_sync", fake_auto_checkpoint_sync)

        with _flow_scope(name="demo_flow"):
            assert (
                agent.run_sync("hello", capabilities=[Hooks(event=cast(Any, hook))])
                is cached_result
            )

        assert agent._last_messages == ["hook-streamed-cached-message"]

    def test_stream_wrapper_capability_turn_checkpoint_disables_cache_sync(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pydantic_ai.capabilities import AbstractCapability

        from kitaru.runtime import _flow_scope

        class StreamingCapability(AbstractCapability[Any]):
            async def wrap_run_event_stream(
                self,
                ctx: Any,
                *,
                stream: AsyncIterable[Any],
            ) -> AsyncIterable[Any]:
                del ctx
                async for event in stream:
                    yield event

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["wrapper-streamed-cached-message"]
        )

        def fake_auto_checkpoint_sync(_body, **kwargs):
            assert kwargs["checkpoint_config"]["cache"] is False
            return cached_result

        monkeypatch.setattr(agent, "_auto_checkpoint_sync", fake_auto_checkpoint_sync)

        with _flow_scope(name="demo_flow"):
            assert (
                agent.run_sync("hello", capabilities=[StreamingCapability()])
                is cached_result
            )

        assert agent._last_messages == ["wrapper-streamed-cached-message"]

    @pytest.mark.anyio
    async def test_event_handler_turn_checkpoint_disables_cache_async(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["streamed-cached-message"]
        )

        async def fake_auto_checkpoint_async(_body, **kwargs):
            assert kwargs["checkpoint_config"]["cache"] is False
            return cached_result

        async def handler(_ctx: Any, _stream: Any) -> None:
            pass

        monkeypatch.setattr(agent, "_auto_checkpoint_async", fake_auto_checkpoint_async)

        with _flow_scope(name="demo_flow"):
            assert (
                await agent.run("hello", event_stream_handler=handler) is cached_result
            )

        assert agent._last_messages == ["streamed-cached-message"]

    @pytest.mark.anyio
    async def test_streaming_hook_capability_turn_checkpoint_disables_cache_async(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pydantic_ai.capabilities.hooks import Hooks

        from kitaru.runtime import _flow_scope

        agent = self._make_agent(persist=True)
        cached_result = SimpleNamespace(
            all_messages=lambda: ["hook-streamed-cached-message"]
        )

        async def fake_auto_checkpoint_async(_body, **kwargs):
            assert kwargs["checkpoint_config"]["cache"] is False
            return cached_result

        def hook(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(agent, "_auto_checkpoint_async", fake_auto_checkpoint_async)

        with _flow_scope(name="demo_flow"):
            assert (
                await agent.run("hello", capabilities=[Hooks(event=cast(Any, hook))])
                is cached_result
            )

        assert agent._last_messages == ["hook-streamed-cached-message"]

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
async def test_granular_tool_checkpoint_uses_structural_args_and_output_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.adapters.pydantic_ai._tracking import EventTracker
    from kitaru.runtime import _checkpoint_scope, _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def add(a: int, b: int) -> int:
        return a + b

    tracker = EventTracker(agent_name="tool_agent", run_label="tool")
    captured_checkpoint_inputs: dict[str, Any] = {}

    async def fake_checkpoint(**kwargs: Any) -> Any:
        captured_checkpoint_inputs.update(kwargs.get("checkpoint_inputs") or {})
        with _checkpoint_scope(
            name=kwargs["step_name"],
            checkpoint_type=kwargs["config"].get("type", "tool_call"),
        ):
            return await kwargs["body"]()

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fake_checkpoint,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.get_current_tracker",
        lambda: tracker,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.save",
        lambda *args, **kwargs: pytest.fail(
            f"granular tool checkpoint should not manually save {args!r} {kwargs!r}"
        ),
    )
    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["add"]

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("add", {"a": 2, "b": 3}, ctx, tool)

    assert result == 5
    assert captured_checkpoint_inputs == {"tool_args": {"a": 2, "b": 3}}
    tool_events = [event for event in tracker.events if event.kind == "tool_call"]
    assert len(tool_events) == 1
    tool_event = cast(Any, tool_events[0])
    assert tool_event.artifacts == {"args": "tool_args", "result": "output"}


@pytest.mark.anyio
async def test_running_mcp_server_bypasses_granular_checkpoint_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._mcp_server import KitaruMCPServer
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def echo(text: str) -> str:
        return f"echo:{text}"

    wrapped = KitaruMCPServer(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["echo"]

    async def fail_checkpoint(**_kwargs: Any) -> Any:
        raise AssertionError("pre-opened MCP calls must stay on the current loop")

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._mcp_server._mcp_server_is_running",
        lambda _server: True,
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fail_checkpoint,
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("echo", {"text": "preopened"}, ctx, tool)

    assert result == "echo:preopened"


@pytest.mark.anyio
async def test_auto_flow_rejects_preopened_mcp_server_before_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import Agent, FunctionToolset
    from pydantic_ai.models.test import TestModel

    from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent
    from kitaru.adapters.pydantic_ai._mcp_server import KitaruMCPServer

    toolset: FunctionToolset[None] = FunctionToolset()
    running_mcp = KitaruMCPServer(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
    )
    agent = KitaruAgent(Agent(TestModel(), name="auto_flow_mcp_guard"))
    agent._toolsets = [running_mcp]
    auto_flow_called = False

    def fail_auto_flow(_body: Any) -> Any:
        nonlocal auto_flow_called
        auto_flow_called = True
        raise AssertionError("auto-flow should fail before the worker-thread bridge")

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._mcp_server._mcp_server_is_running",
        lambda _server: True,
    )
    monkeypatch.setattr(agent, "_invoke_in_auto_flow", fail_auto_flow)

    with pytest.raises(KitaruUsageError) as exc_info:
        await agent.run("hello")

    message = str(exc_info.value)
    assert "already-running PydanticAI MCP server" in message
    assert "`@kitaru.flow`" in message
    assert "auto-connect" in message
    assert auto_flow_called is False


@pytest.mark.anyio
async def test_closed_mcp_server_still_uses_granular_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._mcp_server import KitaruMCPServer
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def echo(text: str) -> str:
        return f"echo:{text}"

    wrapped = KitaruMCPServer(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["echo"]
    checkpoint_steps = _install_checkpoint_step_recorder(monkeypatch)
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._mcp_server._mcp_server_is_running",
        lambda _server: False,
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("echo", {"text": "auto"}, ctx, tool)

    assert result == "echo:auto"
    assert checkpoint_steps == ["echo_tool"]


def test_mcp_server_running_detection() -> None:
    from kitaru.adapters.pydantic_ai._mcp_server import _mcp_server_is_running

    class RunningProperty:
        is_running = True

    class StoppedProperty:
        is_running = False

    class RunningMethod:
        def is_running(self) -> bool:
            return True

    class ClientOnly:
        _client = object()

    class RunningCount:
        _running_count = 1
        _client = object()

    class StoppedCount:
        _running_count = 0
        _client = object()

    class ExitStackOnly:
        _exit_stack = object()

    class NoClient:
        _client = None

    class RaisingRunning:
        @property
        def is_running(self) -> bool:
            raise RuntimeError("boom")

    assert _mcp_server_is_running(RunningProperty()) is True
    assert _mcp_server_is_running(StoppedProperty()) is False
    assert _mcp_server_is_running(RunningMethod()) is True
    assert _mcp_server_is_running(ClientOnly()) is True
    assert _mcp_server_is_running(RunningCount()) is True
    assert _mcp_server_is_running(StoppedCount()) is False
    assert _mcp_server_is_running(ExitStackOnly()) is False
    assert _mcp_server_is_running(NoClient()) is False
    assert _mcp_server_is_running(RaisingRunning()) is False


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
    assert captured.get("tool_args") == {}
    assert captured.get("exception_metadata") == {"channel": "prod"}
    assert call_count["value"] == 2
    assert checkpoint_steps == []


@pytest.mark.anyio
@pytest.mark.parametrize("tool_capture", [None, "metadata"])
async def test_wait_metadata_omits_payload_when_capture_not_full(
    monkeypatch: pytest.MonkeyPatch,
    tool_capture: Any,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import ApprovalRequired
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    approve_state = {"approved": False}
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish(channel: str = "prod") -> str:
        if not approve_state["approved"]:
            raise ApprovalRequired(metadata={"secret_ticket": "SEC-1"})
        return f"published to {channel}"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(
            tool_capture=tool_capture,
            correlate_otel_spans=False,
        ),
        tool_checkpoint_config_by_name={"publish": False},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["publish"]
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **kwargs: (
            captured.update(kwargs["metadata"])
            or approve_state.update(approved=True)
            or True
        ),
    )

    with _flow_scope(name="demo_flow"):
        await wrapped.call_tool("publish", {"channel": "prod"}, ctx, tool)

    assert captured == {
        "adapter": "pydantic_ai",
        "tool_name": "publish",
        "tool_call_id": "call_123",
    }


@pytest.mark.anyio
async def test_toolset_passes_tool_call_id_to_event_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset

    seen_tool_call_ids: list[str | None] = []
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish() -> str:
        return "published"

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            seen_tool_call_ids.append(tool_call_id)
            return "event-1", SimpleNamespace(
                sequence_index=1,
                turn_index=1,
                fan_out_from=None,
            )

        def record_tool_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(tool_capture="metadata", correlate_otel_spans=False),
        tool_checkpoint_config_by_name={"publish": False},
    )
    ctx = _with_tool_call_id(
        RunContext(deps=None, model=TestModel(), usage=RunUsage()),
        "call_publish",
    )
    tool = (await wrapped.get_tools(ctx))["publish"]
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.get_current_tracker",
        lambda: FakeTracker(),
    )

    assert await wrapped.call_tool("publish", {}, ctx, tool) == "published"

    assert seen_tool_call_ids == ["call_publish"]


@pytest.mark.anyio
async def test_tracked_tool_execution_remains_concurrent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import anyio
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset

    started: list[str] = []
    both_started = anyio.Event()
    toolset: FunctionToolset[None] = FunctionToolset()

    async def _mark_started(name: str) -> None:
        started.append(name)
        if len(started) == 2:
            both_started.set()
        await both_started.wait()

    @toolset.tool_plain
    async def alpha() -> str:
        await _mark_started("alpha")
        return "alpha"

    @toolset.tool_plain
    async def beta() -> str:
        await _mark_started("beta")
        return "beta"

    class FakeTracker:
        def __init__(self) -> None:
            self._counter = 0

        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            del tool_call_id
            self._counter += 1
            return f"event-{self._counter}", SimpleNamespace(
                sequence_index=self._counter,
                turn_index=1,
                fan_out_from=None,
            )

        def record_tool_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    fake_tracker = FakeTracker()
    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(tool_capture="metadata", correlate_otel_spans=False),
        tool_checkpoint_config_by_name={"alpha": False, "beta": False},
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.get_current_tracker",
        lambda: fake_tracker,
    )
    results: dict[str, str] = {}

    async def _run_tool(name: str, tool_call_id: str) -> None:
        ctx = _with_tool_call_id(
            RunContext(deps=None, model=TestModel(), usage=RunUsage()),
            tool_call_id,
        )
        tool = (await wrapped.get_tools(ctx))[name]
        results[name] = await wrapped.call_tool(name, {}, ctx, tool)

    with anyio.fail_after(1):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(_run_tool, "alpha", "call_alpha")
            task_group.start_soon(_run_tool, "beta", "call_beta")

    assert set(started) == {"alpha", "beta"}
    assert results == {"alpha": "alpha", "beta": "beta"}


@pytest.mark.anyio
@pytest.mark.parametrize("tool_capture", [None, "metadata"])
async def test_deferred_event_metadata_omits_payload_when_capture_not_full(
    monkeypatch: pytest.MonkeyPatch,
    tool_capture: Any,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.exceptions import ApprovalRequired
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    recorded_deferred_events: list[dict[str, Any]] = []

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, dict[str, Any]]:
            del tool_call_id
            return "event-1", {}

        def record_tool_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def record_deferred_event(self, **kwargs: Any) -> None:
            recorded_deferred_events.append(kwargs)

    approve_state = {"approved": False}
    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def publish(channel: str = "prod") -> str:
        if not approve_state["approved"]:
            raise ApprovalRequired(metadata={"secret_ticket": "SEC-1"})
        return f"published to {channel}"

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(
            tool_capture=tool_capture,
            correlate_otel_spans=False,
        ),
        tool_checkpoint_config_by_name={"publish": False},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["publish"]

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.get_current_tracker",
        lambda: FakeTracker(),
    )
    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_kwargs: approve_state.update(approved=True) or True,
    )

    with _flow_scope(name="demo_flow"):
        await wrapped.call_tool("publish", {"channel": "prod"}, ctx, tool)

    assert recorded_deferred_events
    assert all(event["metadata"] is None for event in recorded_deferred_events)


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
            KitaruUsageError,
            match="tool_checkpoint_config_by_name",
        ),
    ):
        await wrapped.call_tool("publish", {}, ctx, tool)

    assert checkpoint_steps == ["publish_tool"]


@pytest.mark.anyio
async def test_call_deferred_inside_tool_checkpoint_fails_before_schema_inference(
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
    def defer_release():
        raise CallDeferred(metadata={"ticket": "REL-123"})

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["defer_release"]
    checkpoint_steps = _install_checkpoint_step_recorder(
        monkeypatch,
        enter_checkpoint_scope=True,
    )

    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(
            KitaruUsageError,
            match="tool_checkpoint_config_by_name",
        ),
    ):
        await wrapped.call_tool("defer_release", {}, ctx, tool)

    assert checkpoint_steps == ["defer_release_tool"]


@pytest.mark.anyio
async def test_wait_for_input_inside_tool_checkpoint_gets_adapter_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pydantic_ai import FunctionToolset
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    from kitaru.adapters.pydantic_ai import CapturePolicy, wait_for_input
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    toolset: FunctionToolset[None] = FunctionToolset()

    @toolset.tool_plain
    def ask_user() -> str:
        return wait_for_input(schema=str, question="What should happen?")

    wrapped = kitaruify_toolset(
        toolset,
        capture=CapturePolicy(correlate_otel_spans=False),
        tool_checkpoint_config={},
    )
    ctx = _with_tool_call_id(RunContext(deps=None, model=TestModel(), usage=RunUsage()))
    tool = (await wrapped.get_tools(ctx))["ask_user"]
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
            KitaruUsageError,
            match="tool_checkpoint_config_by_name",
        ),
    ):
        await wrapped.call_tool("ask_user", {}, ctx, tool)

    assert checkpoint_steps == ["ask_user_tool"]


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
