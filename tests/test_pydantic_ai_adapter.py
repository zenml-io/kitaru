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


class TestWaitCallSuffix:
    """Per-call wait-name suffix is unique per call, stable on replay."""

    def test_sanitized_tool_call_id_is_used_when_present(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_abc/123.xyz") == "call_abc_123_xyz"

    def test_empty_tool_call_id_falls_back_to_uuid(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        suffix = _wait_call_suffix("")
        assert len(suffix) == 8
        assert all(ch.isalnum() for ch in suffix)

    def test_none_tool_call_id_falls_back_to_uuid(self) -> None:
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        suffix = _wait_call_suffix(None)
        assert len(suffix) == 8
        assert all(ch.isalnum() for ch in suffix)

    def test_same_tool_call_id_produces_same_suffix(self) -> None:
        """Stable across replays: identical tool_call_id => identical suffix."""
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_xyz") == _wait_call_suffix("call_xyz")

    def test_different_tool_call_ids_produce_different_suffixes(self) -> None:
        """Unique per call: different tool_call_ids => different suffixes."""
        from kitaru.adapters.pydantic_ai._toolset import _wait_call_suffix

        assert _wait_call_suffix("call_a") != _wait_call_suffix("call_b")


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
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["approve_release"]

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.kitaru.wait",
        lambda **_: True,
    )

    with _flow_scope(name="demo_flow"):
        result = await wrapped.call_tool("approve_release", {}, ctx, tool)

    assert result is True


@pytest.mark.anyio
async def test_named_hitl_tool_uses_name_as_unique_wait_base(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import Any

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
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
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
async def test_approval_required_routes_through_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import Any

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
        toolset, capture=CapturePolicy(correlate_otel_spans=False)
    )
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["publish"]

    captured: dict[str, Any] = {}

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
    ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
    tool = (await wrapped.get_tools(ctx))["defer_release"]

    with (
        _flow_scope(name="demo_flow"),
        pytest.raises(KitaruUsageError, match="Cannot infer a wait schema"),
    ):
        await wrapped.call_tool("defer_release", {}, ctx, tool)
