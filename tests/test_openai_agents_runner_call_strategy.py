"""Focused tests for OpenAI runner-call checkpointing and RunState bridging."""

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

pytest.importorskip("agents")

from agents import Agent, RunConfig, Runner, function_tool
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import ResponseOutputMessage, ResponseOutputText
from pydantic import BaseModel
from zenml.client import Client

import kitaru.adapters.openai_agents._runner as openai_runner
from kitaru import flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIApprovalDecision,
    OpenAIRunRequest,
    OpenAIRunStateEnvelope,
)


class StructuredSupportAnswer(BaseModel):
    verdict: str
    confidence: float


class StaticTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(self.text, response_id=f"resp_static_{self.call_count}")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


def _text_response(text: str, *, response_id: str) -> ModelResponse:
    return ModelResponse(
        output=[
            ResponseOutputMessage(
                id=f"msg_{response_id}",
                content=[
                    ResponseOutputText(
                        annotations=[],
                        text=text,
                        type="output_text",
                    )
                ],
                role="assistant",
                status="completed",
                type="message",
            )
        ],
        usage=Usage(requests=1, input_tokens=2, output_tokens=3, total_tokens=5),
        response_id=response_id,
    )


def _wait_for_hydrated_run(exec_id: str) -> Any:
    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    if not run.status.is_finished:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    assert run.status.is_successful
    return run.get_hydrated_version()


def _step_names(hydrated_run: Any) -> set[str]:
    return set(hydrated_run.steps)


@dataclass(frozen=True)
class WorkerContext:
    team_id: str
    user_id: str
    thread_id: str
    worker_name: str = "support-worker"
    is_background: bool = False
    group_chat: bool = False
    chat_mode: str | None = None
    scope_user_id: str | None = None
    scope_group_id: str | None = None
    plugin: str | None = None
    project_id: str | None = None
    tool_settings: dict[str, Any] | None = None
    message_id: str | None = None
    doc_id: str | None = None


@pytest.mark.anyio
async def test_async_bridge_forwards_exact_context_to_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    async def fake_run(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    monkeypatch.setattr(Runner, "run", fake_run)

    result = await openai_runner.run_openai_agent(
        agent=SimpleNamespace(name="agent"),
        input="hello",
        max_turns=3,
        run_config=RunConfig(tracing_disabled=True),
        context=ctx,
    )

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_sync_bridge_forwards_exact_context_to_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    def fake_run_sync(*_args: Any, **kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    monkeypatch.setattr(Runner, "run_sync", fake_run_sync)

    result = openai_runner.run_openai_agent_sync(
        agent=SimpleNamespace(name="agent"),
        input="hello",
        max_turns=3,
        run_config=RunConfig(tracing_disabled=True),
        context=ctx,
    )

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_runner_sync_threads_interruption_payload_capture_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.openai_agents import OpenAICapturePolicy, OpenAIRunResult

    seen_save_payloads: list[bool] = []

    def fake_run_openai_agent_sync(**_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(final_output="ok")

    def fake_build_run_result(_sdk_result: Any, **kwargs: Any) -> OpenAIRunResult:
        seen_save_payloads.append(kwargs["save_interruption_payloads"])
        return OpenAIRunResult(status="completed", final_output="ok")

    runner = KitaruRunner(
        SimpleNamespace(name="capture-agent"),
        checkpoint_strategy="runner_call",
        capture=OpenAICapturePolicy(save_interruption_payloads=False),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "build_run_result",
        fake_build_run_result,
    )

    result = runner.run_sync(OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "ok"
    assert seen_save_payloads == [False]


def test_runner_call_run_sync_forwards_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    seen_contexts: list[object] = []

    def fake_run_openai_agent_sync(**kwargs: Any) -> SimpleNamespace:
        seen_contexts.append(kwargs["context"])
        return SimpleNamespace(final_output="ok")

    runner = KitaruRunner(
        SimpleNamespace(name="context-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        fake_run_openai_agent_sync,
    )

    result = runner.run_sync(OpenAIRunRequest.start("hello"), context=ctx)

    assert result.final_output == "ok"
    assert seen_contexts == [ctx]


def test_runner_call_cache_key_omits_context_when_context_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_payloads: list[dict[str, Any]] = []

    def fake_checkpoint_cache_key(payload: dict[str, Any]) -> str:
        seen_payloads.append(payload)
        return "cache-key"

    monkeypatch.setitem(
        KitaruRunner._runner_call_cache_key.__globals__,
        "checkpoint_cache_key",
        fake_checkpoint_cache_key,
    )
    runner = KitaruRunner(
        SimpleNamespace(name="no-context-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    cache_key = runner._runner_call_cache_key(
        OpenAIRunRequest.start("hello"),
        agent=runner.agent,
        run_config=RunConfig(tracing_disabled=True),
        context_cache_identity=None,
    )

    assert cache_key == "cache-key"
    assert "context" not in seen_payloads[0]


def test_runner_call_cache_identity_varies_by_structural_context() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="cache-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    request = OpenAIRunRequest.start("hello")
    run_config = RunConfig(tracing_disabled=True)

    team_a_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=runner._context_cache_identity(
            WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
        ),
    )
    team_b_key = runner._runner_call_cache_key(
        request,
        agent=runner.agent,
        run_config=run_config,
        context_cache_identity=runner._context_cache_identity(
            WorkerContext(team_id="team-b", user_id="user-1", thread_id="thread-1")
        ),
    )

    assert team_a_key != team_b_key


def test_custom_context_cache_identity_can_ignore_per_run_fields() -> None:
    runner = KitaruRunner(
        SimpleNamespace(name="projected-agent"),
        checkpoint_strategy="runner_call",
        context_cache_identity=lambda ctx: {
            "team_id": ctx.team_id,
            "user_id": ctx.user_id,
            "thread_id": ctx.thread_id,
            "worker_name": ctx.worker_name,
            "is_background": ctx.is_background,
            "group_chat": ctx.group_chat,
            "chat_mode": ctx.chat_mode,
            "scope_user_id": ctx.scope_user_id,
            "scope_group_id": ctx.scope_group_id,
            "plugin": ctx.plugin,
            "project_id": ctx.project_id,
            "tool_settings": ctx.tool_settings,
        },
    )

    first = runner._context_cache_identity(
        WorkerContext(
            team_id="team-a",
            user_id="user-1",
            thread_id="thread-1",
            tool_settings={"enabled": True, "limit": 3},
            message_id="msg-1",
            doc_id="doc-1",
        )
    )
    second = runner._context_cache_identity(
        WorkerContext(
            team_id="team-a",
            user_id="user-1",
            thread_id="thread-1",
            tool_settings={"enabled": True, "limit": 3},
            message_id="msg-2",
            doc_id="doc-2",
        )
    )

    assert first == second
    assert "message_id" not in first
    assert "doc_id" not in first
    assert first["tool_settings"] == {"enabled": True, "limit": 3}


def test_default_context_cache_identity_uses_pure_data_structure() -> None:
    runner = KitaruRunner(SimpleNamespace(name="data-agent"))

    first = runner._context_cache_identity(
        WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    )
    second = runner._context_cache_identity(
        WorkerContext(team_id="team-a", user_id="user-1", thread_id="thread-1")
    )

    assert first == second
    assert first["fields"]["team_id"] == "team-a"


def test_context_cache_identity_sorts_sets_deterministically() -> None:
    runner = KitaruRunner(SimpleNamespace(name="set-agent"))

    first = runner._context_cache_identity({"enabled_tools": {"search", "crm"}})
    second = runner._context_cache_identity({"enabled_tools": {"crm", "search"}})

    assert first == second
    assert first["enabled_tools"] == {
        "collection_type": "set",
        "items": ["crm", "search"],
    }


def test_context_cache_identity_is_cycle_safe() -> None:
    runner = KitaruRunner(SimpleNamespace(name="cycle-agent"))
    cycle: list[Any] = []
    cycle.append(cycle)

    identity = runner._context_cache_identity({"cycle": cycle})

    cycle_item = identity["cycle"]["items"][0]
    assert cycle_item["serialization_error"] == "cycle_detected"
    assert "opaque_cache_token" in cycle_item


def test_context_cache_identity_stops_before_oversized_collections() -> None:
    from kitaru.adapters.openai_agents._serialization import stable_cache_identity

    identity = stable_cache_identity(
        ["first", "second", "third"],
        opaque_objects_unique=True,
        max_items=2,
    )

    assert identity["serialization_error"] == "max_items_exceeded"
    assert "items" not in identity
    assert "first" not in repr(identity)


def test_opaque_context_cache_identity_is_distinct_per_object() -> None:
    class OpaqueContext:
        pass

    runner = KitaruRunner(SimpleNamespace(name="opaque-agent"))

    first = runner._context_cache_identity(OpaqueContext())
    second = runner._context_cache_identity(OpaqueContext())

    assert first["python_type"] == second["python_type"]
    assert first != second


def test_runner_call_strategy_preserves_structured_final_output() -> None:
    model = StaticTextModel('{"verdict":"safe","confidence":0.91}')
    agent = Agent(
        name="runner-structured",
        model=model,
        output_type=StructuredSupportAnswer,
    )
    runner = KitaruRunner(
        agent,
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    result = runner.run_sync(OpenAIRunRequest.start("return structured result"))

    assert result.status == "completed"
    assert isinstance(result.final_output, StructuredSupportAnswer)
    assert result.final_output.verdict == "safe"
    assert result.final_output.confidence == 0.91


def test_runner_call_strategy_checkpoints_outer_runner_and_caches(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ = monkeypatch
    model = StaticTextModel("outer call result")
    agent_name = f"openai_runner_call_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def runner_call_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    first = runner_call_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    names = _step_names(first_hydrated)
    assert any("openai_runner_call" in name for name in names)
    assert not any("openai_model_call" in name for name in names)
    assert model.call_count == 1

    second = runner_call_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert model.call_count == 1


def test_runner_call_strategy_does_not_invoke_calls_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._agent as openai_agent_module
    import kitaru.adapters.openai_agents._model as openai_model_module
    import kitaru.adapters.openai_agents._tools as openai_tools_module

    @function_tool
    def double_value(value: int) -> str:
        return f"doubled={value * 2}"

    def fail_model_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap model calls")

    def fail_tool_wrapper(*_args: object, **_kwargs: object) -> object:
        pytest.fail("runner_call must not wrap function tools")

    monkeypatch.setattr(
        openai_model_module,
        "kitaruify_openai_model",
        fail_model_wrapper,
    )
    monkeypatch.setattr(
        openai_tools_module,
        "kitaruify_openai_tools",
        fail_tool_wrapper,
    )
    monkeypatch.setattr(
        openai_agent_module,
        "run_openai_agent_sync",
        lambda **_kwargs: SimpleNamespace(final_output="ok"),
    )

    runner = KitaruRunner(
        Agent(name="coarse", model=StaticTextModel("ok"), tools=[double_value]),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    result = runner.run_sync(OpenAIRunRequest.start("hello"))

    assert result.status == "completed"
    assert result.final_output == "ok"


def test_interruption_summary_omits_payloads_when_capture_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    interruption = {
        "tool_name": "send_email",
        "call_id": "call_sensitive_email",
        "message": "approval required",
        "arguments": {"message": "SECRET_DO_NOT_LOG"},
    }
    sdk_result = SimpleNamespace(
        interruptions=[interruption],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    assert result.status == "interrupted"
    summary = result.interruptions[0]
    assert summary.tool_name == "send_email"
    assert summary.call_id == "call_sensitive_email"
    assert summary.message == "approval required"
    assert summary.arguments is None
    assert summary.arguments_preview is None
    assert "SECRET_DO_NOT_LOG" not in result.model_dump_json()


def test_interruption_summary_does_not_mine_nested_identity_when_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    sdk_result = SimpleNamespace(
        interruptions=[
            {
                "arguments": {
                    "name": "SECRET_DO_NOT_LOG_NAME",
                    "call_id": "SECRET_DO_NOT_LOG_CALL_ID",
                },
            }
        ],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    summary = result.interruptions[0]
    assert summary.tool_name is None
    assert summary.call_id is None
    assert summary.arguments is None
    assert summary.arguments_preview is None
    serialized = result.model_dump_json()
    assert "SECRET_DO_NOT_LOG_NAME" not in serialized
    assert "SECRET_DO_NOT_LOG_CALL_ID" not in serialized


def test_interruption_summary_does_not_promote_argument_message_when_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    sdk_result = SimpleNamespace(
        interruptions=[
            {
                "tool_name": "send_email",
                "call_id": "call_sensitive_email",
                "arguments": {"message": "SECRET_DO_NOT_LOG"},
            }
        ],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
        save_interruption_payloads=False,
    )

    summary = result.interruptions[0]
    assert summary.message is None
    assert summary.arguments is None
    assert summary.arguments_preview is None
    assert "SECRET_DO_NOT_LOG" not in result.model_dump_json()


def test_interruption_summary_keeps_payloads_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")

    class FakeState:
        def to_json(self, **_kwargs: object) -> dict[str, object]:
            return {"current_turn": 1}

    interruption = {
        "tool_name": "send_email",
        "call_id": "call_sensitive_email",
        "message": "approval required",
        "arguments": {"message": "SECRET_VISIBLE"},
    }
    sdk_result = SimpleNamespace(
        interruptions=[interruption],
        to_state=lambda: FakeState(),
        last_response_id="resp_interrupted",
    )

    result = openai_runner.build_run_result(
        sdk_result,
        strict_sdk_version=True,
    )

    summary = result.interruptions[0]
    assert summary.arguments is not None
    assert summary.arguments_preview is not None
    assert "SECRET_VISIBLE" in result.model_dump_json()


def test_run_state_envelope_serialization_and_approval_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._runner as openai_runner

    class FakeState:
        def __init__(self) -> None:
            self._current_step = SimpleNamespace(interruptions=["first", "second"])
            self.approved: list[object] = []
            self.rejected: list[tuple[object, str | None]] = []

        def to_json(self, **kwargs: object) -> dict[str, object]:
            return {
                "current_turn": 1,
                "strict_context": kwargs["strict_context"],
                "has_context_serializer": callable(kwargs["context_serializer"]),
            }

        def approve(self, item: object) -> None:
            self.approved.append(item)

        def reject(
            self,
            item: object,
            *,
            rejection_message: str | None = None,
        ) -> None:
            self.rejected.append((item, rejection_message))

    monkeypatch.setattr(openai_runner, "agents_sdk_version", lambda: "0.15.0")
    state = FakeState()

    envelope = openai_runner.serialize_run_state(
        state,
        strict_sdk_version=True,
        context_serializer=lambda value: value,
        strict_context=True,
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(interruption_index=1, approve=True),
    )
    openai_runner.apply_approval_decision(
        state,
        OpenAIApprovalDecision(
            interruption_index=0,
            approve=False,
            rejection_message="nope",
        ),
    )

    assert envelope.agents_sdk_version == "0.15.0"
    assert envelope.state_json["current_turn"] == 1
    assert state.approved == ["second"]
    assert state.rejected == [("first", "nope")]


def test_runner_resume_applies_decision_before_sdk_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_state = SimpleNamespace(
        _current_step=SimpleNamespace(interruptions=["approval-item"]),
        approved=[],
    )

    def approve(item: object) -> None:
        fake_state.approved.append(item)

    fake_state.approve = approve
    seen_input: list[object] = []

    runner = KitaruRunner(
        SimpleNamespace(name="resume-agent"),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    monkeypatch.setitem(
        runner._sdk_input_sync.__globals__,
        "deserialize_run_state_sync",
        lambda *_args, **_kwargs: fake_state,
    )
    monkeypatch.setitem(
        runner._run_sdk_sync.__globals__,
        "run_openai_agent_sync",
        lambda **kwargs: (
            seen_input.append(kwargs["input"])
            or SimpleNamespace(final_output="resumed")
        ),
    )
    request = OpenAIRunRequest.resume(
        OpenAIRunStateEnvelope(
            agents_sdk_version="0.15.0",
            state_json={"current_turn": 1},
        ),
        OpenAIApprovalDecision(approve=True),
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert result.final_output == "resumed"
    assert fake_state.approved == ["approval-item"]
    assert seen_input == [fake_state]
