"""Focused tests for OpenAI Agents SDK call-level checkpointing."""

import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

pytest.importorskip("agents")

from agents import Agent, RunConfig, RunContextWrapper, function_tool, handoff
from agents.exceptions import ToolInputGuardrailTripwireTriggered
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.tool_guardrails import ToolGuardrailFunctionOutput, tool_input_guardrail
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)
from pydantic import BaseModel
from zenml.client import Client

import kitaru
from kitaru import SandboxCommandResult, flow
from kitaru.adapters.openai_agents import (
    KitaruRunner,
    OpenAIRunRequest,
    sandbox_command_tool,
)
from kitaru.errors import KitaruBackendError, KitaruStateError, KitaruUsageError


def test_synthetic_checkpoint_marks_flow_result_non_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.openai_agents import _utils

    captured: dict[str, Any] = {}

    class FakeCheckpoint:
        _step = object()

    def fake_checkpoint(**kwargs: Any) -> Any:
        captured.update(kwargs)

        def decorate(func: Any) -> FakeCheckpoint:
            captured["decorated_name"] = func.__name__
            return FakeCheckpoint()

        return decorate

    monkeypatch.setattr(_utils, "_synthetic_checkpoint", fake_checkpoint)

    _utils._build_checkpoint_step(
        config={"type": "tool_call", "retries": 2},
        step_name="tool call",
        body=lambda: "ok",
    )

    assert captured["flow_result_candidate"] is False
    assert captured["type"] == "tool_call"
    assert captured["retries"] == 2
    assert captured["decorated_name"] == "tool_call"


class StaticTextModel(Model):
    def __init__(self, text: str) -> None:
        self.text = text
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return _text_response(self.text, response_id=f"resp_text_{self.call_count}")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class RepeatedToolCallingModel(Model):
    def __init__(self) -> None:
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        if self.call_count in {1, 2}:
            return ModelResponse(
                output=[
                    ResponseFunctionToolCall(
                        arguments='{"value": 4}',
                        call_id=f"call_repeat_{self.call_count}",
                        id=f"fc_repeat_{self.call_count}",
                        name="double_value",
                        status="completed",
                        type="function_call",
                    )
                ],
                usage=Usage(
                    requests=1, input_tokens=3, output_tokens=2, total_tokens=5
                ),
                response_id=f"resp_repeat_{self.call_count}",
            )
        return _text_response("repeated tool complete", response_id="resp_repeat_final")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class StructuredSupportAnswer(BaseModel):
    verdict: str
    confidence: float


@dataclass(frozen=True)
class WorkerContext:
    team_id: str
    user_id: str
    thread_id: str
    worker_name: str = "support-worker"
    message_id: str | None = None
    doc_id: str | None = None


class ToolCallingModel(Model):
    def __init__(self) -> None:
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        if self.call_count % 2 == 1:
            return ModelResponse(
                output=[
                    ResponseFunctionToolCall(
                        arguments='{"value": 4}',
                        call_id="call_cached_tool",
                        id="fc_1",
                        name="double_value",
                        status="completed",
                        type="function_call",
                    )
                ],
                usage=Usage(
                    requests=1, input_tokens=3, output_tokens=2, total_tokens=5
                ),
                response_id="resp_tool_call",
            )
        return _text_response("tool complete", response_id="resp_final")

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class HandoffCallingModel(Model):
    def __init__(self, *, handoff_tool_name: str) -> None:
        self.handoff_tool_name = handoff_tool_name
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        return ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_transfer_to_child",
                    id="fc_transfer_to_child",
                    name=self.handoff_tool_name,
                    status="completed",
                    type="function_call",
                )
            ],
            usage=Usage(requests=1, input_tokens=3, output_tokens=2, total_tokens=5),
            response_id="resp_handoff_call",
        )

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class SandboxToolCallingModel(Model):
    def __init__(self, *, tool_name: str = "kitaru_sandbox_command") -> None:
        self.tool_name = tool_name
        self.call_count = 0
        self.seen_function_call_outputs: list[str] = []

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        sdk_input = _args[1] if len(_args) > 1 else None
        function_call_outputs = _function_call_outputs(sdk_input)
        if function_call_outputs:
            self.seen_function_call_outputs.extend(function_call_outputs)
            return _text_response(
                "sandbox command complete",
                response_id=f"resp_sandbox_final_{self.call_count}",
            )
        return ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    arguments='{"command":"python --version","cwd":"/workspace"}',
                    call_id="call_sandbox_command",
                    id="fc_sandbox_command",
                    name=self.tool_name,
                    status="completed",
                    type="function_call",
                )
            ],
            usage=Usage(requests=1, input_tokens=3, output_tokens=2, total_tokens=5),
            response_id="resp_sandbox_tool_call",
        )

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class HandoffSandboxToolCallingModel(SandboxToolCallingModel):
    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        sdk_input = _args[1] if len(_args) > 1 else None
        sandbox_outputs = [
            output
            for output in _function_call_outputs(sdk_input)
            if '"exit_code"' in output
        ]
        if sandbox_outputs:
            self.seen_function_call_outputs.extend(sandbox_outputs)
            return _text_response(
                "sandbox command complete",
                response_id=f"resp_sandbox_final_{self.call_count}",
            )
        return ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    arguments='{"command":"python --version","cwd":"/workspace"}',
                    call_id="call_sandbox_command",
                    id="fc_sandbox_command",
                    name=self.tool_name,
                    status="completed",
                    type="function_call",
                )
            ],
            usage=Usage(requests=1, input_tokens=3, output_tokens=2, total_tokens=5),
            response_id="resp_sandbox_tool_call",
        )


class GuardrailToolCallingModel(Model):
    def __init__(
        self,
        tool_calls: list[ResponseFunctionToolCall],
        *,
        final_text: str = "guardrail tool flow complete",
    ) -> None:
        self.tool_calls = tool_calls
        self.final_text = final_text
        self.call_count = 0
        self.seen_function_call_outputs: list[str] = []

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        sdk_input = _args[1] if len(_args) > 1 else None
        function_call_outputs = _function_call_outputs(sdk_input)
        if function_call_outputs:
            self.seen_function_call_outputs.extend(function_call_outputs)
            return _text_response(
                self.final_text,
                response_id=f"resp_guardrail_final_{self.call_count}",
            )
        return ModelResponse(
            output=cast(Any, self.tool_calls),
            usage=Usage(requests=1, input_tokens=3, output_tokens=2, total_tokens=5),
            response_id="resp_guardrail_tool_call",
        )

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


class ContextToolCallingModel(Model):
    def __init__(self) -> None:
        self.call_count = 0

    async def get_response(self, *_args: Any, **_kwargs: Any) -> ModelResponse:
        self.call_count += 1
        sdk_input = _args[1] if len(_args) > 1 else None
        if _contains_function_call_output(sdk_input):
            return _text_response(
                "context tool complete", response_id="resp_context_final"
            )
        return ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_context_tool",
                    id="fc_context",
                    name="team_label",
                    status="completed",
                    type="function_call",
                )
            ],
            usage=Usage(requests=1, input_tokens=3, output_tokens=2, total_tokens=5),
            response_id="resp_context_tool_call",
        )

    def stream_response(self, *_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError


def _contains_function_call_output(value: Any) -> bool:
    return next(_iter_function_call_outputs(value), None) is not None


def _function_call_outputs(value: Any) -> list[str]:
    return list(_iter_function_call_outputs(value))


def _iter_function_call_outputs(value: Any) -> Iterator[str]:
    if isinstance(value, list | tuple):
        for item in value:
            yield from _iter_function_call_outputs(item)
        return
    if isinstance(value, dict):
        if value.get("type") == "function_call_output":
            output = value.get("output")
            if isinstance(output, str):
                yield output
            return
        for item in value.values():
            yield from _iter_function_call_outputs(item)
        return
    if getattr(value, "type", None) == "function_call_output":
        output = getattr(value, "output", None)
        if isinstance(output, str):
            yield output


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


def _input_names_by_step(hydrated_run: Any) -> list[set[str]]:
    return [set(step.inputs) for step in hydrated_run.steps.values()]


def _artifact_names(hydrated_run: Any) -> list[str]:
    names: list[str] = []
    for step in hydrated_run.steps.values():
        for artifacts in step.outputs.values():
            names.extend(artifact.name for artifact in artifacts)
    return names


def _events(event_map: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [event for events in event_map.values() for event in events]


def _fake_sandbox_result(
    *,
    stdout: str = "Python 3.12.0\n",
    stderr: str = "",
    exit_code: int = 0,
    stdout_truncated: bool = False,
    stderr_truncated: bool = False,
    timed_out: bool = False,
    cleanup: str = "destroy",
    cleanup_succeeded: bool = True,
    cleanup_error: str | None = None,
) -> SandboxCommandResult:
    return SandboxCommandResult(
        command="python --version",
        cwd="/workspace",
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        stdout_truncated=stdout_truncated,
        stderr_truncated=stderr_truncated,
        timed_out=timed_out,
        stack_id="stack-secret",
        stack_name="prod-gpu",
        sandbox_id="sandbox-secret",
        sandbox_name="expensive-sandbox",
        session_id="session-secret",
        cleanup=cast(Any, cleanup),
        cleanup_succeeded=cleanup_succeeded,
        cleanup_error=cleanup_error,
    )


def _active_sandbox_identity(
    *,
    stack_id: str = "stack-id",
    stack_name: str = "dev-stack",
    sandbox_id: str | None = "sandbox-id",
    sandbox_name: str | None = "dev-sandbox",
) -> dict[str, str | None]:
    return {
        "kind": "active_sandbox",
        "stack_id": stack_id,
        "stack_name": stack_name,
        "sandbox_id": sandbox_id,
        "sandbox_name": sandbox_name,
    }


def _patch_active_sandbox_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stack_id: str = "stack-id",
    stack_name: str = "dev-stack",
    sandbox_id: str | None = "sandbox-id",
    sandbox_name: str | None = "dev-sandbox",
) -> dict[str, str | None]:
    import kitaru.config as kitaru_config

    identity = _active_sandbox_identity(
        stack_id=stack_id,
        stack_name=stack_name,
        sandbox_id=sandbox_id,
        sandbox_name=sandbox_name,
    )
    monkeypatch.setattr(
        kitaru_config,
        "_active_sandbox_cache_identity",
        lambda: identity,
    )
    return identity


def _expected_model_input_envelope(
    *,
    system_instructions: str | None = None,
    input_value: Any = None,
    model_settings: Any = None,
    previous_response_id: str | None = None,
    conversation_id: str | None = None,
    prompt: Any = None,
) -> dict[str, Any]:
    return {
        "system_instructions": system_instructions,
        "input": input_value,
        "model_settings": model_settings,
        "previous_response_id": previous_response_id,
        "conversation_id": conversation_id,
        "prompt": prompt,
    }


class TestOpenAISandboxCommandToolFactory:
    def test_factory_returns_function_tool_with_configured_metadata(self) -> None:
        from agents.tool import FunctionTool

        tool = sandbox_command_tool(
            name="run_project_command",
            description="Run one safe project command.",
        )

        assert isinstance(tool, FunctionTool)
        assert tool.name == "run_project_command"
        assert tool.description == "Run one safe project command."
        assert "sandbox_command_tool" not in kitaru.__all__
        assert not hasattr(kitaru, "sandbox_command_tool")

    def test_schema_exposes_only_command_and_cwd(self) -> None:
        tool = sandbox_command_tool()

        schema = tool.params_json_schema
        assert schema["required"] == ["command"]
        assert set(schema["properties"]) == {"command", "cwd"}
        assert "env" not in schema["properties"]
        assert "max_chars" not in schema["properties"]
        assert "timeout_seconds" not in schema["properties"]
        assert "cleanup" not in schema["properties"]
        assert schema["additionalProperties"] is False

    @pytest.mark.anyio
    async def test_success_result_is_compact_model_visible_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen_calls: list[dict[str, Any]] = []

        def fake_run_sandbox_command(
            command: str, **kwargs: Any
        ) -> SandboxCommandResult:
            seen_calls.append({"command": command, **kwargs})
            return _fake_sandbox_result(
                stdout="ok\n",
                stderr="warning\n",
                cleanup_succeeded=False,
                cleanup_error="destroy not supported",
            )

        monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
        tool = sandbox_command_tool(max_chars=20_000, cleanup="close")

        result = await tool.on_invoke_tool(
            cast(Any, SimpleNamespace()),
            '{"command":"python --version","cwd":"/workspace"}',
        )

        assert seen_calls == [
            {
                "command": "python --version",
                "cwd": "/workspace",
                "max_chars": 20_000,
                "timeout_seconds": 30.0,
                "cleanup": "close",
            }
        ]
        payload = json.loads(result)
        assert payload == {
            "stdout": "ok\n",
            "stderr": "warning\n",
            "exit_code": 0,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "timed_out": False,
            "cleanup_succeeded": False,
            "cleanup_error": "destroy not supported",
        }
        assert "stack_id" not in payload
        assert "stack_name" not in payload
        assert "sandbox_id" not in payload
        assert "sandbox_name" not in payload
        assert "session_id" not in payload

    @pytest.mark.anyio
    async def test_nonzero_exit_result_returns_data_not_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            kitaru,
            "run_sandbox_command",
            lambda *_args, **_kwargs: _fake_sandbox_result(
                stdout="",
                stderr="command failed\n",
                exit_code=2,
                stderr_truncated=True,
            ),
        )
        tool = sandbox_command_tool()

        result = await tool.on_invoke_tool(
            cast(Any, SimpleNamespace()),
            '{"command":"bad-command"}',
        )

        payload = json.loads(result)
        assert payload["exit_code"] == 2
        assert payload["stderr"] == "command failed\n"
        assert payload["stderr_truncated"] is True

    @pytest.mark.anyio
    async def test_timeout_result_returns_structured_json(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen_calls: list[dict[str, Any]] = []

        def fake_run_sandbox_command(
            command: str, **kwargs: Any
        ) -> SandboxCommandResult:
            seen_calls.append({"command": command, **kwargs})
            return _fake_sandbox_result(
                stdout="",
                stderr="Sandbox command timed out after 3 seconds.",
                exit_code=-1,
                timed_out=True,
            )

        monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
        tool = sandbox_command_tool(timeout_seconds=3)

        result = await tool.on_invoke_tool(
            cast(Any, SimpleNamespace()),
            '{"command":"sleep 999999"}',
        )

        assert seen_calls == [
            {
                "command": "sleep 999999",
                "cwd": None,
                "max_chars": 1_048_576,
                "timeout_seconds": 3.0,
                "cleanup": "destroy",
            }
        ]
        payload = json.loads(result)
        assert payload["timed_out"] is True
        assert payload["exit_code"] == -1
        assert "timed out after 3 seconds" in payload["stderr"]

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("input_json", "message"),
        [
            ("not json", "valid JSON"),
            ("[]", "JSON object"),
            ('{"command":""}', "non-empty string"),
            ('{"command":"python --version","cwd":3}', "string or null"),
            ('{"command":"python --version","env":{"SECRET":"value"}}', "env"),
        ],
    )
    async def test_invalid_model_input_returns_error_json(
        self,
        input_json: str,
        message: str,
    ) -> None:
        tool = sandbox_command_tool()

        result = await tool.on_invoke_tool(cast(Any, SimpleNamespace()), input_json)

        payload = json.loads(result)
        assert payload["error"]["code"] == "invalid_tool_input"
        assert message in payload["error"]["message"]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": ""},
            {"description": ""},
            {"max_chars": -1},
            {"max_chars": True},
            {"timeout_seconds": 0},
            {"timeout_seconds": float("nan")},
            {"timeout_seconds": True},
            {"cleanup": cast(Any, "keep")},
        ],
    )
    def test_invalid_factory_options_raise_usage_error(
        self,
        kwargs: dict[str, Any],
    ) -> None:
        with pytest.raises(KitaruUsageError):
            sandbox_command_tool(**kwargs)

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        "error",
        [
            KitaruStateError("active stack has no sandbox"),
            KitaruBackendError("sandbox backend failed"),
        ],
    )
    async def test_sandbox_helper_errors_propagate(
        self,
        monkeypatch: pytest.MonkeyPatch,
        error: Exception,
    ) -> None:
        def fake_run_sandbox_command(
            *_args: Any, **_kwargs: Any
        ) -> SandboxCommandResult:
            raise error

        monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
        tool = sandbox_command_tool()

        with pytest.raises(type(error), match=str(error)):
            await tool.on_invoke_tool(
                cast(Any, SimpleNamespace()),
                '{"command":"python --version"}',
            )


def test_openai_artifact_names_use_role_first_suffix_namespace() -> None:
    from kitaru.adapters.openai_agents._tracking import EventTracker, artifact_name

    assert (
        artifact_name("agent_ab12cd34_llm_call_1", "input")
        == "llm_call_1_input__agent_ab12cd34"
    )
    assert (
        artifact_name("agent_ab12cd34_tool_call_2", "result")
        == "tool_call_2_result__agent_ab12cd34"
    )

    tracker = EventTracker(agent_name="Agent Name", run_label="ab12cd34")
    assert tracker.event_log_artifact_name == "event_log__Agent_Name_ab12cd34"
    assert tracker.run_summary_artifact_name == "run_summary__Agent_Name_ab12cd34"


def test_openai_event_tracker_records_checkpoint_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.openai_agents import _tracking

    checkpoint_ids = iter(["checkpoint-model", "checkpoint-tool"])
    checkpoint_names = iter(
        [
            "agent_openai_model_call",
            "agent_double_value_tool_call",
        ]
    )
    monkeypatch.setattr(_tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(
        _tracking,
        "get_current_checkpoint_id",
        lambda: next(checkpoint_ids),
    )
    monkeypatch.setattr(
        _tracking,
        "get_current_checkpoint_name",
        lambda: next(checkpoint_names),
    )

    tracker = _tracking.EventTracker(agent_name="Agent Name", run_label="ab12cd34")
    model_id, model_context = tracker.start_llm_event()
    tool_id, tool_context = tracker.start_tool_event(tool_call_id="call_1")
    tracker.record_event(
        model_id,
        model_context,
        kind="llm_call",
        status="completed",
        duration_ms=1.0,
        artifacts={"response": "output"},
    )
    tracker.record_event(
        tool_id,
        tool_context,
        kind="tool_call",
        status="completed",
        duration_ms=1.0,
        artifacts={"result": "output"},
    )

    events = list(tracker.events)
    assert [event.checkpoint_id for event in events] == [
        "checkpoint-model",
        "checkpoint-tool",
    ]
    assert [event.checkpoint_name for event in events] == [
        "agent_openai_model_call",
        "agent_double_value_tool_call",
    ]


class TestOpenAIEventTrackerToolCallOrdering:
    def _record_completed_model(self, tracker: Any) -> tuple[str, Any]:
        event_id, event_context = tracker.start_llm_event()
        tracker.record_event(
            event_id,
            event_context,
            kind="llm_call",
            status="completed",
            duration_ms=1.0,
            artifacts={},
            metadata={"response_id": "resp_ordering"},
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
        tracker.record_event(
            event_id,
            event_context,
            kind="tool_call",
            status="completed",
            duration_ms=1.0,
            artifacts={},
            metadata={"tool_name": name},
        )

    def test_reserved_tool_ids_follow_model_order_when_start_order_reverses(
        self,
    ) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_id != beta_id
        assert model_context.sequence_index < alpha_context.sequence_index
        assert alpha_context.sequence_index < beta_context.sequence_index

    def test_reverse_completion_order_sorts_events_persisted_log_and_summary(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from kitaru.adapters.openai_agents import _tracking

        tracker = _tracking.EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])
        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(tracker, beta_id, beta_context, name="beta")
        self._record_completed_tool(tracker, alpha_id, alpha_context, name="alpha")

        assert [event.event_id for event in tracker.events] == [
            model_id,
            alpha_id,
            beta_id,
        ]

        logged: dict[str, Any] = {}
        monkeypatch.setattr(_tracking, "is_inside_flow", lambda: True)
        monkeypatch.setattr(
            _tracking.kitaru,
            "log",
            lambda **kwargs: logged.update(kwargs),
        )

        tracker.persist()

        events_dump = logged["openai_agents_events"][tracker.run_label]
        summary_dump = logged["openai_agents_run_summaries"][tracker.run_label]
        assert [event["event_id"] for event in events_dump] == [
            model_id,
            alpha_id,
            beta_id,
        ]
        assert summary_dump["event_ids_in_order"] == [model_id, alpha_id, beta_id]
        assert summary_dump["total_events"] == 3

    def test_missing_or_unreserved_tool_call_id_keeps_counter_fallback(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, model_context = self._record_completed_model(tracker)

        event_id, event_context = tracker.start_tool_event(tool_call_id=None)

        assert event_id.startswith(
            f"{tracker.agent_name}_{tracker.run_label}_tool_call_"
        )
        assert event_context.sequence_index > model_context.sequence_index

    def test_abandoned_reserved_tool_slot_does_not_count_or_leak(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")
        self._record_completed_tool(
            tracker,
            alpha_id,
            alpha_context,
            name="alpha",
        )

        summary = tracker.build_run_summary()

        assert summary["total_events"] == 2
        assert summary["event_ids_in_order"] == [model_id, alpha_id]

        next_model_id, next_model_context = tracker.start_llm_event()
        fallback_id, fallback_context = tracker.start_tool_event(
            tool_call_id="call_beta"
        )
        assert fallback_id != next_model_id
        assert fallback_context.sequence_index > next_model_context.sequence_index

    def test_nested_llm_start_does_not_clear_sibling_tool_reservation(self) -> None:
        from kitaru.adapters.openai_agents._tracking import EventTracker

        tracker = EventTracker(agent_name="ordering_agent", run_label="test")
        _model_id, _model_context = self._record_completed_model(tracker)
        tracker.reserve_tool_call_order(["call_alpha", "call_beta"])

        beta_id, beta_context = tracker.start_tool_event(tool_call_id="call_beta")
        nested_model_id, nested_context = tracker.start_llm_event()
        alpha_id, alpha_context = tracker.start_tool_event(tool_call_id="call_alpha")

        assert alpha_id != beta_id
        assert nested_model_id not in {alpha_id, beta_id}
        assert alpha_context.sequence_index < beta_context.sequence_index
        assert beta_context.sequence_index < nested_context.sequence_index


class TestOpenAIModelToolCallReservations:
    def test_trackable_tool_call_ids_follow_model_response_order(self) -> None:
        from kitaru.adapters.openai_agents._model import _trackable_tool_call_ids

        @function_tool
        def alpha() -> str:
            return "alpha"

        @function_tool
        def beta() -> str:
            return "beta"

        response = SimpleNamespace(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_alpha",
                    id="fc_alpha",
                    name="alpha",
                    status="completed",
                    type="function_call",
                ),
                {"name": "hosted_lookup", "call_id": "call_hosted"},
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_beta",
                    id="fc_beta",
                    name="beta",
                    status="completed",
                    type="function_call",
                ),
            ]
        )

        assert _trackable_tool_call_ids(response, [alpha, beta]) == [
            "call_alpha",
            "call_beta",
        ]

    @pytest.mark.anyio
    async def test_get_response_reserves_tool_order_when_checkpoint_is_cached(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import kitaru.adapters.openai_agents._model as openai_model
        from kitaru.adapters.openai_agents._model import KitaruOpenAIModel
        from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

        @function_tool
        def alpha() -> str:
            return "alpha"

        @function_tool
        def beta() -> str:
            return "beta"

        cached_response = SimpleNamespace(
            output=[
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_alpha",
                    id="fc_alpha",
                    name="alpha",
                    status="completed",
                    type="function_call",
                ),
                ResponseFunctionToolCall(
                    arguments="{}",
                    call_id="call_beta",
                    id="fc_beta",
                    name="beta",
                    status="completed",
                    type="function_call",
                ),
            ],
            usage=None,
            response_id="resp_cached",
        )
        seen_tool_call_ids: list[list[str]] = []
        seen_checkpoint_inputs: list[dict[str, Any] | None] = []

        class FakeTracker:
            def reserve_tool_call_order(self, tool_call_ids: list[str]) -> None:
                seen_tool_call_ids.append(tool_call_ids)

        async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
            seen_checkpoint_inputs.append(kwargs.get("checkpoint_inputs"))
            return cached_response

        monkeypatch.setattr(openai_model, "is_inside_flow", lambda: True)
        monkeypatch.setattr(openai_model, "is_inside_checkpoint", lambda: False)
        monkeypatch.setattr(openai_model, "get_current_tracker", lambda: FakeTracker())
        monkeypatch.setattr(
            openai_model,
            "run_async_in_checkpoint",
            fake_run_async_in_checkpoint,
        )
        model = KitaruOpenAIModel(
            SimpleNamespace(),
            capture=OpenAICapturePolicy(),
            agent_name="cached_agent",
            checkpoint_config={},
        )

        response = await model.get_response(
            None,
            "prompt",
            None,
            [alpha, beta],
            None,
            [],
            None,
            previous_response_id=None,
            conversation_id=None,
            prompt=None,
        )

        assert response is cached_response
        assert seen_tool_call_ids == [["call_alpha", "call_beta"]]
        assert seen_checkpoint_inputs == [
            {"input": _expected_model_input_envelope(input_value="prompt")}
        ]


@pytest.mark.anyio
async def test_openai_model_checkpoint_uses_structural_input_and_output_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._model as openai_model
    from kitaru.adapters.openai_agents._model import KitaruOpenAIModel
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_checkpoint_inputs: list[dict[str, Any] | None] = []
    recorded_artifacts: list[dict[str, str]] = []
    saved: list[tuple[str, str]] = []

    async def fake_wrapped_get_response(*_args: Any, **_kwargs: Any) -> Any:
        return SimpleNamespace(output=[], usage=None, response_id="resp_structural")

    class FakeTracker:
        def start_llm_event(self) -> tuple[str, SimpleNamespace]:
            return "agent_ab12cd34_llm_call_1", SimpleNamespace(sequence_index=1)

        def record_event(self, *_args: Any, **kwargs: Any) -> None:
            recorded_artifacts.append(kwargs["artifacts"])

        def reserve_tool_call_order(self, _tool_call_ids: list[str]) -> None:
            return None

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        seen_checkpoint_inputs.append(kwargs.get("checkpoint_inputs"))
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    monkeypatch.setattr(openai_model, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_model, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(openai_model, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(
        openai_model, "run_async_in_checkpoint", fake_run_async_in_checkpoint
    )
    monkeypatch.setattr(
        openai_model.kitaru,
        "save",
        lambda name, _value, *, type: saved.append((name, type)),
    )

    model = KitaruOpenAIModel(
        SimpleNamespace(get_response=fake_wrapped_get_response),
        capture=OpenAICapturePolicy(save_usage=False),
        agent_name="structural_agent",
        checkpoint_config={},
    )

    response = await model.get_response(
        "system",
        [{"role": "user", "content": "hello"}],
        {"temperature": 0},
        [],
        None,
        [],
        None,
        previous_response_id="previous",
        conversation_id="conversation",
        prompt={"id": "prompt"},
    )

    assert response.response_id == "resp_structural"
    assert seen_checkpoint_inputs == [
        {
            "input": _expected_model_input_envelope(
                system_instructions="system",
                input_value=[{"role": "user", "content": "hello"}],
                model_settings={"temperature": 0},
                previous_response_id="previous",
                conversation_id="conversation",
                prompt={"id": "prompt"},
            )
        }
    ]
    assert recorded_artifacts == [{"input": "input", "response": "output"}]
    assert saved == []


@pytest.mark.anyio
async def test_openai_tool_checkpoint_uses_structural_tool_args_and_output_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_checkpoint_inputs: list[dict[str, Any] | None] = []
    seen_cache_payloads: list[Any] = []
    recorded_artifacts: list[dict[str, str]] = []
    saved: list[tuple[str, str]] = []

    from agents.tool import FunctionTool

    async def invoke_double_value(_context: Any, input_json: str) -> str:
        assert input_json == '{"value": 4}'
        return "doubled=8"

    double_value = FunctionTool(
        name="double_value",
        description="Double a value.",
        params_json_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
        on_invoke_tool=invoke_double_value,
    )

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            assert tool_call_id == "call_structural_tool"
            return "agent_ab12cd34_tool_call_2", SimpleNamespace(sequence_index=2)

        def record_event(self, *_args: Any, **kwargs: Any) -> None:
            recorded_artifacts.append(kwargs["artifacts"])

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        seen_checkpoint_inputs.append(kwargs.get("checkpoint_inputs"))
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    original_checkpoint_cache_key = openai_tools.checkpoint_cache_key

    def fake_checkpoint_cache_key(payload: Any) -> str:
        seen_cache_payloads.append(payload)
        return original_checkpoint_cache_key(payload)

    monkeypatch.setattr(openai_tools, "checkpoint_cache_key", fake_checkpoint_cache_key)
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(
        openai_tools, "run_async_in_checkpoint", fake_run_async_in_checkpoint
    )
    monkeypatch.setattr(
        openai_tools.kitaru,
        "save",
        lambda name, _value, *, type: saved.append((name, type)),
    )

    wrapped = openai_tools._wrap_function_tool(
        double_value,
        capture=OpenAICapturePolicy(),
        agent_name="structural_agent",
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name=None,
    )

    result = await wrapped.on_invoke_tool(
        cast(Any, SimpleNamespace(tool_call_id="call_structural_tool")),
        '{"value": 4}',
    )

    assert result == "doubled=8"
    assert seen_checkpoint_inputs == [
        {
            "tool_args": {
                "tool_name": "double_value",
                "tool_call_id": "call_structural_tool",
                "raw_args": '{"value": 4}',
                "parsed_args": {"value": 4},
            }
        }
    ]
    assert recorded_artifacts == [{"input": "tool_args", "result": "output"}]
    assert "context_cache_key" not in seen_cache_payloads[0]
    assert "tool_cache_identity" not in seen_cache_payloads[0]
    assert saved == []


@pytest.mark.anyio
async def test_sandbox_tool_checkpoint_cache_includes_factory_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_cache_payloads: list[dict[str, Any]] = []

    monkeypatch.setattr(
        kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: _fake_sandbox_result(),
    )
    active_identity = _patch_active_sandbox_identity(monkeypatch)

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    original_checkpoint_cache_key = openai_tools.checkpoint_cache_key

    def fake_checkpoint_cache_key(payload: Any) -> str:
        if isinstance(payload, dict) and payload.get("input_json") is not None:
            seen_cache_payloads.append(payload)
        return original_checkpoint_cache_key(payload)

    monkeypatch.setattr(openai_tools, "checkpoint_cache_key", fake_checkpoint_cache_key)
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(
        openai_tools, "run_async_in_checkpoint", fake_run_async_in_checkpoint
    )

    tools = [
        sandbox_command_tool(max_chars=100, cleanup="destroy"),
        sandbox_command_tool(max_chars=20_000, cleanup="destroy"),
        sandbox_command_tool(max_chars=100, cleanup="close"),
    ]

    for tool in tools:
        wrapped = openai_tools._wrap_function_tool(
            tool,
            capture=OpenAICapturePolicy(emit_child_events=False),
            agent_name="sandbox_cache_agent",
            tool_checkpoint_config={},
            tool_checkpoint_config_by_name=None,
        )
        await wrapped.on_invoke_tool(
            cast(Any, SimpleNamespace(tool_call_id="call_sandbox_command")),
            '{"command":"python --version"}',
        )

    identities = [payload["tool_cache_identity"] for payload in seen_cache_payloads]
    assert identities == [
        {
            "kind": "sandbox_command_tool",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
            "active_sandbox": active_identity,
        },
        {
            "kind": "sandbox_command_tool",
            "max_chars": 20_000,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
            "active_sandbox": active_identity,
        },
        {
            "kind": "sandbox_command_tool",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "close",
            "active_sandbox": active_identity,
        },
    ]
    assert len({json.dumps(identity, sort_keys=True) for identity in identities}) == 3


@pytest.mark.anyio
async def test_sandbox_tool_checkpoint_cache_varies_by_active_sandbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    import kitaru.config as kitaru_config
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_cache_payloads: list[dict[str, Any]] = []
    active_identity = _active_sandbox_identity(stack_id="stack-a")

    monkeypatch.setattr(
        kitaru,
        "run_sandbox_command",
        lambda *_args, **_kwargs: _fake_sandbox_result(),
    )
    monkeypatch.setattr(
        kitaru_config,
        "_active_sandbox_cache_identity",
        lambda: active_identity,
    )

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    original_checkpoint_cache_key = openai_tools.checkpoint_cache_key

    def fake_checkpoint_cache_key(payload: Any) -> str:
        if isinstance(payload, dict) and payload.get("input_json") is not None:
            seen_cache_payloads.append(payload)
        return original_checkpoint_cache_key(payload)

    monkeypatch.setattr(openai_tools, "checkpoint_cache_key", fake_checkpoint_cache_key)
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(
        openai_tools,
        "run_async_in_checkpoint",
        fake_run_async_in_checkpoint,
    )

    wrapped = openai_tools._wrap_function_tool(
        sandbox_command_tool(max_chars=100),
        capture=OpenAICapturePolicy(emit_child_events=False),
        agent_name="sandbox_cache_agent",
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name=None,
    )

    await wrapped.on_invoke_tool(
        cast(Any, SimpleNamespace(tool_call_id="call_sandbox_command")),
        '{"command":"python --version"}',
    )
    active_identity = _active_sandbox_identity(stack_id="stack-b")
    await wrapped.on_invoke_tool(
        cast(Any, SimpleNamespace(tool_call_id="call_sandbox_command")),
        '{"command":"python --version"}',
    )

    assert [
        payload["tool_cache_identity"]["active_sandbox"]["stack_id"]
        for payload in seen_cache_payloads
    ] == ["stack-a", "stack-b"]
    assert seen_cache_payloads[0] != seen_cache_payloads[1]


@pytest.mark.anyio
async def test_calls_strategy_prepare_objects_reuses_shared_handoff_child() -> None:
    child_tool = sandbox_command_tool(max_chars=100)
    child_agent = Agent(
        name="shared_sandbox_child",
        model="gpt-5-nano",
        tools=[child_tool],
    )
    first_handoff = handoff(child_agent)
    second_handoff = handoff(child_agent)
    runner = KitaruRunner(
        Agent(
            name="shared_handoff_parent",
            model="gpt-5-nano",
            handoffs=cast(Any, [first_handoff, second_handoff]),
        ),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    prepared_agent, _run_config = runner._prepare_execution_objects(
        wrap_calls=True,
        context_cache_identity=None,
        context_cache_key=None,
    )

    prepared_handoffs = list(prepared_agent.handoffs)
    assert len(prepared_handoffs) == 2
    prepared_children = [
        prepared_handoff._agent_ref() for prepared_handoff in prepared_handoffs
    ]
    assert prepared_children[0] is prepared_children[1]
    assert prepared_children[0] is not child_agent
    prepared_child = prepared_children[0]
    assert prepared_child.tools[0]._kitaru_wrapped is True
    assert prepared_child.tools[0]._kitaru_original_tool is child_tool

    invoked_first_child = await prepared_handoffs[0].on_invoke_handoff(
        SimpleNamespace(),
        None,
    )
    invoked_second_child = await prepared_handoffs[1].on_invoke_handoff(
        SimpleNamespace(),
        None,
    )
    assert invoked_first_child is prepared_child
    assert invoked_second_child is prepared_child


@pytest.mark.anyio
async def test_calls_strategy_prepare_objects_keeps_cyclic_handoffs_wrapped() -> None:
    parent_tool = sandbox_command_tool(name="parent_command", max_chars=100)
    child_tool = sandbox_command_tool(name="child_command", max_chars=100)
    parent_agent = Agent(
        name="cyclic_parent",
        model="gpt-5-nano",
        tools=[parent_tool],
    )
    child_agent = Agent(
        name="cyclic_child",
        model="gpt-5-nano",
        tools=[child_tool],
    )
    object.__setattr__(parent_agent, "handoffs", cast(Any, [handoff(child_agent)]))
    object.__setattr__(child_agent, "handoffs", cast(Any, [handoff(parent_agent)]))
    runner = KitaruRunner(
        parent_agent,
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    prepared_parent, _run_config = runner._prepare_execution_objects(
        wrap_calls=True,
        context_cache_identity=None,
        context_cache_key=None,
    )

    prepared_child = prepared_parent.handoffs[0]._agent_ref()
    prepared_parent_from_cycle = prepared_child.handoffs[0]._agent_ref()
    assert prepared_parent is not parent_agent
    assert prepared_child is not child_agent
    assert prepared_parent_from_cycle is prepared_parent
    assert prepared_parent_from_cycle is not parent_agent
    assert prepared_parent.tools[0]._kitaru_original_tool is parent_tool
    assert prepared_child.tools[0]._kitaru_original_tool is child_tool

    invoked_child = await prepared_parent.handoffs[0].on_invoke_handoff(
        SimpleNamespace(),
        None,
    )
    invoked_parent = await prepared_child.handoffs[0].on_invoke_handoff(
        SimpleNamespace(),
        None,
    )
    assert invoked_child is prepared_child
    assert invoked_parent is prepared_parent


def test_calls_strategy_prepare_objects_wires_context_cache_key_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools

    seen_kwargs: dict[str, Any] = {}
    original_tools = [SimpleNamespace(name="lookup_customer")]

    def fake_kitaruify_openai_tools(tools: list[Any], **kwargs: Any) -> list[Any]:
        seen_kwargs.update(kwargs)
        return tools

    monkeypatch.setattr(
        openai_tools,
        "kitaruify_openai_tools",
        fake_kitaruify_openai_tools,
    )

    runner = KitaruRunner(
        Agent(
            name="factory-wire-agent",
            model="gpt-5-nano",
            tools=cast(Any, original_tools),
        ),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
        context_cache_identity=lambda ctx: {"team_id": ctx.team_id},
    )
    context = WorkerContext(
        team_id="team-a",
        user_id="user-1",
        thread_id="thread-1",
    )
    context_identity = runner._context_cache_identity(context)
    context_key = runner._context_cache_key(context_identity)

    prepared_agent, _run_config = runner._prepare_execution_objects(
        wrap_calls=True,
        context_cache_identity=context_identity,
        context_cache_key=context_key,
    )

    assert prepared_agent.tools == original_tools
    assert seen_kwargs["context_cache_identity"] == context_identity
    assert seen_kwargs["context_cache_key"] == context_key
    factory = seen_kwargs["context_cache_key_factory"]
    assert callable(factory)
    assert factory(context) == context_key
    assert (
        factory(
            WorkerContext(
                team_id="team-b",
                user_id="user-1",
                thread_id="thread-1",
            )
        )
        != context_key
    )


@pytest.mark.anyio
async def test_openai_tool_checkpoint_uses_callback_context_key_for_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_checkpoint_inputs: list[dict[str, Any] | None] = []
    seen_cache_payloads: list[dict[str, Any]] = []
    seen_team_ids: list[str] = []

    from agents.tool import FunctionTool

    async def invoke_tool(context: Any, _input_json: str) -> str:
        seen_team_ids.append(context.context.team_id)
        return f"team={context.context.team_id}"

    tool = FunctionTool(
        name="lookup_customer",
        description="Look up a customer.",
        params_json_schema={
            "type": "object",
            "properties": {"customer_id": {"type": "string"}},
            "required": ["customer_id"],
            "additionalProperties": False,
        },
        on_invoke_tool=invoke_tool,
    )

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        seen_checkpoint_inputs.append(kwargs.get("checkpoint_inputs"))
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    original_checkpoint_cache_key = openai_tools.checkpoint_cache_key

    def fake_checkpoint_cache_key(payload: Any) -> str:
        if isinstance(payload, dict) and payload.get("tool_name") == "lookup_customer":
            seen_cache_payloads.append(payload)
        return original_checkpoint_cache_key(payload)

    monkeypatch.setattr(openai_tools, "checkpoint_cache_key", fake_checkpoint_cache_key)
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(
        openai_tools,
        "run_async_in_checkpoint",
        fake_run_async_in_checkpoint,
    )

    wrapped = openai_tools._wrap_function_tool(
        tool,
        capture=OpenAICapturePolicy(save_final_output=False),
        agent_name="resume_context_agent",
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name=None,
        context_cache_key_factory=lambda ctx: f"context-key:{ctx.team_id}",
    )

    for team_id in ("team-a", "team-b"):
        result = await wrapped.on_invoke_tool(
            cast(
                Any,
                SimpleNamespace(
                    tool_call_id="call_lookup",
                    context=WorkerContext(
                        team_id=team_id,
                        user_id="user-1",
                        thread_id="thread-1",
                    ),
                ),
            ),
            '{"customer_id": "123"}',
        )
        assert result == f"team={team_id}"

    assert seen_team_ids == ["team-a", "team-b"]
    assert [payload["context_cache_key"] for payload in seen_cache_payloads] == [
        "context-key:team-a",
        "context-key:team-b",
    ]
    assert seen_cache_payloads[0] != seen_cache_payloads[1]
    assert all("team-a" not in repr(inputs) for inputs in seen_checkpoint_inputs)
    assert all("team-b" not in repr(inputs) for inputs in seen_checkpoint_inputs)


@pytest.mark.anyio
async def test_openai_tool_checkpoint_keeps_context_key_out_of_visible_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    inside_checkpoint = False
    seen_checkpoint_inputs: list[dict[str, Any] | None] = []

    from agents.tool import FunctionTool

    async def invoke_tool(_context: Any, _input_json: str) -> str:
        return "ok"

    tool = FunctionTool(
        name="lookup_customer",
        description="Look up a customer.",
        params_json_schema={
            "type": "object",
            "properties": {"customer_id": {"type": "string"}},
            "required": ["customer_id"],
            "additionalProperties": False,
        },
        on_invoke_tool=invoke_tool,
    )

    async def fake_run_async_in_checkpoint(**kwargs: Any) -> Any:
        nonlocal inside_checkpoint
        seen_checkpoint_inputs.append(kwargs.get("checkpoint_inputs"))
        inside_checkpoint = True
        try:
            return await kwargs["body"]()
        finally:
            inside_checkpoint = False

    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: inside_checkpoint)
    monkeypatch.setattr(
        openai_tools, "run_async_in_checkpoint", fake_run_async_in_checkpoint
    )

    wrapped = openai_tools._wrap_function_tool(
        tool,
        capture=OpenAICapturePolicy(save_final_output=False),
        agent_name="context_agent",
        tool_checkpoint_config={},
        tool_checkpoint_config_by_name=None,
        context_cache_identity={"team_id": "team-a", "user_id": "user-1"},
    )

    result = await wrapped.on_invoke_tool(
        cast(Any, SimpleNamespace(tool_call_id="call_lookup")),
        '{"customer_id": "123"}',
    )

    assert result == "ok"
    checkpoint_inputs = seen_checkpoint_inputs[0]
    assert checkpoint_inputs is not None
    tool_args = checkpoint_inputs["tool_args"]
    assert "context_cache_key" not in tool_args
    assert "context_cache_identity" not in tool_args
    assert "team-a" not in repr(tool_args)
    assert "user-1" not in repr(tool_args)


@pytest.mark.anyio
async def test_openai_tool_call_passes_tool_call_id_to_event_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    seen_tool_call_ids: list[str | None] = []

    @function_tool
    def publish() -> str:
        return "unused"

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            seen_tool_call_ids.append(tool_call_id)
            return "event-1", SimpleNamespace(sequence_index=1)

        def record_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    async def callback(_context: Any, _input_json: str) -> str:
        return "published"

    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: True)

    result = await openai_tools._tracked_tool_call(
        callback,
        SimpleNamespace(),
        "{}",
        tool=publish,
        capture=OpenAICapturePolicy(save_input=False, save_final_output=False),
        tool_call_id="call_publish",
    )

    assert result == "published"
    assert seen_tool_call_ids == ["call_publish"]


@pytest.mark.anyio
async def test_openai_tracked_tool_execution_remains_concurrent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import anyio

    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    started: list[str] = []
    both_started = anyio.Event()

    async def _mark_started(name: str) -> None:
        started.append(name)
        if len(started) == 2:
            both_started.set()
        await both_started.wait()

    @function_tool
    def alpha() -> str:
        return "unused"

    @function_tool
    def beta() -> str:
        return "unused"

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
            )

        def record_event(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    fake_tracker = FakeTracker()
    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: fake_tracker)
    monkeypatch.setattr(openai_tools, "is_inside_checkpoint", lambda: True)
    capture = OpenAICapturePolicy(save_input=False, save_final_output=False)
    results: dict[str, str] = {}

    async def _run_tool(name: str, tool: Any, tool_call_id: str) -> None:
        async def callback(_context: Any, _input_json: str) -> str:
            await _mark_started(name)
            return name

        results[name] = await openai_tools._tracked_tool_call(
            callback,
            SimpleNamespace(),
            "{}",
            tool=tool,
            capture=capture,
            tool_call_id=tool_call_id,
        )

    with anyio.fail_after(1):
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(_run_tool, "alpha", alpha, "call_alpha")
            task_group.start_soon(_run_tool, "beta", beta, "call_beta")

    assert set(started) == {"alpha", "beta"}
    assert results == {"alpha": "alpha", "beta": "beta"}


def test_calls_strategy_model_call_runs_inside_checkpoint(primed_zenml) -> None:
    model = StaticTextModel("model checkpointed")
    agent_name = f"openai_model_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def model_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    handle = model_flow.run("same prompt", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)
    assert any("openai_model_call" in name for name in _step_names(hydrated))
    assert model.call_count == 1


def test_calls_strategy_preserves_structured_final_output_and_model_event(
    primed_zenml,
) -> None:
    model = StaticTextModel('{"verdict":"safe","confidence":0.91}')
    agent_name = f"openai_structured_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, output_type=StructuredSupportAnswer),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def structured_output_flow(prompt: str, nonce: str) -> dict[str, Any]:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        assert isinstance(result.final_output, StructuredSupportAnswer)
        assert result.final_output.verdict == "safe"
        assert result.final_output.confidence == 0.91
        return result.final_output.model_dump()

    handle = structured_output_flow.run("return structured safety result", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)

    assert any("openai_model_call" in name for name in _step_names(hydrated))
    events = _events(hydrated.run_metadata["openai_agents_events"])
    assert any(event["kind"] == "llm_call" for event in events)


def test_calls_strategy_model_checkpoint_cache_skips_inner_model(
    primed_zenml,
) -> None:
    model = StaticTextModel("cached model")
    agent_name = f"openai_cached_model_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def cached_model_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = cached_model_flow.run("stable prompt", "first")
    _wait_for_hydrated_run(first.exec_id)
    assert model.call_count == 1

    second = cached_model_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert model.call_count == 1


def test_tool_input_guardrail_rejection_metadata_redacts_when_input_capture_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    @function_tool
    def send_email(message: str) -> str:
        """Send an email message."""
        return message

    recorded: list[dict[str, Any]] = []

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            assert tool_call_id == "call_guarded_email"
            return "event-1", SimpleNamespace(sequence_index=1)

        def record_event(self, *_args: Any, **kwargs: Any) -> None:
            recorded.append(kwargs)

    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)

    openai_tools._record_blocked_tool_input_guardrail_event(
        SimpleNamespace(context=SimpleNamespace(tool_call_id="call_guarded_email")),
        tool=send_email,
        capture=OpenAICapturePolicy(save_input=False),
        guardrail=SimpleNamespace(name="block_sensitive_input"),
        guardrail_index=0,
        behavior_type="reject_content",
        status="completed",
        started_at=0.0,
        rejection_message="blocked SECRET_DO_NOT_LOG",
    )

    metadata = recorded[0]["metadata"]
    assert "rejection_message" not in metadata
    assert metadata["rejection_message_redacted"] is True
    assert "SECRET_DO_NOT_LOG" not in repr(recorded[0])


def test_tool_input_guardrail_exception_summary_redacts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    import kitaru.adapters.openai_agents._tracking as openai_tracking
    from kitaru.adapters.openai_agents._constants import (
        OPENAI_AGENTS_EVENTS_METADATA_KEY,
        OPENAI_AGENTS_RUN_SUMMARIES_METADATA_KEY,
    )
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    @function_tool
    def send_email(message: str) -> str:
        """Send an email message."""
        return message

    logged: dict[str, Any] = {}
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)
    monkeypatch.setattr(openai_tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(
        openai_tracking.kitaru,
        "log",
        lambda **kwargs: logged.update(kwargs),
    )

    with openai_tracking.tracker_scope("guardrail_summary_agent") as tracker:
        monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: tracker)
        openai_tools._record_blocked_tool_input_guardrail_event(
            SimpleNamespace(context=SimpleNamespace(tool_call_id="call_guarded_email")),
            tool=send_email,
            capture=OpenAICapturePolicy(save_input=False),
            guardrail=SimpleNamespace(name="explode_on_secret"),
            guardrail_index=0,
            behavior_type="exception",
            status="failed",
            started_at=0.0,
            error=RuntimeError("guardrail saw SECRET_DO_NOT_LOG"),
        )

    event_map = logged[OPENAI_AGENTS_EVENTS_METADATA_KEY]
    summary_map = logged[OPENAI_AGENTS_RUN_SUMMARIES_METADATA_KEY]
    serialized_events = repr(event_map)
    serialized_summary = repr(summary_map)
    assert "SECRET_DO_NOT_LOG" not in serialized_events
    assert "SECRET_DO_NOT_LOG" not in serialized_summary

    events = next(iter(event_map.values()))
    summaries = next(iter(summary_map.values()))
    event_error = events[0]["error"]
    summary_error = summaries["error"]
    assert event_error["exception_type"] == "RuntimeError"
    assert summary_error["exception_type"] == "RuntimeError"
    assert "details redacted" in event_error["message"]
    assert "details redacted" in summary_error["message"]

    tracker = openai_tracking.EventTracker(agent_name="guardrail_summary_agent")
    redacted_error = RuntimeError("details redacted")
    redacted_error.__dict__["_kitaru_redacted_run_error"] = True
    tracker.set_run_error(redacted_error)
    tracker.set_run_error(RuntimeError("guardrail saw SECRET_DO_NOT_LOG"))
    overwrite_summary = tracker.build_run_summary()
    overwrite_error = cast(dict[str, Any], overwrite_summary["error"])
    assert "SECRET_DO_NOT_LOG" not in repr(overwrite_summary)
    assert overwrite_error is not None
    assert "details redacted" in overwrite_error["message"]


def test_tool_input_guardrail_error_metadata_redacts_when_input_capture_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kitaru.adapters.openai_agents._tools as openai_tools
    from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy

    @function_tool
    def send_email(message: str) -> str:
        """Send an email message."""
        return message

    recorded: list[dict[str, Any]] = []

    class FakeTracker:
        def start_tool_event(
            self,
            *,
            tool_call_id: str | None = None,
        ) -> tuple[str, SimpleNamespace]:
            assert tool_call_id == "call_guarded_email"
            return "event-1", SimpleNamespace(sequence_index=1)

        def record_event(self, *_args: Any, **kwargs: Any) -> None:
            recorded.append(kwargs)

    monkeypatch.setattr(openai_tools, "get_current_tracker", lambda: FakeTracker())
    monkeypatch.setattr(openai_tools, "is_inside_flow", lambda: True)

    openai_tools._record_blocked_tool_input_guardrail_event(
        SimpleNamespace(context=SimpleNamespace(tool_call_id="call_guarded_email")),
        tool=send_email,
        capture=OpenAICapturePolicy(save_input=False),
        guardrail=SimpleNamespace(name="block_sensitive_input"),
        guardrail_index=0,
        behavior_type="exception",
        status="failed",
        started_at=0.0,
        error=RuntimeError("guardrail saw SECRET_DO_NOT_LOG"),
    )

    error = recorded[0]["error"]
    assert isinstance(error, RuntimeError)
    assert "details redacted" in str(error)
    assert "SECRET_DO_NOT_LOG" not in repr(recorded[0])


def test_calls_strategy_tool_input_guardrail_reject_records_blocked_tool_event(
    primed_zenml,
) -> None:
    side_effects: list[str] = []
    seen_call_ids: list[str] = []

    @tool_input_guardrail(name="block_sensitive_input")
    def block_sensitive_input(data: Any) -> ToolGuardrailFunctionOutput:
        seen_call_ids.append(data.context.tool_call_id)
        return ToolGuardrailFunctionOutput.reject_content("blocked by policy")

    @function_tool(tool_input_guardrails=[block_sensitive_input])
    def send_email(message: str) -> str:
        """Send an email message."""
        side_effects.append(message)
        return "sent"

    model = GuardrailToolCallingModel(
        [
            ResponseFunctionToolCall(
                arguments='{"message":"secret"}',
                call_id="call_blocked_email",
                id="fc_blocked_email",
                name="send_email",
                status="completed",
                type="function_call",
            )
        ],
    )
    agent_name = f"openai_guardrail_reject_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[send_email]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def guardrail_reject_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    handle = guardrail_reject_flow.run("send the email", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)

    assert side_effects == []
    assert seen_call_ids == ["call_blocked_email"]
    assert model.seen_function_call_outputs == ["blocked by policy"]
    assert not any("send_email_tool_call" in name for name in _step_names(hydrated))

    events = _events(hydrated.run_metadata["openai_agents_events"])
    tool_events = [event for event in events if event["kind"] == "tool_call"]
    assert len(tool_events) == 1
    blocked_event = tool_events[0]
    assert blocked_event["status"] == "completed"
    assert blocked_event["artifacts"] == {}
    blocked_metadata = blocked_event["metadata"]
    assert blocked_metadata["tool_name"] == "send_email"
    assert blocked_metadata["tool_call_id"] == "call_blocked_email"
    assert blocked_metadata["tool_invoked"] is False
    assert blocked_metadata["blocked_by_guardrail"] is True
    assert blocked_metadata["guardrail_index"] == 0
    assert blocked_metadata["guardrail_name"] == "block_sensitive_input"
    assert blocked_metadata["guardrail_behavior"] == "reject_content"
    assert blocked_metadata["rejection_message"] == "blocked by policy"


def test_calls_strategy_tool_input_guardrail_raise_records_failed_tool_event(
    primed_zenml,
) -> None:
    side_effects: list[str] = []

    @tool_input_guardrail(name="trip_sensitive_input")
    def trip_sensitive_input(_data: Any) -> ToolGuardrailFunctionOutput:
        return ToolGuardrailFunctionOutput.raise_exception(
            output_info={"blocked": True}
        )

    @function_tool(tool_input_guardrails=[trip_sensitive_input])
    def send_email(message: str) -> str:
        """Send an email message."""
        side_effects.append(message)
        return "sent"

    model = GuardrailToolCallingModel(
        [
            ResponseFunctionToolCall(
                arguments='{"message":"secret"}',
                call_id="call_tripped_email",
                id="fc_tripped_email",
                name="send_email",
                status="completed",
                type="function_call",
            )
        ],
    )
    agent_name = f"openai_guardrail_raise_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[send_email]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def guardrail_raise_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        try:
            runner.run_sync(OpenAIRunRequest.start(prompt))
        except ToolInputGuardrailTripwireTriggered as error:
            return type(error).__name__
        raise AssertionError("expected ToolInputGuardrailTripwireTriggered")

    handle = guardrail_raise_flow.run("send the email", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)

    assert side_effects == []
    events = _events(hydrated.run_metadata["openai_agents_events"])
    tool_events = [event for event in events if event["kind"] == "tool_call"]
    assert len(tool_events) == 1
    failed_event = tool_events[0]
    assert failed_event["status"] == "failed"
    assert failed_event["metadata"]["tool_name"] == "send_email"
    assert failed_event["metadata"]["tool_call_id"] == "call_tripped_email"
    assert failed_event["metadata"]["tool_invoked"] is False
    assert failed_event["metadata"]["blocked_by_guardrail"] is True
    assert failed_event["metadata"]["guardrail_name"] == "trip_sensitive_input"
    assert failed_event["metadata"]["guardrail_behavior"] == "raise_exception"
    assert failed_event["error"]["exception_type"] == "RuntimeError"
    assert (
        "requested an exception before tool invocation"
        in failed_event["error"]["message"]
    )


def test_calls_strategy_allowed_and_blocked_guardrails_keep_tool_event_order(
    primed_zenml,
) -> None:
    side_effects: list[int] = []

    @tool_input_guardrail(name="allow_tool_input")
    def allow_tool_input(_data: Any) -> ToolGuardrailFunctionOutput:
        return ToolGuardrailFunctionOutput.allow(output_info={"checked": True})

    @tool_input_guardrail(name="block_tool_input")
    def block_tool_input(_data: Any) -> ToolGuardrailFunctionOutput:
        return ToolGuardrailFunctionOutput.reject_content("blocked second tool")

    @function_tool(tool_input_guardrails=[block_tool_input])
    def blocked_value(value: int) -> str:
        """A tool that should not run when the guardrail blocks it."""
        side_effects.append(value)
        return f"blocked={value}"

    @function_tool(tool_input_guardrails=[allow_tool_input])
    def allowed_value(value: int) -> str:
        """A tool that should run when the guardrail allows it."""
        side_effects.append(value)
        return f"allowed={value}"

    model = GuardrailToolCallingModel(
        [
            ResponseFunctionToolCall(
                arguments='{"value":1}',
                call_id="call_blocked_value",
                id="fc_blocked_value",
                name="blocked_value",
                status="completed",
                type="function_call",
            ),
            ResponseFunctionToolCall(
                arguments='{"value":2}',
                call_id="call_allowed_value",
                id="fc_allowed_value",
                name="allowed_value",
                status="completed",
                type="function_call",
            ),
        ],
    )
    agent_name = f"openai_guardrail_mixed_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[blocked_value, allowed_value]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def guardrail_mixed_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(OpenAIRunRequest.start(prompt))
        assert result.status == "completed"
        return str(result.final_output)

    handle = guardrail_mixed_flow.run("use both tools", "first")
    hydrated = _wait_for_hydrated_run(handle.exec_id)

    assert side_effects == [2]
    events = _events(hydrated.run_metadata["openai_agents_events"])
    tool_events = [event for event in events if event["kind"] == "tool_call"]
    assert [event["metadata"]["tool_call_id"] for event in tool_events] == [
        "call_blocked_value",
        "call_allowed_value",
    ]
    blocked_events = [
        event
        for event in tool_events
        if event["metadata"].get("blocked_by_guardrail") is True
    ]
    assert len(blocked_events) == 1
    assert blocked_events[0]["metadata"]["tool_invoked"] is False
    allowed_events = [
        event
        for event in tool_events
        if event["metadata"].get("tool_call_id") == "call_allowed_value"
    ]
    assert len(allowed_events) == 1
    assert allowed_events[0]["metadata"].get("blocked_by_guardrail") is None
    assert allowed_events[0]["artifacts"].get("input") == "tool_args"
    assert allowed_events[0]["artifacts"].get("result") == "output"


def test_calls_strategy_function_tool_runs_inside_checkpoint_and_caches(
    primed_zenml,
) -> None:
    side_effects: list[int] = []

    @function_tool
    def double_value(value: int) -> str:
        side_effects.append(value)
        return f"doubled={value * 2}"

    model = ToolCallingModel()
    agent_name = f"openai_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[double_value]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = tool_flow.run("please use the tool", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert side_effects == [4]
    assert model.call_count == 2
    assert any("double_value_tool_call" in name for name in _step_names(first_hydrated))
    inputs_by_step = _input_names_by_step(first_hydrated)
    assert any("input" in inputs for inputs in inputs_by_step)
    assert any("tool_args" in inputs for inputs in inputs_by_step)
    event_map = first_hydrated.run_metadata["openai_agents_events"]
    events = _events(event_map)
    assert any(
        event["kind"] == "llm_call"
        and event["artifacts"].get("input") == "input"
        and event["artifacts"].get("response") == "output"
        for event in events
    )
    assert any(
        event["kind"] == "tool_call"
        and event["artifacts"].get("input") == "tool_args"
        and event["artifacts"].get("result") == "output"
        for event in events
    )
    output_events = [
        event
        for event in events
        if event["artifacts"].get("response") == "output"
        or event["artifacts"].get("result") == "output"
    ]
    assert len(output_events) >= 2
    assert all(event.get("checkpoint_id") for event in output_events)
    assert all(event.get("checkpoint_name") for event in output_events)
    assert len({event["checkpoint_id"] for event in output_events}) == len(
        output_events
    )
    artifact_names = _artifact_names(first_hydrated)
    assert not any(
        re.fullmatch(r"llm_call_\d+_input__.*", name) for name in artifact_names
    )
    assert not any(
        re.fullmatch(r"tool_call_\d+_input__.*", name) for name in artifact_names
    )

    second = tool_flow.run("please use the tool", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert side_effects == [4]
    assert model.call_count == 2


def test_calls_strategy_sandbox_tool_runs_inside_checkpoint_and_caches(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox_calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(command: str, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append({"command": command, **kwargs})
        return _fake_sandbox_result(stdout="Python 3.12.0\n")

    monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
    _patch_active_sandbox_identity(monkeypatch)

    model = SandboxToolCallingModel()
    agent_name = f"openai_sandbox_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(
            name=agent_name, model=model, tools=[sandbox_command_tool(max_chars=100)]
        ),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def sandbox_tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = sandbox_tool_flow.run("check the Python version", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert sandbox_calls == [
        {
            "command": "python --version",
            "cwd": "/workspace",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
        }
    ]
    assert model.call_count == 2
    assert any(
        "kitaru_sandbox_command_tool_call" in name
        for name in _step_names(first_hydrated)
    )
    inputs_by_step = _input_names_by_step(first_hydrated)
    assert any("tool_args" in inputs for inputs in inputs_by_step)
    event_map = first_hydrated.run_metadata["openai_agents_events"]
    events = _events(event_map)
    assert any(
        event["kind"] == "tool_call"
        and event["artifacts"].get("input") == "tool_args"
        and event["artifacts"].get("result") == "output"
        for event in events
    )
    assert model.seen_function_call_outputs
    tool_output = json.loads(model.seen_function_call_outputs[0])
    assert tool_output["exit_code"] == 0
    assert tool_output["stdout"] == "Python 3.12.0\n"

    second = sandbox_tool_flow.run("check the Python version", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert sandbox_calls == [
        {
            "command": "python --version",
            "cwd": "/workspace",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
        }
    ]
    assert model.call_count == 2


def test_calls_strategy_explicit_handoff_wraps_child_sandbox_tools(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox_calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(command: str, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append({"command": command, **kwargs})
        return _fake_sandbox_result(stdout="Python 3.12.0\n")

    monkeypatch.setattr(kitaru, "run_sandbox_command", fake_run_sandbox_command)
    _patch_active_sandbox_identity(monkeypatch)

    child_name = f"sandbox_handoff_child_{uuid4().hex[:8]}"
    child_model = HandoffSandboxToolCallingModel()
    child_agent = Agent(
        name=child_name,
        model=child_model,
        tools=[sandbox_command_tool(max_chars=100)],
    )
    parent_model = HandoffCallingModel(handoff_tool_name=f"transfer_to_{child_name}")
    parent_name = f"openai_sandbox_handoff_parent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(
            name=parent_name,
            model=parent_model,
            handoffs=cast(Any, [handoff(child_agent)]),
        ),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def handoff_sandbox_tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = handoff_sandbox_tool_flow.run("delegate to the child", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)

    assert sandbox_calls == [
        {
            "command": "python --version",
            "cwd": "/workspace",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
        }
    ]
    assert parent_model.call_count == 1
    assert child_model.call_count == 2
    assert any(
        "kitaru_sandbox_command_tool_call" in name
        for name in _step_names(first_hydrated)
    )
    event_map = first_hydrated.run_metadata["openai_agents_events"]
    events = _events(event_map)
    assert any(
        event["kind"] == "tool_call" and event["artifacts"].get("result") == "output"
        for event in events
    )

    second = handoff_sandbox_tool_flow.run("delegate to the child", "second")
    _wait_for_hydrated_run(second.exec_id)

    assert sandbox_calls == [
        {
            "command": "python --version",
            "cwd": "/workspace",
            "max_chars": 100,
            "timeout_seconds": 30.0,
            "cleanup": "destroy",
        }
    ]
    assert parent_model.call_count == 2
    assert child_model.call_count == 2


def test_calls_strategy_function_tool_receives_fresh_context(
    primed_zenml,
) -> None:
    seen_team_ids: list[str] = []

    @function_tool
    def team_label(ctx: RunContextWrapper[WorkerContext]) -> str:
        """Return the current team label."""
        seen_team_ids.append(ctx.context.team_id)
        return f"team={ctx.context.team_id}"

    model = ContextToolCallingModel()
    agent_name = f"openai_context_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[team_label]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def context_tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(
            OpenAIRunRequest.start(prompt),
            context=WorkerContext(
                team_id="team-a",
                user_id="user-1",
                thread_id="thread-1",
            ),
        )
        assert result.status == "completed"
        return str(result.final_output)

    first = context_tool_flow.run("please use the context tool", "first")
    _wait_for_hydrated_run(first.exec_id)

    assert seen_team_ids == ["team-a"]


def test_calls_strategy_tool_cache_varies_by_context_identity(
    primed_zenml,
) -> None:
    seen_team_ids: list[str] = []

    @function_tool
    def team_label(ctx: RunContextWrapper[WorkerContext]) -> str:
        """Return the current team label."""
        seen_team_ids.append(ctx.context.team_id)
        return f"team={ctx.context.team_id}"

    model = ContextToolCallingModel()
    agent_name = f"openai_context_cache_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[team_label]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def context_cache_flow(prompt: str, team_id: str, nonce: str) -> str:
        _ = nonce
        result = runner.run_sync(
            OpenAIRunRequest.start(prompt),
            context=WorkerContext(
                team_id=team_id,
                user_id="user-1",
                thread_id="thread-1",
            ),
        )
        assert result.status == "completed"
        return str(result.final_output)

    first = context_cache_flow.run("please use the context tool", "team-a", "first")
    _wait_for_hydrated_run(first.exec_id)
    second = context_cache_flow.run("please use the context tool", "team-b", "second")
    _wait_for_hydrated_run(second.exec_id)

    assert seen_team_ids == ["team-a", "team-b"]


def test_calls_strategy_context_projection_can_reuse_tool_cache(
    primed_zenml,
) -> None:
    seen_team_ids: list[str] = []

    @function_tool
    def team_label(ctx: RunContextWrapper[WorkerContext]) -> str:
        """Return the current team label."""
        seen_team_ids.append(ctx.context.team_id)
        return f"team={ctx.context.team_id}"

    model = ContextToolCallingModel()
    agent_name = f"openai_context_projection_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[team_label]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
        context_cache_identity=lambda ctx: {
            "team_id": ctx.team_id,
            "user_id": ctx.user_id,
            "thread_id": ctx.thread_id,
            "worker_name": ctx.worker_name,
        },
    )

    @flow
    def projected_context_flow(prompt: str, message_id: str, doc_id: str) -> str:
        result = runner.run_sync(
            OpenAIRunRequest.start(prompt),
            context=WorkerContext(
                team_id="team-a",
                user_id="user-1",
                thread_id="thread-1",
                message_id=message_id,
                doc_id=doc_id,
            ),
        )
        assert result.status == "completed"
        return str(result.final_output)

    first = projected_context_flow.run(
        "please use the context tool",
        "msg-1",
        "doc-1",
    )
    _wait_for_hydrated_run(first.exec_id)
    second = projected_context_flow.run(
        "please use the context tool",
        "msg-2",
        "doc-2",
    )
    _wait_for_hydrated_run(second.exec_id)

    assert seen_team_ids == ["team-a"]


def test_same_args_tool_calls_without_visible_call_id_do_not_collide(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    side_effects: list[int] = []

    @function_tool
    def double_value(value: int) -> str:
        side_effects.append(value)
        return f"doubled={value * 2}"

    import kitaru.adapters.openai_agents._tools as openai_tools

    monkeypatch.setattr(openai_tools, "_tool_call_id", lambda _context: None)
    model = RepeatedToolCallingModel()
    agent_name = f"openai_repeat_tool_agent_{uuid4().hex[:8]}"
    runner = KitaruRunner(
        Agent(name=agent_name, model=model, tools=[double_value]),
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )

    @flow
    def repeated_tool_flow(prompt: str, nonce: str) -> str:
        _ = nonce
        return str(runner.run_sync(OpenAIRunRequest.start(prompt)).final_output)

    first = repeated_tool_flow.run("use the repeated tool", "first")
    _wait_for_hydrated_run(first.exec_id)
    assert side_effects == [4, 4]
    assert model.call_count == 3

    second = repeated_tool_flow.run("use the repeated tool", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert side_effects == [4, 4]
    assert model.call_count == 3


def test_calls_strategy_rejects_inside_existing_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = StaticTextModel("nested")
    runner = KitaruRunner(Agent(name=f"nested_{uuid4().hex[:8]}", model=model))
    monkeypatch.setitem(
        runner._require_calls_scope.__globals__,
        "is_inside_checkpoint",
        lambda: True,
    )

    with pytest.raises(KitaruUsageError, match="must run from a flow body"):
        runner.run_sync(OpenAIRunRequest.start("hello"))
