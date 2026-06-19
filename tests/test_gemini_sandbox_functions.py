"""No-network tests for Gemini custom function calls run through sandboxes."""

from __future__ import annotations

import importlib
import json
import types
from typing import Any

import pytest
from pydantic import ValidationError

from kitaru.config import DEFAULT_SANDBOX_COMMAND_MAX_CHARS, SandboxCommandResult
from kitaru.errors import KitaruStateError, KitaruUsageError
from kitaru.runtime import _checkpoint_scope, _flow_scope
from tests._gemini_fake_sdk import (
    install_fake_google_genai,
    purge_gemini_adapter_modules,
)


@pytest.fixture
def fake_google_genai(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    return install_fake_google_genai(monkeypatch)


@pytest.fixture
def gemini_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_google_genai: types.ModuleType,
) -> types.ModuleType:
    purge_gemini_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.gemini")


def _sandbox_result(
    *,
    exit_code: int = 0,
    stdout: str = "Python 3.12.0\n",
    stderr: str | None = None,
    cleanup_succeeded: bool = True,
    cleanup_error: str | None = None,
) -> SandboxCommandResult:
    return SandboxCommandResult(
        command="python --version",
        cwd=None,
        stdout=stdout,
        stderr=("" if exit_code == 0 else "boom\n") if stderr is None else stderr,
        exit_code=exit_code,
        stdout_truncated=False,
        stderr_truncated=False,
        stack_id="stack-1",
        stack_name="dev",
        sandbox_id="sandbox-1",
        sandbox_name="dev-sandbox",
        session_id="session-1",
        cleanup="destroy",
        cleanup_succeeded=cleanup_succeeded,
        cleanup_error=cleanup_error,
    )


def _default_payload_data(execution: Any) -> dict[str, Any]:
    payload = execution.function_result_payload
    assert payload == [
        {
            "type": "text",
            "text": payload[0]["text"],
        }
    ]
    return json.loads(payload[0]["text"])


def _requires_action_result(
    gemini_adapter: types.ModuleType,
    *,
    steps: list[Any] | None = None,
) -> Any:
    return gemini_adapter.GeminiInteractionResult(
        status="requires_action",
        interaction_id="interaction-1",
        model="gemini-test",
        steps=steps
        if steps is not None
        else [
            gemini_adapter.GeminiInteractionStepSummary(
                index=0,
                type="function_call",
                call_id="call-1",
                tool_name="sandbox_python_version",
            )
        ],
    )


def test_function_call_view_reads_safe_step_metadata(
    gemini_adapter: types.ModuleType,
) -> None:
    result = _requires_action_result(gemini_adapter)

    assert result.function_calls == [
        gemini_adapter.GeminiInteractionFunctionCall(
            index=0,
            step_type="function_call",
            call_id="call-1",
            function_name="sandbox_python_version",
        )
    ]


def test_function_call_view_skips_incomplete_and_non_call_steps(
    gemini_adapter: types.ModuleType,
) -> None:
    result = _requires_action_result(
        gemini_adapter,
        steps=[
            gemini_adapter.GeminiInteractionStepSummary(index=0, type="message"),
            gemini_adapter.GeminiInteractionStepSummary(
                index=1,
                type="FunctionCallStep",
                call_id=None,
                tool_name="missing_call_id",
            ),
            gemini_adapter.GeminiInteractionStepSummary(
                index=2,
                type="tool_call",
                call_id="call-2",
                tool_name=None,
            ),
        ],
    )

    assert [step.index for step in result.function_call_steps] == [1, 2]
    assert result.function_calls == []


def test_execute_gemini_sandbox_function_call_success(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        calls.append({"command": command, **kwargs})
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {
            "sandbox_python_version": gemini_adapter.GeminiSandboxFunctionSpec(
                function_name="sandbox_python_version",
                command="python --version",
                cwd="/workspace",
            )
        },
    )

    assert calls == [
        {
            "command": "python --version",
            "cwd": "/workspace",
            "env": None,
            "max_chars": 1_048_576,
            "cleanup": "destroy",
        }
    ]
    assert execution.call.call_id == "call-1"
    assert _default_payload_data(execution) == {
        "ok": True,
        "exit_code": 0,
        "stdout": "Python 3.12.0\n",
        "stderr": "",
        "stdout_truncated": False,
        "stderr_truncated": False,
        "stdout_payload_truncated": False,
        "stderr_payload_truncated": False,
        "payload_output_max_chars": 4_000,
        "cleanup": {"policy": "destroy", "succeeded": True},
    }
    request = execution.function_result_request
    assert request.kind == "function_result"
    assert request.previous_interaction_id == "interaction-1"
    assert request.function_call_id == "call-1"
    assert request.function_name == "sandbox_python_version"
    assert request.model == "gemini-test"
    assert request.input == [
        {
            "type": "function_result",
            "call_id": "call-1",
            "name": "sandbox_python_version",
            "result": execution.function_result_payload,
        }
    ]


def test_gemini_sandbox_function_execution_is_json_serializable(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": "python --version"},
    )

    dumped = execution.model_dump(mode="json")
    assert dumped["call"] == {
        "index": 0,
        "step_type": "function_call",
        "call_id": "call-1",
        "function_name": "sandbox_python_version",
    }
    assert dumped["sandbox_result"]["exit_code"] == 0
    assert dumped["function_result_request"]["kind"] == "function_result"


def test_execute_gemini_sandbox_function_call_outside_flow_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": "python --version"},
    )

    assert sandbox_calls == ["python --version"]


def test_execute_gemini_sandbox_function_call_inside_checkpoint_is_allowed(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    with (
        _flow_scope(name="flow"),
        _checkpoint_scope(name="checkpoint", checkpoint_type="agent_call"),
    ):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
        )

    assert sandbox_calls == ["python --version"]


def test_execute_gemini_sandbox_function_call_rejects_direct_flow_body_by_default(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    with (
        _flow_scope(name="flow"),
        pytest.raises(KitaruUsageError, match="directly inside a Kitaru flow body"),
    ):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
        )

    assert sandbox_calls == []


def test_execute_gemini_sandbox_function_call_allows_explicit_direct_flow_body_opt_in(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    with _flow_scope(name="flow"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
            allow_direct_execution_inside_flow_body=True,
        )

    assert sandbox_calls == ["python --version"]


def test_execute_gemini_sandbox_function_call_rejects_provider_agent_result(
    gemini_adapter: types.ModuleType,
) -> None:
    result = _requires_action_result(gemini_adapter)
    result.agent = "antigravity-preview-05-2026"
    result.model = None

    with pytest.raises(KitaruUsageError, match="Provider-agent and Antigravity"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            result,
            {"sandbox_python_version": "python --version"},
        )


def test_execute_gemini_sandbox_function_call_requires_model_continuation_target(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)
    result = _requires_action_result(gemini_adapter)
    result.model = None

    with pytest.raises(KitaruUsageError, match="model continuation target"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            result,
            {"sandbox_python_version": "python --version"},
        )

    assert sandbox_calls == []


def test_execute_gemini_sandbox_function_call_rejects_agent_override(
    gemini_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="Do not pass agent"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
            agent="caller-owned-agent",
        )


def test_execute_gemini_sandbox_function_call_rejects_store_false_before_command(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    with pytest.raises(KitaruUsageError, match="requires store=True"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
            store=False,
        )

    assert sandbox_calls == []


@pytest.mark.parametrize(
    ("result_factory", "kwargs", "error_match"),
    [
        (
            lambda adapter: _requires_action_result(adapter),
            {"model": ""},
            "non-empty",
        ),
        (
            lambda adapter: _requires_action_result(adapter),
            {"timeout_s": 0},
            "positive",
        ),
        (
            lambda adapter: _requires_action_result(
                adapter,
                steps=[
                    adapter.GeminiInteractionStepSummary(
                        index=0,
                        type="function_call",
                        call_id="",
                        tool_name="sandbox_python_version",
                    )
                ],
            ),
            {},
            "non-empty",
        ),
    ],
)
def test_execute_gemini_sandbox_function_call_preflights_continuation_request(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    result_factory: Any,
    kwargs: dict[str, Any],
    error_match: str,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(
        command: Any, **run_kwargs: Any
    ) -> SandboxCommandResult:
        sandbox_calls.append({"command": command, **run_kwargs})
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    with pytest.raises(ValidationError, match=error_match):
        gemini_adapter.execute_gemini_sandbox_function_call(
            result_factory(gemini_adapter),
            {"sandbox_python_version": "python --version"},
            **kwargs,
        )

    assert sandbox_calls == []


def test_execute_gemini_sandbox_function_call_normalizes_tuple_command(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        calls.append({"command": command, **kwargs})
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": ("python", "--version")},
    )

    assert calls[0]["command"] == ["python", "--version"]
    assert calls[0]["max_chars"] == DEFAULT_SANDBOX_COMMAND_MAX_CHARS


@pytest.mark.parametrize(
    ("spec_kwargs", "error_match"),
    [
        ({"command": ()}, "command list cannot be empty"),
        ({"command": ["echo", 1]}, "non-empty strings"),
        ({"command": "echo", "env": {"A": 1}}, "keys and values"),
        ({"command": "echo", "max_chars": -1}, ">= 0"),
        ({"command": "echo", "cleanup": "keep"}, "destroy.*close"),
    ],
)
def test_execute_gemini_sandbox_function_call_preflights_sandbox_spec_before_command(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
    spec_kwargs: dict[str, Any],
    error_match: str,
) -> None:
    sandbox_calls: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        sandbox_calls.append({"command": command, **kwargs})
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)
    spec = gemini_adapter.GeminiSandboxFunctionSpec(
        function_name="sandbox_python_version",
        **spec_kwargs,
    )

    with pytest.raises(KitaruUsageError, match=error_match):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            [spec],
        )

    assert sandbox_calls == []


def test_execute_gemini_sandbox_function_call_accepts_call_aware_command_builder(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    commands: list[Any] = []

    def fake_run_sandbox_command(command: Any, **kwargs: Any) -> SandboxCommandResult:
        commands.append(command)
        return _sandbox_result()

    monkeypatch.setattr("kitaru.run_sandbox_command", fake_run_sandbox_command)

    spec = gemini_adapter.GeminiSandboxFunctionSpec(
        function_name="sandbox_python_version",
        command=lambda call: ["echo", call.call_id, call.function_name],
    )
    gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        [spec],
    )

    assert commands == [["echo", "call-1", "sandbox_python_version"]]


def test_execute_gemini_sandbox_function_call_accepts_custom_payload_builder(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(),
    )
    spec = gemini_adapter.GeminiSandboxFunctionSpec(
        function_name="sandbox_python_version",
        command="python --version",
        result_payload_builder=lambda call, result: {
            "function": call.function_name,
            "stdout_lines": result.stdout.splitlines(),
        },
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        [spec],
    )

    assert execution.function_result_payload == {
        "function": "sandbox_python_version",
        "stdout_lines": ["Python 3.12.0"],
    }


def test_execute_gemini_sandbox_function_call_returns_non_zero_exit_as_payload(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(exit_code=7),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": "python --version"},
    )

    payload = _default_payload_data(execution)
    assert payload["ok"] is False
    assert payload["exit_code"] == 7
    assert payload["stderr"] == "boom\n"


def test_default_function_result_payload_omits_cleanup_error_from_model_payload(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(
            cleanup_succeeded=False,
            cleanup_error="verbose provider cleanup failure with local details",
        ),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": "python --version"},
    )

    payload = _default_payload_data(execution)
    assert payload["cleanup"] == {"policy": "destroy", "succeeded": False}
    assert "error" not in payload["cleanup"]
    assert (
        execution.sandbox_result.cleanup_error
        == "verbose provider cleanup failure with local details"
    )


def test_default_function_result_payload_caps_stdout_and_stderr(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    long_stdout = "o" * 4_050
    long_stderr = "e" * 4_010
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(
            stdout=long_stdout,
            stderr=long_stderr,
        ),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        {"sandbox_python_version": "python --version"},
    )

    payload = _default_payload_data(execution)
    assert payload["stdout"] == long_stdout[:4_000]
    assert payload["stderr"] == long_stderr[:4_000]
    assert payload["stdout_truncated"] is True
    assert payload["stderr_truncated"] is True
    assert payload["stdout_payload_truncated"] is True
    assert payload["stderr_payload_truncated"] is True
    assert payload["payload_output_max_chars"] == 4_000
    assert execution.sandbox_result.stdout == long_stdout
    assert execution.sandbox_result.stderr == long_stderr


def test_default_function_result_payload_clips_before_redacting(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    visible_stdout = "o" * 4_000
    hidden_secret = "explicit-env-secret"
    stdout = f"{visible_stdout}{hidden_secret}"
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(stdout=stdout),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        [
            gemini_adapter.GeminiSandboxFunctionSpec(
                function_name="sandbox_python_version",
                command="python --version",
                env={"TOKEN": hidden_secret},
            )
        ],
    )

    payload = _default_payload_data(execution)
    assert payload["stdout"] == visible_stdout
    assert payload["stdout_truncated"] is True
    assert payload["stdout_payload_truncated"] is True
    assert "stdout_redacted" not in payload
    assert "warnings" not in payload
    assert execution.sandbox_result.stdout == stdout


def test_default_function_result_payload_redacts_env_secret_crossing_clip_boundary(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    visible_stdout = "o" * 3_998
    visible_stderr = "e" * 3_998
    boundary_secret = "explicit-env-secret"
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(
            stdout=f"{visible_stdout}{boundary_secret}",
            stderr=f"{visible_stderr}{boundary_secret}",
        ),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        [
            gemini_adapter.GeminiSandboxFunctionSpec(
                function_name="sandbox_python_version",
                command="python --version",
                env={"TOKEN": boundary_secret},
            )
        ],
    )

    payload = _default_payload_data(execution)
    assert payload["stdout"] == f"{'o' * 3_990}<redacted>"
    assert payload["stderr"] == f"{'e' * 3_990}<redacted>"
    assert len(payload["stdout"]) == 4_000
    assert len(payload["stderr"]) == 4_000
    assert payload["stdout_truncated"] is True
    assert payload["stderr_truncated"] is True
    assert payload["stdout_payload_truncated"] is True
    assert payload["stderr_payload_truncated"] is True
    assert payload["stdout_redacted"] is True
    assert payload["stderr_redacted"] is True
    assert payload["warnings"] == [
        "Potential secret-like values were redacted from sandbox stdout/stderr "
        "before sending this payload to Gemini."
    ]
    assert boundary_secret not in payload["stdout"]
    assert boundary_secret not in payload["stderr"]
    assert not payload["stdout"].endswith("ex")
    assert not payload["stderr"].endswith("ex")


def test_default_function_result_payload_redacts_model_facing_output(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    stdout = (
        "env value explicit-env-secret "
        "OPENAI_API_KEY=openai-secret "
        "Authorization: Bearer bearer-secret "
        "Bearer standalone-secret"
    )
    stderr = "password=hunter2 api_key='quoted-secret'"
    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(stdout=stdout, stderr=stderr),
    )

    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        _requires_action_result(gemini_adapter),
        [
            gemini_adapter.GeminiSandboxFunctionSpec(
                function_name="sandbox_python_version",
                command="python --version",
                env={"TOKEN": "explicit-env-secret"},
            )
        ],
    )

    payload = _default_payload_data(execution)
    assert payload["stdout_redacted"] is True
    assert payload["stderr_redacted"] is True
    assert payload["warnings"] == [
        "Potential secret-like values were redacted from sandbox stdout/stderr "
        "before sending this payload to Gemini."
    ]
    assert "<redacted>" in payload["stdout"]
    assert "<redacted>" in payload["stderr"]
    for leaked in (
        "explicit-env-secret",
        "openai-secret",
        "bearer-secret",
        "standalone-secret",
        "hunter2",
        "quoted-secret",
    ):
        assert leaked not in payload["stdout"]
        assert leaked not in payload["stderr"]
    assert execution.sandbox_result.stdout == stdout
    assert execution.sandbox_result.stderr == stderr


@pytest.mark.parametrize(
    ("result_factory", "error_match"),
    [
        (
            lambda adapter: adapter.GeminiInteractionResult(
                status="completed", interaction_id="interaction-1"
            ),
            "requires_action",
        ),
        (
            lambda adapter: adapter.GeminiInteractionResult(
                status="requires_action", interaction_id=None
            ),
            "interaction_id",
        ),
        (
            lambda adapter: adapter.GeminiInteractionResult(
                status="requires_action", interaction_id="interaction-1", steps=[]
            ),
            "no function-call steps",
        ),
        (
            lambda adapter: _requires_action_result(
                adapter,
                steps=[
                    adapter.GeminiInteractionStepSummary(
                        index=0,
                        type="function_call",
                        call_id=None,
                        tool_name="sandbox_python_version",
                    )
                ],
            ),
            "without call IDs",
        ),
        (
            lambda adapter: _requires_action_result(
                adapter,
                steps=[
                    adapter.GeminiInteractionStepSummary(
                        index=0,
                        type="function_call",
                        call_id="call-1",
                        tool_name=None,
                    )
                ],
            ),
            "without function names",
        ),
        (
            lambda adapter: _requires_action_result(
                adapter,
                steps=[
                    adapter.GeminiInteractionStepSummary(
                        index=0,
                        type="function_call",
                        call_id="call-1",
                        tool_name="unknown",
                    )
                ],
            ),
            "not registered",
        ),
    ],
)
def test_execute_gemini_sandbox_function_call_rejects_invalid_handoffs(
    gemini_adapter: types.ModuleType,
    result_factory: Any,
    error_match: str,
) -> None:
    with pytest.raises(KitaruUsageError, match=error_match):
        gemini_adapter.execute_gemini_sandbox_function_call(
            result_factory(gemini_adapter),
            {"sandbox_python_version": "python --version"},
        )


def test_execute_gemini_sandbox_function_call_requires_call_id_for_multiple_matches(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    result = _requires_action_result(
        gemini_adapter,
        steps=[
            gemini_adapter.GeminiInteractionStepSummary(
                index=0,
                type="function_call",
                call_id="call-1",
                tool_name="sandbox_python_version",
            ),
            gemini_adapter.GeminiInteractionStepSummary(
                index=1,
                type="function_call",
                call_id="call-2",
                tool_name="sandbox_python_version",
            ),
        ],
    )

    with pytest.raises(KitaruUsageError, match="multiple registered"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            result,
            {"sandbox_python_version": "python --version"},
        )

    monkeypatch.setattr(
        "kitaru.run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(),
    )
    execution = gemini_adapter.execute_gemini_sandbox_function_call(
        result,
        {"sandbox_python_version": "python --version"},
        call_id="call-2",
    )

    assert execution.call.call_id == "call-2"


def test_execute_gemini_sandbox_function_call_propagates_sandbox_errors(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    def fail(command: Any, **kwargs: Any) -> SandboxCommandResult:
        raise KitaruStateError("no sandbox")

    monkeypatch.setattr("kitaru.run_sandbox_command", fail)

    with pytest.raises(KitaruStateError, match="no sandbox"):
        gemini_adapter.execute_gemini_sandbox_function_call(
            _requires_action_result(gemini_adapter),
            {"sandbox_python_version": "python --version"},
        )


def test_gemini_sandbox_function_public_exports(
    gemini_adapter: types.ModuleType,
) -> None:
    assert gemini_adapter.GeminiInteractionFunctionCall
    assert gemini_adapter.GeminiSandboxFunctionSpec
    assert gemini_adapter.GeminiSandboxFunctionExecution
    assert gemini_adapter.execute_gemini_sandbox_function_call
    assert "GeminiInteractionFunctionCall" in gemini_adapter.__all__
    assert "GeminiSandboxFunctionSpec" in gemini_adapter.__all__
    assert "GeminiSandboxFunctionExecution" in gemini_adapter.__all__
    assert "execute_gemini_sandbox_function_call" in gemini_adapter.__all__
