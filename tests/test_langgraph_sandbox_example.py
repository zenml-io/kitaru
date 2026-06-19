"""Tests for the LangGraph sandbox example wiring."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("langchain")
pytest.importorskip("langgraph")

_EXAMPLE_PATH = (
    Path(__file__).resolve().parent.parent
    / "examples"
    / "integrations"
    / "langgraph_agent"
    / "langgraph_adapter.py"
)


def _load_example_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load the LangGraph adapter example by path under a test-local module key."""
    module_name = "_langgraph_adapter_example_under_test"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    spec = importlib.util.spec_from_file_location(module_name, _EXAMPLE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _sandbox_tool_message(module: ModuleType, content: str) -> SimpleNamespace:
    return SimpleNamespace(
        type="tool",
        tool_call_id=module.SANDBOX_DEMO_TOOL_CALL_ID,
        content=content,
    )


def _sandbox_tool_result_content(
    *,
    exit_code: int = 0,
    stdout: str | None = None,
    stderr: str = "",
) -> str:
    return json.dumps(
        {
            "exit_code": exit_code,
            "stdout": stdout
            if stdout is not None
            else json.dumps({"cwd": "/workspace", "python": "3.12.0"}) + "\n",
            "stderr": stderr,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "cleanup_succeeded": True,
            "sandbox_name": "local",
            "stack_name": "sandbox-stack",
        }
    )


class _FakeModelRequest:
    """Tiny request object that records LangChain-style override calls."""

    def __init__(
        self,
        messages: list[Any],
        *,
        model_settings: dict[str, Any] | None = None,
    ) -> None:
        self.messages = messages
        self.model_settings = model_settings
        self.overrides: list[dict[str, Any]] = []

    def override(self, **kwargs: Any) -> Any:
        self.overrides.append(kwargs)
        return SimpleNamespace(messages=self.messages, override_kwargs=kwargs)


def test_validated_sandbox_demo_result_accepts_successful_tool_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    result = module._validated_sandbox_demo_result(
        [_sandbox_tool_message(module, _sandbox_tool_result_content())]
    )

    assert result["exit_code"] == 0
    assert result["stdout"] == {"cwd": "/workspace", "python": "3.12.0"}
    assert result["sandbox_name"] == "local"
    assert result["stack_name"] == "sandbox-stack"


def test_validated_sandbox_demo_result_accepts_dict_tool_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    result = module._validated_sandbox_demo_result(
        [
            {
                "role": "tool",
                "tool_call_id": module.SANDBOX_DEMO_TOOL_CALL_ID,
                "content": _sandbox_tool_result_content(),
            }
        ]
    )

    assert result["exit_code"] == 0
    assert result["stdout"] == {"cwd": "/workspace", "python": "3.12.0"}


def test_validated_sandbox_demo_result_rejects_non_zero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    with pytest.raises(RuntimeError, match="exit_code=2"):
        module._validated_sandbox_demo_result(
            [
                _sandbox_tool_message(
                    module,
                    _sandbox_tool_result_content(exit_code=2, stderr="boom"),
                )
            ]
        )


def test_validated_sandbox_demo_result_rejects_missing_expected_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    with pytest.raises(RuntimeError, match="expected JSON"):
        module._validated_sandbox_demo_result(
            [
                _sandbox_tool_message(
                    module, _sandbox_tool_result_content(stdout="hello\n")
                )
            ]
        )

    with pytest.raises(RuntimeError, match="'python'"):
        module._validated_sandbox_demo_result(
            [
                _sandbox_tool_message(
                    module,
                    _sandbox_tool_result_content(stdout=json.dumps({"cwd": "/tmp"})),
                )
            ]
        )


def test_validated_sandbox_demo_result_rejects_malformed_tool_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    with pytest.raises(RuntimeError, match="not valid JSON"):
        module._validated_sandbox_demo_result(
            [_sandbox_tool_message(module, "not-json")]
        )


def test_validated_sandbox_demo_result_rejects_duplicate_matching_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    with pytest.raises(RuntimeError, match="expected exactly one"):
        module._validated_sandbox_demo_result(
            [
                _sandbox_tool_message(module, _sandbox_tool_result_content()),
                _sandbox_tool_message(module, _sandbox_tool_result_content()),
            ]
        )


def test_validated_sandbox_demo_result_requires_matching_tool_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)

    with pytest.raises(RuntimeError, match="did not find"):
        module._validated_sandbox_demo_result([])

    with pytest.raises(RuntimeError, match="did not find"):
        module._validated_sandbox_demo_result(
            [SimpleNamespace(type="tool", tool_call_id="wrong", content="{}")]
        )


def test_force_sandbox_tool_choice_forces_specific_tool_only_initially(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest([{"role": "user", "content": "run the command"}])
    handled_requests: list[Any] = []
    response = SimpleNamespace(
        result=[
            SimpleNamespace(
                tool_calls=[
                    {
                        "name": module.SANDBOX_COMMAND_TOOL_NAME,
                        "args": {"command": "model-chosen-command"},
                        "id": "sandbox-call-1",
                    }
                ]
            )
        ]
    )

    def handler(model_request: Any) -> Any:
        handled_requests.append(model_request)
        return response

    result = middleware.wrap_model_call(request, handler)

    assert result is response
    assert len(handled_requests) == 1
    assert handled_requests[0].override_kwargs == request.overrides[0]
    assert request.overrides == [
        {
            "tool_choice": {
                "type": "function",
                "function": {"name": module.SANDBOX_COMMAND_TOOL_NAME},
            },
            "model_settings": {"parallel_tool_calls": False},
        }
    ]


def test_force_sandbox_tool_choice_preserves_existing_model_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest(
        [{"role": "user", "content": "run the command"}],
        model_settings={"temperature": 0, "parallel_tool_calls": True},
    )
    response = SimpleNamespace(
        result=[
            SimpleNamespace(
                tool_calls=[
                    {
                        "name": module.SANDBOX_COMMAND_TOOL_NAME,
                        "args": {"command": "model-chosen-command"},
                        "id": "sandbox-call-1",
                    }
                ]
            )
        ]
    )

    middleware.wrap_model_call(request, lambda _request: response)

    assert request.overrides == [
        {
            "tool_choice": {
                "type": "function",
                "function": {"name": module.SANDBOX_COMMAND_TOOL_NAME},
            },
            "model_settings": {"temperature": 0, "parallel_tool_calls": False},
        }
    ]


def test_force_sandbox_tool_choice_rewrites_tool_args_to_demo_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest([{"role": "user", "content": "run the command"}])
    sandbox_tool_call = {
        "name": module.SANDBOX_COMMAND_TOOL_NAME,
        "args": {"command": "python -c 'print(999)'", "cwd": "/tmp"},
        "id": "sandbox-call-1",
        "tool_call_id": "sandbox-call-1",
        "call_id": "sandbox-call-1",
    }
    other_tool_call = {
        "name": "other_tool",
        "args": {"command": "leave this alone"},
        "id": "other-call-1",
    }
    response = SimpleNamespace(
        result=[SimpleNamespace(tool_calls=[sandbox_tool_call, other_tool_call])]
    )

    result = middleware.wrap_model_call(request, lambda _request: response)

    assert result is response
    assert sandbox_tool_call == {
        "name": module.SANDBOX_COMMAND_TOOL_NAME,
        "args": {"command": module.SANDBOX_DEMO_COMMAND},
        "id": module.SANDBOX_DEMO_TOOL_CALL_ID,
        "tool_call_id": module.SANDBOX_DEMO_TOOL_CALL_ID,
        "call_id": module.SANDBOX_DEMO_TOOL_CALL_ID,
    }
    assert other_tool_call == {
        "name": "other_tool",
        "args": {"command": "leave this alone"},
        "id": "other-call-1",
    }


def test_force_sandbox_tool_choice_rejects_multiple_sandbox_tool_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest([{"role": "user", "content": "run the command"}])
    response = SimpleNamespace(
        result=[
            SimpleNamespace(
                tool_calls=[
                    {
                        "name": module.SANDBOX_COMMAND_TOOL_NAME,
                        "args": {"command": "first"},
                        "id": "sandbox-call-1",
                    },
                    {
                        "name": module.SANDBOX_COMMAND_TOOL_NAME,
                        "args": {"command": "second"},
                        "id": "sandbox-call-2",
                    },
                ]
            )
        ]
    )

    with pytest.raises(RuntimeError, match="expected exactly one"):
        middleware.wrap_model_call(request, lambda _request: response)


def test_force_sandbox_tool_choice_does_not_force_after_tool_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest(
        [
            {"role": "user", "content": "run the command"},
            SimpleNamespace(type="tool", content='{"stdout": "hello\\n"}'),
        ]
    )
    handled_requests: list[Any] = []
    response = SimpleNamespace(result=[SimpleNamespace(content="done", tool_calls=[])])

    def handler(model_request: Any) -> Any:
        handled_requests.append(model_request)
        return response

    result = middleware.wrap_model_call(request, handler)

    assert result is response
    assert handled_requests == [request]
    assert request.overrides == []


def test_force_sandbox_tool_choice_rejects_sandbox_tool_after_tool_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_example_module(monkeypatch)
    middleware = module.ForceSandboxToolChoiceMiddleware()
    request = _FakeModelRequest(
        [
            {"role": "user", "content": "run the command"},
            SimpleNamespace(type="tool", content='{"stdout": "hello\\n"}'),
        ]
    )
    handled_requests: list[Any] = []
    response = SimpleNamespace(
        result=[
            SimpleNamespace(
                tool_calls=[
                    {
                        "name": module.SANDBOX_COMMAND_TOOL_NAME,
                        "args": {"command": "second-command"},
                        "id": "sandbox-call-2",
                    }
                ]
            )
        ]
    )

    def handler(model_request: Any) -> Any:
        handled_requests.append(model_request)
        return response

    with pytest.raises(RuntimeError, match="expected no additional"):
        middleware.wrap_model_call(request, handler)

    assert handled_requests == [request]
    assert request.overrides == []
