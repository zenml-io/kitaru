"""Tests for the LangGraph sandbox example wiring."""

from __future__ import annotations

import importlib.util
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


class _FakeModelRequest:
    """Tiny request object that records LangChain-style override calls."""

    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages
        self.overrides: list[dict[str, Any]] = []

    def override(self, **kwargs: Any) -> Any:
        self.overrides.append(kwargs)
        return SimpleNamespace(messages=self.messages, override_kwargs=kwargs)


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
            }
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
        "id": "sandbox-call-1",
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

    def handler(model_request: Any) -> Any:
        handled_requests.append(model_request)
        return model_request

    result = middleware.wrap_model_call(request, handler)

    assert result is request
    assert handled_requests == [request]
    assert request.overrides == []
