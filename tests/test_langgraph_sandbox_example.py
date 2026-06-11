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

    def handler(model_request: Any) -> Any:
        handled_requests.append(model_request)
        return model_request

    result = middleware.wrap_model_call(request, handler)

    assert result is handled_requests[0]
    assert request.overrides == [
        {
            "tool_choice": {
                "type": "function",
                "function": {"name": module.SANDBOX_COMMAND_TOOL_NAME},
            }
        }
    ]


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
