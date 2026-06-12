"""Cheap contract tests for the coding-agent example.

These tests deliberately stop at import/model/helper coverage. The real agent loop
uses provider SDKs and belongs in live/manual coverage, not default PR pytest.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

_EXAMPLE_DIR = (
    Path(__file__).resolve().parent.parent / "examples" / "end_to_end" / "coding_agent"
)
_EXAMPLE_MODULES = {
    "agent",
    "llm",
    "materializers",
    "models",
    "prompts",
    "tools",
}


def _module_lives_in_coding_agent_example(module: ModuleType) -> bool:
    module_file = getattr(module, "__file__", None)
    if not module_file:
        return False
    return Path(module_file).resolve().is_relative_to(_EXAMPLE_DIR)


def _purge_coding_agent_modules(monkeypatch: Any) -> None:
    for module_name in list(sys.modules):
        module = sys.modules[module_name]
        if module_name in _EXAMPLE_MODULES and _module_lives_in_coding_agent_example(
            module
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)


def _import_coding_agent_module(
    monkeypatch: Any,
    module_name: str,
) -> ModuleType:
    monkeypatch.syspath_prepend(str(_EXAMPLE_DIR))
    monkeypatch.setenv("CODING_AGENT_MODEL", "openai/gpt-4o-mini")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key-never-used")
    _purge_coding_agent_modules(monkeypatch)
    return importlib.import_module(module_name)


def test_coding_agent_models_round_trip_tool_call_messages(monkeypatch) -> None:
    """The checkpoint-safe model objects produce provider-compatible messages."""
    models = _import_coding_agent_module(monkeypatch, "models")

    response = models.LLMResponse(
        role="assistant",
        content=None,
        tool_calls=[
            models.ToolCallRequest(
                id="call_1",
                function=models.ToolCallFunction(
                    name="read_file",
                    arguments='{"path": "README.md"}',
                ),
            )
        ],
    )

    assert response.has_tool_calls is True
    assert response.to_message() == {
        "role": "assistant",
        "content": None,
        "tool_calls": [
            {
                "id": "call_1",
                "type": "function",
                "function": {
                    "name": "read_file",
                    "arguments": '{"path": "README.md"}',
                },
            }
        ],
    }


def test_coding_agent_entrypoint_imports_without_running_provider_loop(
    monkeypatch,
) -> None:
    """Importing the script should build the flow but not call a provider."""
    agent = _import_coding_agent_module(monkeypatch, "agent")

    assert agent.MODEL == "openai/gpt-4o-mini"
    assert agent.coding_agent is not None
    assert agent.main.name == "main"
    assert agent._parse_tool_arguments('{"path": "README.md"}') == (
        {"path": "README.md"},
        None,
    )
    parsed, parse_error = agent._parse_tool_arguments("not-json")
    assert parsed == {}
    assert parse_error is not None
    assert "Invalid JSON" in parse_error
    assert agent._make_display_name("read_file", "Read README!", 7) == "read_readme__7"
