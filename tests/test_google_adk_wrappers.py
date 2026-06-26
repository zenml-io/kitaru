"""Model/tool wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import asyncio
import importlib
import sys
from types import ModuleType
from typing import Any

import pytest

from kitaru.errors import KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    google = ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.adk", ModuleType("google.adk"))
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    model_module = importlib.import_module("kitaru.adapters.google_adk._model")
    tool_module = importlib.import_module("kitaru.adapters.google_adk._tool")
    return adapter, model_module, tool_module


class FakeModel:
    model = "gemini-fake"

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        yield {"text": f"response:{llm_request['prompt']}", "stream": stream}


class FakeTool:
    name = "search"
    description = "Search fake data."

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        return {"result": args["query"], "context": type(tool_context).__name__}


async def _collect_model(model: Any, request: dict[str, Any]) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def test_model_wrapper_buffers_generate_content_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, _tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(model_module, "run_async_in_checkpoint", fake_checkpoint)

    wrapped = adapter.KitaruADKModel(FakeModel())
    responses = asyncio.run(_collect_model(wrapped, {"prompt": "hi"}))

    assert responses == [{"text": "response:hi", "stream": False}]
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["model_input"]["model"]
        == "gemini-fake"
    )


def test_tool_wrapper_runs_tool_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    wrapped = adapter.wrap_tool(FakeTool())
    result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=object())
    )

    assert result["result"] == "cats"
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["tool_args"]["tool_name"] == "search"
    )


def test_model_wrapper_nested_checkpoint_limitation_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, _tool_module = _modules(monkeypatch)

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: True)

    wrapped = adapter.KitaruADKModel(FakeModel())

    with pytest.raises(KitaruUsageError, match="inside a Kitaru checkpoint"):
        asyncio.run(_collect_model(wrapped, {"prompt": "hi"}))


def test_tool_wrapper_metadata_only_nested_policy_runs_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: True)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    policy = adapter.ADKCallCheckpointPolicy(nested_checkpoint_policy="metadata_only")
    wrapped = adapter.wrap_tool(FakeTool(), call_policy=policy)
    result = asyncio.run(
        wrapped.run_async(args={"query": "dogs"}, tool_context=object())
    )

    assert result["result"] == "dogs"
    assert checkpoint_calls == []
