"""Model/tool wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    model_module = importlib.import_module("kitaru.adapters.google_adk._model")
    tool_module = importlib.import_module("kitaru.adapters.google_adk._tool")
    return adapter, model_module, tool_module


class FakeModel:
    model = "gemini-fake"

    def supported_models(self) -> list[str]:
        return ["gemini-fake", "gemini-fake-pro"]

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        yield {"text": f"response:{llm_request['prompt']}", "stream": stream}


class FakeTool:
    name = "search"
    description = "Search fake data."

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        return {"result": args["query"], "context": type(tool_context).__name__}


class FakeToolContext:
    def __init__(self, state: dict[str, Any] | None = None) -> None:
        self.state = dict(state or {})


class StatefulTool(FakeTool):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["lookup_marker"] = "local-cat-fact"
        return {"answer": "local-cat-fact", "query": args["query"]}


class NestedStatefulTool(FakeTool):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["nested"] = {"answer": "local-cat-fact"}
        return {"answer": "local-cat-fact", "query": args["query"]}


class SyncProcessTool(FakeTool):
    def __init__(self) -> None:
        self.processed: list[tuple[Any, Any]] = []

    def process_llm_request(self, *, tool_context: Any, llm_request: Any) -> None:
        self.processed.append((tool_context, llm_request))


class AsyncProcessTool(FakeTool):
    def __init__(self) -> None:
        self.processed: list[tuple[Any, Any]] = []

    async def process_llm_request(self, *, tool_context: Any, llm_request: Any) -> None:
        self.processed.append((tool_context, llm_request))


async def _collect_model(model: Any, request: dict[str, Any]) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def test_model_wrapper_is_base_llm_and_delegates_supported_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    from google.adk.models.base_llm import BaseLlm

    wrapped = adapter.KitaruADKModel(FakeModel())

    assert isinstance(wrapped, BaseLlm)
    assert wrapped.supported_models() == ["gemini-fake", "gemini-fake-pro"]


def test_model_wrapper_without_supported_models_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    model_without_registry = object()

    wrapped = adapter.KitaruADKModel(model_without_registry)

    assert wrapped.supported_models() == []


def test_tool_wrapper_is_base_tool_and_accepts_sync_process_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    from google.adk.tools.base_tool import BaseTool

    tool = SyncProcessTool()
    wrapped = adapter.wrap_tool(tool)
    context = object()
    request = {"prompt": "hi"}

    assert isinstance(wrapped, BaseTool)
    asyncio.run(wrapped.process_llm_request(tool_context=context, llm_request=request))

    assert tool.processed == [(context, request)]


def test_tool_wrapper_process_llm_request_accepts_async_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    tool = AsyncProcessTool()
    wrapped = adapter.wrap_tool(tool)
    context = object()
    request = {"prompt": "hi"}

    asyncio.run(wrapped.process_llm_request(tool_context=context, llm_request=request))

    assert tool.processed == [(context, request)]


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


def test_tool_checkpoint_replay_restores_tool_context_state_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)
    first_context = FakeToolContext({"seed": "same"})
    second_context = FakeToolContext({"seed": "same"})

    first_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=first_context)
    )
    second_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=second_context)
    )

    assert (
        first_result
        == second_result
        == {
            "answer": "local-cat-fact",
            "query": "cats",
        }
    )
    assert tool.calls == [{"query": "cats"}]
    assert first_context.state == {
        "seed": "same",
        "lookup_marker": "local-cat-fact",
    }
    assert second_context.state == {
        "seed": "same",
        "lookup_marker": "local-cat-fact",
    }


def test_tool_checkpoint_input_includes_starting_state_to_avoid_stale_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}
    checkpoint_inputs: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_inputs.append(kwargs["checkpoint_inputs"]["tool_args"])
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)
    first_context = FakeToolContext({"seed": "first"})
    second_context = FakeToolContext({"seed": "second"})

    asyncio.run(wrapped.run_async(args={"query": "cats"}, tool_context=first_context))
    asyncio.run(wrapped.run_async(args={"query": "cats"}, tool_context=second_context))

    assert tool.calls == [{"query": "cats"}, {"query": "cats"}]
    assert len(checkpoint_cache) == 2
    assert [
        item["tool_context_state_before"]["value"]["seed"] for item in checkpoint_inputs
    ] == ["first", "second"]


def test_tool_checkpoint_cache_identity_keeps_redacted_state_values_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)

    asyncio.run(
        wrapped.run_async(
            args={"query": "cats"},
            tool_context=FakeToolContext({"api_token": "first"}),
        )
    )
    asyncio.run(
        wrapped.run_async(
            args={"query": "cats"},
            tool_context=FakeToolContext({"api_token": "second"}),
        )
    )

    assert tool.calls == [{"query": "cats"}, {"query": "cats"}]
    assert len(checkpoint_cache) == 2


def test_tool_checkpoint_replay_copies_nested_state_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = NestedStatefulTool()
    wrapped = adapter.wrap_tool(tool)

    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=FakeToolContext())
    )
    replayed_context = FakeToolContext()
    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=replayed_context)
    )
    replayed_context.state["nested"]["answer"] = "corrupted"
    later_replayed_context = FakeToolContext()
    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=later_replayed_context)
    )

    assert tool.calls == [{"query": "cats"}]
    assert later_replayed_context.state["nested"] == {"answer": "local-cat-fact"}


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
