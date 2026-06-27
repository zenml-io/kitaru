"""Installed Google ADK public API contract tests.

These tests use real ``google-adk`` classes when the optional dependency is
installed. They do not instantiate hosted provider clients, run an ADK runner,
or make network calls.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest


def _import_adk_module(module_path: str) -> Any:
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        missing_requested_module = exc.name is not None and (
            module_path == exc.name or module_path.startswith(f"{exc.name}.")
        )
        if not missing_requested_module:
            raise
        if os.environ.get("KITARU_REQUIRE_GOOGLE_ADK_CONTRACT") == "1":
            pytest.fail(
                "google-adk contract tests were required, but "
                f"`{module_path}` could not be imported. Install the optional "
                "extra in a no-dev environment, for example: "
                "uv run --no-dev --extra google-adk --with pytest pytest "
                "tests/test_google_adk_installed_contract.py"
            )
        pytest.skip("google-adk optional dependency is not installed")


def _adk_contract_classes() -> tuple[type[Any], type[Any], type[Any]]:
    _import_adk_module("google.adk")
    base_llm_module = _import_adk_module("google.adk.models.base_llm")
    base_tool_module = _import_adk_module("google.adk.tools.base_tool")
    try:
        agents_module = _import_adk_module("google.adk.agents")
        llm_agent = agents_module.LlmAgent
    except (AttributeError, ImportError):
        llm_agent_module = _import_adk_module("google.adk.agents.llm_agent")
        llm_agent = llm_agent_module.LlmAgent
    return base_llm_module.BaseLlm, base_tool_module.BaseTool, llm_agent


def _adapter_wrappers() -> tuple[type[Any], type[Any]]:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            del sys.modules[cached]
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    return adapter.KitaruADKModel, adapter.KitaruADKTool


async def _collect_model_events(model: Any, request: Any) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def test_kitaru_adk_model_preserves_real_base_llm_public_behavior() -> None:
    BaseLlm, _BaseTool, _LlmAgent = _adk_contract_classes()
    KitaruADKModel, _KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield {"request": llm_request, "stream": stream}

    wrapped = KitaruADKModel(ContractLlm(model="kitaru-contract-model"))

    assert wrapped.model == "kitaru-contract-model"
    assert wrapped.supported_models() == ["kitaru-contract-model"]
    signature = inspect.signature(wrapped.generate_content_async)
    assert "llm_request" in signature.parameters
    assert "stream" in signature.parameters

    events = asyncio.run(_collect_model_events(wrapped, {"prompt": "local only"}))
    assert events == [{"request": {"prompt": "local only"}, "stream": False}]


def test_real_llm_agent_accepts_kitaru_model_wrapper() -> None:
    BaseLlm, _BaseTool, LlmAgent = _adk_contract_classes()
    KitaruADKModel, _KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield {"request": llm_request, "stream": stream}

    wrapped = KitaruADKModel(ContractLlm(model="kitaru-contract-model"))

    assert isinstance(wrapped, BaseLlm)
    agent = LlmAgent(name="contract_agent", model=wrapped)
    assert agent.model is wrapped
    assert isinstance(agent.model, BaseLlm)

    events = asyncio.run(_collect_model_events(agent.model, {"prompt": "local only"}))
    assert events == [{"request": {"prompt": "local only"}, "stream": False}]


def test_kitaru_adk_tool_preserves_real_base_tool_public_behavior() -> None:
    _BaseLlm, BaseTool, _LlmAgent = _adk_contract_classes()
    _KitaruADKModel, KitaruADKTool = _adapter_wrappers()

    class ContractTool(BaseTool):  # type: ignore[misc, valid-type]
        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            return {"args": args, "context": type(tool_context).__name__}

    class SyncProcessTool(ContractTool):
        def __init__(self) -> None:
            super().__init__(
                name="contract_tool",
                description="Local contract tool",
                custom_metadata={"contract": True},
            )
            self.processed: list[tuple[Any, Any]] = []

        def process_llm_request(
            self,
            *,
            tool_context: Any,
            llm_request: Any,
        ) -> None:
            self.processed.append((tool_context, llm_request))

    class AsyncProcessTool(ContractTool):
        def __init__(self) -> None:
            super().__init__(
                name="async_contract_tool",
                description="Local async contract tool",
                custom_metadata={"contract": True},
            )
            self.processed: list[tuple[Any, Any]] = []

        async def process_llm_request(
            self,
            *,
            tool_context: Any,
            llm_request: Any,
        ) -> None:
            self.processed.append((tool_context, llm_request))

    sync_tool = SyncProcessTool()
    wrapped = KitaruADKTool(sync_tool)
    context = object()
    request = {"prompt": "local only"}

    assert wrapped.name == "contract_tool"
    assert wrapped.description == "Local contract tool"
    assert wrapped.is_long_running is False
    assert wrapped.custom_metadata == {"contract": True}
    assert asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=context)
    ) == {"args": {"query": "cats"}, "context": "object"}
    assert (
        asyncio.run(
            wrapped.process_llm_request(tool_context=context, llm_request=request)
        )
        is None
    )
    assert sync_tool.processed == [(context, request)]

    async_tool = AsyncProcessTool()
    wrapped_async = KitaruADKTool(async_tool)
    asyncio.run(
        wrapped_async.process_llm_request(tool_context=context, llm_request=request)
    )
    assert async_tool.processed == [(context, request)]


def test_real_llm_agent_accepts_kitaru_tool_wrapper() -> None:
    BaseLlm, BaseTool, LlmAgent = _adk_contract_classes()
    _KitaruADKModel, KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            if False:
                yield {"request": llm_request, "stream": stream}

    class ContractTool(BaseTool):  # type: ignore[misc, valid-type]
        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            return {"args": args, "context": type(tool_context).__name__}

    model = ContractLlm(model="kitaru-contract-model")
    tool = ContractTool(name="contract_tool", description="Local contract tool")
    wrapped = KitaruADKTool(tool)

    assert isinstance(wrapped, BaseTool)
    agent = LlmAgent(name="contract_agent", model=model, tools=[wrapped])
    assert agent.tools[0] is wrapped
    assert asyncio.run(
        agent.tools[0].run_async(args={"query": "cats"}, tool_context=object())
    ) == {"args": {"query": "cats"}, "context": "object"}
