"""Focused tests for fast-agent call-level checkpointing."""

from __future__ import annotations

import asyncio
import importlib
import sys
import types
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from zenml.client import Client

from kitaru import flow
from kitaru.errors import KitaruUsageError


def _purge_fast_agent_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.fast_agent"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fast_agent_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    _purge_fast_agent_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "fast_agent", types.ModuleType("fast_agent"))
    return importlib.import_module("kitaru.adapters.fast_agent")


class FakeLLM:
    def __init__(self) -> None:
        self.model_name = "fake-model"
        self.provider = "fake-provider"
        self.generate_calls = 0
        self.structured_calls = 0
        self.structured_schema_calls = 0

    async def generate(self, prompt: str) -> str:
        self.generate_calls += 1
        return f"model:{prompt}:{self.generate_calls}"

    async def structured(
        self,
        prompt: str,
        schema: type[object],
    ) -> tuple[str, str, int]:
        self.structured_calls += 1
        return schema.__name__, prompt, self.structured_calls

    async def structured_schema(
        self,
        prompt: str,
        schema: dict[str, Any],
    ) -> tuple[dict[str, Any], str, int]:
        self.structured_schema_calls += 1
        return schema, prompt, self.structured_schema_calls


class FakeAgent:
    def __init__(self, name: str) -> None:
        self.name = name
        self._llm = FakeLLM()
        self.tool_calls = 0

    @property
    def llm(self) -> Any:
        return self._llm

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.tool_calls += 1
        return {
            "name": name,
            "arguments": arguments or {},
            "call_number": self.tool_calls,
        }


class FakeApp:
    def __init__(self, agent: FakeAgent) -> None:
        self._agents = {agent.name: agent}


class FakeFastAgent:
    def __init__(self, app: FakeApp) -> None:
        self._app = app

    @asynccontextmanager
    async def run(self) -> AsyncIterator[FakeApp]:
        yield self._app


class PlannedToolCall:
    def __init__(self, name: str, arguments: dict[str, Any]) -> None:
        self.name = name
        self.arguments = arguments


class OpaqueToolCall:
    __slots__ = ("_secret",)

    def __init__(self, secret: str) -> None:
        self._secret = secret


def _wait_for_hydrated_run(exec_id: str) -> Any:
    run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    if not run.status.is_finished:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    assert run.status.is_successful
    return run.get_hydrated_version()


def _step_names(hydrated_run: Any) -> set[str]:
    return set(hydrated_run.steps)


def test_calls_strategy_model_checkpoint_cache_skips_inner_model(
    fast_agent_adapter: types.ModuleType,
    primed_zenml: None,
) -> None:
    del primed_zenml
    agent = FakeAgent(name=f"fast_agent_model_{uuid4().hex[:8]}")
    runner = fast_agent_adapter.KitaruFastAgent(FakeFastAgent(FakeApp(agent)))

    @flow
    def fast_agent_model_flow(prompt: str, nonce: str) -> str:
        _ = nonce

        async def exercise() -> str:
            async with runner.run():
                return await agent.llm.generate(prompt)

        return asyncio.run(exercise())

    first = fast_agent_model_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert any("generate_model_call" in name for name in _step_names(first_hydrated))
    assert agent.llm.original_llm.generate_calls == 1

    second = fast_agent_model_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert agent.llm.original_llm.generate_calls == 1

    third = fast_agent_model_flow.run("different prompt", "third")
    _wait_for_hydrated_run(third.exec_id)
    assert agent.llm.original_llm.generate_calls == 2


def test_calls_strategy_tool_checkpoint_cache_skips_inner_tool(
    fast_agent_adapter: types.ModuleType,
    primed_zenml: None,
) -> None:
    del primed_zenml
    agent = FakeAgent(name=f"fast_agent_tool_{uuid4().hex[:8]}")
    runner = fast_agent_adapter.KitaruFastAgent(FakeFastAgent(FakeApp(agent)))

    @flow
    def fast_agent_tool_flow(topic: str, nonce: str) -> dict[str, Any]:
        _ = nonce

        async def exercise() -> dict[str, Any]:
            async with runner.run():
                return await agent.call_tool("lookup", {"topic": topic})

        return asyncio.run(exercise())

    first = fast_agent_tool_flow.run("kitaru", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert any("lookup_tool_call" in name for name in _step_names(first_hydrated))
    assert agent.tool_calls == 1

    second = fast_agent_tool_flow.run("kitaru", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert agent.tool_calls == 1

    third = fast_agent_tool_flow.run("different topic", "third")
    _wait_for_hydrated_run(third.exec_id)
    assert agent.tool_calls == 2


def test_calls_strategy_structured_checkpoint_cache_skips_inner_model(
    fast_agent_adapter: types.ModuleType,
    primed_zenml: None,
) -> None:
    del primed_zenml
    agent = FakeAgent(name=f"fast_agent_structured_{uuid4().hex[:8]}")
    runner = fast_agent_adapter.KitaruFastAgent(FakeFastAgent(FakeApp(agent)))

    @flow
    def fast_agent_structured_flow(prompt: str, nonce: str) -> tuple[str, str, int]:
        _ = nonce

        async def exercise() -> tuple[str, str, int]:
            async with runner.run():
                return await agent.llm.structured(prompt, dict)

        return asyncio.run(exercise())

    first = fast_agent_structured_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert any("structured_model_call" in name for name in _step_names(first_hydrated))
    assert agent.llm.original_llm.structured_calls == 1

    second = fast_agent_structured_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert agent.llm.original_llm.structured_calls == 1

    third = fast_agent_structured_flow.run("different prompt", "third")
    _wait_for_hydrated_run(third.exec_id)
    assert agent.llm.original_llm.structured_calls == 2


def test_calls_strategy_structured_schema_checkpoint_cache_skips_inner_model(
    fast_agent_adapter: types.ModuleType,
    primed_zenml: None,
) -> None:
    del primed_zenml
    agent = FakeAgent(name=f"fast_agent_schema_{uuid4().hex[:8]}")
    runner = fast_agent_adapter.KitaruFastAgent(FakeFastAgent(FakeApp(agent)))

    @flow
    def fast_agent_schema_flow(
        prompt: str,
        nonce: str,
    ) -> tuple[dict[str, Any], str, int]:
        _ = nonce

        async def exercise() -> tuple[dict[str, Any], str, int]:
            async with runner.run():
                return await agent.llm.structured_schema(
                    prompt,
                    {"type": "object", "properties": {"answer": {"type": "string"}}},
                )

        return asyncio.run(exercise())

    first = fast_agent_schema_flow.run("stable prompt", "first")
    first_hydrated = _wait_for_hydrated_run(first.exec_id)
    assert any(
        "structured_schema_model_call" in name for name in _step_names(first_hydrated)
    )
    assert agent.llm.original_llm.structured_schema_calls == 1

    second = fast_agent_schema_flow.run("stable prompt", "second")
    _wait_for_hydrated_run(second.exec_id)
    assert agent.llm.original_llm.structured_schema_calls == 1

    third = fast_agent_schema_flow.run("different prompt", "third")
    _wait_for_hydrated_run(third.exec_id)
    assert agent.llm.original_llm.structured_schema_calls == 2


def test_checkpoint_identity_uses_public_logical_inputs_not_object_identity(
    fast_agent_adapter: types.ModuleType,
) -> None:
    from kitaru.adapters.fast_agent._utils import checkpoint_cache_key
    from kitaru.adapters.fast_agent._wrapping import (
        FastAgentCall,
        _checkpoint_call_input,
    )

    first_request = PlannedToolCall("lookup", {"topic": "kitaru"})
    second_request = PlannedToolCall("lookup", {"topic": "kitaru"})

    first = _checkpoint_call_input(
        FastAgentCall(
            agent_name="researcher",
            kind="tool",
            operation="call_tool",
            args=(first_request,),
            kwargs={},
            tool_name="lookup",
        )
    )
    second = _checkpoint_call_input(
        FastAgentCall(
            agent_name="researcher",
            kind="tool",
            operation="call_tool",
            args=(second_request,),
            kwargs={},
            tool_name="lookup",
        )
    )

    assert first == second
    assert checkpoint_cache_key(first) == checkpoint_cache_key(second)
    assert "object at 0x" not in repr(first)
    assert "object at 0x" not in repr(second)


def test_checkpoint_config_rejects_isolated_runtime(
    fast_agent_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        fast_agent_adapter.KitaruFastAgent(
            FakeFastAgent(FakeApp(FakeAgent("researcher"))),
            model_checkpoint_config={"runtime": "isolated"},
        )


def test_checkpoint_identity_rejects_opaque_objects_without_stable_fields(
    fast_agent_adapter: types.ModuleType,
) -> None:
    from kitaru.adapters.fast_agent._wrapping import (
        FastAgentCall,
        _checkpoint_call_input,
    )

    with pytest.raises(KitaruUsageError, match="stable fast-agent checkpoint input"):
        _checkpoint_call_input(
            FastAgentCall(
                agent_name="researcher",
                kind="tool",
                operation="call_tool",
                args=(OpaqueToolCall("hidden-a"),),
                kwargs={},
                tool_name="lookup",
            )
        )
