"""Foundation tests for the fast-agent adapter wrapper skeleton."""

from __future__ import annotations

import asyncio
import importlib
import sys
import types
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from kitaru.errors import KitaruFeatureNotAvailableError


def _purge_fast_agent_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.fast_agent"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_fast_agent(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    module = types.ModuleType("fast_agent")
    monkeypatch.setitem(sys.modules, "fast_agent", module)
    return module


@pytest.fixture
def fast_agent_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_fast_agent: types.ModuleType,
) -> types.ModuleType:
    _purge_fast_agent_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.fast_agent")


class FakeLLM:
    def __init__(self) -> None:
        self.model_name = "fake-model"
        self.provider = "fake-provider"
        self.generate_calls = 0
        self.structured_calls = 0

    async def generate(self, prompt: str) -> str:
        self.generate_calls += 1
        return f"model:{prompt}"

    async def structured(self, prompt: str, schema: type[object]) -> tuple[str, str]:
        self.structured_calls += 1
        return schema.__name__, f"structured:{prompt}"


class FakeAgent:
    def __init__(self, name: str = "researcher") -> None:
        self.name = name
        self._llm = FakeLLM()
        self.tool_calls = 0

    @property
    def llm(self) -> FakeLLM:
        return self._llm

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.tool_calls += 1
        return {"name": name, "arguments": arguments or {}}

    async def spawn_detached_instance(self, *, name: str | None = None) -> FakeAgent:
        return FakeAgent(name or f"{self.name}[clone]")


class FakeApp:
    def __init__(self, agents: dict[str, FakeAgent]) -> None:
        self._agents = agents


class FakeFastAgent:
    def __init__(self, app: FakeApp) -> None:
        self.app = app

    @asynccontextmanager
    async def run(self) -> AsyncIterator[FakeApp]:
        yield self.app


def test_import_without_fast_agent_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_fast_agent_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "fast_agent", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="fast-agent-mcp"):
        importlib.import_module("kitaru.adapters.fast_agent")


def test_model_calls_route_through_wrapper(
    fast_agent_adapter: types.ModuleType,
) -> None:
    agent = FakeAgent()
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)

    generated = asyncio.run(agent.llm.generate("hello"))
    structured = asyncio.run(agent.llm.structured("hello", dict))

    assert generated == "model:hello"
    assert structured == ("dict", "structured:hello")
    assert agent.llm.original_llm.generate_calls == 1
    assert agent.llm.original_llm.structured_calls == 1
    assert [(call.kind, call.operation, call.agent_name) for call in recorded] == [
        ("model", "generate", "researcher"),
        ("model", "structured", "researcher"),
    ]
    assert recorded[0].model_name == "fake-model"
    assert recorded[0].provider == "fake-provider"


def test_local_tool_calls_route_through_wrapper(
    fast_agent_adapter: types.ModuleType,
) -> None:
    agent = FakeAgent()
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)

    result = asyncio.run(agent.call_tool("lookup", {"topic": "kitaru"}))

    assert result == {"name": "lookup", "arguments": {"topic": "kitaru"}}
    assert agent.tool_calls == 1
    assert [(call.kind, call.operation, call.tool_name) for call in recorded] == [
        ("tool", "call_tool", "lookup")
    ]


def test_detached_clone_is_wrapped_recursively(
    fast_agent_adapter: types.ModuleType,
) -> None:
    agent = FakeAgent()
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)
    clone = asyncio.run(agent.spawn_detached_instance(name="researcher[1]"))

    result = asyncio.run(clone.llm.generate("child prompt"))

    assert result == "model:child prompt"
    assert clone.llm.original_llm.generate_calls == 1
    assert [(call.kind, call.operation, call.agent_name) for call in recorded] == [
        ("model", "generate", "researcher[1]")
    ]


def test_wrapping_is_idempotent(fast_agent_adapter: types.ModuleType) -> None:
    agent = FakeAgent()
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)
    wrapped_llm = agent.llm
    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)

    result = asyncio.run(agent.llm.generate("once"))

    assert result == "model:once"
    assert agent.llm is wrapped_llm
    assert agent.llm.original_llm.generate_calls == 1
    assert [(call.kind, call.operation) for call in recorded] == [("model", "generate")]


def test_app_wrapper_wraps_agents_from_private_agent_mapping(
    fast_agent_adapter: types.ModuleType,
) -> None:
    agent = FakeAgent(name="writer")
    app = FakeApp({"writer": agent})
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    returned = fast_agent_adapter.wrap_fast_agent_app(app, recorder=recorder)

    assert returned is app
    assert asyncio.run(agent.llm.generate("draft")) == "model:draft"
    assert [(call.kind, call.agent_name) for call in recorded] == [("model", "writer")]


def test_public_runner_wraps_app_after_fast_agent_run(
    fast_agent_adapter: types.ModuleType,
) -> None:
    agent = FakeAgent(name="runner-agent")
    app = FakeApp({"runner-agent": agent})
    fast = FakeFastAgent(app)
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    runner = fast_agent_adapter.KitaruFastAgent(fast, call_recorder=recorder)

    async def exercise() -> str:
        async with runner.run() as returned:
            assert returned is app
            return await agent.llm.generate("from runner")

    assert asyncio.run(exercise()) == "model:from runner"
    assert [(call.kind, call.agent_name) for call in recorded] == [
        ("model", "runner-agent")
    ]
