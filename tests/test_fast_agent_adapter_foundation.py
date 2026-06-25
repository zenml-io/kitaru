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

from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError


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
        self.structured_schema_calls = 0

    async def generate(self, prompt: str) -> str:
        self.generate_calls += 1
        return f"model:{prompt}"

    async def structured(self, prompt: str, schema: type[object]) -> tuple[str, str]:
        self.structured_calls += 1
        return schema.__name__, f"structured:{prompt}"

    async def structured_schema(
        self,
        prompt: str,
        schema: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        self.structured_schema_calls += 1
        return schema, f"structured-schema:{prompt}"


class FakeAgent:
    def __init__(self, name: str = "researcher") -> None:
        self.name = name
        self._llm: Any = FakeLLM()
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

    with pytest.raises(KitaruFeatureNotAvailableError) as exc_info:
        importlib.import_module("kitaru.adapters.fast_agent")

    message = str(exc_info.value)
    assert "fast-agent-mcp" in message
    assert "kitaru[fast-agent]" in message


def test_import_with_fast_agent_exposes_public_contract(
    fast_agent_adapter: types.ModuleType,
) -> None:
    public_names = set(fast_agent_adapter.__all__)

    assert {
        "CheckpointConfig",
        "FastAgentCall",
        "FastAgentCallRecorder",
        "FastAgentUsageSummary",
        "KitaruFastAgent",
        "KitaruFastAgentCallRecorder",
        "kitaru_call_recorder",
        "passthrough_call_recorder",
        "wrap_fast_agent_agent",
        "wrap_fast_agent_app",
    } <= public_names


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
    structured_schema = asyncio.run(
        agent.llm.structured_schema("hello", {"type": "object"})
    )

    assert generated == "model:hello"
    assert structured == ("dict", "structured:hello")
    assert structured_schema == ({"type": "object"}, "structured-schema:hello")
    assert agent.llm.original_llm.generate_calls == 1
    assert agent.llm.original_llm.structured_calls == 1
    assert agent.llm.original_llm.structured_schema_calls == 1
    assert [(call.kind, call.operation, call.agent_name) for call in recorded] == [
        ("model", "generate", "researcher"),
        ("model", "structured", "researcher"),
        ("model", "structured_schema", "researcher"),
    ]
    assert recorded[0].model_name == "fake-model"
    assert recorded[0].provider == "fake-provider"


def test_optional_llm_methods_are_only_exposed_when_original_supports_them(
    fast_agent_adapter: types.ModuleType,
) -> None:
    class GenerateOnlyLLM:
        model_name = "minimal-model"
        provider = "memory"

        def __init__(self) -> None:
            self.generate_calls = 0

        async def generate(self, prompt: str) -> str:
            self.generate_calls += 1
            return f"minimal:{prompt}"

    agent = FakeAgent()
    agent._llm = GenerateOnlyLLM()
    recorded: list[Any] = []

    def recorder(call: Any, proceed: Callable[[], Any]) -> Any:
        recorded.append(call)
        return proceed()

    fast_agent_adapter.wrap_fast_agent_agent(agent, recorder=recorder)

    assert hasattr(agent.llm, "generate")
    assert not hasattr(agent.llm, "structured")
    assert not hasattr(agent.llm, "structured_schema")
    assert asyncio.run(agent.llm.generate("hello")) == "minimal:hello"
    assert agent.llm.original_llm.generate_calls == 1
    assert [(call.kind, call.operation) for call in recorded] == [("model", "generate")]


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


def test_app_wrapper_requires_discoverable_agent_mapping(
    fast_agent_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="Could not discover fast-agent agents"):
        fast_agent_adapter.wrap_fast_agent_app(types.SimpleNamespace())


def test_app_wrapper_requires_non_empty_agent_mapping(
    fast_agent_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="non-empty"):
        fast_agent_adapter.wrap_fast_agent_app(FakeApp({}))


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


def test_public_runner_uses_default_recorder_when_omitted(
    fast_agent_adapter: types.ModuleType,
) -> None:
    runner = fast_agent_adapter.KitaruFastAgent(
        FakeFastAgent(FakeApp({"a": FakeAgent()}))
    )

    assert isinstance(
        runner._call_recorder,
        fast_agent_adapter.KitaruFastAgentCallRecorder,
    )


def test_public_runner_keeps_none_call_recorder_as_passthrough(
    fast_agent_adapter: types.ModuleType,
) -> None:
    runner = fast_agent_adapter.KitaruFastAgent(
        FakeFastAgent(FakeApp({"a": FakeAgent()})),
        call_recorder=None,
    )

    assert runner._call_recorder is None


def test_public_runner_rejects_usage_options_with_passthrough_recorder(
    fast_agent_adapter: types.ModuleType,
) -> None:
    with pytest.raises(
        KitaruUsageError,
        match="save_usage and cost_calculator only apply",
    ):
        fast_agent_adapter.KitaruFastAgent(
            FakeFastAgent(FakeApp({"a": FakeAgent()})),
            call_recorder=None,
            save_usage=False,
        )

    with pytest.raises(
        KitaruUsageError,
        match="save_usage and cost_calculator only apply",
    ):
        fast_agent_adapter.KitaruFastAgent(
            FakeFastAgent(FakeApp({"a": FakeAgent()})),
            call_recorder=None,
            cost_calculator=lambda usage: 0.0,
        )


def test_public_runner_records_non_sensitive_analytics(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[Any, dict[str, Any] | None]] = []
    monkeypatch.setattr(
        "kitaru.adapters.fast_agent._agent.track",
        lambda event, metadata=None: events.append((event, metadata)) or True,
    )

    fast_agent_adapter.KitaruFastAgent(
        FakeFastAgent(FakeApp({"a": FakeAgent()})),
        call_recorder=None,
        model_checkpoint_config={"cache": True},
    )

    assert len(events) == 1
    _, metadata = events[0]
    assert metadata == {
        "checkpoint_strategy": "calls",
        "call_recorder": "passthrough",
        "has_model_checkpoint_config": True,
        "has_tool_checkpoint_config": False,
        "save_usage": True,
        "has_cost_calculator": False,
    }
