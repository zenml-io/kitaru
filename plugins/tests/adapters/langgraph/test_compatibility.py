"""Public compatibility contracts for supported LangGraph constructions."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from typing import Any, TypedDict, cast

import pytest
from langchain_core.runnables import RunnableLambda
from langgraph.func import entrypoint
from langgraph.graph import END, START, StateGraph

from adapters.langgraph import (
    CapabilityOperation,
    CapabilityTargetKind,
    LocalSubagentFactorySpec,
    UnsupportedCapabilityError,
)
from adapters.langgraph.capability import (
    _direct_capability_view,
    _make_capability_manifest,
    _require_operation,
    _validate_manifest,
)
from kitaru.api_models.v1.replay_config import ReplayOverride


def test_direct_wrapper_reports_only_universal_operations() -> None:
    view = _direct_capability_view()

    target = view.get_target("main")
    assert target is not None
    assert target.kind is CapabilityTargetKind.MAIN
    assert target.supports(CapabilityOperation.RECORD)
    assert target.supports(CapabilityOperation.REPLACE_INPUT)
    assert not target.supports(CapabilityOperation.SUBSTITUTE_TOOL_RESULT)


def test_manifest_reports_injected_and_opaque_targets() -> None:
    middleware = object()
    manifest = _make_capability_manifest(
        middleware,
        local_subagents=("researcher",),
        opaque_targets=("remote",),
    )

    assert [target.name for target in manifest.view.targets] == [
        "main",
        "researcher",
        "remote",
    ]
    researcher = manifest.view.get_target("researcher")
    remote = manifest.view.get_target("remote")
    assert researcher is not None
    assert remote is not None
    assert researcher.supports(CapabilityOperation.SUBSTITUTE_TOOL_RESULT)
    assert not remote.supports(CapabilityOperation.SUBSTITUTE_TOOL_RESULT)


def test_detached_manifest_is_rejected() -> None:
    manifest = _make_capability_manifest(object())

    with pytest.raises(UnsupportedCapabilityError, match="detached"):
        _validate_manifest(manifest, object())


def test_reporting_and_preflight_use_same_operation_view() -> None:
    view = _direct_capability_view()

    with pytest.raises(UnsupportedCapabilityError, match="substitute_tool_result"):
        _require_operation(view, CapabilityOperation.SUBSTITUTE_TOOL_RESULT)


def test_local_subagent_spec_copies_caller_kwargs() -> None:
    original = {"middleware": [object()]}
    spec = LocalSubagentFactorySpec("worker", lambda **_: None, original)

    copied = spec.copied_kwargs()
    copied["added"] = True

    assert "added" not in original


def test_factory_is_called_once_and_kitaru_middleware_is_outermost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import langchain.agents

    from adapters.langgraph import KitaruGraphRunner
    from adapters.langgraph.langchain import KitaruLangGraphMiddleware

    calls: list[dict[str, Any]] = []
    caller_middleware = object()

    def factory(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return RunnableLambda(lambda value: value)

    monkeypatch.setattr(langchain.agents, "create_agent", factory)
    factory_kwargs = {
        "model": "provider:model",
        "tools": [],
        "middleware": [caller_middleware],
    }

    runner = KitaruGraphRunner.from_agent_factory(
        factory, factory_kwargs=factory_kwargs
    )

    assert len(calls) == 1
    assert isinstance(calls[0]["middleware"][0], KitaruLangGraphMiddleware)
    assert calls[0]["middleware"][1] is caller_middleware
    assert factory_kwargs["middleware"] == [caller_middleware]
    main = runner.capabilities.get_target("main")
    assert main is not None
    assert main.supports(CapabilityOperation.SUBSTITUTE_TOOL_RESULT)


def test_factory_rejects_duplicate_kitaru_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import langchain.agents

    from adapters.langgraph import KitaruGraphRunner
    from adapters.langgraph.langchain import KitaruLangGraphMiddleware

    def factory(**_: Any) -> Any:
        return RunnableLambda(lambda value: value)

    monkeypatch.setattr(langchain.agents, "create_agent", factory)

    with pytest.raises(ValueError, match="already contains"):
        KitaruGraphRunner.from_agent_factory(
            factory,
            factory_kwargs={
                "model": "provider:model",
                "middleware": [KitaruLangGraphMiddleware(requested_model=None)],
            },
        )


def test_deep_agent_local_factories_are_built_before_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deepagents
    import langchain.agents

    from adapters.langgraph import KitaruGraphRunner

    events: list[str] = []

    def local_factory(**_: Any) -> Any:
        events.append("local")
        return RunnableLambda(lambda value: value)

    def parent_factory(**kwargs: Any) -> Any:
        events.append("parent")
        assert kwargs["subagents"][0]["name"] == "researcher"
        return RunnableLambda(lambda value: value)

    monkeypatch.setattr(langchain.agents, "create_agent", local_factory)
    monkeypatch.setattr(deepagents, "create_deep_agent", parent_factory)

    runner = KitaruGraphRunner.from_agent_factory(
        parent_factory,
        factory_kwargs={"model": "provider:model"},
        local_subagents=(
            LocalSubagentFactorySpec(
                name="researcher",
                factory=local_factory,
                factory_kwargs={"model": "provider:child"},
            ),
        ),
    )

    assert events == ["local", "parent"]
    researcher = runner.capabilities.get_target("researcher")
    assert researcher is not None
    assert researcher.supports(CapabilityOperation.OVERRIDE_MODEL)


def test_local_subagent_rejects_wrapped_supported_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import deepagents
    import langchain.agents

    from adapters.langgraph import KitaruGraphRunner

    def local_factory(**_: Any) -> Any:
        return RunnableLambda(lambda value: value)

    def parent_factory(**_: Any) -> Any:
        return RunnableLambda(lambda value: value)

    monkeypatch.setattr(langchain.agents, "create_agent", local_factory)
    monkeypatch.setattr(deepagents, "create_deep_agent", parent_factory)

    def wrapped_local_factory(**kwargs: Any) -> Any:
        return local_factory(**kwargs)

    with pytest.raises(ValueError, match="local subagent factory is unsupported"):
        KitaruGraphRunner.from_agent_factory(
            parent_factory,
            factory_kwargs={"model": "provider:parent"},
            local_subagents=(
                LocalSubagentFactorySpec(
                    name="researcher",
                    factory=wrapped_local_factory,
                    factory_kwargs={"model": "provider:child"},
                ),
            ),
        )


def test_mapped_model_override_requires_string_construction_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import langchain.agents

    from adapters.langgraph import KitaruGraphRunner

    def factory(**_: Any) -> Any:
        return RunnableLambda(lambda value: value)

    monkeypatch.setattr(langchain.agents, "create_agent", factory)
    runner = KitaruGraphRunner.from_agent_factory(
        factory,
        factory_kwargs={"model": object(), "tools": []},
    )
    recorder = SimpleNamespace(
        override=ReplayOverride(model={"provider:original": "provider:replacement"}),
        replay=None,
    )

    with pytest.raises(UnsupportedCapabilityError, match="string model"):
        runner._preflight(cast(Any, recorder))


def test_real_state_graph_uses_same_runner(fake_client: Any) -> None:
    from adapters.langgraph import KitaruGraphRunner

    class State(TypedDict, total=False):
        question: str
        answer: int

    builder = StateGraph(cast(Any, State))
    builder.add_node("answer", lambda state: {**state, "answer": 42})
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)

    result = KitaruGraphRunner(builder.compile()).invoke(
        cast(Any, {"question": "life"})
    )

    assert result == {"question": "life", "answer": 42}
    assert len(fake_client.instances) == 1


def test_functional_api_runnable_uses_same_runner() -> None:
    from adapters.langgraph import KitaruGraphRunner

    @entrypoint()
    def workflow(value: str) -> dict[str, str]:
        return {"value": value}

    result = KitaruGraphRunner(workflow).invoke("functional")

    assert result == {"value": "functional"}


def test_real_langchain_factory_is_supported() -> None:
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    from adapters.langgraph import KitaruGraphRunner

    model = FakeListChatModel(responses=["done"])
    langchain_runner = KitaruGraphRunner.from_agent_factory(
        create_agent, factory_kwargs={"model": model, "tools": []}
    )
    main = langchain_runner.capabilities.get_target("main")
    assert main is not None
    assert main.supports(CapabilityOperation.OVERRIDE_MODEL)


async def test_concurrent_async_calls_are_isolated(fake_client: Any) -> None:
    from adapters.langgraph import KitaruGraphRunner

    async def run(value: str) -> dict[str, str]:
        await asyncio.sleep(0)
        return {"value": value}

    runner = KitaruGraphRunner(RunnableLambda(run))

    results = await asyncio.gather(runner.ainvoke("first"), runner.ainvoke("second"))

    assert results == [{"value": "first"}, {"value": "second"}]
    assert len(fake_client.instances) == 2
    assert {client.sessions.created[0].inputs for client in fake_client.instances} == {
        "first",
        "second",
    }


def test_concurrent_sync_calls_are_isolated(fake_client: Any) -> None:
    from adapters.langgraph import KitaruGraphRunner

    runner = KitaruGraphRunner(RunnableLambda(lambda value: {"value": value}))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(runner.invoke, ["first", "second"]))

    assert results == [{"value": "first"}, {"value": "second"}]
    assert len(fake_client.instances) == 2
