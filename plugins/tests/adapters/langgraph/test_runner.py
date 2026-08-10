"""Transparent runner lifecycle contracts."""

import asyncio
import copy
import json
import threading
import uuid
from types import SimpleNamespace
from typing import Any, TypedDict, cast

import pytest
from langchain_core.runnables import Runnable, RunnableConfig, RunnableLambda
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, GraphOutput, Interrupt

import kitaru_langgraph.agent as agent_module
from kitaru.api_models.v1.replay_config import (
    PassthroughConfig,
    ReplayOverride,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolPolicy,
    ToolPolicyOnMiss,
)
from kitaru_langgraph import (
    KitaruGraphRunner,
    UnsupportedCapabilityError,
    UnsupportedInvocationError,
    UnsupportedWorkerInterruptError,
)
from kitaru_langgraph.capability import _make_capability_manifest
from kitaru_langgraph.recording import get_active_invocation


class _RaisingRunnable(Runnable[Any, Any]):
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    def invoke(
        self, input: Any, config: RunnableConfig | None = None, **kwargs: Any
    ) -> Any:
        self.calls += 1
        raise self.error


def _nodes(client: Any) -> list[Any]:
    return [node for _, batch in client.sessions.node_batches for node in batch.nodes]


def test_sync_result_identity_and_config_are_preserved(fake_client: Any) -> None:
    result = {"answer": object()}
    seen: dict[str, Any] = {}

    def run(value: Any, config: RunnableConfig) -> Any:
        seen["input"] = value
        seen["config"] = config
        assert get_active_invocation() is not None
        return result

    original_config: RunnableConfig = {
        "tags": ["caller"],
        "metadata": {"safe": True},
        "configurable": {"thread_id": "thread-1"},
    }
    before = copy.deepcopy(original_config)
    runner = KitaruGraphRunner(RunnableLambda(run))

    returned = runner.invoke({"question": "hello"}, original_config)

    assert returned is result
    assert original_config == before
    assert seen["input"] == {"question": "hello"}
    assert seen["config"]["tags"] == ["caller"]
    assert get_active_invocation() is None
    client = fake_client.instances[0]
    assert client.closed
    assert client.sessions.updated[-1][1].status.value == "completed"
    assert _nodes(client)[0].index == 0
    assert _nodes(client)[-1].index == 0


async def test_async_result_identity_is_preserved(fake_client: Any) -> None:
    result = {"answer": object()}

    async def run(_: Any) -> Any:
        assert get_active_invocation() is not None
        await asyncio.sleep(0)
        return result

    returned = await KitaruGraphRunner(RunnableLambda(run)).ainvoke("hello")

    assert returned is result
    assert get_active_invocation() is None
    assert fake_client.instances[0].closed


async def test_async_recording_failure_preserves_result_and_warns_safely(
    fake_client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    result = {"answer": object()}

    async def run(_: Any) -> Any:
        assert get_active_invocation() is not None

        async def fail_ingest(*_: Any) -> Any:
            raise RuntimeError("sentinel secret recording body")

        fake_client.instances[0].sessions.ingest_nodes = fail_ingest
        return result

    returned = await KitaruGraphRunner(RunnableLambda(run)).ainvoke("hello")

    assert returned is result
    assert get_active_invocation() is None
    client = fake_client.instances[0]
    assert client.closed
    assert len(caplog.records) == 1
    assert "sentinel secret recording body" not in caplog.text
    record = caplog.records[0]
    assert record.__dict__["kitaru_stage"] == "finalize"
    assert record.__dict__["kitaru_exception_class"] == "RuntimeError"
    assert record.__dict__["kitaru_graph_succeeded"] is True


async def test_async_graph_error_remains_primary_when_recording_fails(
    fake_client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    graph_error = ValueError("graph failed")

    async def run(_: Any) -> Any:
        assert get_active_invocation() is not None

        async def fail_ingest(*_: Any) -> Any:
            raise RuntimeError("sentinel secret recording body")

        fake_client.instances[0].sessions.ingest_nodes = fail_ingest
        raise graph_error

    with pytest.raises(ValueError) as raised:
        await KitaruGraphRunner(RunnableLambda(run)).ainvoke("hello")

    assert raised.value is graph_error
    assert raised.value.__cause__ is None
    assert get_active_invocation() is None
    client = fake_client.instances[0]
    assert client.closed
    assert len(caplog.records) == 1
    assert "sentinel secret recording body" not in caplog.text
    record = caplog.records[0]
    assert record.__dict__["kitaru_stage"] == "finalize"
    assert record.__dict__["kitaru_exception_class"] == "RuntimeError"
    assert record.__dict__["kitaru_graph_succeeded"] is False


def test_graph_exception_identity_and_cause_are_preserved() -> None:
    error = ValueError("graph failed")
    graph = _RaisingRunnable(error)

    with pytest.raises(ValueError) as raised:
        KitaruGraphRunner(graph).invoke("hello")

    assert raised.value is error
    assert raised.value.__cause__ is None
    assert graph.calls == 1


def test_setup_failure_prevents_graph_execution(fake_client: Any) -> None:
    fake_client.next_ingest_error_at = 1
    called = False

    def run(_: Any) -> Any:
        nonlocal called
        called = True

    with pytest.raises(RuntimeError, match="sentinel recording failure"):
        KitaruGraphRunner(RunnableLambda(run)).invoke("hello")

    assert not called


def test_post_delegation_recording_failure_preserves_success(
    fake_client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    fake_client.next_ingest_error_at = 2
    result = {"answer": "done", "password": "sentinel"}

    returned = KitaruGraphRunner(RunnableLambda(lambda _: result)).invoke("hello")

    assert returned is result
    assert len(caplog.records) == 1
    assert "sentinel" not in caplog.text
    record = caplog.records[0]
    assert record.__dict__["kitaru_stage"]
    assert record.__dict__["kitaru_exception_class"] == "RuntimeError"


def test_sync_blocked_finalization_times_out_and_preserves_result(
    fake_client: Any,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agent_module, "_FINALIZATION_TIMEOUT_SECONDS", 0.01, raising=False
    )
    result = {"answer": object()}
    blocked = threading.Event()
    cancelled = threading.Event()

    def run(_: Any) -> Any:
        async def block_ingest(*_: Any) -> Any:
            blocked.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        fake_client.instances[0].sessions.ingest_nodes = block_ingest
        return result

    returned = KitaruGraphRunner(RunnableLambda(run)).invoke("hello")

    assert returned is result
    assert blocked.is_set()
    assert cancelled.wait(timeout=1)
    assert get_active_invocation() is None
    client = fake_client.instances[0]
    assert client.closed
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.__dict__["kitaru_stage"] == "finalize_timeout"
    assert record.__dict__["kitaru_exception_class"] == "TimeoutError"
    assert record.__dict__["kitaru_graph_succeeded"] is True


async def test_async_cancellation_bounds_blocked_finalization_and_preserves_error(
    fake_client: Any,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agent_module, "_FINALIZATION_TIMEOUT_SECONDS", 0.01, raising=False
    )
    graph_started = asyncio.Event()
    finalization_started = asyncio.Event()
    finalization_cancelled = asyncio.Event()
    seen: dict[str, asyncio.CancelledError] = {}

    async def run(_: Any) -> Any:
        graph_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as error:
            seen["error"] = error

            async def block_ingest(*_: Any) -> Any:
                finalization_started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    finalization_cancelled.set()

            fake_client.instances[0].sessions.ingest_nodes = block_ingest
            raise

    task = asyncio.create_task(KitaruGraphRunner(RunnableLambda(run)).ainvoke("hello"))
    await graph_started.wait()
    task.cancel("caller cancellation")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert raised.value is seen["error"]
    assert finalization_started.is_set()
    assert finalization_cancelled.is_set()
    assert get_active_invocation() is None
    client = fake_client.instances[0]
    assert client.closed
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.__dict__["kitaru_stage"] == "finalize_timeout"
    assert record.__dict__["kitaru_exception_class"] == "TimeoutError"
    assert record.__dict__["kitaru_graph_succeeded"] is False


def test_task_input_replaces_whole_input_but_command_wins(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    del fake_client
    seen: list[Any] = []
    graph = RunnableLambda(lambda value: seen.append(value) or value)
    runner = KitaruGraphRunner(graph)
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps({"replacement": True}))

    replaced = runner.invoke({"caller": True})
    command = Command(resume="continue")
    resumed = runner.invoke(command)

    assert replaced == {"replacement": True}
    assert resumed is command
    assert seen == [{"replacement": True}, command]


def test_sync_invocation_works_inside_a_running_event_loop() -> None:
    async def call_sync() -> Any:
        return KitaruGraphRunner(RunnableLambda(lambda value: value)).invoke(
            {"ok": True}
        )

    assert asyncio.run(call_sync()) == {"ok": True}


@pytest.mark.parametrize(
    "method",
    ["stream", "batch", "batch_as_completed"],
)
def test_unsupported_sync_modes_fail_before_setup(
    method: str, fake_client: Any
) -> None:
    runner = KitaruGraphRunner(RunnableLambda(lambda value: value))

    with pytest.raises(UnsupportedInvocationError, match=method):
        getattr(runner, method)(["hello"])

    assert fake_client.instances == []


def test_constructor_rejects_invalid_values() -> None:
    with pytest.raises(TypeError, match="Runnable"):
        KitaruGraphRunner(cast(Any, object()))
    with pytest.raises(ValueError, match="batch_size"):
        KitaruGraphRunner(RunnableLambda(lambda value: value), batch_size=0)


def test_direct_interrupt_is_returned_unchanged_and_recorded(
    fake_client: Any,
) -> None:
    result = GraphOutput(value={"partial": True}, interrupts=(Interrupt("pause"),))

    returned = KitaruGraphRunner(RunnableLambda(lambda _: result)).invoke("hello")

    assert returned is result
    update = fake_client.instances[0].sessions.updated[-1][1]
    assert update.status.value == "completed"
    assert update.metadata["interrupted"] is True


def test_worker_interrupt_is_recorded_failed_and_rejected(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = GraphOutput(value={"partial": True}, interrupts=(Interrupt("pause"),))
    monkeypatch.setenv("KITARU_TASK_ID", str(uuid.uuid4()))
    monkeypatch.setenv("KITARU_TASK_INPUTS", json.dumps("worker input"))

    with pytest.raises(UnsupportedWorkerInterruptError):
        KitaruGraphRunner(RunnableLambda(lambda _: result)).invoke("caller")

    update = fake_client.instances[0].sessions.updated[-1][1]
    assert update.status.value == "failed"
    assert update.metadata["interrupted"] is True


def test_direct_replay_override_without_factory_capability_fails_before_graph(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client.next_replay = SimpleNamespace(
        override=ReplayOverride(system_prompt="replacement"),
        tool_policy=ToolPolicy(default=PassthroughConfig()),
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    called = False

    def run(_: Any) -> Any:
        nonlocal called
        called = True

    with pytest.raises(UnsupportedCapabilityError, match="override_system_prompt"):
        KitaruGraphRunner(RunnableLambda(run)).invoke("hello")

    assert not called
    client = fake_client.instances[0]
    assert client.closed
    assert client.sessions.updated[-1][1].status.value == "failed"


def test_tool_substitution_preflight_rejects_opaque_target_before_graph(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client.next_replay = SimpleNamespace(
        override=None,
        tool_policy=ToolPolicy(
            default=StaticConfig(
                cases=[
                    StaticCase(
                        match=None,
                        match_mode=StaticMatchMode.EXACT,
                        result={"answer": "recorded"},
                    )
                ],
                on_miss=ToolPolicyOnMiss.FAIL,
            )
        ),
    )
    monkeypatch.setenv("KITARU_REPLAY_ID", str(uuid.uuid4()))
    called = False

    def run(_: Any) -> Any:
        nonlocal called
        called = True

    runner = KitaruGraphRunner(RunnableLambda(run))
    middleware = object()
    runner._middleware = middleware
    runner._manifest = _make_capability_manifest(
        middleware, opaque_targets=("opaque_subagents",)
    )

    with pytest.raises(UnsupportedCapabilityError, match="opaque_subagents"):
        runner.invoke("hello")

    assert not called
    client = fake_client.instances[0]
    assert client.closed
    assert client.sessions.updated[-1][1].status.value == "failed"


def test_real_interrupt_and_resume_preserve_langgraph_checkpointer() -> None:
    from langgraph.types import interrupt

    def pause(state: dict[str, Any]) -> dict[str, Any]:
        answer = interrupt("continue?")
        return {**state, "answer": answer}

    class State(TypedDict, total=False):
        question: str
        answer: str

    builder = StateGraph(cast(Any, State))
    builder.add_node("pause", pause)
    builder.add_edge(START, "pause")
    builder.add_edge("pause", END)
    graph = builder.compile(checkpointer=InMemorySaver())
    runner = KitaruGraphRunner(graph)
    config: RunnableConfig = {"configurable": {"thread_id": "thread-1"}}

    first = runner.invoke(cast(Any, {"question": "ready"}), config, version="v2")
    second = runner.invoke(cast(Any, Command(resume="yes")), config, version="v2")

    assert isinstance(first, GraphOutput)
    assert first.interrupts
    assert isinstance(second, GraphOutput)
    assert second.value == {"question": "ready", "answer": "yes"}
