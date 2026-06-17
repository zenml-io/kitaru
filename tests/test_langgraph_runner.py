"""Focused tests for the first-pass LangGraph adapter."""

from __future__ import annotations

import asyncio
import time
from typing import Any, cast
from uuid import uuid4

import pytest
from typing_extensions import TypedDict
from zenml.client import Client

pytest.importorskip("langgraph")

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from kitaru import flow
from kitaru.adapters.langgraph import (
    KitaruGraphRunner,
    LangGraphCapturePolicy,
    LangGraphCheckpointSelector,
    LangGraphDurabilityPolicy,
    LangGraphRunRequest,
    LangGraphRunResult,
    build_resume_request,
)
from kitaru.errors import KitaruUsageError


def _wait_for_hydrated_run(exec_id: str) -> Any:
    deadline = time.monotonic() + 10
    last_run: Any | None = None
    while time.monotonic() < deadline:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
        last_run = run
        if run.status.is_finished:
            assert run.status.is_successful
            return run.get_hydrated_version()
        time.sleep(0.1)
    assert last_run is not None
    raise AssertionError(
        f"Pipeline run {exec_id} did not finish within 10 seconds; "
        f"last status was {last_run.status}."
    )


class CountState(TypedDict):
    count: int


def _count_graph(*, with_checkpointer: bool = True):
    builder = StateGraph(cast(Any, CountState))

    def add_one(state: CountState) -> CountState:
        return {"count": state["count"] + 1}

    builder.add_node("add_one", add_one)
    builder.add_edge(START, "add_one")
    builder.add_edge("add_one", END)
    if not with_checkpointer:
        return builder.compile()
    return builder.compile(checkpointer=InMemorySaver())


def test_invoke_runs_deterministic_local_graph_with_thread_id() -> None:
    graph = _count_graph()
    runner = KitaruGraphRunner(graph, name="counter")

    result = runner.invoke(
        LangGraphRunRequest.start({"count": 1}, thread_id="counter-thread")
    )

    assert result.status == "completed"
    assert result.output == {"count": 2}
    assert result.thread_id == "counter-thread"
    assert result.latest_checkpoint_id
    assert result.state_summary is not None
    assert result.state_summary.values is None
    assert any("InMemorySaver" in warning for warning in result.warnings)


def test_invoke_runs_graph_without_langgraph_checkpointer() -> None:
    graph = _count_graph(with_checkpointer=False)
    runner = KitaruGraphRunner(graph, name="counter")

    result = runner.invoke(
        LangGraphRunRequest.start({"count": 1}, thread_id="counter-no-checkpointer")
    )

    assert result.status == "completed"
    assert result.output == {"count": 2}
    assert any("No LangGraph checkpointer" in warning for warning in result.warnings)


def test_full_state_values_are_opt_in() -> None:
    runner = KitaruGraphRunner(
        _count_graph(),
        name="counter",
        capture=LangGraphCapturePolicy(save_state_values=True),
    )

    result = runner.invoke(
        LangGraphRunRequest.start({"count": 1}, thread_id="counter-values-thread")
    )

    assert result.state_summary is not None
    assert result.state_summary.values == {"count": 2}


def test_invoke_inside_flow_creates_one_outer_graph_call_checkpoint(
    primed_zenml,
) -> None:
    _ = primed_zenml
    runner = KitaruGraphRunner(
        _count_graph(),
        name=f"counter_{uuid4().hex[:8]}",
    )

    @flow
    def langgraph_flow(count: int, thread_id: str) -> int:
        result = runner.invoke(
            LangGraphRunRequest.start({"count": count}, thread_id=thread_id)
        )
        output = cast(dict[str, Any], result.output)
        return cast(int, output["count"])

    handle = langgraph_flow.run(1, f"flow-thread-{uuid4().hex[:8]}")
    hydrated = _wait_for_hydrated_run(handle.exec_id)
    step_names = set(hydrated.steps)

    matching_steps = [name for name in step_names if "langgraph_call" in name]
    assert len(matching_steps) == 1


def test_ainvoke_runs_when_graph_supports_async() -> None:
    runner = KitaruGraphRunner(_count_graph(), name="counter")

    async def run() -> LangGraphRunResult:
        return await runner.ainvoke(
            LangGraphRunRequest.start({"count": 2}, thread_id="async-thread")
        )

    result = asyncio.run(run())

    assert result.status == "completed"
    assert result.output == {"count": 3}


class InterruptState(TypedDict, total=False):
    value: int
    answer: object


def _interrupt_graph():
    builder = StateGraph(cast(Any, InterruptState))

    def ask(state: InterruptState) -> InterruptState:
        answer = interrupt({"question": "approve?"})
        return {"answer": answer, "value": state["value"] + 1}

    builder.add_node("ask", ask)
    builder.add_edge(START, "ask")
    builder.add_edge("ask", END)
    return builder.compile(checkpointer=InMemorySaver())


def test_interrupt_result_and_resume_helper() -> None:
    runner = KitaruGraphRunner(_interrupt_graph(), name="approval")

    interrupted = runner.invoke(
        LangGraphRunRequest.start({"value": 1}, thread_id="approval-thread")
    )

    assert interrupted.status == "interrupted"
    assert interrupted.pending_state is not None
    assert interrupted.pending_state.thread_id == "approval-thread"
    assert interrupted.interrupts
    assert interrupted.interrupts[0].value == {"question": "approve?"}

    resume_request = build_resume_request(interrupted, {"approved": True})

    assert resume_request.kind == "resume"
    assert isinstance(resume_request.command, Command)

    resumed = runner.invoke(resume_request)

    assert resumed.status == "completed"
    output = cast(dict[str, Any], resumed.output)
    assert output["answer"] == {"approved": True}
    assert output["value"] == 2


class ForkState(TypedDict, total=False):
    count: int
    variant: str
    summary: str


def _forkable_graph(*, with_checkpointer: bool = True) -> tuple[Any, list[str]]:
    calls: list[str] = []
    builder = StateGraph(cast(Any, ForkState))

    def collect(state: ForkState) -> ForkState:
        calls.append("collect")
        return {"count": state["count"] + 1}

    def summarize(state: ForkState) -> ForkState:
        calls.append(f"summarize:{state['variant']}")
        return {"summary": f"variant={state['variant']}; count={state['count']}"}

    builder.add_node("collect", collect)
    builder.add_node("summarize", summarize)
    builder.add_edge(START, "collect")
    builder.add_edge("collect", "summarize")
    builder.add_edge("summarize", END)
    if not with_checkpointer:
        return builder.compile(), calls
    return builder.compile(checkpointer=InMemorySaver()), calls


def test_runner_fork_updates_state_and_resumes_without_checkpoint_id() -> None:
    graph, calls = _forkable_graph()
    runner = KitaruGraphRunner(
        graph,
        name="forkable",
        durability=LangGraphDurabilityPolicy(require_checkpointer=True),
        capture=LangGraphCapturePolicy(save_state_values=True),
    )
    thread_id = f"fork-thread-{uuid4().hex[:8]}"

    baseline = runner.invoke(
        LangGraphRunRequest.start(
            {"count": 1, "variant": "baseline"},
            thread_id=thread_id,
        )
    )
    fork = runner.fork(
        thread_id=thread_id,
        select=LangGraphCheckpointSelector(next_nodes=("summarize",)),
        update_values={"variant": "candidate"},
        metadata={"run_role": "candidate_fork"},
    )

    assert baseline.output == {
        "count": 2,
        "variant": "baseline",
        "summary": "variant=baseline; count=2",
    }
    assert fork.run_result.output == {
        "count": 2,
        "variant": "candidate",
        "summary": "variant=candidate; count=2",
    }
    assert fork.selected_checkpoint_id
    assert fork.selected_next_nodes == ["summarize"]
    assert fork.fork_checkpoint_id
    assert fork.fork_checkpoint_ns is None
    assert fork.updated_state_keys == ["variant"]
    assert fork.checkpoint_strategy == "graph_call"
    assert calls == ["collect", "summarize:baseline", "summarize:candidate"]


def test_runner_fork_requires_langgraph_checkpointer() -> None:
    graph, _calls = _forkable_graph(with_checkpointer=False)
    runner = KitaruGraphRunner(graph, name="forkable")

    with pytest.raises(
        KitaruUsageError, match="requires a real LangGraph checkpointer"
    ):
        runner.fork(
            thread_id="thread-1",
            select=LangGraphCheckpointSelector(next_nodes=("summarize",)),
            update_values={"variant": "candidate"},
        )
