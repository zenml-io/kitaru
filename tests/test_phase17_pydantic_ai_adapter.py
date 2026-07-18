"""Integration tests for the PydanticAI adapter."""

import asyncio
import importlib
import multiprocessing
import re
import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from queue import Empty
from typing import Any, cast
from uuid import uuid4

import pytest
from zenml.client import Client

pytest.importorskip("pydantic_ai")

from pydantic_ai import Agent
from pydantic_ai.agent.abstract import AbstractAgent
from pydantic_ai.capabilities.hooks import Hooks
from pydantic_ai.models.test import TestModel

from kitaru import checkpoint, flow
from kitaru._checkpoint_metadata import (
    adapter_checkpoint_metadata,
    checkpoint_metadata_from_step,
    checkpoint_metadata_value,
)
from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import KitaruAgent, _tracking, hitl_tool
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError
from kitaru.wait import _resolve_zenml_wait


@dataclass(frozen=True)
class _FakeCheckpointScope:
    execution_id: str | None
    checkpoint_id: str | None
    name: str


def _make_test_agent(*, name_prefix: str) -> Agent[Any, str]:
    agent = Agent(
        TestModel(call_tools=["add"]),
        name=f"{name_prefix}_{uuid4().hex[:8]}",
        output_type=str,
    )

    @agent.tool_plain
    def add(a: int = 0, b: int = 0) -> int:
        return a + b

    return agent


def _make_wrapped_agent(
    *, name_prefix: str, granular_checkpoints: bool
) -> KitaruAgent[Any, str]:
    return KitaruAgent(
        _make_test_agent(name_prefix=name_prefix),
        granular_checkpoints=granular_checkpoints,
    )


def _artifact_names(hydrated_run: Any) -> list[str]:
    names: list[str] = []
    for step in hydrated_run.steps.values():
        for artifacts in step.outputs.values():
            names.extend(artifact.name for artifact in artifacts)
    return names


def _input_names_by_step(hydrated_run: Any) -> list[set[str]]:
    return [set(step.inputs) for step in hydrated_run.steps.values()]


def _has_step_input(hydrated_run: Any, input_name: str) -> bool:
    return any(input_name in inputs for inputs in _input_names_by_step(hydrated_run))


def _has_step_metadata(hydrated_run: Any, **expected: Any) -> bool:
    for step in hydrated_run.steps.values():
        metadata = checkpoint_metadata_from_step(step)
        if all(
            checkpoint_metadata_value(metadata, key) == value
            for key, value in expected.items()
        ):
            return True
    return False


def _wait_for_hydrated_run(exec_id: str) -> Any:
    client = Client()
    deadline = time.time() + 30
    while True:
        run = client.get_pipeline_run(exec_id, allow_name_prefix_match=False)
        if run.status.is_finished:
            assert run.status.is_successful
            return run.get_hydrated_version()
        if time.time() >= deadline:
            raise AssertionError(f"Pipeline run {exec_id} did not finish in time.")
        time.sleep(0.2)


def _require_wait_support() -> None:
    try:
        _resolve_zenml_wait()
    except KitaruFeatureNotAvailableError:
        pytest.skip("Installed ZenML build does not expose wait support yet.")


def _metadata_dict_from_steps(hydrated_run: Any, key: str) -> dict[str, Any]:
    for step in hydrated_run.steps.values():
        if key in step.run_metadata:
            return step.run_metadata[key]
    raise AssertionError(f"No step metadata contained key {key!r}.")


def _events(event_map: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [event for events in event_map.values() for event in events]


def _event_kinds(event_map: dict[str, list[dict[str, Any]]]) -> set[str]:
    return {event["kind"] for event in _events(event_map)}


def _assert_event_artifacts_use_display_names(
    event_map: dict[str, list[dict[str, Any]]], allowed_refs: list[str] | set[str]
) -> None:
    allowed = set(allowed_refs)
    for event in _events(event_map):
        event_id = event["event_id"]
        for artifact_name in event.get("artifacts", {}).values():
            assert artifact_name in allowed
            assert not artifact_name.startswith(f"{event_id}_")


_DIRECT_WAIT_AGENT: KitaruAgent[Any, str] | None = None
_STREAMING_WAIT_AGENT: KitaruAgent[Any, str] | None = None
_GUARDED_WAIT_AGENT: KitaruAgent[Any, str] | None = None
_HITL_WAIT_AGENT: KitaruAgent[Any, str] | None = None
_CHILD_EVENT_ZENML_WAIT_REACHED = "zenml_wait_reached"
_CHILD_EVENT_COMPLETED = "completed"
_CHILD_EVENT_ERROR = "error"


@dataclass(frozen=True)
class _ChildEvent:
    kind: str
    detail: str | None = None
    message: str | None = None


def _put_child_event(
    queue: Any,
    kind: str,
    detail: object | None = None,
    message: object | None = None,
) -> None:
    queue.put(
        _ChildEvent(
            kind=kind,
            detail=None if detail is None else str(detail),
            message=None if message is None else str(message),
        )
    )


def _install_child_wait_reached_event(queue: Any) -> Callable[[], None]:
    """Report after `kitaru.wait()` passes guards and resolves ZenML's wait."""
    wait_module = cast(Any, importlib.import_module("kitaru.wait"))
    original_resolve = cast(
        Callable[[], Callable[..., Any]],
        wait_module._resolve_zenml_wait,
    )

    def recording_resolve() -> Callable[..., Any]:
        zenml_wait = original_resolve()

        def recording_wait(**kwargs: Any) -> Any:
            _put_child_event(queue, _CHILD_EVENT_ZENML_WAIT_REACHED, kwargs.get("name"))
            return zenml_wait(**kwargs)

        return recording_wait

    wait_module._resolve_zenml_wait = recording_resolve

    def cleanup() -> None:
        wait_module._resolve_zenml_wait = original_resolve

    return cleanup


def _child_run_status(exec_id: str | None) -> str | None:
    if exec_id is None:
        return None
    try:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
    except Exception as exc:  # pragma: no cover - reported to parent for diagnostics
        return f"lookup_failed:{type(exc).__name__}:{exc}"
    return str(getattr(run.status, "value", run.status))


def _run_flow_and_report(
    queue: Any,
    flow_definition: Any,
    cleanup: Callable[[], None],
) -> None:
    try:
        handle = flow_definition.run(cache=False)
        exec_id = getattr(handle, "exec_id", None)
        _put_child_event(
            queue,
            _CHILD_EVENT_COMPLETED,
            exec_id,
            _child_run_status(exec_id),
        )
    except BaseException as exc:
        _put_child_event(queue, _CHILD_EVENT_ERROR, type(exc).__name__, str(exc))
    finally:
        cleanup()


@checkpoint(cache=False)
def pydantic_ai_streaming_wait_persist_history(history: list[str]) -> list[str]:
    return history


async def _consume_event_stream(_ctx: Any, stream: Any) -> None:
    async for _event in stream:
        pass


@flow
def pydantic_ai_direct_wait_flow() -> str:
    assert _DIRECT_WAIT_AGENT is not None
    return _DIRECT_WAIT_AGENT.run_sync("Ask the human for context.").output


@flow
def pydantic_ai_streaming_wait_flow() -> str:
    assert _STREAMING_WAIT_AGENT is not None
    return _STREAMING_WAIT_AGENT.run_sync(
        "Ask the human for context.",
        event_stream_handler=_consume_event_stream,
    ).output


@flow
def pydantic_ai_guarded_wait_flow() -> str:
    assert _GUARDED_WAIT_AGENT is not None
    return _GUARDED_WAIT_AGENT.run_sync("Ask the human for context.").output


@flow
def pydantic_ai_hitl_wait_flow() -> str:
    assert _HITL_WAIT_AGENT is not None
    return _HITL_WAIT_AGENT.run_sync("Ask the human for context.").output


def _run_direct_wait_flow_until_pause(queue: Any) -> None:
    global _DIRECT_WAIT_AGENT
    wait_cleanup = _install_child_wait_reached_event(queue)

    agent = Agent(
        TestModel(call_tools=["ask_user"]),
        name=f"direct_wait_agent_{uuid4().hex[:8]}",
        output_type=str,
    )

    @agent.tool_plain
    def ask_user() -> str:
        return kp.wait_for_input(
            schema=str,
            name="direct_tool_wait",
            question="What context should the agent use?",
            timeout=0,
        )

    _DIRECT_WAIT_AGENT = KitaruAgent(
        agent,
        tool_checkpoint_config_by_name={"ask_user": False},
        allow_sync_tool_body_waits=True,
    )

    def cleanup() -> None:
        global _DIRECT_WAIT_AGENT
        wait_cleanup()
        _DIRECT_WAIT_AGENT = None

    _run_flow_and_report(queue, pydantic_ai_direct_wait_flow, cleanup)


def _run_streaming_wait_flow_until_pause(queue: Any) -> None:
    global _STREAMING_WAIT_AGENT
    wait_cleanup = _install_child_wait_reached_event(queue)

    agent = Agent(
        TestModel(call_tools=["ask_user"]),
        name=f"streaming_wait_agent_{uuid4().hex[:8]}",
        output_type=str,
    )

    @agent.tool_plain
    def ask_user() -> str:
        pydantic_ai_streaming_wait_persist_history(["before_wait"])
        return kp.wait_for_input(
            schema=str,
            name="streaming_tool_wait",
            question="What context should the streamed agent use?",
            timeout=0,
        )

    _STREAMING_WAIT_AGENT = KitaruAgent(
        agent,
        tool_checkpoint_config_by_name={"ask_user": False},
        allow_sync_tool_body_waits=True,
    )

    def cleanup() -> None:
        global _STREAMING_WAIT_AGENT
        wait_cleanup()
        _STREAMING_WAIT_AGENT = None

    _run_flow_and_report(queue, pydantic_ai_streaming_wait_flow, cleanup)


def _run_hitl_wait_flow_until_pause(queue: Any) -> None:
    global _HITL_WAIT_AGENT
    wait_cleanup = _install_child_wait_reached_event(queue)

    @hitl_tool(question="What should the tool return?", schema=str)
    def ask_human() -> str:
        return "body should not run"

    agent = Agent(
        TestModel(call_tools=["ask_human"]),
        name=f"hitl_wait_agent_{uuid4().hex[:8]}",
        output_type=str,
        tools=[ask_human],
    )
    _HITL_WAIT_AGENT = KitaruAgent(agent)

    def cleanup() -> None:
        global _HITL_WAIT_AGENT
        wait_cleanup()
        _HITL_WAIT_AGENT = None

    _run_flow_and_report(queue, pydantic_ai_hitl_wait_flow, cleanup)


def _assert_child_flow_pauses(
    target: Any,
    *,
    required_event: str,
    timeout: float = 90.0,
    post_event_grace: float = 1.0,
) -> None:
    # Prefer forkserver over raw fork. Raw fork inherits the parent process's
    # already-open ZenML/SQLite resources; if another test has already run a
    # flow in this xdist worker, the child can fail with stale file descriptors
    # before it reaches the wait. Forkserver still inherits the isolated test
    # environment, but forks from a clean server process instead of the live
    # pytest worker.
    main = sys.modules.get("__main__")
    if main is not None:
        main_file = getattr(main, "__file__", None)
        if main_file is None or Path(main_file).is_dir():
            main.__file__ = __file__

    start_methods = multiprocessing.get_all_start_methods()
    if "forkserver" in start_methods:
        context = cast(Any, multiprocessing.get_context("forkserver"))
    elif "fork" in start_methods:
        context = cast(Any, multiprocessing.get_context("fork"))
    else:
        pytest.skip("Child wait/pause tests require fork or forkserver isolation.")
    queue = context.Queue()
    process = context.Process(target=target, args=(queue,), daemon=True)
    process.start()

    saw_required_event = False
    grace_deadline: float | None = None
    deadline = time.time() + timeout

    def assert_child_reported_waiting(event: _ChildEvent) -> None:
        if event.detail is None:
            pytest.fail(
                "Child flow reached the real wait callable, then exited without "
                "reporting an execution id to verify paused status."
            )
        if event.message == "paused":
            return
        pytest.fail(
            "Child flow reached the real wait callable and exited, but execution "
            f"{event.detail} did not report paused/waiting status from the child; "
            f"reported status was {event.message!r}."
        )

    def handle_exit_after_wait() -> None:
        try:
            event = queue.get(timeout=0.5)
        except Empty:
            pytest.fail(
                "Child flow exited after reaching the real wait callable, but no "
                "structured error/completed event was reported."
            )
        if event.kind == _CHILD_EVENT_ERROR:
            pytest.fail(
                "Child flow reached the real wait callable, then failed instead "
                f"of pausing: {event.detail}: {event.message}"
            )
        if event.kind == _CHILD_EVENT_COMPLETED:
            assert_child_reported_waiting(event)
            return
        pytest.fail(
            "Child flow exited after reaching the real wait callable with "
            f"unexpected event {event.kind!r}."
        )

    try:
        while time.time() < deadline:
            try:
                event = queue.get(timeout=0.2)
            except Empty:
                if saw_required_event:
                    assert grace_deadline is not None
                    if process.is_alive() and time.time() >= grace_deadline:
                        return
                    if not process.is_alive():
                        handle_exit_after_wait()
                elif not process.is_alive():
                    pytest.fail(
                        "Child flow exited before pausing; required event "
                        f"{required_event!r} was not observed."
                    )
                continue

            if event.kind == required_event:
                saw_required_event = True
                grace_deadline = time.time() + post_event_grace
                continue
            if event.kind == _CHILD_EVENT_ERROR:
                if saw_required_event:
                    pytest.fail(
                        "Child flow reached the real wait callable, then failed "
                        f"instead of pausing: {event.detail}: {event.message}"
                    )
                pytest.fail(
                    f"Child flow failed before pausing: {event.detail}: {event.message}"
                )
            if event.kind == _CHILD_EVENT_COMPLETED:
                if saw_required_event:
                    assert_child_reported_waiting(event)
                    return
                pytest.fail(f"Child flow completed before reaching {required_event!r}.")
        pytest.fail(f"Child flow did not reach {required_event!r} before timeout.")
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=10.0)


def test_phase17_event_artifact_names_use_short_display_shape() -> None:
    """Display artifact names should omit internal agent/run event prefixes."""
    event_id = "agent_name_ab12cd34_llm_call_1"

    assert _tracking.artifact_name(event_id, "prompt") == "llm_call_1_prompt"
    assert _tracking.artifact_name(event_id, "response") == "llm_call_1_response"
    assert (
        _tracking.artifact_name(event_id, "stream_transcript")
        == "llm_call_1_stream_transcript"
    )
    assert (
        _tracking._namespaced_artifact_name(
            event_id,
            "stream_transcript",
            namespace="agent_ab12cd34_tracker_2",
        )
        == "llm_call_1_stream_transcript__agent_ab12cd34_tracker_2"
    )


def test_phase17_run_kwargs_forwarded_to_pydantic_ai_run_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The adapter should mirror and forward Pydantic AI per-run kwargs."""
    durable_agent = _make_wrapped_agent(
        name_prefix="capability_agent", granular_checkpoints=True
    )
    capabilities = [Hooks()]
    conversation_id = "conversation-phase17"
    output_retries = 2

    from kitaru.adapters.pydantic_ai._agent import _UPSTREAM_RUN_RETRIES_PARAM

    expected_forwarded_kwargs = {
        "capabilities": capabilities,
        "conversation_id": conversation_id,
        _UPSTREAM_RUN_RETRIES_PARAM: output_retries,
    }
    captured: dict[str, dict[str, object]] = {}
    run_result = object()
    stream_result = object()
    iter_result = object()

    async def run_async_direct(body: Any, **_: Any) -> Any:
        return await body()

    def run_sync_direct(body: Any, **_: Any) -> Any:
        return body()

    def capture_forwarded_kwargs(kwargs: dict[str, object]) -> dict[str, object]:
        return {key: kwargs[key] for key in expected_forwarded_kwargs}

    async def fake_run(self: Any, *args: Any, **kwargs: Any) -> object:
        captured["run"] = capture_forwarded_kwargs(kwargs)
        return run_result

    def fake_run_sync(self: Any, *args: Any, **kwargs: Any) -> object:
        captured["run_sync"] = capture_forwarded_kwargs(kwargs)
        return run_result

    @asynccontextmanager
    async def fake_run_stream(self: Any, *args: Any, **kwargs: Any) -> Any:
        captured["run_stream"] = capture_forwarded_kwargs(kwargs)
        yield stream_result

    @asynccontextmanager
    async def fake_iter(*args: Any, **kwargs: Any) -> Any:
        captured["iter"] = capture_forwarded_kwargs(kwargs)
        yield iter_result

    monkeypatch.setattr(durable_agent, "_run_async", run_async_direct)
    monkeypatch.setattr(durable_agent, "_run_sync", run_sync_direct)
    monkeypatch.setattr(durable_agent, "_kitaru_overrides", nullcontext)
    monkeypatch.setattr(durable_agent, "_tracking_scope", nullcontext)
    monkeypatch.setattr(durable_agent, "_allow_internal_iter", nullcontext)
    monkeypatch.setattr(durable_agent, "_remember_messages", lambda _result: None)
    monkeypatch.setattr(
        durable_agent, "_require_explicit_checkpoint", lambda _method: None
    )
    monkeypatch.setattr(AbstractAgent, "run", fake_run)
    monkeypatch.setattr(AbstractAgent, "run_sync", fake_run_sync)
    monkeypatch.setattr(AbstractAgent, "run_stream", fake_run_stream)
    monkeypatch.setattr(durable_agent.wrapped, "iter", fake_iter)

    async def exercise_async_surfaces() -> None:
        result = await durable_agent.run(
            "prompt",
            capabilities=capabilities,
            conversation_id=conversation_id,
            output_retries=output_retries,
        )
        assert result is run_result
        async with durable_agent.run_stream(
            "prompt",
            capabilities=capabilities,
            conversation_id=conversation_id,
            output_retries=output_retries,
        ) as streamed_result:
            assert streamed_result is stream_result
        async with durable_agent.iter(
            "prompt",
            capabilities=capabilities,
            conversation_id=conversation_id,
            output_retries=output_retries,
        ) as agent_run:
            assert agent_run is iter_result

    asyncio.run(exercise_async_surfaces())
    assert (
        durable_agent.run_sync(
            "prompt",
            capabilities=capabilities,
            conversation_id=conversation_id,
            output_retries=output_retries,
        )
        is run_result
    )

    assert captured == {
        "run": expected_forwarded_kwargs,
        "run_stream": expected_forwarded_kwargs,
        "iter": expected_forwarded_kwargs,
        "run_sync": expected_forwarded_kwargs,
    }


def test_phase17_turn_mode_tracks_events_and_artifacts(primed_zenml) -> None:
    """Turn strategy should persist tracker metadata and checkpoint artifacts."""
    durable_agent = KitaruAgent(
        _make_test_agent(name_prefix="turn_agent"),
        checkpoint_strategy="turn",
    )

    @flow
    def turn_flow(prompt: str) -> str:
        return durable_agent.run_sync(prompt).output

    handle = turn_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    summary_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_run_summaries")
    event_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_events")
    summary = next(iter(summary_map.values()))

    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)

    assert _has_step_input(hydrated_run, "user_prompt")

    artifact_names = _artifact_names(hydrated_run)
    run_label = summary["run_label"]
    assert any(
        re.fullmatch(rf"event_log__[a-zA-Z0-9_]+_{run_label}_tracker_1", name)
        for name in artifact_names
    )
    assert any(
        re.fullmatch(rf"run_summary__[a-zA-Z0-9_]+_{run_label}_tracker_1", name)
        for name in artifact_names
    )
    assert any(
        re.fullmatch(rf"llm_call_1_prompt__[a-zA-Z0-9_]+_{run_label}_tracker_1", name)
        for name in artifact_names
    )
    assert any(
        re.fullmatch(rf"llm_call_1_response__[a-zA-Z0-9_]+_{run_label}_tracker_1", name)
        for name in artifact_names
    )
    assert any(re.fullmatch(r"tool_call_\d+_args__.*", name) for name in artifact_names)
    assert any(
        re.fullmatch(r"tool_call_\d+_result__.*", name) for name in artifact_names
    )
    _assert_event_artifacts_use_display_names(event_map, artifact_names)


def test_phase17_turn_mode_tracks_effective_history_input_for_continuations(
    primed_zenml,
) -> None:
    """Continuation turns should expose message_history without a fake prompt input."""
    durable_agent = _make_wrapped_agent(
        name_prefix="history_agent",
        granular_checkpoints=False,
    )

    @flow
    def continuation_flow() -> str:
        first = durable_agent.run_sync("first turn")
        second = durable_agent.run_sync(
            user_prompt=None, message_history=first.all_messages()
        )
        return second.output

    handle = continuation_flow.run()
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    inputs_by_step = _input_names_by_step(hydrated_run)
    assert any("message_history" in inputs for inputs in inputs_by_step)
    assert any(
        "message_history" in inputs and "user_prompt" not in inputs
        for inputs in inputs_by_step
    )
    artifact_names = _artifact_names(hydrated_run)
    assert any(name.startswith("llm_call_1_prompt__") for name in artifact_names)
    assert any(name.startswith("llm_call_1_response__") for name in artifact_names)


def test_phase17_turn_mode_omits_absent_prompt_and_history_inputs(
    primed_zenml,
) -> None:
    """Instructions-only turns should not create prompt/history placeholders."""
    agent = Agent(
        TestModel(),
        name=f"empty_input_agent_{uuid4().hex[:8]}",
        output_type=str,
        instructions="Reply successfully.",
    )
    durable_agent = KitaruAgent(agent, granular_checkpoints=False)

    @flow
    def instructions_only_flow() -> str:
        return durable_agent.run_sync(user_prompt=None).output

    handle = instructions_only_flow.run()
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    assert not _has_step_input(hydrated_run, "user_prompt")
    assert not _has_step_input(hydrated_run, "message_history")
    artifact_names = _artifact_names(hydrated_run)
    assert any(name.startswith("llm_call_1_prompt__") for name in artifact_names)
    assert any(name.startswith("llm_call_1_response__") for name in artifact_names)


def test_phase17_direct_wait_tool_with_explicit_thread_opt_in_reaches_wait(
    primed_zenml,
) -> None:
    """A normal sync tool-body wait should opt into workflow-thread execution."""
    del primed_zenml
    _require_wait_support()
    _assert_child_flow_pauses(
        _run_direct_wait_flow_until_pause,
        required_event=_CHILD_EVENT_ZENML_WAIT_REACHED,
    )


def test_phase17_streaming_wait_tool_with_checkpoint_reaches_wait(
    primed_zenml,
) -> None:
    """Issue #425: streamed fallback must let opted-out sync tools run at flow scope."""
    del primed_zenml
    _require_wait_support()
    _assert_child_flow_pauses(
        _run_streaming_wait_flow_until_pause,
        required_event=_CHILD_EVENT_ZENML_WAIT_REACHED,
    )


def test_phase17_direct_wait_tool_without_opt_out_keeps_checkpoint_guard(
    primed_zenml,
) -> None:
    """Default granular checkpointing should still reject checkpoint-contained waits."""
    del primed_zenml
    _require_wait_support()
    agent = Agent(
        TestModel(call_tools=["ask_user"]),
        name=f"guarded_wait_agent_{uuid4().hex[:8]}",
        output_type=str,
    )

    @agent.tool_plain
    def ask_user() -> str:
        return kp.wait_for_input(
            schema=str,
            question="This should not be created inside a tool checkpoint.",
            timeout=0,
        )

    global _GUARDED_WAIT_AGENT
    _GUARDED_WAIT_AGENT = KitaruAgent(agent)
    try:
        with pytest.raises(KitaruUsageError) as exc_info:
            pydantic_ai_guarded_wait_flow.run(cache=False)
        message = str(exc_info.value)
        assert "tool_checkpoint_config_by_name" in message
        assert "allow_sync_tool_body_waits=True" in message
    finally:
        _GUARDED_WAIT_AGENT = None


def test_phase17_hitl_tool_still_reaches_wait(primed_zenml) -> None:
    """The declarative HITL path should still pause through adapter-managed wait."""
    del primed_zenml
    _require_wait_support()
    _assert_child_flow_pauses(
        _run_hitl_wait_flow_until_pause,
        required_event=_CHILD_EVENT_ZENML_WAIT_REACHED,
    )


def test_phase17_default_granular_mode_tracks_at_flow_scope(primed_zenml) -> None:
    """Default granular mode should return final output and flush run metadata."""
    durable_agent = KitaruAgent(_make_test_agent(name_prefix="granular_agent"))

    @flow
    def granular_flow(prompt: str) -> str:
        return durable_agent.run_sync(prompt).output

    handle = granular_flow.run("use the add tool")
    result = handle.wait()
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    assert isinstance(result, str)
    assert result
    assert "pydantic_ai_run_summaries" in hydrated_run.run_metadata
    assert "pydantic_ai_events" in hydrated_run.run_metadata

    summary_map = hydrated_run.run_metadata["pydantic_ai_run_summaries"]
    event_map = hydrated_run.run_metadata["pydantic_ai_events"]
    summary = next(iter(summary_map.values()))

    assert summary["model_call_count"] >= 1
    assert summary["tool_call_count"] >= 1
    assert {"llm_call", "tool_call"} <= _event_kinds(event_map)
    artifact_names = _artifact_names(hydrated_run)
    assert len(hydrated_run.steps) >= 2
    assert _has_step_input(hydrated_run, "messages")
    assert _has_step_input(hydrated_run, "tool_args")
    assert _has_step_metadata(
        hydrated_run,
        **adapter_checkpoint_metadata(
            adapter="pydantic_ai",
            kind="model_request",
            input_slots=[],
            output_slots=["output"],
        ),
    )
    assert _has_step_metadata(
        hydrated_run,
        **adapter_checkpoint_metadata(
            adapter="pydantic_ai",
            kind="tool_call",
            input_slots=["tool_args"],
            output_slots=["output"],
        ),
    )
    assert not any(name.startswith("llm_call_1_prompt__") for name in artifact_names)
    assert not any(name.startswith("llm_call_1_response__") for name in artifact_names)
    assert not any(
        re.fullmatch(r"tool_call_\d+_args__.*", name) for name in artifact_names
    )
    assert not any(
        re.fullmatch(r"tool_call_\d+_result__.*", name) for name in artifact_names
    )
    allowed_event_refs = set(artifact_names) | {"messages", "tool_args", "output"}
    _assert_event_artifacts_use_display_names(event_map, allowed_event_refs)
    assert all(
        event.get("checkpoint_name")
        for event in _events(event_map)
        if "output" in event.get("artifacts", {}).values()
    )
    assert "event_log" not in artifact_names
    assert "run_summary" not in artifact_names
    assert not any(name.startswith("event_log__") for name in artifact_names)
    assert not any(name.startswith("run_summary__") for name in artifact_names)


_AUTO_FLOW_AGENT: KitaruAgent[Any, str] | None = None


def _invoke_shared_auto_flow_agent() -> str:
    assert _AUTO_FLOW_AGENT is not None
    return _AUTO_FLOW_AGENT.run_sync("use the add tool").output


def test_phase17_tracker_namespace_allocation_is_checkpoint_shared(monkeypatch) -> None:
    """Concurrent trackers in one checkpoint should not all get plain names."""
    checkpoint_scope = _FakeCheckpointScope(
        execution_id="exec-1",
        checkpoint_id="checkpoint-1",
        name="shared_checkpoint",
    )
    monkeypatch.setattr(_tracking, "get_current_checkpoint", lambda: checkpoint_scope)
    monkeypatch.setattr(
        _tracking, "get_current_checkpoint_name", lambda: checkpoint_scope.name
    )
    monkeypatch.setattr(
        _tracking, "get_current_execution_id", lambda: checkpoint_scope.execution_id
    )

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            namespaces = list(
                executor.map(
                    lambda index: (
                        _tracking.EventTracker(
                            agent_name=f"agent_{index}"
                        ).artifact_namespace
                    ),
                    range(4),
                )
            )

        assert None not in namespaces
        assert len(namespaces) == len(set(namespaces))
        assert all("_tracker_" in namespace for namespace in namespaces if namespace)
        suffixes = sorted(
            int(match.group(1))
            for namespace in namespaces
            if (match := re.search(r"_tracker_(\d+)$", namespace or ""))
        )
        assert suffixes == [1, 2, 3, 4]
    finally:
        _tracking._reset_artifact_namespace_state(checkpoint_scope)


def test_phase17_multiple_tracker_scopes_at_flow_scope_get_unique_namespaces(
    primed_zenml,
) -> None:
    """Multiple flow-scope trackers should not collide on bare artifact names."""
    first_agent = KitaruAgent(_make_test_agent(name_prefix="flow_first_agent"))
    second_agent = KitaruAgent(_make_test_agent(name_prefix="flow_second_agent"))

    @flow
    def multi_agent_flow(prompt: str) -> str:
        first = first_agent.run_sync(prompt).output
        second = second_agent.run_sync(prompt).output
        return f"{first}\n{second}"

    handle = multi_agent_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)

    summary_map = hydrated_run.run_metadata["pydantic_ai_run_summaries"]
    event_map = hydrated_run.run_metadata["pydantic_ai_events"]
    artifact_names = _artifact_names(hydrated_run)
    summaries = list(summary_map.values())
    run_labels = {summary["run_label"] for summary in summaries}
    assert len(run_labels) == 2

    model_artifact_refs = [
        stored_name
        for event in _events(event_map)
        if event["kind"] == "llm_call"
        for stored_name in event.get("artifacts", {}).values()
    ]
    assert model_artifact_refs
    assert {"messages", "output"} <= set(model_artifact_refs)
    assert _has_step_input(hydrated_run, "messages")
    assert _has_step_input(hydrated_run, "tool_args")
    assert not any(name.startswith("llm_call_1_prompt__") for name in artifact_names)
    assert not any(name.startswith("llm_call_1_response__") for name in artifact_names)
    assert not any(name.startswith("event_log__") for name in artifact_names)
    assert not any(name.startswith("run_summary__") for name in artifact_names)
    allowed_event_refs = set(artifact_names) | {"messages", "tool_args", "output"}
    _assert_event_artifacts_use_display_names(event_map, allowed_event_refs)


def test_phase17_multiple_tracker_scopes_in_checkpoint_get_namespaces(
    primed_zenml,
) -> None:
    """All trackers in the same checkpoint should get unique namespaces."""
    first_agent = _make_wrapped_agent(
        name_prefix="first_agent",
        granular_checkpoints=False,
    )
    second_agent = _make_wrapped_agent(
        name_prefix="second_agent",
        granular_checkpoints=False,
    )

    @checkpoint
    def run_both_agents(prompt: str) -> str:
        first = first_agent.run_sync(prompt).output
        second = second_agent.run_sync(prompt).output
        return f"{first}\n{second}"

    @flow
    def multi_agent_flow(prompt: str) -> str:
        return run_both_agents(prompt)

    handle = multi_agent_flow.run("use the add tool")
    hydrated_run = _wait_for_hydrated_run(handle.exec_id)
    event_map = _metadata_dict_from_steps(hydrated_run, "pydantic_ai_events")
    artifact_names = _artifact_names(hydrated_run)

    summaries = list(
        _metadata_dict_from_steps(hydrated_run, "pydantic_ai_run_summaries").values()
    )
    run_labels = {summary["run_label"] for summary in summaries}
    assert len(run_labels) == 2
    for tracker_index in (1, 2):
        assert any(
            re.fullmatch(rf"event_log__[a-zA-Z0-9_]+_tracker_{tracker_index}", name)
            for name in artifact_names
        )
        assert any(
            re.fullmatch(rf"run_summary__[a-zA-Z0-9_]+_tracker_{tracker_index}", name)
            for name in artifact_names
        )
        assert any(
            re.fullmatch(
                rf"llm_call_1_prompt__[a-zA-Z0-9_]+_tracker_{tracker_index}", name
            )
            for name in artifact_names
        )
        assert any(
            re.fullmatch(
                rf"llm_call_1_response__[a-zA-Z0-9_]+_tracker_{tracker_index}", name
            )
            for name in artifact_names
        )
    assert all(
        any(run_label in name for name in artifact_names) for run_label in run_labels
    )
    _assert_event_artifacts_use_display_names(event_map, artifact_names)
    event_artifact_names = [
        stored_name
        for event in _events(event_map)
        for stored_name in event.get("artifacts", {}).values()
    ]
    assert any(
        re.fullmatch(r"llm_call_1_prompt__[a-zA-Z0-9_]+_tracker_2", name)
        for name in event_artifact_names
    )

    event_log_names = [
        name for name in artifact_names if name.startswith("event_log__")
    ]
    run_summary_names = [
        name for name in artifact_names if name.startswith("run_summary__")
    ]
    assert len(event_log_names) == 2
    assert len(run_summary_names) == 2
    assert len(event_log_names) == len(set(event_log_names))
    assert len(run_summary_names) == len(set(run_summary_names))


def test_registered_agent_auto_flow_runs_end_to_end(
    primed_zenml,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`run_sync()` outside any flow auto-opens a flow and completes."""
    global _AUTO_FLOW_AGENT
    _AUTO_FLOW_AGENT = KitaruAgent(_make_test_agent(name_prefix="auto_flow_agent"))
    repository_root = Path(Client.find_repository())
    entrypoint_module = repository_root / "registered_agent.py"
    entrypoint_module.write_text(
        "from tests.test_phase17_pydantic_ai_adapter import _AUTO_FLOW_AGENT\n"
    )
    (repository_root / ".gitignore").write_text(".kitaru/\n")
    subprocess.run(["git", "init", "-q", str(repository_root)], check=True)
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repository_root),
            "add",
            ".gitignore",
            entrypoint_module.name,
        ],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository_root), "commit", "-qm", "test entrypoint"],
        check=True,
    )
    sys.modules.pop("registered_agent", None)
    monkeypatch.syspath_prepend(repository_root)
    importlib.invalidate_caches()
    try:
        _AUTO_FLOW_AGENT.register(
            entrypoint="registered_agent:_AUTO_FLOW_AGENT",
        )
        result = _invoke_shared_auto_flow_agent()
    finally:
        _AUTO_FLOW_AGENT = None
    assert isinstance(result, str)


def test_phase17_persist_message_history_extends_across_runs(primed_zenml) -> None:
    """Two successive `run_sync` calls on the same instance accumulate history."""
    durable_agent = KitaruAgent(_make_test_agent(name_prefix="chat_agent"))
    durable_agent._persist_message_history = True

    @flow
    def chat_flow() -> str:
        durable_agent.run_sync("first turn")
        second = durable_agent.run_sync("second turn")
        return second.output

    handle = chat_flow.run()
    _wait_for_hydrated_run(handle.exec_id)

    assert durable_agent._last_messages is not None
    assert len(durable_agent._last_messages) >= 4
