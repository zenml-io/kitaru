"""Deterministic tests for LangGraph calls-mode LangChain middleware."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast

from kitaru.adapters.langgraph import (
    KitaruGraphRunner,
    LangGraphCallCheckpointPolicy,
    LangGraphCapturePolicy,
    LangGraphRunRequest,
)
from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware


def _patch_runner_summary_runtime(monkeypatch) -> tuple[Any, dict[str, object]]:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    logged: dict[str, object] = {}

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking.kitaru, "log", lambda **kwargs: logged.update(kwargs))
    return agent_module, logged


def _model_request(*, tool_call_ids: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        model=SimpleNamespace(model_name="fake-model"),
        messages=[{"role": "user", "content": "hello"}],
        system_message=None,
        tool_choice=None,
        tools=[{"name": "alpha"}, {"name": "beta"}],
        response_format=None,
        model_settings={"temperature": 0, "api_key": "SECRET"},
        runtime=SimpleNamespace(node_name="model_node"),
        _tool_call_ids=tool_call_ids or [],
    )


def _model_response(*tool_call_ids: str) -> SimpleNamespace:
    return SimpleNamespace(
        result=[
            SimpleNamespace(
                content="ok",
                tool_calls=[
                    {"id": tool_call_id, "name": "alpha", "args": {}}
                    for tool_call_id in tool_call_ids
                ],
                usage_metadata={"total_tokens": 3},
            )
        ],
        usage_metadata={"total_tokens": 3},
    )


def _tool_request(
    *,
    name: str = "alpha",
    call_id: str | None = "call-alpha",
    args: dict[str, object] | None = None,
) -> SimpleNamespace:
    tool_call: dict[str, object] = {"name": name, "args": args or {"value": 2}}
    if call_id is not None:
        tool_call["id"] = call_id
    return SimpleNamespace(
        tool_call=tool_call,
        tool=SimpleNamespace(name=name),
        state={},
        runtime=SimpleNamespace(node_name="tool_node"),
    )


def _patch_runtime(
    monkeypatch, *, inside_flow: bool, inside_checkpoint: bool = False
) -> tuple[Any, Any, list[dict[str, object]]]:
    middleware_module = importlib.import_module("kitaru.adapters.langgraph.langchain")
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    seen_checkpoints: list[dict[str, object]] = []
    checkpoint_state = {"inside": inside_checkpoint}

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        seen_checkpoints.append(dict(kwargs))
        body = cast(Callable[[], object], kwargs["body"])
        previous = checkpoint_state["inside"]
        checkpoint_state["inside"] = True
        try:
            return body()
        finally:
            checkpoint_state["inside"] = previous

    monkeypatch.setattr(middleware_module, "is_inside_flow", lambda: inside_flow)
    monkeypatch.setattr(
        middleware_module,
        "is_inside_checkpoint",
        lambda: checkpoint_state["inside"],
    )
    monkeypatch.setattr(
        middleware_module,
        "get_current_checkpoint_id",
        lambda: "checkpoint-from-body",
    )
    monkeypatch.setattr(
        middleware_module,
        "get_current_checkpoint_name",
        lambda: "checkpoint_name_from_body",
    )
    monkeypatch.setattr(
        middleware_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )
    return middleware_module, tracking, seen_checkpoints


def _logged_events(logged: dict[str, object]) -> list[dict[str, object]]:
    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    event_payload = cast(
        dict[str, list[dict[str, object]]],
        logged[constants.LANGGRAPH_EVENTS_METADATA_KEY],
    )
    return next(iter(event_payload.values()))


def test_runner_calls_mode_uses_middleware_without_outer_graph_checkpoint(
    monkeypatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    middleware_module, tracking, middleware_checkpoints = _patch_runtime(
        monkeypatch,
        inside_flow=True,
    )
    runner_checkpoints: list[str] = []
    logged: dict[str, object] = {}
    middleware = KitaruLangGraphMiddleware()

    def fake_runner_checkpoint(**kwargs: object) -> object:
        runner_checkpoints.append(cast(str, kwargs["step_name"]))
        body = cast(Callable[[], object], kwargs["body"])
        return body()

    class FakeGraph:
        name = "calls_graph"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            model_response = middleware.wrap_model_call(
                _model_request(),
                lambda _request: _model_response("call-alpha"),
            )
            tool_result = middleware.wrap_tool_call(
                _tool_request(),
                lambda request: {"echo": request.tool_call["args"]},
            )
            return {"input": input, "model": model_response, "tool": tool_result}

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fake_runner_checkpoint)
    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking.kitaru, "log", lambda **kwargs: logged.update(kwargs))
    monkeypatch.setattr(middleware_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(middleware_module, "is_inside_checkpoint", lambda: False)

    runner = KitaruGraphRunner(FakeGraph(), checkpoint_strategy="calls")
    result = runner.invoke(LangGraphRunRequest.start({"x": 1}, thread_id="thread-1"))

    all_step_names = [
        *runner_checkpoints,
        *[cast(str, checkpoint["step_name"]) for checkpoint in middleware_checkpoints],
    ]
    assert result.status == "completed"
    assert not any(name.endswith("_langgraph_call") for name in all_step_names)
    assert any(name.startswith("model_call__") for name in all_step_names)
    assert any(
        name.startswith("tool_call__alpha_call_alpha_") for name in all_step_names
    )
    assert any(
        name.startswith("langgraph_summary__calls_graph_") for name in all_step_names
    )

    events = _logged_events(logged)
    assert [event["kind"] for event in events] == [
        "graph_call_started",
        "model_call",
        "tool_call",
        "graph_call_completed",
    ]
    assert events[1]["checkpoint_mode"] == "true"
    assert events[2]["checkpoint_mode"] == "true"


def test_sync_model_and_tool_checkpoints_use_structural_refs_and_tool_overrides(
    monkeypatch,
) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=True)
    middleware = KitaruLangGraphMiddleware()
    policy = LangGraphCallCheckpointPolicy(
        tool_checkpoint_config_by_name={
            "disabled tool": False,
            "custom-tool": {"cache": True},
        },
    )
    capture = LangGraphCapturePolicy()

    with tracking.tracker_scope(
        "structural_graph",
        call_checkpoint_policy=policy,
        capture=capture,
    ) as tracker:
        response = middleware.wrap_model_call(
            _model_request(),
            lambda _request: _model_response("call-custom"),
        )
        custom_result = middleware.wrap_tool_call(
            _tool_request(name="custom-tool", call_id="call-custom"),
            lambda _request: {"ok": True},
        )
        disabled_result = middleware.wrap_tool_call(
            _tool_request(name="disabled tool", call_id="call-disabled"),
            lambda _request: {"disabled": True},
        )
        events = [event.model_dump(mode="json") for event in tracker.events]

    assert response.result[0].content == "ok"
    assert custom_result == {"ok": True}
    assert disabled_result == {"disabled": True}
    assert [checkpoint["checkpoint_inputs"] for checkpoint in checkpoints] == [
        {
            "model_input": {
                "model": {
                    "model_name": "fake-model",
                    "python_type": "types.SimpleNamespace",
                },
                "messages": [{"role": "user", "content": "hello"}],
                "system_message": None,
                "tool_choice": None,
                "tools": [
                    {"name": "alpha", "python_type": "dict"},
                    {"name": "beta", "python_type": "dict"},
                ],
                "response_format": None,
                "model_settings": {"temperature": 0, "api_key": "[REDACTED]"},
            }
        },
        {
            "tool_args": {
                "tool_name": "custom-tool",
                "tool_call_id": "call-custom",
                "args": {"value": 2},
                "tool_call": {
                    "name": "custom-tool",
                    "args": {"value": 2},
                    "id": "call-custom",
                },
            }
        },
    ]
    first_config = cast(dict[str, object], checkpoints[0]["config"])
    second_config = cast(dict[str, object], checkpoints[1]["config"])
    assert first_config["type"] == "model_call"
    assert second_config["type"] == "tool_call"
    assert second_config["cache"] is True
    assert events[0]["artifacts"] == {"model_input": "model_input", "output": "output"}
    assert events[0]["checkpoint_id"] == "checkpoint-from-body"
    assert events[0]["checkpoint_name"] == "checkpoint_name_from_body"
    assert events[1]["artifacts"] == {"tool_args": "tool_args", "output": "output"}
    assert events[1]["tool_name"] == "custom-tool"
    assert events[1]["checkpoint_id"] == "checkpoint-from-body"
    assert events[2]["tool_name"] == "disabled tool"
    assert events[2]["checkpoint_mode"] == "metadata_only"


def test_real_langchain_create_agent_calls_mode_reaches_middleware_contextvars(
    monkeypatch,
) -> None:
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import (
        FakeMessagesListChatModel,
    )
    from langchain_core.messages import AIMessage
    from langgraph.checkpoint.memory import InMemorySaver

    class BindableFakeMessagesListChatModel(FakeMessagesListChatModel):
        def bind_tools(
            self,
            tools: Any,
            *,
            tool_choice: str | None = None,
            **kwargs: Any,
        ) -> Any:
            return self

    def add_one(value: int) -> str:
        """Add one to the provided value."""
        return str(value + 1)

    _middleware_module, _tracking, middleware_checkpoints = _patch_runtime(
        monkeypatch,
        inside_flow=True,
    )
    agent_module, logged = _patch_runner_summary_runtime(monkeypatch)
    runner_checkpoints: list[str] = []

    def fake_runner_checkpoint(**kwargs: object) -> object:
        runner_checkpoints.append(cast(str, kwargs["step_name"]))
        body = cast(Callable[[], object], kwargs["body"])
        return body()

    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fake_runner_checkpoint)

    model = BindableFakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[{"name": "add_one", "args": {"value": 1}, "id": "call-1"}],
            ),
            AIMessage(content="done"),
        ]
    )
    graph = create_agent(
        model,
        tools=[add_one],
        middleware=[KitaruLangGraphMiddleware()],
        checkpointer=InMemorySaver(),
        name="real_langchain_agent",
    )
    runner = KitaruGraphRunner(
        graph,
        name="real_langchain_agent",
        checkpoint_strategy="calls",
        capture=LangGraphCapturePolicy(save_state_snapshot=False),
    )

    result = runner.invoke(
        LangGraphRunRequest.start(
            {"messages": [{"role": "user", "content": "use the tool"}]},
            thread_id="real-langchain-thread",
        )
    )

    all_step_names = [
        *runner_checkpoints,
        *[cast(str, checkpoint["step_name"]) for checkpoint in middleware_checkpoints],
    ]
    assert result.status == "completed"
    assert not any(name.endswith("_langgraph_call") for name in all_step_names)
    assert any(name.startswith("model_call__") for name in all_step_names)
    assert any(name.startswith("tool_call__add_one_call_1_") for name in all_step_names)
    assert any(
        name.startswith("langgraph_summary__real_langchain_agent_")
        for name in all_step_names
    )

    events = _logged_events(logged)
    assert [event["kind"] for event in events] == [
        "graph_call_started",
        "model_call",
        "tool_call",
        "model_call",
        "graph_call_completed",
    ]
    assert [
        event["checkpoint_mode"]
        for event in events
        if event["kind"] in {"model_call", "tool_call"}
    ] == ["true", "true", "true"]
    assert events[2]["tool_name"] == "add_one"
    assert events[2]["tool_call_id"] == "call-1"


def test_checkpoint_cache_identity_does_not_collapse_when_capture_is_disabled(
    monkeypatch,
) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=True)
    middleware = KitaruLangGraphMiddleware()
    capture = LangGraphCapturePolicy(save_model_input=False, save_tool_args=False)

    with tracking.tracker_scope(
        "cache_identity_graph",
        call_checkpoint_policy=LangGraphCallCheckpointPolicy(
            model_checkpoint_config={"cache": True},
            tool_checkpoint_config={"cache": True},
        ),
        capture=capture,
    ):
        middleware.wrap_model_call(
            _model_request(),
            lambda _request: _model_response(),
        )
        second_request = _model_request()
        second_request.messages = [{"role": "user", "content": "different"}]
        middleware.wrap_model_call(
            second_request,
            lambda _request: _model_response(),
        )
        middleware.wrap_tool_call(
            _tool_request(args={"value": 1}, call_id="call-1"),
            lambda _request: "one",
        )
        middleware.wrap_tool_call(
            _tool_request(args={"value": 2}, call_id="call-2"),
            lambda _request: "two",
        )

    assert [checkpoint.get("checkpoint_inputs") for checkpoint in checkpoints] == [
        None,
        None,
        None,
        None,
    ]
    cache_keys = [checkpoint["cache_key"] for checkpoint in checkpoints]
    assert cache_keys[0] != cache_keys[1]
    assert cache_keys[2] != cache_keys[3]


def test_tool_checkpoint_inputs_redact_secrets_but_cache_identity_stays_distinct(
    monkeypatch,
) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=True)
    middleware = KitaruLangGraphMiddleware()
    policy = LangGraphCallCheckpointPolicy(tool_checkpoint_config={"cache": True})

    with tracking.tracker_scope(
        "privacy_graph",
        call_checkpoint_policy=policy,
        capture=LangGraphCapturePolicy(),
    ):
        first_result = middleware.wrap_tool_call(
            _tool_request(
                name="secret-tool",
                call_id="reused-call-id",
                args={
                    "value": 2,
                    "api_key": "SECRET-ONE",
                    "nested": {"password": "PASSWORD-ONE", "safe": "same"},
                },
            ),
            lambda _request: "first",
        )
        second_result = middleware.wrap_tool_call(
            _tool_request(
                name="secret-tool",
                call_id="reused-call-id",
                args={
                    "value": 2,
                    "api_key": "SECRET-TWO",
                    "nested": {"password": "PASSWORD-TWO", "safe": "same"},
                },
            ),
            lambda _request: "second",
        )

    assert first_result == "first"
    assert second_result == "second"
    persisted_inputs = [checkpoint["checkpoint_inputs"] for checkpoint in checkpoints]
    expected_redacted_tool_args = {
        "tool_name": "secret-tool",
        "tool_call_id": "reused-call-id",
        "args": {
            "value": 2,
            "api_key": "[REDACTED]",
            "nested": {"password": "[REDACTED]", "safe": "same"},
        },
        "tool_call": {
            "name": "secret-tool",
            "args": {
                "value": 2,
                "api_key": "[REDACTED]",
                "nested": {"password": "[REDACTED]", "safe": "same"},
            },
            "id": "reused-call-id",
        },
    }
    assert persisted_inputs == [
        {"tool_args": expected_redacted_tool_args},
        {"tool_args": expected_redacted_tool_args},
    ]
    assert "SECRET-" not in repr(persisted_inputs)
    assert "PASSWORD-" not in repr(persisted_inputs)
    assert checkpoints[0]["cache_key"] != checkpoints[1]["cache_key"]


def test_tool_checkpoint_names_include_sequence_for_duplicate_tool_call_ids(
    monkeypatch,
) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=True)
    middleware = KitaruLangGraphMiddleware()

    with tracking.tracker_scope(
        "duplicate_id_graph",
        call_checkpoint_policy=LangGraphCallCheckpointPolicy(),
        capture=LangGraphCapturePolicy(),
    ):
        middleware.wrap_tool_call(
            _tool_request(name="alpha", call_id="repeated-call"),
            lambda _request: "first",
        )
        middleware.wrap_tool_call(
            _tool_request(name="alpha", call_id="repeated-call"),
            lambda _request: "second",
        )

    step_names = [cast(str, checkpoint["step_name"]) for checkpoint in checkpoints]
    assert len(step_names) == 2
    assert len(set(step_names)) == 2
    assert "tool_call__alpha_repeated_call_1__" in step_names[0]
    assert "tool_call__alpha_repeated_call_2__" in step_names[1]


def test_calls_mode_summary_checkpoint_failure_falls_back_to_logged_metadata(
    monkeypatch,
) -> None:
    agent_module, logged = _patch_runner_summary_runtime(monkeypatch)

    class FakeGraph:
        name = "summary_fallback"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def failing_summary_checkpoint(**_kwargs: object) -> object:
        raise RuntimeError("summary checkpoint unavailable")

    monkeypatch.setattr(
        agent_module,
        "run_sync_in_checkpoint",
        failing_summary_checkpoint,
    )

    runner = KitaruGraphRunner(FakeGraph(), checkpoint_strategy="calls")
    result = runner.invoke(
        LangGraphRunRequest.start({"input": "value"}, thread_id="thread-1")
    )

    assert result.status == "completed"
    events = _logged_events(logged)
    assert [event["kind"] for event in events] == [
        "graph_call_started",
        "graph_call_completed",
    ]

    constants = importlib.import_module("kitaru.adapters.langgraph._constants")
    summaries = cast(
        dict[str, dict[str, object]],
        logged[constants.LANGGRAPH_RUN_SUMMARIES_METADATA_KEY],
    )
    summary = next(iter(summaries.values()))
    assert summary["summary_checkpoint_failed"] is True
    failures = cast(list[dict[str, object]], summary["persistence_failures"])
    assert result.run_summary_artifact_name is not None
    assert failures == [
        {
            "operation": "save_run_summary",
            "artifact_name": result.run_summary_artifact_name.replace(
                "run_summary__", "langgraph_summary__", 1
            ),
            "exception_type": "RuntimeError",
            "message": "summary checkpoint unavailable",
        }
    ]


def test_handler_exception_records_failed_call_event_and_reraises(monkeypatch) -> None:
    _, tracking, _checkpoints = _patch_runtime(monkeypatch, inside_flow=False)
    middleware = KitaruLangGraphMiddleware()

    with tracking.tracker_scope("failure_graph") as tracker:
        try:
            middleware.wrap_tool_call(
                _tool_request(name="boom", call_id="call-boom"),
                lambda _request: (_ for _ in ()).throw(RuntimeError("tool boom")),
            )
        except RuntimeError as error:
            assert str(error) == "tool boom"
        else:  # pragma: no cover - defensive assertion
            raise AssertionError("handler error was not reraised")
        event = tracker.events[0].model_dump(mode="json")

    assert event["kind"] == "tool_call"
    assert event["status"] == "failed"
    assert event["checkpoint_mode"] == "metadata_only"
    assert event["error"] == {"exception_type": "RuntimeError", "message": "tool boom"}


def test_outside_flow_records_metadata_only_without_checkpoint(monkeypatch) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=False)
    middleware = KitaruLangGraphMiddleware()

    with tracking.tracker_scope("outside_flow") as tracker:
        result = middleware.wrap_model_call(
            _model_request(),
            lambda _request: _model_response(),
        )
        event = tracker.events[0].model_dump(mode="json")

    assert result.result[0].content == "ok"
    assert checkpoints == []
    assert event["kind"] == "model_call"
    assert event["checkpoint_mode"] == "metadata_only"
    assert event["artifacts"] == {}


def test_middleware_reserves_tool_order_from_model_response(monkeypatch) -> None:
    _, tracking, _checkpoints = _patch_runtime(monkeypatch, inside_flow=False)
    middleware = KitaruLangGraphMiddleware()

    with tracking.tracker_scope("ordering_graph") as tracker:
        middleware.wrap_model_call(
            _model_request(),
            lambda _request: _model_response("call-a", "call-b"),
        )
        middleware.wrap_tool_call(
            _tool_request(name="beta", call_id="call-b"),
            lambda _request: "second",
        )
        middleware.wrap_tool_call(
            _tool_request(name="alpha", call_id="call-a"),
            lambda _request: "first",
        )
        events = [event.model_dump(mode="json") for event in tracker.events]

    assert [event["kind"] for event in events] == [
        "model_call",
        "tool_call",
        "tool_call",
    ]
    assert [event["tool_call_id"] for event in events[1:]] == ["call-a", "call-b"]
    assert events[1]["parent_event_ids"] == [events[0]["event_id"]]
    assert events[2]["parent_event_ids"] == [events[0]["event_id"]]


def test_async_hooks_are_metadata_only_even_inside_flow(monkeypatch) -> None:
    _, tracking, checkpoints = _patch_runtime(monkeypatch, inside_flow=True)
    middleware = KitaruLangGraphMiddleware()

    async def run() -> list[dict[str, object]]:
        with tracking.tracker_scope("async_graph") as tracker:
            model_result = await middleware.awrap_model_call(
                _model_request(),
                lambda _request: _async_value(_model_response("async-call")),
            )
            tool_result = await middleware.awrap_tool_call(
                _tool_request(name="async_tool", call_id="async-call"),
                lambda _request: _async_value({"async": True}),
            )
            assert model_result.result[0].content == "ok"
            assert tool_result == {"async": True}
            return [event.model_dump(mode="json") for event in tracker.events]

    events = asyncio.run(run())

    assert checkpoints == []
    assert [event["kind"] for event in events] == ["model_call", "tool_call"]
    assert {event["checkpoint_mode"] for event in events} == {"metadata_only"}


async def _async_value(value: Any) -> Any:
    return value
