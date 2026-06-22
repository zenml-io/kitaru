"""Focused tests for LangGraph graph-call streaming support."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import AsyncIterator, Callable, Iterator
from types import SimpleNamespace
from typing import Any, cast

import pytest
from typing_extensions import TypedDict

pytest.importorskip("langgraph")

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

import kitaru.adapters.langgraph._streaming as langgraph_streaming
from kitaru.adapters.langgraph import (
    LANGGRAPH_STREAM_COMPLETED,
    LANGGRAPH_STREAM_CUSTOM,
    LANGGRAPH_STREAM_FAILED,
    LANGGRAPH_STREAM_MESSAGES,
    LANGGRAPH_STREAM_STARTED,
    LANGGRAPH_STREAM_UPDATES,
    KitaruGraphRunner,
    LangGraphCapturePolicy,
    LangGraphRunRequest,
    LangGraphStreamPolicy,
)
from kitaru.errors import KitaruRuntimeError, KitaruUsageError


def _contains_kitaru_truncation(value: Any) -> bool:
    if isinstance(value, dict):
        return "_kitaru_truncated" in value or any(
            _contains_kitaru_truncation(nested) for nested in value.values()
        )
    if isinstance(value, list):
        return any(_contains_kitaru_truncation(item) for item in value)
    return False


def _nested_wide_payload(*, width: int = 8, depth: int = 4) -> dict[str, Any]:
    payload: Any = "x" * 100
    for level in range(depth):
        payload = {f"level-{level}-key-{index}": payload for index in range(width)}
    return cast(dict[str, Any], payload)


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


def _async_count_graph():
    builder = StateGraph(cast(Any, CountState))

    async def add_one(state: CountState) -> CountState:
        return {"count": state["count"] + 1}

    builder.add_node("add_one", add_one)
    builder.add_edge(START, "add_one")
    builder.add_edge("add_one", END)
    return builder.compile(checkpointer=InMemorySaver())


def _custom_stream_graph():
    from langgraph.config import get_stream_writer

    builder = StateGraph(cast(Any, CountState))

    def add_one(state: CountState) -> CountState:
        writer = get_stream_writer()
        writer({"step": "add_one", "message": "incrementing"})
        return {"count": state["count"] + 1}

    builder.add_node("add_one", add_one)
    builder.add_edge(START, "add_one")
    builder.add_edge("add_one", END)
    return builder.compile(checkpointer=InMemorySaver())


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


def test_langgraph_v2_sync_stream_parts_are_uniform_dicts() -> None:
    graph = _count_graph()

    parts = list(
        graph.stream(
            {"count": 1},
            config={"configurable": {"thread_id": "shape-sync"}},
            stream_mode=["updates", "values"],
            version="v2",
        )
    )

    assert parts
    assert all(isinstance(part, dict) for part in parts)
    assert all({"type", "ns", "data"} <= set(part) for part in parts)
    assert parts[-1]["type"] == "values"
    assert parts[-1]["data"] == {"count": 2}


def test_langgraph_v2_async_stream_parts_are_uniform_dicts() -> None:
    graph = _async_count_graph()

    async def collect() -> list[dict[str, Any]]:
        return [
            part
            async for part in graph.astream(
                {"count": 2},
                config={"configurable": {"thread_id": "shape-async"}},
                stream_mode=["updates", "values"],
                version="v2",
            )
        ]

    parts = asyncio.run(collect())

    assert parts
    assert all(isinstance(part, dict) for part in parts)
    assert all({"type", "ns", "data"} <= set(part) for part in parts)
    assert parts[-1]["type"] == "values"
    assert parts[-1]["data"] == {"count": 3}


def test_langgraph_messages_stream_data_is_chunk_metadata_pair() -> None:
    from langchain_core.language_models.fake_chat_models import FakeListChatModel
    from langchain_core.messages import HumanMessage

    class MessageState(TypedDict):
        messages: list[Any]

    model = FakeListChatModel(responses=["hello"])
    builder = StateGraph(cast(Any, MessageState))

    def call_model(state: MessageState) -> MessageState:
        return {"messages": [model.invoke(state["messages"])]}

    builder.add_node("call_model", call_model)
    builder.add_edge(START, "call_model")
    builder.add_edge("call_model", END)
    graph = builder.compile(checkpointer=InMemorySaver())

    parts = list(
        graph.stream(
            {"messages": [HumanMessage(content="hi")]},
            config={"configurable": {"thread_id": "messages-shape"}},
            stream_mode=["messages", "values"],
            version="v2",
        )
    )
    message_parts = [part for part in parts if part["type"] == "messages"]

    assert message_parts
    chunk, metadata = message_parts[0]["data"]
    assert isinstance(metadata, dict)
    assert "langgraph_node" in metadata
    assert chunk.content


def test_stream_returns_result_from_final_values_and_extracts_usage() -> None:
    runner = KitaruGraphRunner(_count_graph(), name="counter")

    result = runner.stream(
        {"count": 1, "usage": {"total_tokens": 7}},
        thread_id="stream-values-thread",
        stream_mode="updates",
    )

    assert result.status == "completed"
    assert result.output == {"count": 2}


def test_stream_runs_graph_without_langgraph_checkpointer() -> None:
    runner = KitaruGraphRunner(_count_graph(with_checkpointer=False), name="counter")

    result = runner.stream(
        {"count": 1},
        thread_id="stream-no-checkpointer-thread",
        stream_mode="updates",
    )

    assert result.status == "completed"
    assert result.output == {"count": 2}
    assert any("No LangGraph checkpointer" in warning for warning in result.warnings)


class UsageGraph:
    name = "usage_graph"
    checkpointer = object()

    def stream(self, input: object, **_kwargs: object) -> Iterator[dict[str, object]]:
        yield {"type": "values", "ns": (), "data": input}
        yield {
            "type": "values",
            "ns": (),
            "data": {
                "answer": "ok",
                "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
            },
        }

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


class SubgraphValuesAfterRootGraph:
    name = "subgraph_values_after_root"
    checkpointer = object()

    def stream(self, _input: object, **_kwargs: object) -> Iterator[dict[str, object]]:
        yield {"type": "values", "ns": (), "data": {"answer": "root"}}
        yield {"type": "values", "ns": ("child",), "data": {"answer": "subgraph"}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


def test_stream_usage_extraction_depends_on_final_values_payload() -> None:
    runner = KitaruGraphRunner(UsageGraph())

    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="usage-thread")
    )

    assert result.usage is not None
    assert result.usage.total_tokens == 5
    assert result.output == {
        "answer": "ok",
        "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
    }


def test_stream_reconstructs_output_from_root_values_only() -> None:
    runner = KitaruGraphRunner(SubgraphValuesAfterRootGraph())

    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="subgraph-thread"),
        stream_mode="values",
        subgraphs=True,
    )

    assert result.output == {"answer": "root"}


def test_stream_output_candidate_requires_present_data_key() -> None:
    assert langgraph_streaming.stream_part_output_candidate(
        {"type": "values", "ns": ()}
    ) == (False, None)
    assert langgraph_streaming.stream_part_output_candidate(
        {"type": "values", "ns": (), "data": None}
    ) == (True, None)


def test_stream_missing_values_data_does_not_count_as_output() -> None:
    class MissingDataValuesGraph:
        name = "missing_data_values_graph"
        checkpointer = object()

        def stream(
            self, _input: object, **_kwargs: object
        ) -> Iterator[dict[str, object]]:
            yield {"type": "values", "ns": ()}

        def get_state(self, _config: object) -> object:
            return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())

    runner = KitaruGraphRunner(MissingDataValuesGraph())

    with pytest.raises(KitaruUsageError, match="without a `values` part"):
        runner.stream(
            LangGraphRunRequest.start({"prompt": "hi"}, thread_id="missing-data")
        )


def test_interrupted_stream_preserves_interrupt_result_invariants() -> None:
    runner = KitaruGraphRunner(_interrupt_graph(), name="approval")

    result = runner.stream(
        LangGraphRunRequest.start({"value": 1}, thread_id="stream-interrupt-thread"),
        stream_mode="updates",
    )

    assert result.status == "interrupted"
    assert result.output is None
    assert result.pending_state is not None
    assert result.interrupts
    assert result.interrupts[0].value == {"question": "approve?"}

    resumed = runner.stream(
        LangGraphRunRequest.resume(
            Command(resume={"approved": True}),
            thread_id="stream-interrupt-thread",
        ),
        stream_mode="updates",
    )

    assert resumed.status == "completed"
    assert cast(dict[str, Any], resumed.output)["answer"] == {"approved": True}


def test_stream_policy_validation_and_option_resolution() -> None:
    policy = LangGraphStreamPolicy()
    options = langgraph_streaming.resolve_stream_options(
        None,
        policy=policy,
        subgraphs=False,
    )

    assert options.requested_modes == ("messages", "updates", "custom")
    assert options.upstream_modes == ("messages", "updates", "custom", "values")
    assert options.published_modes == options.requested_modes

    explicit = langgraph_streaming.resolve_stream_options(
        ["values", "updates", "values"],
        policy=policy,
        subgraphs=True,
    )
    assert explicit.requested_modes == ("values", "updates")
    assert explicit.upstream_modes == ("values", "updates")
    assert explicit.subgraphs is True

    with pytest.raises(KitaruUsageError, match="Unsupported LangGraph stream mode"):
        langgraph_streaming.resolve_stream_options(
            cast(Any, "events"), policy=policy, subgraphs=False
        )
    with pytest.raises(KitaruUsageError, match="allow_debug"):
        langgraph_streaming.resolve_stream_options(
            "debug", policy=policy, subgraphs=False
        )


def test_stream_publishes_real_langgraph_custom_writer_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, payload, *, flush=False: published.append((kind, payload)),
    )
    runner = KitaruGraphRunner(_custom_stream_graph(), name="custom_writer_graph")

    result = runner.stream(
        LangGraphRunRequest.start({"count": 1}, thread_id="custom-writer-thread"),
        stream_mode="custom",
    )

    custom_payloads = [
        payload for kind, payload in published if kind == LANGGRAPH_STREAM_CUSTOM
    ]
    assert result.output == {"count": 2}
    assert custom_payloads
    assert "custom" not in custom_payloads[0]
    assert custom_payloads[0]["display"] == "Custom stream payload"
    assert "incrementing" not in repr(custom_payloads[0])


def test_stream_publisher_normalizes_safe_payloads_and_best_effort_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any], bool]] = []

    def fake_publish(
        kind: str, payload: dict[str, Any], *, flush: bool = False
    ) -> None:
        published.append((kind, payload, flush))

    monkeypatch.setattr(langgraph_streaming.kitaru_events, "publish", fake_publish)
    options = langgraph_streaming.resolve_stream_options(
        ["messages", "updates", "custom"],
        policy=LangGraphStreamPolicy(max_display_chars=12),
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=LangGraphStreamPolicy(max_display_chars=12),
        options=options,
    )

    publisher.started()
    publisher.part(
        {
            "type": "messages",
            "ns": (),
            "data": (
                SimpleNamespace(type="AIMessageChunk", content="hello streaming world"),
                {"langgraph_node": "model", "api_key": "SECRET"},
            ),
        }
    )
    publisher.part(
        {"type": "updates", "ns": (), "data": {"node": {"answer": "SECRET"}}}
    )
    publisher.part(
        {"type": "custom", "ns": (), "data": {"api_key": "SECRET", "status": "ok"}}
    )
    publisher.completed(
        status="completed", stats=langgraph_streaming.LangGraphStreamStats()
    )

    assert [item[0] for item in published] == [
        LANGGRAPH_STREAM_STARTED,
        LANGGRAPH_STREAM_MESSAGES,
        LANGGRAPH_STREAM_UPDATES,
        LANGGRAPH_STREAM_CUSTOM,
        LANGGRAPH_STREAM_COMPLETED,
    ]
    assert "text_delta" not in published[1][1]
    assert published[1][1]["display"] == "AIMessage..."
    assert published[1][1]["metadata"] == {"langgraph_node": "model"}
    assert "hello streaming world" not in repr(published[1][1])
    assert "SECRET" not in repr(published[1][1])
    assert published[2][1]["node_names"] == ["node"]
    assert "custom" not in published[3][1]
    assert published[3][1]["display"] == "Custom stream payload"
    assert published[3][1]["custom_summary"] == {
        "python_type": "dict",
        "key_count": 2,
        "summary": "mapping with 2 keys",
    }
    assert "api_key" not in repr(published[3][1])
    assert "status" not in repr(published[3][1])
    assert "SECRET" not in repr(published[3][1])
    assert published[-1][2] is True

    calls = 0

    def broken_publish(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise KitaruUsageError("outside checkpoint")

    monkeypatch.setattr(langgraph_streaming.kitaru_events, "publish", broken_publish)
    publisher.part({"type": "updates", "ns": (), "data": {}})
    publisher.failed(
        RuntimeError("boom"), stats=langgraph_streaming.LangGraphStreamStats()
    )
    assert calls == 2


def test_stream_publisher_includes_message_and_custom_content_when_opted_in() -> None:
    policy = LangGraphStreamPolicy(
        max_display_chars=16,
        include_message_text_deltas=True,
        include_custom_payload=True,
    )
    options = langgraph_streaming.resolve_stream_options(
        ["messages", "updates", "custom", "values"],
        policy=policy,
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=policy,
        options=options,
    )

    _, message_payload = publisher.normalize_part(
        {
            "type": "messages",
            "ns": (),
            "data": (
                SimpleNamespace(
                    type="AIMessageChunk",
                    content=[{"text": "x" * 100}, {"text": "y" * 100}],
                ),
                {
                    "tags": [f"tag-{index}" for index in range(25)],
                    "thread_id": "t" * 100,
                },
            ),
        }
    )
    _, update_payload = publisher.normalize_part(
        {
            "type": "updates",
            "ns": (),
            "data": {
                f"node-{node_index}": {
                    f"key-{key_index}": key_index for key_index in range(25)
                }
                for node_index in range(25)
            },
        }
    )
    _, custom_payload = publisher.normalize_part(
        {
            "type": "custom",
            "ns": (),
            "data": {f"key-{index}": "z" * 100 for index in range(25)},
        }
    )
    _, values_payload = publisher.normalize_part(
        {
            "type": "values",
            "ns": (),
            "data": {f"key-{index}": index for index in range(25)},
        }
    )

    assert len(message_payload["text_delta"]) <= policy.max_display_chars
    assert len(message_payload["metadata"]["thread_id"]) <= policy.max_display_chars
    assert len(message_payload["metadata"]["tags"]) == 21
    assert message_payload["metadata"]["tags"][-1] == {"_kitaru_omitted_items": 5}
    assert update_payload["node_count"] == 25
    assert len(update_payload["node_names"]) == 20
    assert update_payload["nodes_truncated"] == 5
    assert len(update_payload["updated_keys_by_node"]["node-0"]) == 20
    assert update_payload["updated_key_counts_by_node"]["node-0"] == 25
    assert custom_payload["custom"]["_kitaru_omitted_keys"] == 5
    assert values_payload["summary"]["keys_truncated"] == 5


def test_stream_publisher_summarizes_structural_modes_and_malformed_parts() -> None:
    options = langgraph_streaming.resolve_stream_options(
        "values",
        policy=LangGraphStreamPolicy(),
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=LangGraphStreamPolicy(),
        options=options,
    )

    values_kind, values_payload = publisher.normalize_part(
        {"type": "values", "ns": ("subgraph",), "data": {"secret_token": "SECRET"}}
    )
    malformed_kind, malformed_payload = publisher.normalize_part(["not", "a", "dict"])
    unknown_kind, unknown_payload = publisher.normalize_part(
        {"type": "events", "ns": (), "data": {}}
    )

    assert values_kind == langgraph_streaming.LANGGRAPH_STREAM_VALUES
    assert values_payload["summary"]["keys"] == ["secret_token"]
    assert "raw" not in values_payload
    assert values_payload["langgraph"]["subgraph"] is True
    assert malformed_kind == langgraph_streaming.LANGGRAPH_STREAM_DEBUG
    assert malformed_payload["category"] == "stream_part_normalization_failed"
    assert unknown_kind == langgraph_streaming.LANGGRAPH_STREAM_DEBUG
    assert unknown_payload["category"] == "stream_part_unknown_mode"


def test_stream_publisher_bounds_opted_in_raw_payloads() -> None:
    class DangerousRepr:
        def __repr__(self) -> str:
            raise AssertionError("raw payload fallback must not call repr")

    policy = LangGraphStreamPolicy(
        include_raw_payloads=True,
        max_display_chars=16,
    )
    options = langgraph_streaming.resolve_stream_options(
        ["updates", "values"],
        policy=policy,
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=policy,
        options=options,
    )
    large_data = {
        "unsupported": {"value": DangerousRepr()},
        **{f"node-{node_index}": {"message": "x" * 100} for node_index in range(25)},
    }

    _, update_payload = publisher.normalize_part(
        {"type": "updates", "ns": (), "data": large_data}
    )
    _, values_payload = publisher.normalize_part(
        {"type": "values", "ns": (), "data": large_data}
    )

    assert update_payload["raw"]["_kitaru_omitted_keys"] == 6
    assert values_payload["raw"]["_kitaru_omitted_keys"] == 6
    assert len(update_payload["raw"]["node-0"]["message"]) <= policy.max_display_chars
    assert len(values_payload["raw"]["node-0"]["message"]) <= policy.max_display_chars
    expected_unsupported_metadata = {
        "python_type": DangerousRepr.__qualname__,
        "serialization_error": "unsupported_stream_value",
    }
    assert (
        update_payload["raw"]["unsupported"]["value"] == expected_unsupported_metadata
    )
    assert (
        values_payload["raw"]["unsupported"]["value"] == expected_unsupported_metadata
    )
    assert "repr" not in update_payload["raw"]["unsupported"]["value"]


def test_stream_publisher_bounds_nested_wide_opted_in_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(langgraph_streaming, "_MAX_STREAM_TOTAL_ITEMS", 24)
    policy = LangGraphStreamPolicy(
        include_raw_payloads=True,
        include_custom_payload=True,
        max_display_chars=16,
    )
    options = langgraph_streaming.resolve_stream_options(
        ["updates", "custom"],
        policy=policy,
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=policy,
        options=options,
    )
    data = _nested_wide_payload()

    _, update_payload = publisher.normalize_part(
        {"type": "updates", "ns": (), "data": data}
    )
    _, custom_payload = publisher.normalize_part(
        {"type": "custom", "ns": (), "data": data}
    )

    assert _contains_kitaru_truncation(update_payload["raw"])
    assert _contains_kitaru_truncation(custom_payload["custom"])
    assert len(repr(update_payload["raw"])) < 5_000
    assert len(repr(custom_payload["custom"])) < 5_000


def test_stream_publisher_bounds_huge_mapping_keys_in_opted_in_payloads() -> None:
    huge_key = "x" * 100_000
    policy = LangGraphStreamPolicy(
        include_raw_payloads=True,
        include_custom_payload=True,
        max_display_chars=16,
    )
    options = langgraph_streaming.resolve_stream_options(
        ["updates", "custom"],
        policy=policy,
        subgraphs=False,
    )
    publisher = langgraph_streaming.LangGraphStreamPublisher(
        graph_name="graph",
        thread_id="thread-1",
        policy=policy,
        options=options,
    )

    _, update_payload = publisher.normalize_part(
        {"type": "updates", "ns": (), "data": {huge_key: {huge_key: "small"}}}
    )
    _, custom_payload = publisher.normalize_part(
        {"type": "custom", "ns": (), "data": {huge_key: "small"}}
    )

    update_raw_repr = repr(update_payload["raw"])
    custom_repr = repr(custom_payload["custom"])
    assert len(update_raw_repr) < 500
    assert len(custom_repr) < 500
    assert huge_key not in update_raw_repr
    assert huge_key not in custom_repr
    assert all(len(key) <= policy.max_display_chars for key in custom_payload["custom"])
    assert all(len(key) <= policy.max_display_chars for key in update_payload["raw"])
    assert (
        len(update_payload["node_names"][0])
        <= langgraph_streaming._MAX_STREAM_LABEL_CHARS
    )
    clipped_node_label = update_payload["node_names"][0]
    assert all(
        len(key) <= langgraph_streaming._MAX_STREAM_LABEL_CHARS
        for key in update_payload["updated_keys_by_node"][clipped_node_label]
    )


class UnknownPartGraph:
    name = "unknown_part_graph"
    checkpointer = object()

    def stream(self, _input: object, **_kwargs: object) -> Iterator[object]:
        yield ["not", "a", "dict"]
        yield {"type": "events", "ns": (), "data": {}}
        yield {"type": "values", "ns": (), "data": {"answer": 42}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


class FakeStreamGraph:
    name = "fake_stream_graph"
    checkpointer = object()

    def __init__(self) -> None:
        self.calls: list[tuple[object, dict[str, Any], dict[str, Any]]] = []

    def stream(
        self, input: object, config: dict[str, Any], **kwargs: Any
    ) -> Iterator[dict[str, object]]:
        self.calls.append((input, config, kwargs))
        yield {"type": "updates", "ns": (), "data": {"node": {"x": 1}}}
        yield {"type": "values", "ns": (), "data": {"answer": 42}}

    async def astream(
        self, input: object, config: dict[str, Any], **kwargs: Any
    ) -> AsyncIterator[dict[str, object]]:
        self.calls.append((input, config, kwargs))
        yield {"type": "updates", "ns": (), "data": {"node": {"x": 1}}}
        yield {"type": "values", "ns": (), "data": {"answer": 43}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(
            config={"configurable": {"checkpoint_id": "cp-1"}}, next=(), tasks=()
        )


class StreamWithoutRequiredKwargsGraph:
    name = "stream_without_required_kwargs_graph"
    checkpointer = object()

    def __init__(self) -> None:
        self.calls = 0

    def stream(
        self, _input: object, *, config: dict[str, Any]
    ) -> Iterator[dict[str, object]]:
        self.calls += 1
        yield {"type": "values", "ns": (), "data": {"answer": 42}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


class AsyncStreamWithoutRequiredKwargsGraph:
    name = "async_stream_without_required_kwargs_graph"
    checkpointer = object()

    def __init__(self) -> None:
        self.calls = 0

    async def astream(
        self, _input: object, *, config: dict[str, Any]
    ) -> AsyncIterator[dict[str, object]]:
        self.calls += 1
        yield {"type": "values", "ns": (), "data": {"answer": 43}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


class StreamWithoutSubgraphsKwargGraph:
    name = "stream_without_subgraphs_kwarg_graph"
    checkpointer = object()

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str]] = []

    def stream(
        self,
        _input: object,
        *,
        config: dict[str, Any],
        stream_mode: list[str],
        version: str,
    ) -> Iterator[dict[str, object]]:
        self.calls.append((stream_mode, version))
        yield {"type": "values", "ns": (), "data": {"answer": 42}}

    def get_state(self, _config: object) -> object:
        return SimpleNamespace(config={"configurable": {}}, next=(), tasks=())


def test_stream_rejects_missing_required_kwargs_before_graph_execution() -> None:
    graph = StreamWithoutRequiredKwargsGraph()
    runner = KitaruGraphRunner(graph)

    with pytest.raises(KitaruUsageError, match=r"stream_mode.*version"):
        runner.stream(
            LangGraphRunRequest.start({"prompt": "hi"}, thread_id="missing-thread"),
            stream_mode="updates",
        )

    assert graph.calls == 0


@pytest.mark.anyio
async def test_astream_rejects_missing_required_kwargs_before_graph_execution() -> None:
    graph = AsyncStreamWithoutRequiredKwargsGraph()
    runner = KitaruGraphRunner(graph)

    with pytest.raises(KitaruUsageError, match=r"stream_mode.*version"):
        await runner.astream(
            LangGraphRunRequest.start(
                {"prompt": "hi"}, thread_id="missing-async-thread"
            ),
            stream_mode="updates",
        )

    assert graph.calls == 0


def test_stream_requires_subgraphs_kwarg_only_when_requested() -> None:
    graph = StreamWithoutSubgraphsKwargGraph()
    runner = KitaruGraphRunner(graph)

    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="no-subgraphs-thread"),
        stream_mode="updates",
    )

    assert result.output == {"answer": 42}
    assert graph.calls == [(["updates", "values"], "v2")]

    requested_subgraphs_graph = StreamWithoutSubgraphsKwargGraph()
    requested_subgraphs_runner = KitaruGraphRunner(requested_subgraphs_graph)
    with pytest.raises(KitaruUsageError, match="subgraphs"):
        requested_subgraphs_runner.stream(
            LangGraphRunRequest.start({"prompt": "hi"}, thread_id="subgraphs-thread"),
            stream_mode="updates",
            subgraphs=True,
        )

    assert requested_subgraphs_graph.calls == []


def test_stream_passes_required_kwargs_when_signature_uninspectable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    original_signature = agent_module.inspect.signature

    def uninspectable_stream(method: Callable[..., Any]) -> object:
        if getattr(method, "__name__", "") == "stream":
            raise ValueError("cannot inspect stream")
        return original_signature(method)

    graph = FakeStreamGraph()
    monkeypatch.setattr(agent_module.inspect, "signature", uninspectable_stream)

    result = KitaruGraphRunner(graph).stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="uninspectable-thread"),
        stream_mode="updates",
        subgraphs=True,
    )

    assert result.output == {"answer": 42}
    kwargs_seen = graph.calls[0][2]
    assert kwargs_seen["stream_mode"] == ["updates", "values"]
    assert kwargs_seen["version"] == "v2"
    assert kwargs_seen["subgraphs"] is True


def test_stream_publishes_debug_for_reachable_unknown_and_malformed_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, payload, *, flush=False: published.append((kind, payload)),
    )
    runner = KitaruGraphRunner(UnknownPartGraph())

    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="unknown-thread"),
        stream_mode="updates",
    )

    debug_payloads = [
        payload
        for kind, payload in published
        if kind == langgraph_streaming.LANGGRAPH_STREAM_DEBUG
    ]
    assert result.output == {"answer": 42}
    assert [payload["category"] for payload in debug_payloads] == [
        "stream_part_normalization_failed",
        "stream_part_unknown_mode",
    ]


def test_stream_forwards_config_context_modes_and_returns_canonical_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[str] = []
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, _payload, *, flush=False: published.append(kind),
    )
    graph = FakeStreamGraph()
    runner = KitaruGraphRunner(
        graph,
        context_factory=lambda _request: {"tenant": "acme"},
    )

    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="thread-1"),
        stream_mode="updates",
        subgraphs=True,
    )

    assert result.status == "completed"
    assert result.output == {"answer": 42}
    input_seen, config_seen, kwargs_seen = graph.calls[0]
    assert input_seen == {"prompt": "hi"}
    assert config_seen["configurable"]["thread_id"] == "thread-1"
    assert kwargs_seen["context"] == {"tenant": "acme"}
    assert kwargs_seen["stream_mode"] == ["updates", "values"]
    assert kwargs_seen["version"] == "v2"
    assert kwargs_seen["subgraphs"] is True
    assert LANGGRAPH_STREAM_UPDATES in published


@pytest.mark.anyio
async def test_astream_matches_sync_stream_behavior() -> None:
    graph = FakeStreamGraph()
    runner = KitaruGraphRunner(graph)

    result = await runner.astream(
        {"prompt": "hi"},
        thread_id="async-thread",
        stream_mode="updates",
    )

    assert result.status == "completed"
    assert result.output == {"answer": 43}
    assert graph.calls[0][2]["stream_mode"] == ["updates", "values"]


def test_stream_rejects_calls_strategy_before_raw_thread_validation() -> None:
    graph = FakeStreamGraph()
    runner = KitaruGraphRunner(graph, checkpoint_strategy="calls")

    with pytest.raises(KitaruUsageError, match="graph_call"):
        runner.stream(cast(Any, {"prompt": "hi"}))

    assert graph.calls == []


@pytest.mark.anyio
async def test_astream_rejects_calls_strategy_before_raw_thread_validation() -> None:
    graph = FakeStreamGraph()
    runner = KitaruGraphRunner(graph, checkpoint_strategy="calls")

    with pytest.raises(KitaruUsageError, match="graph_call"):
        await runner.astream(cast(Any, {"prompt": "hi"}))

    assert graph.calls == []


def test_stream_cache_identity_is_distinct_and_cache_hits_do_not_republish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    cache: dict[str | None, Any] = {}
    graph = FakeStreamGraph()
    published: list[str] = []

    def fake_run_sync_in_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in cache:
            cache[cache_key] = kwargs["body"]()
        return cache[cache_key]

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, _payload, *, flush=False: published.append(kind),
    )

    runner = KitaruGraphRunner(
        graph,
        run_checkpoint_config={"cache": True},
    )
    request = LangGraphRunRequest.start({"prompt": "hi"}, thread_id="cache-thread")
    options = langgraph_streaming.resolve_stream_options(
        "updates", policy=runner.stream_policy, subgraphs=False
    )

    invoke_key = runner._graph_call_cache_key(request)
    stream_key = runner._graph_call_cache_key(
        request,
        surface="stream",
        stream_identity=runner._stream_cache_identity(options, method_name="stream"),
    )
    astream_key = runner._graph_call_cache_key(
        request,
        surface="stream",
        stream_identity=runner._stream_cache_identity(options, method_name="astream"),
    )
    raw_policy_runner = KitaruGraphRunner(
        FakeStreamGraph(),
        stream_policy=LangGraphStreamPolicy(include_raw_payloads=True),
    )
    raw_policy_options = langgraph_streaming.resolve_stream_options(
        "updates", policy=raw_policy_runner.stream_policy, subgraphs=False
    )
    raw_policy_stream_key = raw_policy_runner._graph_call_cache_key(
        request,
        surface="stream",
        stream_identity=raw_policy_runner._stream_cache_identity(
            raw_policy_options,
            method_name="stream",
        ),
    )

    assert invoke_key != stream_key
    assert stream_key != astream_key
    assert stream_key != raw_policy_stream_key

    first = runner.stream(request, stream_mode="updates")
    first_publish_count = len(published)
    second = runner.stream(request, stream_mode="updates")

    assert first.output == second.output == {"answer": 42}
    assert len(graph.calls) == 1
    assert first_publish_count == 3
    assert len(published) == first_publish_count


def test_stream_tracker_metadata_is_compact_and_not_per_chunk_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    saved: list[tuple[str, object, str]] = []

    def fake_save(name: str, value: object, *, type: str) -> None:
        saved.append((name, value, type))

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking.kitaru, "save", fake_save)
    monkeypatch.setattr(tracking.kitaru, "log", lambda **_kwargs: None)

    runner = KitaruGraphRunner(
        FakeStreamGraph(),
        capture=LangGraphCapturePolicy(save_state_snapshot=False),
    )
    result = runner.stream(
        LangGraphRunRequest.start({"prompt": "hi"}, thread_id="compact-thread"),
        stream_mode="updates",
    )

    event_log = cast(list[dict[str, object]], saved[0][1])
    run_summary = cast(dict[str, object], saved[1][1])

    started_metadata = cast(dict[str, object], event_log[0]["metadata"])

    assert result.status == "completed"
    assert [event["kind"] for event in event_log] == [
        "graph_call_started",
        "graph_call_completed",
    ]
    assert started_metadata["surface"] == "stream"
    assert started_metadata["stream_modes_requested"] == ["updates"]
    assert run_summary["surface"] == "stream"
    assert run_summary["stream_modes_upstream"] == ["updates", "values"]
    assert run_summary["stream_event_counts_by_mode"] == {"updates": 1, "values": 1}
    assert run_summary["stream_part_count"] == 2


def test_stream_strict_persistence_failure_publishes_terminal_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    published: list[str] = []

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, _payload, *, flush=False: published.append(kind),
    )
    monkeypatch.setattr(
        tracking.kitaru,
        "save",
        lambda _name, _value, *, type: (_ for _ in ()).throw(
            RuntimeError("artifact store unavailable")
        ),
    )
    monkeypatch.setattr(tracking.kitaru, "log", lambda **_kwargs: None)

    runner = KitaruGraphRunner(
        FakeStreamGraph(),
        capture=LangGraphCapturePolicy(fail_on_event_persistence_error=True),
    )

    with pytest.raises(KitaruRuntimeError, match="persistence failed"):
        runner.stream(
            LangGraphRunRequest.start({"prompt": "hi"}, thread_id="strict-thread"),
            stream_mode="updates",
        )

    assert published == [
        LANGGRAPH_STREAM_STARTED,
        LANGGRAPH_STREAM_UPDATES,
        LANGGRAPH_STREAM_FAILED,
    ]


def test_stream_failure_publishes_failed_and_persists_compact_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracking = importlib.import_module("kitaru.adapters.langgraph._tracking")
    published: list[str] = []
    saved: list[tuple[str, object, str]] = []

    class BrokenGraph(FakeStreamGraph):
        def stream(
            self, input: object, config: dict[str, Any], **kwargs: Any
        ) -> Iterator[dict[str, object]]:
            self.calls.append((input, config, kwargs))
            yield {"type": "updates", "ns": (), "data": {"node": {"x": 1}}}
            raise RuntimeError("stream exploded")

    monkeypatch.setattr(tracking, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(
        langgraph_streaming.kitaru_events,
        "publish",
        lambda kind, _payload, *, flush=False: published.append(kind),
    )
    monkeypatch.setattr(
        tracking.kitaru,
        "save",
        lambda name, value, *, type: saved.append((name, value, type)),
    )
    monkeypatch.setattr(tracking.kitaru, "log", lambda **_kwargs: None)

    runner = KitaruGraphRunner(BrokenGraph())

    with pytest.raises(RuntimeError, match="stream exploded"):
        runner.stream(
            LangGraphRunRequest.start({"prompt": "hi"}, thread_id="broken-thread"),
            stream_mode="updates",
        )

    assert published[-1] == LANGGRAPH_STREAM_FAILED
    event_log = cast(list[dict[str, object]], saved[0][1])
    run_summary = cast(dict[str, object], saved[1][1])
    assert [event["kind"] for event in event_log] == [
        "graph_call_started",
        "graph_call_failed",
    ]
    assert run_summary["status"] == "failed"
    assert run_summary["stream_event_counts_by_mode"] == {"updates": 1}
    assert run_summary["stream_part_count"] == 1
