"""Mastra export normalization and ingestion regression tests."""

import copy
import json
import os
import subprocess
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionCreateRequest, SessionResponse
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.cache_keys import compute_tool_cache_key
from kitaru.client.api_client import KitaruAPIClient
from kitaru.task.importer import ImportedSession, flatten_nodes, ingest_session
from kitaru_mastra_importer.importer import InvalidImport, parse

FIXTURE = Path(__file__).parent / "fixtures/mastra/1.51.0/traces.json"


@pytest.fixture
def traces() -> list[dict[str, Any]]:
    """Load genuine getTrace responses from two related agent invocations."""
    return json.loads(FIXTURE.read_bytes())


def _parse(value: Any, **params: Any) -> list[ImportedSession | ImportFailure]:
    return list(parse(json.dumps(value).encode(), params))


def _get_sessions(value: Any, **params: Any) -> list[ImportedSession]:
    results = _parse(value, **params)
    assert all(isinstance(item, ImportedSession) for item in results), results
    return [item for item in results if isinstance(item, ImportedSession)]


def test_preserves_invocations_and_graph_independent_of_export_order(
    traces: list[dict[str, Any]],
) -> None:
    expected = _get_sessions(traces)
    reordered = copy.deepcopy(traces[::-1])
    for trace in reordered:
        trace["spans"].reverse()
    actual = _get_sessions(reordered)

    assert actual == expected
    assert len(actual) == 2
    assert [session.external_id for session in actual] == [
        trace["traceId"] for trace in traces
    ]
    assert actual[0].started_at is not None
    assert actual[1].started_at is not None
    assert actual[0].started_at < actual[1].started_at
    assert {session.metadata["mastra"]["conversation_id"] for session in actual} == {
        "fixture-thread"
    }
    for session, trace in zip(actual, traces, strict=True):
        source = {span["spanId"]: span for span in trace["spans"]}
        nodes = flatten_nodes(session.nodes)
        assert len(nodes) == len(source)
        for node in nodes:
            assert node.trace_id == trace["traceId"]
            assert node.parent_external_id == source[node.external_id]["parentSpanId"]
        assert session.framework == "mastra"


def test_preserves_messages_and_raw_tool_input_and_result(
    traces: list[dict[str, Any]],
) -> None:
    first, second = _get_sessions(traces)
    assert first.inputs == [{"role": "user", "content": "Double 3."}]
    assert second.inputs == [
        {"role": "user", "content": "Double 3."},
        {"role": "assistant", "content": "The result is 6."},
        {"role": "user", "content": "What was the previous result?"},
    ]
    assert second.outputs == {"text": "The previous result was 6.", "files": []}
    assert second.input_text_selector == "/2/content"
    assert second.output_text_selector == "/text"
    assert sum(node.node_type == NodeType.LLM_CALL for node in first.nodes) == 2
    assert sum(node.node_type == NodeType.LLM_CALL for node in second.nodes) == 1
    tool = next(node for node in first.nodes if node.node_type == NodeType.TOOL_CALL)
    assert tool.tool_name == "double"
    # The tool executes a coerced number plus a default label; the export records
    # the original call arguments, which must not be replaced with guessed input.
    assert tool.inputs == {"value": "3"}
    assert tool.outputs == {"doubled": 6, "label": "defaulted"}
    for session, trace in zip((first, second), traces, strict=True):
        source = {span["spanId"]: span for span in trace["spans"]}
        for node in session.nodes:
            assert node.inputs == source[node.external_id].get("input")
            assert node.outputs == source[node.external_id].get("output")
        replay = session.metadata["mastra"]["replay"]
        assert replay["eligible"] is False
        assert any("#1050" in reason for reason in replay["reasons"])


def test_equal_time_steps_keep_the_recorded_step_sequence(
    traces: list[dict[str, Any]],
) -> None:
    steps = [span for span in traces[0]["spans"] if span["spanType"] == "model_step"]
    steps.sort(key=lambda span: span["attributes"]["stepIndex"])
    steps[1]["startedAt"] = steps[0]["startedAt"]
    ids = {steps[0]["spanId"]: "z-first", steps[1]["spanId"]: "a-second"}
    for span in traces[0]["spans"]:
        span["spanId"] = ids.get(span["spanId"], span["spanId"])
        span["parentSpanId"] = ids.get(span["parentSpanId"], span["parentSpanId"])
    session = _get_sessions(traces[0])[0]
    assert [
        node.external_id for node in session.nodes if node.external_id in ids.values()
    ] == ["z-first", "a-second"]


def test_counts_generation_usage_once_and_preserves_source_breakdowns(
    traces: list[dict[str, Any]],
) -> None:
    generation = next(
        span for span in traces[0]["spans"] if span["spanType"] == "model_generation"
    )
    generation["attributes"]["costContext"] = {
        "estimatedCost": "0.0042",
        "costUnit": "USD",
    }
    session = _get_sessions(traces[0])[0]
    assert (
        sum(node.tokens.input_tokens or 0 for node in session.nodes if node.tokens)
        == 41
    )
    assert (
        sum(node.tokens.output_tokens or 0 for node in session.nodes if node.tokens)
        == 11
    )
    assert sum(node.cost or Decimal(0) for node in session.nodes) == Decimal("0.0042")
    step = next(
        node
        for node in session.nodes
        if node.metadata["mastra"]["spanType"] == "model_step"
    )
    assert step.tokens is None
    assert step.attributes["usage"]["inputTokens"] == 20


def test_namespace_changes_identity_without_changing_source_trace(
    traces: list[dict[str, Any]],
) -> None:
    original = _get_sessions(traces)[0]
    namespaced = _get_sessions(traces, source_namespace="deployment-a")[0]
    assert namespaced == _get_sessions(traces, source_namespace="deployment-a")[0]
    assert (
        len(
            {
                original.external_id,
                namespaced.external_id,
                _get_sessions(traces, source_namespace="deployment-b")[0].external_id,
            }
        )
        == 3
    )
    assert namespaced.metadata["mastra"]["trace_id"] == original.external_id
    assert namespaced.nodes == original.nodes


async def test_reimport_ingests_nodes_into_the_existing_session(
    traces: list[dict[str, Any]],
) -> None:
    client = Mock(spec=KitaruAPIClient)
    stored: dict[tuple[str | None, str | None], SessionResponse] = {}

    async def create(request: SessionCreateRequest) -> SessionResponse:
        key = (request.imported_from, request.external_id)
        if key in stored:
            return stored[key]
        session = Mock(spec=SessionResponse)
        session.id = uuid4()
        stored[key] = session
        return session

    client.sessions = Mock()
    client.sessions.create = AsyncMock(side_effect=create)
    client.sessions.ingest_nodes = AsyncMock()
    agent_id = uuid4()
    first = [
        await ingest_session(client, s, agent_id, "mastra")
        for s in _get_sessions(traces)
    ]
    second = [
        await ingest_session(client, s, agent_id, "mastra")
        for s in _get_sessions(traces)
    ]
    assert [session.id for session in second] == [session.id for session in first]
    assert len(stored) == 2
    assert client.sessions.ingest_nodes.await_count == 4
    batches = client.sessions.ingest_nodes.await_args_list
    assert [call.args[0] for call in batches] == [
        first[0].id,
        first[1].id,
        first[0].id,
        first[1].id,
    ]
    assert [len(call.args[1].nodes) for call in batches] == [10, 5, 10, 5]


@pytest.mark.parametrize(
    "problem",
    [
        "missing_trace",
        "missing_span",
        "duplicate_span",
        "missing_parent",
        "cycle",
        "bad_usage",
        "bad_cost",
        "invalid_cost",
    ],
)
def test_bad_trace_is_isolated_from_valid_invocation(
    traces: list[dict[str, Any]],
    problem: str,
) -> None:
    bad = traces[0]
    spans = bad["spans"]
    if problem == "missing_trace":
        del bad["traceId"]
    elif problem == "missing_span":
        del spans[0]["spanId"]
    elif problem == "duplicate_span":
        spans.append(copy.deepcopy(spans[0]))
    elif problem == "missing_parent":
        spans[1]["parentSpanId"] = "not-exported"
    elif problem == "cycle":
        spans[1]["parentSpanId"] = spans[2]["spanId"]
        spans[2]["parentSpanId"] = spans[1]["spanId"]
    elif problem == "bad_usage":
        spans[1]["attributes"]["usage"]["inputTokens"] = -1
    elif problem == "bad_cost":
        spans[1]["attributes"]["costContext"] = {
            "estimatedCost": "NaN",
            "costUnit": "USD",
        }
    elif problem == "invalid_cost":
        spans[1]["attributes"]["costContext"] = {
            "estimatedCost": "garbage",
            "costUnit": "USD",
        }
    results = _parse(traces)
    assert len(results) == 2
    failure, valid = results
    assert isinstance(failure, ImportFailure)
    assert failure.line == 1
    assert failure.error
    assert isinstance(valid, ImportedSession)
    assert valid.external_id == traces[1]["traceId"]


def test_duplicate_trace_is_skipped_but_conflicting_copy_fails(
    traces: list[dict[str, Any]],
) -> None:
    original = traces[0]
    assert len(_get_sessions([original, original])) == 1
    conflicting = copy.deepcopy(original)
    conflicting["spans"][0]["output"] = "Different answer"
    results = _parse([original, conflicting, traces[1]])
    failures = [item for item in results if isinstance(item, ImportFailure)]
    assert len(failures) == 1
    assert "conflicting duplicate traceId" in failures[0].error
    assert [
        item.external_id for item in results if isinstance(item, ImportedSession)
    ] == [traces[1]["traceId"]]


@pytest.mark.parametrize(
    "params", [{"unknown": True}, {"source_namespace": ""}, {"source_namespace": 1}]
)
def test_rejects_unsupported_parameters(params: dict[str, Any]) -> None:
    with pytest.raises(InvalidImport):
        list(parse(b"{}", params))


@pytest.mark.parametrize("payload", [b"[]", b"not json", b"\xff"])
def test_rejects_invalid_envelopes(payload: bytes) -> None:
    with pytest.raises(InvalidImport):
        list(parse(payload, {}))


def test_instant_events_are_complete_and_do_not_make_trace_unfinished(
    traces: list[dict[str, Any]],
) -> None:
    session = _get_sessions(traces[0])[0]
    event = next(node for node in session.nodes if node.metadata["mastra"]["isEvent"])
    assert event.ended_at is None
    assert event.status == NodeStatus.COMPLETED
    assert not any(
        "unfinished" in reason
        for reason in session.metadata["mastra"]["replay"]["reasons"]
    )


def test_empty_response_model_falls_back_to_requested_model(
    traces: list[dict[str, Any]],
) -> None:
    session = _get_sessions(traces[0])[0]
    generation = next(
        node
        for node in session.nodes
        if node.metadata["mastra"]["spanType"] == "model_generation"
    )
    assert generation.attributes["responseModel"] == ""
    assert generation.model == "fixture-model"
    assert generation.requested_model == "fixture-model"


@pytest.mark.parametrize("remove_step_usage", [False, True])
def test_missing_generation_usage_preserves_lower_level_totals_once(
    traces: list[dict[str, Any]],
    remove_step_usage: bool,
) -> None:
    for span in traces[0]["spans"]:
        if span["spanType"] == "model_generation" or (
            remove_step_usage and span["spanType"] == "model_step"
        ):
            span["attributes"].pop("usage", None)
    session = _get_sessions(traces[0])[0]
    usages = [node.tokens for node in session.nodes if node.tokens is not None]
    assert sum(usage.input_tokens or 0 for usage in usages) == 41
    assert sum(usage.output_tokens or 0 for usage in usages) == 11


def test_generation_nested_under_tool_counts_as_independent_model_work(
    traces: list[dict[str, Any]],
) -> None:
    spans = traces[0]["spans"]
    outer = next(span for span in spans if span["spanType"] == "model_generation")
    outer["attributes"]["costContext"] = {"estimatedCost": "0.0042", "costUnit": "USD"}
    nested = copy.deepcopy(outer)
    nested["spanId"] = "nested-generation"
    nested["parentSpanId"] = next(
        span["spanId"] for span in spans if span["spanType"] == "tool_call"
    )
    nested["attributes"]["usage"] = {"inputTokens": 7, "outputTokens": 2}
    nested["attributes"]["costContext"] = {"estimatedCost": "0.0007", "costUnit": "USD"}
    spans.append(nested)
    session = _get_sessions(traces[0])[0]
    usages = [node.tokens for node in session.nodes if node.tokens is not None]
    assert sum(usage.input_tokens or 0 for usage in usages) == 48
    assert sum(usage.output_tokens or 0 for usage in usages) == 13
    assert sum(node.cost or Decimal(0) for node in session.nodes) == Decimal("0.0049")


def test_preserves_initial_model_context_and_reports_when_it_is_missing(
    traces: list[dict[str, Any]],
) -> None:
    session = _get_sessions(traces[1])[0]
    replay = session.metadata["mastra"]["replay"]
    context = next(
        node for node in session.nodes if node.external_id == replay["context_span_id"]
    )
    assert replay["context_input_pointer"] == "/messages"
    assert [message["role"] for message in context.inputs["messages"]] == [
        "system",
        "user",
        "assistant",
        "user",
    ]
    assert context.system_prompt_selector == "/messages/0/content"
    assert context.input_text_selector == "/messages/3/content/0/text"
    source_context = next(
        span for span in traces[1]["spans"] if span["spanId"] == context.external_id
    )
    source_context.pop("input")
    incomplete = _get_sessions(traces[1])[0].metadata["mastra"]["replay"]
    assert incomplete["eligible"] is False
    assert incomplete["context_span_id"] is None
    assert any(
        "full model-message context" in reason for reason in incomplete["reasons"]
    )


def test_preserves_explicit_reasoning_and_partial_usage(
    traces: list[dict[str, Any]],
) -> None:
    generation = next(
        span for span in traces[0]["spans"] if span["spanType"] == "model_generation"
    )
    generation["output"]["reasoning"] = [
        {"type": "reasoning", "text": "Use the recorded arithmetic result."}
    ]
    generation["attributes"]["usage"].pop("outputTokens")
    session = _get_sessions(traces[0])[0]
    node = next(
        node for node in session.nodes if node.external_id == generation["spanId"]
    )
    assert node.reasoning == "Use the recorded arithmetic result."
    assert (
        sum(node.tokens.input_tokens or 0 for node in session.nodes if node.tokens)
        == 41
    )
    assert (
        sum(node.tokens.output_tokens or 0 for node in session.nodes if node.tokens)
        == 11
    )


def test_utf16_is_not_silently_accepted_as_utf8() -> None:
    with pytest.raises(InvalidImport, match="UTF-8"):
        list(parse("{}".encode("utf-16"), {}))


def test_token_counts_that_overflow_database_totals_are_contained(
    traces: list[dict[str, Any]],
) -> None:
    generation = next(
        span for span in traces[0]["spans"] if span["spanType"] == "model_generation"
    )
    generation["attributes"]["usage"]["inputTokens"] = 2**63
    results = _parse(traces)
    assert isinstance(results[0], ImportFailure)
    assert "64-bit" in results[0].error
    assert isinstance(results[1], ImportedSession)


def test_unserializable_source_text_is_contained(
    traces: list[dict[str, Any]],
) -> None:
    traces[0]["spans"][0]["input"] = "\ud800"
    results = _parse(traces)
    assert isinstance(results[0], ImportFailure)
    results[0].model_dump_json()
    assert isinstance(results[1], ImportedSession)


@pytest.mark.skipif(
    os.environ.get("KITARU_MASTRA_REPLAY_TEST") != "1",
    reason="Replay probe requires built TypeScript packages and Node 22",
)
def test_imported_messages_and_tool_history_replay_through_context_adapter(
    traces: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Exercise real Mastra and the compiled adapter with imported Python values."""
    sessions = _get_sessions(traces, replay_context="history-only")
    history = [
        {
            "tool_name": node.tool_name,
            "inputs": node.inputs,
            "cache_key": compute_tool_cache_key(node.tool_name, node.inputs),
            "result": node.outputs,
            "status": node.status.value,
        }
        for node in sessions[0].nodes
        if node.node_type == NodeType.TOOL_CALL and node.tool_name is not None
    ]
    payload = tmp_path / "normalized.json"
    payload.write_text(
        json.dumps(
            {
                "sessions": [session.model_dump(mode="json") for session in sessions],
                "history": history,
            }
        )
    )
    result = subprocess.run(
        ["node", str(FIXTURE.parent / "replay.mjs"), str(payload)],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    receipt = json.loads(result.stdout)
    assert receipt["replayedSessions"] == 2
    assert receipt["historyLookups"] == 1
    assert receipt["liveToolExecutions"] == 0
    assert receipt["priorHistoryVerified"] is True
    assert receipt["memoryOptionsRemoved"] is True
    assert receipt["fullSnapshotRestored"] is True
    assert receipt["recordedSystemRestored"] is True


def test_declared_history_context_preserves_supplied_and_snapshot_messages(
    traces: list[dict[str, Any]],
) -> None:
    sessions = _get_sessions(traces, replay_context="history-only")
    for session, trace in zip(sessions, traces, strict=True):
        root = next(span for span in trace["spans"] if span["parentSpanId"] is None)
        generation = next(
            span for span in trace["spans"] if span["spanType"] == "model_generation"
        )
        assert session.inputs == {
            "mastra_conversation_context": {
                "version": 1,
                "source": "recalled",
                "complete": True,
                "messages": generation["input"]["messages"],
            },
            "supplied_messages": root["input"],
        }
        replay = session.metadata["mastra"]["replay"]
        assert replay["eligible"] is True
        assert replay["history_only_declared"] is True
        assert replay["reasons"] == []
        assert replay["context_span_id"] == generation["spanId"]
        assert replay["context_input_pointer"] == "/messages"
        generation_node = next(
            node for node in session.nodes if node.external_id == generation["spanId"]
        )
        assert generation_node.inputs == generation["input"]
        assert generation_node.system_prompt_selector == "/messages/0/content"
        assert session.outputs == root["output"]
    assert sessions[0].input_text_selector == "/supplied_messages/0/content"
    assert sessions[1].input_text_selector == "/supplied_messages/2/content"
    # The second snapshot retains the earlier answer as well as the new message.
    snapshot = sessions[1].inputs["mastra_conversation_context"]["messages"]
    assert [message["role"] for message in snapshot] == [
        "system",
        "user",
        "assistant",
        "user",
    ]


@pytest.mark.parametrize(
    ("problem", "reason_fragment"),
    [
        ("missing_messages", "initial full model-message context"),
        ("empty_messages", "initial full model-message context"),
        ("unsupported_role", "unsupported message records"),
        ("unfinished_generation", "unfinished spans"),
        ("missing_invocation_input", "omits invocation input"),
        ("missing_thread", "memory-dependent invocation"),
    ],
)
def test_declared_incomplete_context_records_adapter_rejection_reason(
    traces: list[dict[str, Any]],
    problem: str,
    reason_fragment: str,
) -> None:
    trace = traces[0]
    root = next(span for span in trace["spans"] if span["parentSpanId"] is None)
    generation = next(
        span for span in trace["spans"] if span["spanType"] == "model_generation"
    )
    if problem == "missing_messages":
        del generation["input"]["messages"]
    elif problem == "empty_messages":
        generation["input"]["messages"] = []
    elif problem == "unsupported_role":
        generation["input"]["messages"][0]["role"] = "unknown"
    elif problem == "unfinished_generation":
        generation["endedAt"] = None
    elif problem == "missing_invocation_input":
        del root["input"]
    elif problem == "missing_thread":
        root.pop("threadId", None)
        root["metadata"].pop("threadId", None)
        root["attributes"].pop("conversationId", None)
    session = _get_sessions(trace, replay_context="history-only")[0]
    context = session.inputs["mastra_conversation_context"]
    assert context["version"] == 1
    assert context["source"] == "recalled"
    assert context["complete"] is False
    assert reason_fragment in context["reason"]
    assert session.inputs["supplied_messages"] == root.get("input")
    assert session.metadata["mastra"]["replay"]["eligible"] is False
    assert reason_fragment in "; ".join(session.metadata["mastra"]["replay"]["reasons"])
    if problem in {"missing_messages", "empty_messages"}:
        assert context["messages"] == []
    else:
        assert context["messages"] == generation["input"]["messages"]


@pytest.mark.parametrize("mode", ["live-memory", "", False, 1])
def test_rejects_unrecognized_replay_context_mode(mode: Any) -> None:
    with pytest.raises(InvalidImport, match="replay_context"):
        list(parse(b"{}", {"replay_context": mode}))


def test_live_history_export_preserves_recalled_context_and_tool_key() -> None:
    """Normalize the real-provider fixture without making provider requests."""
    trace = json.loads((FIXTURE.parent / "live/trace.json").read_bytes())
    session = _get_sessions(trace, replay_context="history-only")[0]
    context = session.inputs["mastra_conversation_context"]
    assert context["complete"] is True
    assert session.metadata["mastra"]["replay"]["eligible"] is True
    assert "ORBIT-47" not in json.dumps(session.inputs["supplied_messages"])
    assert "ORBIT-47" in json.dumps(context["messages"])
    assert len(session.nodes) == 14
    assert sum(node.node_type == NodeType.LLM_CALL for node in session.nodes) == 2
    tool = next(node for node in session.nodes if node.node_type == NodeType.TOOL_CALL)
    assert tool.inputs == {"value": 3}
    assert tool.outputs == {"doubled": 6}
    assert tool.tool_name == "double"
    assert session.outputs["text"] == (
        "Your secret code is ORBIT-47, and double 3 is 6."
    )
