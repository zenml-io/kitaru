#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Arize Phoenix trace importer plugin tests."""

import json
from decimal import Decimal
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_phoenix_importer.importer import InvalidImport, PhoenixTraceImporter


def span(
    span_id: str,
    *,
    trace_id: str = "trace-1",
    parent_id: str | None = None,
    span_kind: str = "UNKNOWN",
    start_time: str | None = "2026-08-20T06:30:00Z",
    attributes: dict[str, Any] | None = None,
    status_code: str = "UNSET",
    status_message: str = "",
    **extra: Any,
) -> dict[str, Any]:
    """Build one Phoenix span export row."""
    return {
        "attributes": attributes or {},
        "context": {"trace_id": trace_id, "span_id": span_id},
        "end_time": "2026-08-20T06:30:01Z",
        "events": [],
        "id": span_id,
        "name": span_id,
        "parent_id": parent_id,
        "span_kind": span_kind,
        "start_time": start_time,
        "status_code": status_code,
        "status_message": status_message,
        **extra,
    }


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def parse(content: bytes) -> list[ImportedSession | ImportFailure]:
    """Parse one test payload."""
    return list(PhoenixTraceImporter().parse(content, {}))


def flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def test_parses_ui_jsonl_and_reconstructs_out_of_order_graph() -> None:
    """Map UI JSONL spans and rebuild parent relationships independent of order."""
    content = jsonl(
        span(
            "tool",
            parent_id="root",
            span_kind="TOOL",
            start_time="2026-08-20T06:30:00.200000Z",
            attributes={
                "gen_ai.tool.name": "get_weather",
                "gen_ai.tool.call.arguments": {"city": "Paris"},
                "gen_ai.tool.call.result": {"temperature": 21},
            },
        ),
        span(
            "llm",
            parent_id="root",
            span_kind="LLM",
            start_time="2026-08-20T06:30:00.100000Z",
            attributes={
                "gen_ai.input.messages": [{"role": "user", "content": "Weather?"}],
                "gen_ai.output.messages": [
                    {"role": "assistant", "content": "Checking"}
                ],
                "gen_ai.provider.name": "anthropic",
                "gen_ai.request.model": "claude-haiku-4-5",
                "gen_ai.response.model": "claude-haiku-4-5-20251001",
                "gen_ai.usage.input_tokens": 42,
                "gen_ai.usage.output_tokens": 8,
                "operation.cost": 0.00012,
                "model_request_parameters": {"temperature": 0},
            },
        ),
        span(
            "root",
            span_kind="AGENT",
            attributes={
                "input.value": "Weather in Paris?",
                "output.value": "It is 21 C.",
                "pydantic_ai.all_messages": [],
            },
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.external_id == "trace-1"
    assert session.status is SessionStatus.COMPLETED
    assert session.inputs == "Weather in Paris?"
    assert session.outputs == "It is 21 C."
    assert session.framework == "pydantic-ai"
    assert session.metadata["phoenix.trace_id"] == "trace-1"
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    assert [node.external_id for node in session.nodes] == ["trace-1:root"]
    assert {node.external_id for node in nodes["trace-1:root"].children} == {
        "trace-1:llm",
        "trace-1:tool",
    }
    llm = nodes["trace-1:llm"]
    assert llm.node_type is NodeType.LLM_CALL
    assert llm.requested_model == "claude-haiku-4-5"
    assert llm.model == "claude-haiku-4-5-20251001"
    assert llm.model_provider == "anthropic"
    assert llm.tokens is not None
    assert llm.tokens.input_tokens == 42
    assert llm.tokens.output_tokens == 8
    assert llm.cost == Decimal("0.00012")
    assert llm.model_params == {"temperature": 0}
    tool = nodes["trace-1:tool"]
    assert tool.node_type is NodeType.TOOL_CALL
    assert tool.tool_name == "get_weather"
    assert tool.inputs == {"city": "Paris"}
    assert tool.outputs == {"temperature": 21}


def test_maps_native_openinference_llm_attributes() -> None:
    """Map Phoenix's native OpenInference model, usage, and cost attributes."""
    content = jsonl(
        span(
            "llm",
            span_kind="LLM",
            attributes={
                "llm.input_messages.0.message.role": "user",
                "llm.input_messages.0.message.content": "Hello",
                "llm.output_messages.0.message.role": "assistant",
                "llm.output_messages.0.message.content": "Hi",
                "llm.invocation_parameters": '{"temperature": 0.2}',
                "gen_ai.request.model": "gemini-flash",
                "llm.model_name": "gemini-3.5-flash-lite",
                "llm.provider": "google",
                "llm.system": "vertexai",
                "llm.token_count.prompt": 12,
                "llm.token_count.completion": 5,
                "llm.token_count.prompt_details.cache_read": 4,
                "llm.token_count.completion_details.reasoning": 2,
                "llm.cost.total": 0.0003,
            },
        )
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    [node] = session.nodes
    assert node.requested_model == "gemini-flash"
    assert node.model == "gemini-3.5-flash-lite"
    assert node.model_provider == "google"
    assert node.inputs == [{"message.role": "user", "message.content": "Hello"}]
    assert node.outputs == [{"message.role": "assistant", "message.content": "Hi"}]
    assert node.tokens is not None
    assert node.tokens.input_tokens == 12
    assert node.tokens.output_tokens == 5
    assert node.tokens.cached_input_tokens == 4
    assert node.tokens.reasoning_tokens == 2
    assert node.cost == Decimal("0.0003")
    assert node.model_params == {"temperature": 0.2}


def test_parses_cli_trace_envelopes_and_preserves_annotations() -> None:
    """Accept CLI trace objects as JSON arrays and retain trace annotations."""
    payload = json.dumps(
        [
            {
                "traceId": "trace-1",
                "spans": [span("root")],
                "rootSpan": span("root"),
                "annotations": [{"name": "quality", "score": 0.9}],
                "notes": ["reviewed"],
            },
            {"traceId": "trace-2", "spans": [span("root-2", trace_id="trace-2")]},
        ]
    ).encode()

    sessions = parse(payload)

    assert [
        item.external_id for item in sessions if isinstance(item, ImportedSession)
    ] == [
        "trace-1",
        "trace-2",
    ]
    first = sessions[0]
    assert isinstance(first, ImportedSession)
    assert first.metadata["phoenix.annotations"] == [{"name": "quality", "score": 0.9}]
    assert first.metadata["phoenix.notes"] == ["reviewed"]
    assert len(first.nodes) == 1


def test_emits_sessions_in_first_appearance_order() -> None:
    """Follow payload order, not trace id order, when emitting sessions."""
    content = jsonl(
        span("root-b", trace_id="trace-b"),
        span("root-a", trace_id="trace-a"),
    )

    sessions = parse(content)

    assert [
        item.external_id for item in sessions if isinstance(item, ImportedSession)
    ] == ["trace-b", "trace-a"]


def test_merges_metadata_when_a_trace_spans_cli_envelopes() -> None:
    """Retain metadata from every envelope contributing to one trace."""
    payload = jsonl(
        {
            "traceId": "trace-1",
            "spans": [span("root")],
            "annotations": [{"name": "quality", "score": 0.9}],
        },
        {
            "traceId": "trace-1",
            "spans": [span("child", parent_id="root")],
            "annotations": [{"name": "safety", "score": 1.0}],
            "notes": ["reviewed"],
        },
    )

    [session] = parse(payload)

    assert isinstance(session, ImportedSession)
    assert session.metadata["phoenix.annotations"] == [
        {"name": "quality", "score": 0.9},
        {"name": "safety", "score": 1.0},
    ]
    assert session.metadata["phoenix.notes"] == ["reviewed"]


def test_uses_root_status_after_recovered_child_failure() -> None:
    """Keep the session completed when a failed child was recovered."""
    content = jsonl(
        span("root", span_kind="AGENT", status_code="OK"),
        span(
            "tool",
            parent_id="root",
            span_kind="TOOL",
            status_code="ERROR",
            status_message="rate limited",
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.status is SessionStatus.COMPLETED
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    assert nodes["trace-1:tool"].status is NodeStatus.FAILED
    assert nodes["trace-1:tool"].error == "rate limited"


def test_maps_failed_root_and_exception_event() -> None:
    """Use Phoenix exception events when the root has no status message."""
    failed = span("root", status_code="ERROR")
    failed["events"] = [
        {
            "name": "exception",
            "timestamp": "2026-08-20T06:30:00.5Z",
            "attributes": {"exception.message": "upstream unavailable"},
        }
    ]
    failed["attributes"] = {"custom.nested": {"kept": True}}
    failed["annotations"] = [{"name": "quality", "score": 0.1}]
    failed["notes"] = ["needs review"]

    [session] = parse(jsonl(failed))

    assert isinstance(session, ImportedSession)
    assert session.status is SessionStatus.FAILED
    assert session.error == "upstream unavailable"
    assert session.nodes[0].error == "upstream unavailable"
    assert session.nodes[0].attributes["phoenix.events"] == failed["events"]
    assert session.nodes[0].attributes["phoenix.attributes"] == failed["attributes"]
    assert session.nodes[0].attributes["phoenix.annotations"] == failed["annotations"]
    assert session.nodes[0].attributes["phoenix.notes"] == failed["notes"]


def test_keeps_missing_parents_as_roots_with_warning() -> None:
    """Import partial span selections without inventing a parent."""
    [session] = parse(jsonl(span("child", parent_id="not-exported")))

    assert isinstance(session, ImportedSession)
    assert session.nodes[0].external_id == "trace-1:child"
    assert "references missing parent" in session.metadata["normalization_warnings"][0]


def test_prefers_true_root_over_earlier_orphan() -> None:
    """Use the null-parent span for session fields in a partial export."""
    content = jsonl(
        span(
            "orphan",
            parent_id="not-exported",
            start_time=None,
            status_code="OK",
        ),
        span(
            "root",
            status_code="ERROR",
            status_message="boom",
            start_time="2026-08-20T06:30:01Z",
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.name == "root"
    assert session.status is SessionStatus.FAILED
    assert session.error == "boom"


def test_isolates_invalid_trace_graphs() -> None:
    """Report one invalid trace without discarding another valid trace."""
    payload = jsonl(
        span("root", trace_id="valid"),
        span("a", trace_id="broken", parent_id="b"),
        span("b", trace_id="broken", parent_id="a"),
    )

    parsed = parse(payload)

    assert any(
        isinstance(item, ImportedSession) and item.external_id == "valid"
        for item in parsed
    )
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert failure.external_id == "broken"
    assert failure.error == "The imported span graph contains a parent cycle"
    assert failure.line == 2


def test_isolates_duplicate_span_ids() -> None:
    """Reject one trace with duplicate span ids while importing another trace."""
    parsed = parse(
        jsonl(
            span("duplicate", trace_id="broken"),
            span("duplicate", trace_id="broken"),
            span("root", trace_id="valid"),
        )
    )

    assert any(
        isinstance(item, ImportedSession) and item.external_id == "valid"
        for item in parsed
    )
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert failure.external_id == "broken"
    assert failure.error == "The import contains duplicate span ids"


def test_isolates_a_malformed_jsonl_line() -> None:
    """Import valid traces around a malformed JSONL record."""
    payload = b"\n".join(
        (
            json.dumps(span("first", trace_id="first")).encode(),
            b'{"truncated":',
            json.dumps(span("second", trace_id="second")).encode(),
        )
    )

    parsed = parse(payload)

    assert {
        item.external_id for item in parsed if isinstance(item, ImportedSession)
    } == {"first", "second"}
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert failure.line == 2
    assert failure.error == "Line 2 is not valid JSON"


def test_accepts_a_utf8_bom() -> None:
    """Accept JSONL re-saved by editors that add a UTF-8 BOM."""
    [session] = parse(b"\xef\xbb\xbf" + jsonl(span("root")))

    assert isinstance(session, ImportedSession)


def test_prefers_structured_llm_messages_to_output_value() -> None:
    """Keep the assistant message instead of an opaque output stub."""
    [session] = parse(
        jsonl(
            span(
                "llm",
                span_kind="LLM",
                attributes={
                    "output.value": '{"id":"message-1","model":"model-1"}',
                    "gen_ai.output.messages": (
                        '[{"role":"assistant","parts":[{"type":"text",'
                        '"content":"Useful answer"}]}]'
                    ),
                },
            )
        )
    )

    assert isinstance(session, ImportedSession)
    assert session.nodes[0].outputs == [
        {
            "role": "assistant",
            "parts": [{"type": "text", "content": "Useful answer"}],
        }
    ]


def test_does_not_use_tool_schema_as_call_arguments() -> None:
    """Do not mistake OpenInference tool parameters for invocation input."""
    [session] = parse(
        jsonl(
            span(
                "tool",
                span_kind="TOOL",
                attributes={
                    "tool.name": "lookup",
                    "tool.parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                    },
                },
            )
        )
    )

    assert isinstance(session, ImportedSession)
    assert session.nodes[0].inputs is None


def test_maps_legacy_genai_token_names() -> None:
    """Accept the older OpenTelemetry GenAI token attribute names."""
    [session] = parse(
        jsonl(
            span(
                "llm",
                span_kind="LLM",
                attributes={
                    "gen_ai.usage.prompt_tokens": 10,
                    "gen_ai.usage.completion_tokens": 4,
                },
            )
        )
    )

    assert isinstance(session, ImportedSession)
    assert session.nodes[0].tokens is not None
    assert session.nodes[0].tokens.input_tokens == 10
    assert session.nodes[0].tokens.output_tokens == 4


@pytest.mark.parametrize(
    ("malformed", "message"),
    [
        ({"traceId": "broken", "spans": "not-a-list"}, "non-object spans"),
        ({"traceId": "broken", "spans": [1]}, "non-object spans"),
        ({"spans": [{}]}, "lacks a trace id"),
    ],
)
def test_isolates_malformed_cli_envelopes(
    malformed: dict[str, Any], message: str
) -> None:
    """Report malformed CLI envelopes without discarding valid envelopes."""
    payload = json.dumps(
        [
            malformed,
            {"traceId": "valid", "spans": [span("root", trace_id="valid")]},
        ]
    ).encode()

    parsed = parse(payload)

    assert any(
        isinstance(item, ImportedSession) and item.external_id == "valid"
        for item in parsed
    )
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert message in failure.error


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "Import file contains no JSON records"),
        (b"not json", "Line 1 is not valid JSON"),
        (b"\xff", "Import file must be UTF-8 JSON or JSONL"),
    ],
)
def test_rejects_invalid_files(payload: bytes, message: str) -> None:
    """Reject invalid payloads with actionable errors."""
    with pytest.raises(InvalidImport, match=message):
        parse(payload)


@pytest.mark.parametrize("value", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity", -1])
def test_invalid_cost_isolates_session(value: Any) -> None:
    """Reject invalid costs without losing neighboring sessions."""
    records = []
    for trace_id in ("good-before", "bad", "good-after"):
        attributes = {"operation.cost": value if trace_id == "bad" else "0.25"}
        records.append(span("root", trace_id=trace_id, attributes=attributes))
    results = parse(jsonl(*records))
    sessions = [item for item in results if isinstance(item, ImportedSession)]
    failures = [item for item in results if isinstance(item, ImportFailure)]
    assert len(sessions) == 2
    assert len(failures) == 1
    assert "cost" in failures[0].error.lower()
    assert all(session.nodes[0].cost == Decimal("0.25") for session in sessions)
    for item in results:
        item.model_dump_json()


@pytest.mark.parametrize(
    "field",
    [
        "gen_ai.usage.input_tokens",
        "gen_ai.usage.output_tokens",
        "gen_ai.usage.details.cache_read_tokens",
        "gen_ai.usage.details.reasoning_tokens",
    ],
)
@pytest.mark.parametrize("value", [-1, -0.25, float("inf"), float("nan")])
def test_invalid_tokens_isolate_session(field: str, value: Any) -> None:
    """Reject negative or non-finite token counts at the session boundary."""
    records = []
    for trace_id in ("good-before", "bad", "good-after"):
        attributes = {field: value if trace_id == "bad" else 3}
        records.append(span("root", trace_id=trace_id, attributes=attributes))
    results = parse(jsonl(*records))
    assert sum(isinstance(item, ImportedSession) for item in results) == 2
    assert sum(isinstance(item, ImportFailure) for item in results) == 1


@pytest.mark.parametrize("depth", [63, 64, 65, 1200])
@pytest.mark.parametrize("reverse", [False, True])
def test_parent_depth_boundary_preserves_other_sessions(
    depth: int, reverse: bool
) -> None:
    """Accept 64 levels with intact parent links and contain deeper traces."""
    records = [
        span(str(index), parent_id=(str(index - 1) if index else None))
        for index in range(depth)
    ]
    if reverse:
        records.reverse()
    attributes = {"gen_ai.conversation.id": "healthy"}
    records.append(span("healthy", trace_id="healthy", attributes=attributes))
    results = parse(jsonl(*records))
    assert len(results) == 2
    if depth <= 64:
        sessions = [item for item in results if isinstance(item, ImportedSession)]
        assert len(sessions) == 2
        chain = next(item for item in sessions if len(flatten(item.nodes)) == depth)
        current = chain.nodes[0]
        for index in range(depth):
            assert current.external_id == f"trace-1:{index}"
            if index + 1 < depth:
                [current] = current.children
        chain.model_dump_json()
    else:
        [failure] = [item for item in results if isinstance(item, ImportFailure)]
        assert "64" in failure.error
        assert sum(isinstance(item, ImportedSession) for item in results) == 1


@pytest.mark.parametrize("location", ["trace_id", "name", "input", "attribute"])
@pytest.mark.parametrize("text", ["\ud800", "\udfff"])
def test_surrogate_failure_is_serializable(location: str, text: str) -> None:
    """Contain malformed Unicode while preserving valid Unicode neighbors."""
    attributes = {"gen_ai.conversation.id": "bad"}
    bad = span("bad", trace_id="bad", attributes=attributes)
    if location == "trace_id":
        bad["context"]["trace_id"] = text
    elif location == "name":
        bad["name"] = text
    elif location == "input":
        bad["attributes"]["input.value"] = {"nested": text}
    else:
        bad["attributes"][text] = text
    healthy = span(
        "healthy",
        trace_id="healthy",
        attributes={"gen_ai.conversation.id": "healthy", "input.value": "café 😀"},
    )
    results = parse(jsonl(bad, healthy))
    assert sum(isinstance(item, ImportedSession) for item in results) == 1
    assert sum(isinstance(item, ImportFailure) for item in results) == 1
    for item in results:
        item.model_dump_json()


def test_nested_attribute_json_decode_is_contained() -> None:
    """Contain decoder recursion inside a trace and retain a valid neighbor."""
    nested = "[" * 2000 + "0" + "]" * 2000
    bad = span(
        "bad",
        trace_id="bad",
        attributes={"gen_ai.conversation.id": "bad", "input.value": nested},
    )
    healthy = span(
        "healthy", trace_id="healthy", attributes={"gen_ai.conversation.id": "healthy"}
    )
    results = parse(jsonl(bad, healthy))
    assert sum(isinstance(item, ImportedSession) for item in results) == 1
    assert sum(isinstance(item, ImportFailure) for item in results) == 1


@pytest.mark.parametrize(
    "index", ["²", "\u0661", "9" * 5000], ids=["superscript", "arabic", "long"]
)
def test_malformed_message_index_preserves_valid_messages(index: str) -> None:
    """Ignore non-ASCII and invalid indexes without losing real messages."""
    [session] = parse(
        jsonl(
            span(
                "llm",
                span_kind="LLM",
                attributes={
                    f"llm.input_messages.{index}.message.content": "bad",
                    "llm.input_messages.0.message.content": "hello",
                },
            )
        )
    )
    assert isinstance(session, ImportedSession)
    assert session.nodes[0].inputs == [{"message.content": "hello"}]


@pytest.mark.parametrize(
    "record",
    [
        {"name": "no identifiers"},
        {"trace_id": "\ud800"},
        {"traceId": "\udfff", "spans": None},
    ],
)
def test_malformed_identity_failures_are_serializable(record: dict[str, Any]) -> None:
    """Safely report absent or malformed identifiers before trace grouping."""
    results = parse(jsonl(record, span("healthy")))
    assert sum(isinstance(item, ImportedSession) for item in results) == 1
    assert sum(isinstance(item, ImportFailure) for item in results) == 1
    for item in results:
        item.model_dump_json()


@pytest.mark.parametrize("value", [None, "", 0, "0", 2, "2"])
def test_valid_numeric_attributes_are_preserved(value: Any) -> None:
    """Keep missing values, zero, and ordinary numeric strings valid."""
    [session] = parse(
        jsonl(
            span(
                "root",
                attributes={
                    "operation.cost": value,
                    "gen_ai.usage.input_tokens": value,
                },
            )
        )
    )
    assert isinstance(session, ImportedSession)
    node = session.nodes[0]
    if value in (None, ""):
        assert node.cost is None
        assert node.tokens is None
    else:
        assert node.cost == Decimal(str(value))
        assert node.tokens is not None
        assert node.tokens.input_tokens == int(value)
    session.model_dump_json()
