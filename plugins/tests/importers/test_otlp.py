#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""OTLP JSON importer plugin tests."""

import json
from pathlib import Path
from typing import Any

import pytest

from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession, ImportFailure
from kitaru.worker.process import parse_inline_dependencies
from kitaru_importer_opentelemetry import importer as importer_module
from kitaru_importer_opentelemetry.importer import (
    InvalidImport,
    OTLPJSONImporter,
    parse,
)

TRACE_1 = "0af7651916cd43dd8448eb211c80319c"
TRACE_2 = "1af7651916cd43dd8448eb211c80319c"
ROOT = "b7ad6b7169203331"
CHILD = "b7ad6b7169203332"


def any_value(value: Any) -> dict[str, Any]:
    """Encode a Python value as an OTLP AnyValue."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [any_value(item) for item in value]}}
    if isinstance(value, dict):
        return {
            "kvlistValue": {
                "values": [
                    {"key": key, "value": any_value(item)}
                    for key, item in value.items()
                ]
            }
        }
    return {"stringValue": str(value)}


def attributes(**values: Any) -> list[dict[str, Any]]:
    """Encode OTLP attributes, replacing double underscores with dots."""
    return [
        {"key": key.replace("__", "."), "value": any_value(value)}
        for key, value in values.items()
    ]


def span(
    trace_id: str,
    span_id: str,
    *,
    parent_id: str | None = None,
    name: str = "agent",
    start: int = 1_700_000_000_000_000_000,
    attrs: list[dict[str, Any]] | None = None,
    status: dict[str, Any] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build one OTLP span."""
    result: dict[str, Any] = {
        "traceId": trace_id,
        "spanId": span_id,
        "name": name,
        "kind": 1,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(start + 1_000_000_000),
        "attributes": attrs or [],
        "status": status or {"code": 1},
    }
    if parent_id:
        result["parentSpanId"] = parent_id
    if events:
        result["events"] = events
    return result


def request(*spans: dict[str, Any]) -> dict[str, Any]:
    """Build one OTLP ExportTraceServiceRequest."""
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": attributes(
                        service__namespace="acme",
                        service__name="document-agent",
                        telemetry__sdk__name="opentelemetry",
                        telemetry__sdk__version="1.42.0",
                    )
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "pydantic-ai", "version": "1.0"},
                        "spans": list(spans),
                    }
                ],
            }
        ]
    }


def payload(*spans: dict[str, Any]) -> bytes:
    """Encode one OTLP request."""
    return json.dumps(request(*spans)).encode()


def params(source_instance: str | None = None) -> dict[str, Any]:
    """Build importer parameters."""
    result: dict[str, Any] = {"filename": "traces.jsonl"}
    if source_instance:
        result["source_instance"] = source_instance
    return result


def sessions(
    content: bytes, importer_params: dict[str, Any] | None = None
) -> list[ImportedSession]:
    """Return successfully imported sessions."""
    return [
        item
        for item in OTLPJSONImporter().parse(content, importer_params or params())
        if isinstance(item, ImportedSession)
    ]


def failures(
    content: bytes, importer_params: dict[str, Any] | None = None
) -> list[ImportFailure]:
    """Return isolated import failures."""
    return [
        item
        for item in OTLPJSONImporter().parse(content, importer_params or params())
        if isinstance(item, ImportFailure)
    ]


def flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first for assertions."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def test_imports_span_graph_and_genai_fields() -> None:
    """Map OTLP hierarchy, GenAI fields, usage, and replay content."""
    parsed = OTLPJSONImporter().parse(
        payload(
            span(
                TRACE_1,
                ROOT,
                attrs=attributes(
                    gen_ai__conversation__id="document-run-1",
                    input__value='{"document": "invoice.pdf"}',
                    output__value='{"category": "invoice"}',
                ),
            ),
            span(
                TRACE_1,
                CHILD,
                parent_id=ROOT,
                name="classify document",
                attrs=attributes(
                    gen_ai__operation__name="chat",
                    gen_ai__request__model="gpt-5-mini",
                    gen_ai__response__model="gpt-5-mini-2026-06-01",
                    gen_ai__provider__name="openai",
                    gen_ai__usage__input_tokens=120,
                    gen_ai__usage__output_tokens=12,
                    gen_ai__request__temperature=0.2,
                    gen_ai__input__messages=[{"role": "user", "content": "Classify"}],
                    gen_ai__output__messages=[
                        {"role": "assistant", "content": "invoice"}
                    ],
                ),
            ),
        ),
        params(),
    )

    assert len(parsed) == 1
    session = parsed[0]
    assert isinstance(session, ImportedSession)
    assert session.external_id == "acme/document-agent:document-run-1"
    assert session.inputs["turns"][0]["inputs"] == {"document": "invoice.pdf"}
    assert session.outputs == {"category": "invoice"}
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    child = nodes[f"{TRACE_1}:{CHILD}"]
    assert child in nodes[f"{TRACE_1}:{ROOT}"].children
    assert child.node_type is NodeType.LLM_CALL
    assert child.requested_model == "gpt-5-mini"
    assert child.model == "gpt-5-mini-2026-06-01"
    assert child.provider == "openai"
    assert child.input_text_selector == '$[0]["content"]'
    assert child.output_text_selector == '$[0]["content"]'
    assert child.tokens and child.tokens.input_tokens == 120
    assert child.model_params == {"temperature": 0.2}


def test_imports_flattened_otlp_jsonl_and_surfaces_prompts() -> None:
    """Normalize flattened Arize and Logfire records through the OTLP path."""
    messages = [
        {"role": "system", "parts": [{"type": "text", "content": "Be brief."}]},
        {"role": "user", "parts": [{"type": "text", "content": "Classify it."}]},
    ]
    record = {
        "trace_id": TRACE_1,
        "span_id": ROOT,
        "parent_span_id": None,
        "span_name": "chat model",
        "start_timestamp": "2026-07-22T10:00:00Z",
        "end_timestamp": "2026-07-22T10:00:01Z",
        "otel_status_code": "UNSET",
        "attributes": {
            "gen_ai.conversation.id": "conversation-1",
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "gpt-5-mini",
            "pydantic_ai.all_messages": messages,
            "final_result": {
                "answer": "invoice",
                "reasoning": "The document contains an invoice number.",
            },
        },
        "service_name": "document-agent",
        "otel_scope_name": "pydantic-ai",
    }

    session = sessions((json.dumps(record) + "\n").encode())[0]
    node = session.nodes[0]

    assert session.framework == "pydantic-ai"
    assert session.system_prompt == "Be brief."
    assert node.input_text_selector == '$[1]["parts"][0]["content"]'
    assert node.output_text_selector == '$["answer"]'
    assert node.system_prompt_selector == '$[0]["parts"][0]["content"]'
    assert node.reasoning == "The document contains an invoice number."
    assert node.inputs == messages
    assert node.outputs == {
        "answer": "invoice",
        "reasoning": "The document contains an invoice number.",
    }


def test_decodes_nullable_snake_case_any_values() -> None:
    """Read the populated member from generated snake-case AnyValue objects."""
    encoded_value = {
        "string_value": None,
        "bool_value": None,
        "int_value": 42,
        "double_value": None,
        "array_value": None,
        "kvlist_value": None,
        "bytes_value": None,
    }
    record = {
        "trace_id": TRACE_1,
        "span_id": ROOT,
        "span_name": "chat model",
        "start_timestamp": "2026-07-22T10:00:00Z",
        "end_timestamp": "2026-07-22T10:00:01Z",
        "attributes": [
            {"key": "gen_ai.operation.name", "value": {"string_value": "chat"}},
            {"key": "gen_ai.usage.input_tokens", "value": encoded_value},
        ],
    }

    node = sessions(json.dumps(record).encode())[0].nodes[0]

    assert node.tokens is not None
    assert node.tokens.input_tokens == 42


def test_imports_logfire_query_rows_as_one_session() -> None:
    """Normalize query envelopes, stable sessions, model calls, and functions."""
    common = {
        "trace_id": TRACE_1,
        "start_timestamp": "2026-07-22T10:00:00Z",
        "end_timestamp": "2026-07-22T10:00:01Z",
        "otel_status_code": "OK",
    }
    root = {
        **common,
        "span_id": ROOT,
        "parent_span_id": None,
        "span_name": "agent run",
        "attributes": {
            "session.id": "support-session",
            "gen_ai.conversation.id": "agent-stage",
            "input.value": {"question": "Where is order 42?"},
        },
    }
    model = {
        **common,
        "span_id": CHILD,
        "parent_span_id": ROOT,
        "span_name": "chat model",
        "attributes": {
            "session.id": "support-session",
            "gen_ai.conversation.id": "model-stage",
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "gpt-test",
            "gen_ai.system_instructions": "Use order data only.",
            "gen_ai.input.messages": [
                {"role": "user", "content": "Where is order 42?"}
            ],
            "gen_ai.output.messages": [
                {"role": "assistant", "content": "I will check."}
            ],
        },
    }
    tool = {
        **common,
        "span_id": "b7ad6b7169203333",
        "parent_span_id": CHILD,
        "span_name": "Function: lookup_order",
        "attributes": {
            "session.id": "support-session",
            "gen_ai.conversation.id": "tool-stage",
            "name": "lookup_order",
            "input.value": {"order_id": "42"},
            "output.value": {"status": "shipped"},
        },
    }
    content = b"\n".join(
        json.dumps(item).encode()
        for item in (
            {"type": "schema", "schema": {"fields": []}},
            {"type": "data", "rows": [root, model, tool]},
            {"type": "end", "row_count": 3},
        )
    )

    [session] = sessions(content)
    nodes = flatten(session.nodes)

    assert session.external_id.endswith(":support-session")
    assert session.system_prompt == "Use order data only."
    assert session.outputs == {"status": "shipped"}
    assert sum(node.node_type is NodeType.LLM_CALL for node in nodes) == 1
    tool_node = next(node for node in nodes if node.node_type is NodeType.TOOL_CALL)
    assert tool_node.tool_name == "lookup_order"
    assert tool_node.inputs == {"order_id": "42"}
    assert tool_node.outputs == {"status": "shipped"}


def test_imports_arize_flat_span_jsonl() -> None:
    """Read Arize span ids from the nested context object."""
    record = {
        "context": {"trace_id": TRACE_1, "span_id": ROOT},
        "name": "invoke_agent agent",
        "span_kind": "AGENT",
        "parent_id": None,
        "start_time": "2026-07-22T14:09:41Z",
        "end_time": "2026-07-22T14:09:42Z",
        "status_code": "OK",
        "status_message": "",
        "attributes": {
            "input.value": "Classify it.",
            "output.value": "invoice",
            "session.id": "session-1",
        },
        "events": [],
    }

    session = sessions(json.dumps(record).encode())[0]
    node = session.nodes[0]

    assert node.trace_id == TRACE_1
    assert node.input_text_selector == "$"
    assert node.output_text_selector == "$"


def test_imports_tool_call_content() -> None:
    """Map a GenAI tool span into a tool node."""
    session = sessions(
        payload(
            span(
                TRACE_1,
                ROOT,
                attrs=attributes(input__value="invoice.pdf"),
            ),
            span(
                TRACE_1,
                CHILD,
                parent_id=ROOT,
                name="read_pdf",
                attrs=attributes(
                    gen_ai__operation__name="execute_tool",
                    gen_ai__tool__name="read_pdf",
                    gen_ai__tool__call__arguments='{"path": "invoice.pdf"}',
                    gen_ai__tool__call__result='{"pages": 2}',
                ),
            ),
        ),
        params("upload-1"),
    )[0]

    tool = next(
        node for node in flatten(session.nodes) if node.node_type is NodeType.TOOL_CALL
    )
    assert tool.tool_name == "read_pdf"
    assert tool.inputs == {"path": "invoice.pdf"}
    assert tool.outputs == {"pages": 2}


def test_groups_jsonl_requests_into_ordered_turns() -> None:
    """Treat each trace in a conversation as an ordered turn."""
    first = request(
        span(
            TRACE_1,
            ROOT,
            start=1_700_000_000_000_000_000,
            attrs=attributes(
                gen_ai__conversation__id="conversation-1", input__value="first"
            ),
        )
    )
    second = request(
        span(
            TRACE_2,
            ROOT,
            start=1_700_000_100_000_000_000,
            attrs=attributes(
                gen_ai__conversation__id="conversation-1", input__value="second"
            ),
        )
    )
    content = b"\n".join(json.dumps(item).encode() for item in (second, first))

    parsed = sessions(content)

    assert len(parsed) == 1
    assert parsed[0].metadata["otlp.trace_ids"] == [TRACE_1, TRACE_2]
    assert [turn["inputs"] for turn in parsed[0].inputs["turns"]] == [
        "first",
        "second",
    ]


def test_uses_trace_id_without_conversation_and_explicit_source_override() -> None:
    """Fall back to trace grouping and honor an explicit source instance."""
    session = sessions(
        payload(span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))),
        params("my-collector"),
    )[0]

    assert session.external_id == f"my-collector:{TRACE_1}"
    assert session.metadata["normalization_warnings"] == []


def test_reports_missing_parent_and_failed_exception() -> None:
    """Preserve a detached span and surface its OTLP exception."""
    missing_parent = "b7ad6b7169203999"
    exception = {
        "name": "exception",
        "timeUnixNano": "1700000000100000000",
        "attributes": attributes(exception__message="PDF is encrypted"),
    }
    session = sessions(
        payload(
            span(
                TRACE_1,
                ROOT,
                parent_id=missing_parent,
                attrs=attributes(input__value="invoice.pdf"),
                status={"code": 2},
                events=[exception],
            )
        )
    )[0]

    assert session.status.value == "failed"
    assert session.error == "PDF is encrypted"
    assert session.nodes[0].status is NodeStatus.FAILED
    warnings = session.metadata["normalization_warnings"]
    assert isinstance(warnings, list)
    assert any("missing parent" in warning for warning in warnings)


def test_isolates_invalid_and_conflicting_duplicate_spans() -> None:
    """Keep valid traces when other spans cannot be normalized."""
    valid = span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))
    duplicate = span(TRACE_2, ROOT, attrs=attributes(input__value="first"))
    conflicting = span(TRACE_2, ROOT, attrs=attributes(input__value="second"))
    invalid = span("bad-trace", CHILD)

    content = payload(valid, duplicate, conflicting, invalid)
    parsed_sessions = sessions(content)
    parsed_failures = failures(content)

    assert [session.metadata["otlp.session_id"] for session in parsed_sessions] == [
        TRACE_1
    ]
    assert len(parsed_failures) == 2
    assert any("invalid trace id" in failure.error for failure in parsed_failures)
    assert any("conflicting duplicate" in failure.error for failure in parsed_failures)


def test_node_order_is_stable_across_span_order() -> None:
    """Build the same node order for reordered OTLP spans."""
    root = span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))
    child = span(TRACE_1, CHILD, parent_id=ROOT)

    first = sessions(payload(root, child))[0]
    second = sessions(payload(child, root))[0]

    assert [node.external_id for node in first.nodes] == [
        node.external_id for node in second.nodes
    ]


def test_unified_entrypoint_and_pep723_metadata() -> None:
    """Expose the v2 parser and keep the importer upload self-describing."""
    items = list(
        parse(
            payload(span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))),
            {"source_instance": "collector"},
        )
    )

    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)
    assert items[0].external_id == f"collector:{TRACE_1}"
    assert parse_inline_dependencies(Path(importer_module.__file__)) == []


def test_rejects_malformed_whole_file() -> None:
    """Reject content that is not an OTLP trace request."""
    with pytest.raises(InvalidImport, match="resourceSpans"):
        OTLPJSONImporter().parse(b'{"not": "otlp"}', params())
