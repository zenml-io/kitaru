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
"""OTLP JSON importer tests."""

import json
from pathlib import Path
from typing import Any

import pytest
from kitaru_importer_otlp import OTLPJSONImporter, parse
from kitaru_importer_otlp import importer as importer_module

from kitaru.importers import ImportContext, InvalidImport, NodeStatus, NodeType
from kitaru.task.importer import ParsedSession
from kitaru.worker.process import parse_inline_dependencies

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


def context(source_instance: str | None = None) -> ImportContext:
    """Build an import context."""
    return ImportContext(source_instance=source_instance, filename="traces.jsonl")


def test_imports_span_graph_and_genai_fields() -> None:
    """Map OTLP hierarchy, GenAI fields, usage, and replay content."""
    batch = OTLPJSONImporter().parse(
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
        context(),
    )

    assert batch.errors == []
    session = batch.sessions[0]
    assert session.source_id == "document-run-1"
    assert session.source_instance == "acme/document-agent"
    assert session.turns[0].inputs == {"document": "invoice.pdf"}
    assert session.outputs == {"category": "invoice"}
    nodes = {node.source_id: node for node in session.nodes}
    child = nodes[f"{TRACE_1}:{CHILD}"]
    assert child.parent_source_id == f"{TRACE_1}:{ROOT}"
    assert child.node_type is NodeType.LLM_CALL
    assert child.requested_model == "gpt-5-mini"
    assert child.model == "gpt-5-mini-2026-06-01"
    assert child.provider == "openai"
    assert child.tokens and child.tokens.input_tokens == 120
    assert child.model_params == {"temperature": 0.2}
    assert session.readiness.level == "ready"


def test_imports_tool_call_content() -> None:
    """Map a GenAI tool span into a replayable tool node."""
    batch = OTLPJSONImporter().parse(
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
        context("upload-1"),
    )

    tool = next(
        node for node in batch.sessions[0].nodes if node.node_type is NodeType.TOOL_CALL
    )
    assert tool.tool_name == "read_pdf"
    assert tool.inputs == {"path": "invoice.pdf"}
    assert tool.outputs == {"pages": 2}
    assert batch.sessions[0].readiness.replayable_tool_call_count == 1


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

    batch = OTLPJSONImporter().parse(content, context())

    assert len(batch.sessions) == 1
    assert [turn.trace_id for turn in batch.sessions[0].turns] == [TRACE_1, TRACE_2]
    assert [turn["inputs"] for turn in batch.sessions[0].inputs["turns"]] == [
        "first",
        "second",
    ]


def test_uses_trace_id_without_conversation_and_explicit_source_override() -> None:
    """Fall back to trace grouping and honor an explicit source instance."""
    batch = OTLPJSONImporter().parse(
        payload(span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))),
        context("my-collector"),
    )

    assert batch.sessions[0].source_id == TRACE_1
    assert batch.sessions[0].source_instance == "my-collector"
    assert batch.sessions[0].warnings == []


def test_reports_missing_parent_and_failed_exception() -> None:
    """Preserve a detached span and surface its OTLP exception."""
    missing_parent = "b7ad6b7169203999"
    exception = {
        "name": "exception",
        "timeUnixNano": "1700000000100000000",
        "attributes": attributes(exception__message="PDF is encrypted"),
    }
    batch = OTLPJSONImporter().parse(
        payload(
            span(
                TRACE_1,
                ROOT,
                parent_id=missing_parent,
                attrs=attributes(input__value="invoice.pdf"),
                status={"code": 2},
                events=[exception],
            )
        ),
        context(),
    )

    session = batch.sessions[0]
    assert session.status.value == "failed"
    assert session.error == "PDF is encrypted"
    assert session.nodes[0].status is NodeStatus.FAILED
    assert session.readiness.graph_complete is False
    assert any("missing parent" in warning for warning in session.warnings)


def test_isolates_invalid_and_conflicting_duplicate_spans() -> None:
    """Keep valid traces when other spans cannot be normalized."""
    valid = span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))
    duplicate = span(TRACE_2, ROOT, attrs=attributes(input__value="first"))
    conflicting = span(TRACE_2, ROOT, attrs=attributes(input__value="second"))
    invalid = span("bad-trace", CHILD)

    batch = OTLPJSONImporter().parse(
        payload(valid, duplicate, conflicting, invalid), context()
    )

    assert [session.source_id for session in batch.sessions] == [TRACE_1]
    assert len(batch.errors) == 2
    assert any("invalid trace id" in error.message for error in batch.errors)
    assert any("conflicting duplicate" in error.message for error in batch.errors)


def test_digest_is_stable_across_span_order() -> None:
    """Build the same content digest for reordered OTLP spans."""
    root = span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))
    child = span(TRACE_1, CHILD, parent_id=ROOT)

    first = OTLPJSONImporter().parse(payload(root, child), context()).sessions[0]
    second = OTLPJSONImporter().parse(payload(child, root), context()).sessions[0]

    assert first.content_digest == second.content_digest


def test_unified_entrypoint_and_pep723_metadata() -> None:
    """Expose the v2 parser and keep the importer upload self-describing."""
    items = list(
        parse(
            payload(span(TRACE_1, ROOT, attrs=attributes(input__value="hello"))),
            {"source_instance": "collector"},
        )
    )

    assert len(items) == 1
    assert isinstance(items[0], ParsedSession)
    assert items[0].external_id == f"collector:{TRACE_1}"
    assert parse_inline_dependencies(Path(importer_module.__file__)) == []


def test_rejects_malformed_whole_file() -> None:
    """Reject content that is not an OTLP trace request."""
    with pytest.raises(InvalidImport, match="resourceSpans"):
        OTLPJSONImporter().parse(b'{"not": "otlp"}', context())
