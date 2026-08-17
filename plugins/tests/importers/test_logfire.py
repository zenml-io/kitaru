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
"""Logfire records-query importer plugin tests."""

import json
from decimal import Decimal
from typing import Any

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_logfire_importer.importer import LogfireRecordsImporter


def row(
    span_id: str,
    *,
    trace_id: str = "trace-1",
    parent_span_id: str | None = None,
    span_name: str | None = None,
    start_timestamp: str = "2026-07-22T13:15:00Z",
    attributes: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build one sanitized Logfire records-table row."""
    return {
        "project_id": "project-1",
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "span_name": span_name or span_id,
        "message": span_name or span_id,
        "kind": "span",
        "level": 9,
        "start_timestamp": start_timestamp,
        "end_timestamp": "2026-07-22T13:15:01Z",
        "service_name": "support-agent",
        "deployment_environment": "test",
        "otel_scope_name": "pydantic-ai",
        "attributes": {
            "gen_ai.conversation.id": "conversation-1",
            "gen_ai.agent.name": "support-agent",
            **(attributes or {}),
        },
        **extra,
    }


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def parse(
    content: bytes, params: dict[str, Any] | None = None
) -> list[ImportedSession | ImportFailure]:
    """Parse one test payload."""
    return list(LogfireRecordsImporter().parse(content, params or {}))


def flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def test_maps_trace_corpus_genai_spans() -> None:
    """Map the GenAI fields represented in the Kitaru trace corpus."""
    content = jsonl(
        row(
            "tool",
            parent_span_id="root",
            span_name="execute_tool lookup_order",
            start_timestamp="2026-07-22T13:15:00.200000Z",
            attributes={
                "gen_ai.operation.name": "execute_tool",
                "gen_ai.tool.name": "lookup_order",
                "gen_ai.tool.call.arguments": {"order_reference": "KIT-2481"},
                "gen_ai.tool.call.result": {"status": "in_transit"},
            },
        ),
        row(
            "llm",
            parent_span_id="root",
            span_name="chat claude-haiku-4-5",
            start_timestamp="2026-07-22T13:15:00.100000Z",
            attributes={
                "gen_ai.operation.name": "chat",
                "gen_ai.input.messages": [
                    {"role": "user", "parts": [{"content": "Track KIT-2481"}]}
                ],
                "gen_ai.output.messages": [
                    {"role": "assistant", "parts": [{"content": "Checking"}]}
                ],
                "gen_ai.provider.name": "anthropic",
                "gen_ai.request.model": "claude-haiku-4-5",
                "gen_ai.response.model": "claude-haiku-4-5-20251001",
                "gen_ai.usage.input_tokens": 507,
                "gen_ai.usage.output_tokens": 77,
                "operation.cost": 0.000892,
                "model_request_parameters": {"temperature": 0},
            },
        ),
        row(
            "root",
            span_name="invoke_agent support-agent",
            attributes={
                "gen_ai.operation.name": "invoke_agent",
                "input": "Track KIT-2481",
                "final_result": "In transit",
            },
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:conversation-1"
    assert session.status is SessionStatus.COMPLETED
    assert session.outputs == "In transit"
    assert session.framework == "pydantic-ai"
    assert session.metadata["logfire.project_id"] == "project-1"
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    llm = nodes["trace-1:llm"]
    assert llm.node_type is NodeType.LLM_CALL
    assert llm.model_provider == "anthropic"
    assert llm.requested_model == "claude-haiku-4-5"
    assert llm.model == "claude-haiku-4-5-20251001"
    assert llm.tokens is not None
    assert llm.tokens.input_tokens == 507
    assert llm.tokens.output_tokens == 77
    assert llm.cost == Decimal("0.000892")
    assert llm.model_params == {"temperature": 0}
    tool = nodes["trace-1:tool"]
    assert tool.node_type is NodeType.TOOL_CALL
    assert tool.tool_name == "lookup_order"
    assert tool.inputs == {"order_reference": "KIT-2481"}
    assert tool.outputs == {"status": "in_transit"}
    assert tool in nodes["trace-1:root"].children


def test_groups_conversation_traces_as_ordered_turns() -> None:
    """Group out-of-order traces by conversation and sort their turns."""
    content = jsonl(
        row(
            "root-2",
            trace_id="trace-2",
            start_timestamp="2026-07-22T13:16:00Z",
            attributes={"input": "second", "final_result": "two"},
        ),
        row(
            "root-1",
            trace_id="trace-1",
            attributes={"input": "first", "final_result": "one"},
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert [turn["source_trace_id"] for turn in session.inputs["turns"]] == [
        "trace-1",
        "trace-2",
    ]
    assert session.outputs == "two"
    assert session.metadata["source_trace_count"] == 2


def test_parses_streaming_query_api_ndjson() -> None:
    """Read schema, batched rows, and terminal messages from Query API v2."""
    payload = b"\n".join(
        json.dumps(message).encode()
        for message in (
            {"type": "schema", "schema": {"fields": []}},
            {"type": "data", "rows": [row("root")]},
            {"type": "end", "row_count": 1},
        )
    )

    [session] = parse(payload)

    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:conversation-1"


def test_uses_configured_join_path() -> None:
    """Use a caller-selected attribute for session grouping."""
    record = row("root", attributes={"customer.case/id": "case-9"})

    [session] = parse(jsonl(record), {"join_on": "/attributes/customer.case~1id"})

    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:case-9"
    assert session.metadata["logfire.join_paths"] == ["/attributes/customer.case~1id"]


def test_falls_back_to_trace_id_with_warning() -> None:
    """Keep traces separate when no conversation identity was recorded."""
    record = row(
        "root",
        attributes={
            "gen_ai.agent.name": "support-agent",
            "gen_ai.conversation.id": None,
        },
    )

    [session] = parse(jsonl(record))

    assert isinstance(session, ImportedSession)
    assert session.external_id == "project-1:trace-1"
    assert "grouped by trace id" in session.metadata["normalization_warnings"][0]


def test_isolates_rows_missing_trace_identity() -> None:
    """Import valid traces while reporting malformed rows."""
    invalid = row("missing-trace")
    invalid.pop("trace_id")

    parsed = parse(jsonl(row("root"), invalid))

    assert len(parsed) == 2
    assert any(isinstance(item, ImportedSession) for item in parsed)
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert failure.error == "Logfire row lacks trace_id or span_id"


def test_maps_failed_trace_and_missing_parent() -> None:
    """Preserve failures and keep orphan spans importable."""
    content = jsonl(
        row(
            "failed",
            parent_span_id="missing",
            is_exception=True,
            exception_message="lookup failed",
        )
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.status is SessionStatus.FAILED
    assert session.nodes[0].status is NodeStatus.FAILED
    assert session.nodes[0].error == "lookup failed"
    assert "references missing parent" in session.metadata["normalization_warnings"][0]


def test_uses_root_status_after_recovered_child_failure() -> None:
    """Keep a completed session when an internal failed span was recovered."""
    content = jsonl(
        row("root"),
        row(
            "failed-tool",
            parent_span_id="root",
            is_exception=True,
            exception_message="retryable failure",
        ),
    )

    [session] = parse(content)

    assert isinstance(session, ImportedSession)
    assert session.status is SessionStatus.COMPLETED
    assert flatten(session.nodes)[1].status is NodeStatus.FAILED


def test_rejects_parent_cycles_per_session() -> None:
    """Report a cyclic span graph without aborting the file parser."""
    parsed = parse(
        jsonl(
            row("one", parent_span_id="two"),
            row("two", parent_span_id="one"),
        )
    )

    [failure] = parsed
    assert isinstance(failure, ImportFailure)
    assert "parent cycle" in failure.error
