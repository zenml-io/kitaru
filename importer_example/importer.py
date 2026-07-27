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
"""Importer for the JSONL trace format of the mock agent."""

import json
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from typing import Any

from kitaru.api_models.v1.session_nodes import NodeStatus, NodeType
from kitaru.api_models.v1.sessions import TokenUsage
from kitaru.importing import ParsedNode, ParsedSession, ParseFailure

SPAN_NODE_TYPES = {"llm": NodeType.LLM_CALL, "tool": NodeType.TOOL_CALL}


def _timestamp(value: Any) -> datetime | None:
    """Read an ISO 8601 timestamp."""
    if not isinstance(value, str):
        return None
    return datetime.fromisoformat(value)


def _tokens(usage: Any) -> TokenUsage | None:
    """Read the token counts of a span."""
    if not isinstance(usage, dict):
        return None
    return TokenUsage(
        input_tokens=usage.get("input"), output_tokens=usage.get("output")
    )


def _cost(value: Any) -> Decimal | None:
    """Read the cost of a span."""
    if value is None:
        return None
    return Decimal(str(value))


def _span_node(span: dict[str, Any]) -> ParsedNode:
    """Build the node of one trace span."""
    kind = span.get("kind")
    if kind not in SPAN_NODE_TYPES:
        raise ValueError(f"Unknown span kind {kind!r}")
    node_type = SPAN_NODE_TYPES[kind]
    error = span.get("error")
    return ParsedNode(
        node_type=node_type,
        name=span["name"],
        status=NodeStatus.FAILED if error else NodeStatus.COMPLETED,
        error=error,
        started_at=_timestamp(span.get("started_at")),
        ended_at=_timestamp(span.get("ended_at")),
        inputs=span.get("input"),
        outputs=span.get("output"),
        requested_model=span.get("model"),
        model=span.get("model"),
        provider=span.get("provider"),
        tokens=_tokens(span.get("usage")),
        cost=_cost(span.get("cost")),
        model_params=span.get("params"),
        tool_name=span["name"] if node_type is NodeType.TOOL_CALL else None,
        external_id=span.get("span_id"),
    )


def _span_nodes(spans: Any) -> list[ParsedNode]:
    """Build the span tree, nesting tool calls under the requesting LLM call."""
    if not isinstance(spans, list):
        raise ValueError("Trace spans are not a list")
    nodes: list[ParsedNode] = []
    last_llm_call: ParsedNode | None = None
    for span in spans:
        if not isinstance(span, dict):
            raise ValueError("Trace span is not an object")
        node = _span_node(span)
        if node.node_type is NodeType.TOOL_CALL and last_llm_call is not None:
            last_llm_call.children.append(node)
            continue
        nodes.append(node)
        if node.node_type is NodeType.LLM_CALL:
            last_llm_call = node
    return nodes


def _parsed_session(record: dict[str, Any]) -> ParsedSession:
    """Build the session of one trace record."""
    external_id = record.get("trace_id")
    if not isinstance(external_id, str) or not external_id:
        raise ValueError("Trace record has no trace id")
    started_at = _timestamp(record.get("started_at"))
    ended_at = _timestamp(record.get("ended_at"))
    root = ParsedNode(
        node_type=NodeType.SPAN,
        name="trace",
        started_at=started_at,
        ended_at=ended_at,
        inputs=record.get("input"),
        outputs=record.get("output"),
        external_id=external_id,
        children=_span_nodes(record.get("spans", [])),
    )
    return ParsedSession(
        external_id=external_id,
        name=record.get("name"),
        inputs=record.get("input"),
        outputs=record.get("output"),
        expected=record.get("expected"),
        started_at=started_at,
        ended_at=ended_at,
        metadata=record.get("metadata", {}),
        nodes=[root],
    )


def parse(payload: bytes) -> Iterator[ParsedSession | ParseFailure]:
    """Parse a JSONL trace export into sessions, one record per line."""
    for line, raw in enumerate(payload.decode("utf-8").splitlines(), start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            yield ParseFailure(line=line, error=f"Invalid JSON: {exc}")
            continue
        if not isinstance(record, dict):
            yield ParseFailure(line=line, error="Trace record is not an object")
            continue
        external_id = record.get("trace_id")
        try:
            session = _parsed_session(record)
        except Exception as exc:
            yield ParseFailure(
                line=line,
                external_id=external_id if isinstance(external_id, str) else None,
                error=f"{type(exc).__name__}: {exc}",
            )
            continue
        yield session
