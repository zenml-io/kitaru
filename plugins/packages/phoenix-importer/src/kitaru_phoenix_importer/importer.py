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
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Arize Phoenix JSON and JSONL trace importer plugin."""

import json
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
MAX_PARENT_DEPTH = 64


class InvalidImport(ValueError):
    """Raised when a Phoenix payload cannot be parsed."""


def _escape_failure_text(value: str) -> str:
    """Escape unencodable characters in failure diagnostics only."""
    return value.encode("utf-8", errors="backslashreplace").decode("utf-8")


def _decode_json(value: Any) -> Any:
    """Decode JSON-encoded attributes while preserving ordinary strings."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in '[{"':
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value
    except (RecursionError, ValueError) as exc:
        raise InvalidImport("Embedded JSON exceeds decoding limits") from exc


def _datetime(value: Any) -> datetime | None:
    """Parse a Phoenix ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_token_count(value: Any) -> int | None:
    """Parse a nonnegative token count."""
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidImport("Token count must be a nonnegative integer") from exc
    if parsed < 0 or (isinstance(value, float) and value < 0):
        raise InvalidImport("Token count must be a nonnegative integer")
    return parsed


def _decimal(value: Any) -> Decimal | None:
    """Parse a decimal attribute."""
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidImport("Cost must be finite and nonnegative") from exc
    if not parsed.is_finite() or parsed < 0:
        raise InvalidImport("Cost must be finite and nonnegative")
    return parsed


def _attributes(span: dict[str, Any]) -> dict[str, Any]:
    """Return the Phoenix span attributes."""
    attributes = _decode_json(span.get("attributes"))
    return attributes if isinstance(attributes, dict) else {}


def _attribute(attributes: dict[str, Any], *keys: str) -> Any:
    """Return the first populated Phoenix attribute."""
    for key in keys:
        value = attributes.get(key)
        if value not in (None, ""):
            return _decode_json(value)
    return None


def _indexed_messages(
    attributes: dict[str, Any], key: str
) -> list[dict[str, Any]] | None:
    """Collect Phoenix's indexed OpenInference message attributes."""
    messages: dict[int, dict[str, Any]] = defaultdict(dict)
    prefix = f"{key}."
    for attribute_key, value in attributes.items():
        if not attribute_key.startswith(prefix):
            continue
        index, separator, field = attribute_key.removeprefix(prefix).partition(".")
        if separator and index.isascii() and index.isdigit():
            try:
                message_index = int(index)
            except ValueError:
                continue
            messages[message_index][field] = _decode_json(value)
    return [messages[index] for index in sorted(messages)] or None


def _llm_messages(attributes: dict[str, Any], direction: str) -> Any:
    """Return structured GenAI or indexed OpenInference messages."""
    messages = _attribute(attributes, f"gen_ai.{direction}.messages")
    if messages is not None:
        return messages
    return _indexed_messages(attributes, f"llm.{direction}_messages")


def _parse_values(
    content: bytes,
) -> tuple[list[tuple[int, dict[str, Any]]], list[ImportFailure]]:
    """Parse Phoenix UI or CLI JSON and JSONL values."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Phoenix import exceeds the 50 MiB upload limit")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no JSON records")

    try:
        decoded = json.loads(text)
    except (ValueError, RecursionError):
        values: list[tuple[int, dict[str, Any]]] = []
        failures: list[ImportFailure] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (ValueError, RecursionError):
                failures.append(
                    ImportFailure(
                        line=line_number,
                        error=f"Line {line_number} is not valid JSON",
                    )
                )
                continue
            if not isinstance(value, dict):
                failures.append(
                    ImportFailure(
                        line=line_number,
                        error="Every Phoenix record must be a JSON object",
                    )
                )
                continue
            values.append((line_number, value))
        if not values:
            raise InvalidImport(failures[0].error) from None
    else:
        decoded_values = decoded if isinstance(decoded, list) else [decoded]
        if not decoded_values:
            raise InvalidImport("Import file contains no JSON records")
        if not all(isinstance(value, dict) for value in decoded_values):
            raise InvalidImport("Every Phoenix record must be a JSON object")
        values = [
            (record_number, value)
            for record_number, value in enumerate(decoded_values, start=1)
        ]
        failures = []

    return values, failures


def _trace_id(span: dict[str, Any]) -> str | None:
    """Read a trace identifier from one Phoenix span."""
    context = span.get("context")
    if isinstance(context, dict) and context.get("trace_id") not in (None, ""):
        return str(context["trace_id"])
    value = span.get("trace_id") or span.get("traceId")
    return str(value) if value not in (None, "") else None


def _span_id(span: dict[str, Any]) -> str | None:
    """Read a span identifier from one Phoenix span."""
    context = span.get("context")
    if isinstance(context, dict) and context.get("span_id") not in (None, ""):
        return str(context["span_id"])
    value = span.get("span_id") or span.get("spanId") or span.get("id")
    return str(value) if value not in (None, "") else None


def _parent_id(span: dict[str, Any]) -> str | None:
    """Read a parent span identifier."""
    value = (
        span.get("parent_id") or span.get("parent_span_id") or span.get("parentSpanId")
    )
    return str(value) if value not in (None, "") else None


def _expand_values(
    values: list[tuple[int, dict[str, Any]]],
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, Any]],
    dict[str, int],
    list[ImportFailure],
]:
    """Expand CLI trace envelopes and group flat spans by trace id."""
    traces: dict[str, list[dict[str, Any]]] = defaultdict(list)
    trace_metadata: dict[str, dict[str, Any]] = {}
    trace_lines: dict[str, int] = {}
    failures: list[ImportFailure] = []
    for line_number, value in values:
        if "spans" in value:
            spans = value.get("spans")
            envelope_trace_id = value.get("traceId") or value.get("trace_id")
            if not isinstance(spans, list) or not all(
                isinstance(span, dict) for span in spans
            ):
                failures.append(
                    ImportFailure(
                        line=line_number,
                        external_id=(
                            _escape_failure_text(str(envelope_trace_id))
                            if envelope_trace_id
                            else None
                        ),
                        error="Phoenix trace envelope has a non-object spans value",
                    )
                )
                continue
            if not envelope_trace_id:
                envelope_trace_id = next(
                    (trace_id for span in spans if (trace_id := _trace_id(span))),
                    None,
                )
            if not envelope_trace_id:
                failures.append(
                    ImportFailure(
                        line=line_number,
                        error="Phoenix trace envelope lacks a trace id",
                    )
                )
                continue
            trace_id = str(envelope_trace_id)
            traces[trace_id].extend(spans)
            trace_lines.setdefault(trace_id, line_number)
            metadata = trace_metadata.setdefault(trace_id, {})
            for key in ("annotations", "notes"):
                new_value = value.get(key)
                if new_value in (None, [], ""):
                    continue
                existing = metadata.get(key)
                if isinstance(existing, list) and isinstance(new_value, list):
                    existing.extend(new_value)
                elif key not in metadata:
                    metadata[key] = (
                        list(new_value) if isinstance(new_value, list) else new_value
                    )
            continue

        trace_id = _trace_id(value)
        if not trace_id or not _span_id(value):
            failures.append(
                ImportFailure(
                    line=line_number,
                    external_id=_escape_failure_text(trace_id)
                    if trace_id is not None
                    else None,
                    error="Phoenix span lacks trace_id or span_id",
                )
            )
            continue
        traces[trace_id].append(value)
        trace_lines.setdefault(trace_id, line_number)
    return traces, trace_metadata, trace_lines, failures


def _node_type(
    span: dict[str, Any], attributes: dict[str, Any]
) -> tuple[NodeType, str | None]:
    """Map an OpenInference span kind to a Kitaru node type."""
    span_kind = str(span.get("span_kind") or "").upper()
    if span_kind == "LLM":
        return NodeType.LLM_CALL, None
    if span_kind == "TOOL":
        tool_name = _attribute(attributes, "gen_ai.tool.name", "tool.name")
        return NodeType.TOOL_CALL, str(tool_name) if tool_name else None
    return NodeType.SPAN, None


def _node_status(span: dict[str, Any]) -> NodeStatus:
    """Map Phoenix status codes to terminal Kitaru node states."""
    return (
        NodeStatus.FAILED
        if str(span.get("status_code") or "").upper() == "ERROR"
        else NodeStatus.COMPLETED
    )


def _error(span: dict[str, Any], attributes: dict[str, Any]) -> str | None:
    """Extract an error message from status fields or exception events."""
    message = span.get("status_message") or _attribute(
        attributes, "exception.message", "error.message", "error.type"
    )
    if message:
        return str(message)
    events = span.get("events")
    if not isinstance(events, list):
        return None
    for event in events:
        if not isinstance(event, dict):
            continue
        attributes = event.get("attributes")
        if not isinstance(attributes, dict):
            continue
        for key in ("exception.message", "error.message", "exception.type"):
            if attributes.get(key):
                return str(attributes[key])
    return None


def _tokens(attributes: dict[str, Any]) -> TokenUsage | None:
    """Map OpenTelemetry GenAI token attributes."""
    values = (
        _parse_token_count(
            _attribute(
                attributes,
                "gen_ai.usage.input_tokens",
                "gen_ai.usage.prompt_tokens",
                "llm.token_count.prompt",
                "llm.usage.prompt_tokens",
            )
        ),
        _parse_token_count(
            _attribute(
                attributes,
                "gen_ai.usage.output_tokens",
                "gen_ai.usage.completion_tokens",
                "llm.token_count.completion",
                "llm.usage.completion_tokens",
            )
        ),
        _parse_token_count(
            _attribute(
                attributes,
                "gen_ai.usage.details.cache_read_tokens",
                "llm.token_count.prompt_details.cache_read",
            )
        ),
        _parse_token_count(
            _attribute(
                attributes,
                "gen_ai.usage.details.reasoning_tokens",
                "llm.token_count.completion_details.reasoning",
            )
        ),
    )
    if all(value is None for value in values):
        return None
    return TokenUsage(
        input_tokens=values[0],
        output_tokens=values[1],
        cached_input_tokens=values[2],
        reasoning_tokens=values[3],
    )


def _framework(spans: list[dict[str, Any]]) -> str | None:
    """Detect a framework only from provider-specific span evidence."""
    keys = {key for span in spans for key in _attributes(span)}
    if any(key.startswith("pydantic_ai.") for key in keys):
        return "pydantic-ai"
    if any(key.startswith("gcp.vertex.agent.") for key in keys):
        return "google-adk"
    return None


def _build_tree(
    nodes_with_parents: list[tuple[ImportedNode, str | None]],
    warnings: list[str],
) -> list[ImportedNode]:
    """Build an acyclic node tree, preserving partial-export orphans."""
    ids = [node.external_id for node, _ in nodes_with_parents]
    if len(ids) != len(set(ids)):
        raise InvalidImport("The import contains duplicate span ids")
    by_id = {node.external_id: node for node, _ in nodes_with_parents}
    parents = {
        node.external_id: parent
        for node, parent in nodes_with_parents
        if node.external_id is not None and parent in by_id
    }
    depths: dict[str, int] = {}
    for node_id in parents:
        path: list[str] = []
        seen: set[str] = set()
        current: str | None = node_id
        while current in parents and current not in depths:
            if current in seen:
                raise InvalidImport("The imported span graph contains a parent cycle")
            seen.add(current)
            path.append(current)
            if len(path) >= MAX_PARENT_DEPTH:
                raise InvalidImport("The imported span graph exceeds 64 parent levels")
            current = parents[current]
        depth = depths.get(current, 1) if current is not None else 1
        for ancestor in reversed(path):
            depth += 1
            if depth > MAX_PARENT_DEPTH:
                raise InvalidImport("The imported span graph exceeds 64 parent levels")
            depths[ancestor] = depth

    roots: list[ImportedNode] = []
    for node, parent in nodes_with_parents:
        if parent in by_id:
            by_id[parent].children.append(node)
        else:
            if parent is not None:
                warnings.append(
                    f"Span '{node.external_id}' references missing parent '{parent}'"
                )
            roots.append(node)
    return roots


def _node(span: dict[str, Any], trace_id: str) -> tuple[ImportedNode, str | None]:
    """Normalize one Phoenix span and its parent identity."""
    span_id = _span_id(span)
    if not span_id:
        raise InvalidImport("Phoenix span lacks trace_id or span_id")
    raw_attributes = _attributes(span)
    node_type, tool_name = _node_type(span, raw_attributes)
    status = _node_status(span)
    if node_type is NodeType.LLM_CALL:
        input_messages = _llm_messages(raw_attributes, "input")
        output_messages = _llm_messages(raw_attributes, "output")
    else:
        input_messages = output_messages = None
    attributes: dict[str, Any] = {"phoenix.attributes": raw_attributes}
    if span.get("events") not in (None, []):
        attributes["phoenix.events"] = span["events"]
    if span.get("annotations") not in (None, []):
        attributes["phoenix.annotations"] = span["annotations"]
    if span.get("notes") not in (None, []):
        attributes["phoenix.notes"] = span["notes"]
    parent_id = _parent_id(span)
    return (
        ImportedNode(
            external_id=f"{trace_id}:{span_id}",
            trace_id=trace_id,
            node_type=node_type,
            name=str(span.get("name") or span_id),
            status=status,
            error=(
                _error(span, raw_attributes) or "Phoenix span failed"
                if status is NodeStatus.FAILED
                else None
            ),
            started_at=_datetime(span.get("start_time")),
            ended_at=_datetime(span.get("end_time")),
            inputs=(
                input_messages
                if node_type is NodeType.LLM_CALL and input_messages is not None
                else _attribute(
                    raw_attributes,
                    "input.value",
                    "gen_ai.prompt",
                    "gen_ai.tool.call.arguments",
                    "gcp.vertex.agent.tool_call_args",
                    "gcp.vertex.agent.llm_request",
                )
            ),
            outputs=(
                output_messages
                if node_type is NodeType.LLM_CALL and output_messages is not None
                else _attribute(
                    raw_attributes,
                    "output.value",
                    "gen_ai.completion",
                    "gen_ai.tool.call.result",
                    "tool.result",
                    "gcp.vertex.agent.tool_response",
                    "gcp.vertex.agent.llm_response",
                )
            ),
            requested_model=(
                str(value)
                if (value := _attribute(raw_attributes, "gen_ai.request.model"))
                else None
            ),
            model=(
                str(value)
                if (
                    value := _attribute(
                        raw_attributes,
                        "gen_ai.response.model",
                        "llm.model_name",
                        "gen_ai.request.model",
                    )
                )
                else None
            ),
            model_provider=(
                str(value)
                if (
                    value := _attribute(
                        raw_attributes,
                        "gen_ai.provider.name",
                        "gen_ai.system",
                        "llm.provider",
                        "llm.system",
                    )
                )
                else None
            ),
            tokens=_tokens(raw_attributes),
            cost=_decimal(
                _attribute(
                    raw_attributes,
                    "gen_ai.usage.cost",
                    "gen_ai.cost.total",
                    "llm.cost.total",
                    "operation.cost",
                )
            ),
            model_params=(
                value
                if isinstance(
                    value := _attribute(
                        raw_attributes,
                        "gen_ai.request.parameters",
                        "llm.invocation_parameters",
                        "model_request_parameters",
                    ),
                    dict,
                )
                else None
            ),
            tool_name=tool_name,
            attributes=attributes,
            metadata={
                "phoenix.span_id": span_id,
                "phoenix.span_kind": span.get("span_kind"),
            },
            children=[],
        ),
        f"{trace_id}:{parent_id}" if parent_id else None,
    )


class PhoenixTraceImporter:
    """Normalize Arize Phoenix UI and CLI trace exports."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedSession | ImportFailure]:
        """Parse Phoenix traces into one Kitaru session per trace."""
        del params
        values, failures = _parse_values(content)
        traces, trace_metadata, trace_lines, expansion_failures = _expand_values(values)
        failures.extend(expansion_failures)
        # traces preserves the payload's span order, so iterating it
        # directly emits sessions in first-appearance order.
        for trace_id, spans in traces.items():
            try:
                session = self._parse_trace(
                    trace_id, spans, trace_metadata.get(trace_id, {})
                )
                session.model_dump_json()
                yield session
            except (InvalidImport, PydanticSerializationError) as exc:
                yield ImportFailure(
                    line=trace_lines[trace_id],
                    external_id=_escape_failure_text(trace_id),
                    error=_escape_failure_text(str(exc)),
                )
        yield from failures

    def _parse_trace(
        self,
        trace_id: str,
        spans: list[dict[str, Any]],
        trace_metadata: dict[str, Any],
    ) -> ImportedSession:
        """Normalize one Phoenix trace."""
        if not spans:
            raise InvalidImport("Phoenix trace contains no spans")
        for span in spans:
            span_trace_id = _trace_id(span)
            if span_trace_id and span_trace_id != trace_id:
                raise InvalidImport(
                    f"Phoenix trace '{trace_id}' contains span from trace "
                    f"'{span_trace_id}'"
                )
            if not _span_id(span):
                raise InvalidImport("Phoenix span lacks trace_id or span_id")

        ordered = sorted(
            spans,
            key=lambda span: (
                _datetime(span.get("start_time")) or datetime.min.replace(tzinfo=UTC),
                _span_id(span) or "",
            ),
        )
        nodes_with_parents = [_node(span, trace_id) for span in ordered]
        node_ids = {node.external_id for node, _ in nodes_with_parents}
        root_indexes = [
            index
            for index, (_, parent_id) in enumerate(nodes_with_parents)
            if parent_id is None or parent_id not in node_ids
        ]
        true_root_indexes = [
            index
            for index, (_, parent_id) in enumerate(nodes_with_parents)
            if parent_id is None
        ]
        root_index = (true_root_indexes or root_indexes or [0])[0]
        root = ordered[root_index]
        root_node = nodes_with_parents[root_index][0]
        warnings: list[str] = []
        if len(root_indexes) != 1:
            warnings.append(f"Trace '{trace_id}' has {len(root_indexes)} root spans")
        nodes = _build_tree(nodes_with_parents, warnings)
        session_status = (
            SessionStatus.FAILED
            if root_node.status is NodeStatus.FAILED
            else SessionStatus.COMPLETED
        )
        metadata: dict[str, Any] = {
            "phoenix.trace_id": trace_id,
            "source_trace_count": 1,
            "source_completeness": "full" if not warnings else "partial",
            "normalization_warnings": warnings,
        }
        for key, value in trace_metadata.items():
            metadata[f"phoenix.{key}"] = value
        return ImportedSession(
            external_id=trace_id,
            name=str(root.get("name") or trace_id),
            status=session_status,
            inputs=(
                root_node.inputs
                if root_node.inputs is not None
                else next(
                    (
                        node.inputs
                        for node, _ in nodes_with_parents
                        if node.inputs is not None
                    ),
                    None,
                )
            ),
            outputs=(
                root_node.outputs
                if root_node.outputs is not None
                else next(
                    (
                        node.outputs
                        for node, _ in reversed(nodes_with_parents)
                        if node.outputs is not None
                    ),
                    None,
                )
            ),
            error=root_node.error if session_status is SessionStatus.FAILED else None,
            started_at=min(
                (node.started_at for node, _ in nodes_with_parents if node.started_at),
                default=None,
            ),
            ended_at=max(
                (node.ended_at for node, _ in nodes_with_parents if node.ended_at),
                default=None,
            ),
            metadata=metadata,
            framework=_framework(ordered),
            nodes=nodes,
        )


def parse(
    content: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse an Arize Phoenix trace export through the importer contract."""
    yield from PhoenixTraceImporter().parse(content, params)
