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
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""OpenTelemetry OTLP JSON trace importer plugin."""

import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ParsedNode,
    ParsedSession,
    detect_framework,
    populate_node_display_fields,
)

MAX_UPLOAD_BYTES = 64 * 1024 * 1024
_TRACE_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_SPAN_ID = re.compile(r"^[0-9a-fA-F]{16}$")
_CONVERSATION_KEYS = (
    "gen_ai.conversation.id",
    "session.id",
    "conversation.id",
    "messaging.message.conversation_id",
)
_INPUT_KEYS = (
    "gen_ai.input.messages",
    "gen_ai.prompt",
    "gen_ai.request.messages",
    "llm.prompts",
    "input.value",
    "input",
    "agent.input",
    "pydantic_ai.all_messages",
)
_OUTPUT_KEYS = (
    "gen_ai.output.messages",
    "gen_ai.completion",
    "gen_ai.response.messages",
    "llm.completions",
    "output.value",
    "output",
    "agent.output",
    "final_result",
)
_TOOL_INPUT_KEYS = (
    "gen_ai.tool.call.arguments",
    "tool.call.arguments",
    "tool.arguments",
)
_TOOL_OUTPUT_KEYS = (
    "gen_ai.tool.call.result",
    "tool.call.result",
    "tool.result",
)
_METADATA_KEYS = {
    "deployment.environment",
    "deployment.environment.name",
    "gen_ai.agent.id",
    "gen_ai.agent.name",
    "gen_ai.operation.name",
    "gen_ai.provider.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.system",
    "gen_ai.tool.call.id",
    "gen_ai.tool.name",
    "gen_ai.workflow.name",
    "service.instance.id",
    "service.name",
    "service.namespace",
    "service.version",
    "telemetry.sdk.language",
    "telemetry.sdk.name",
    "telemetry.sdk.version",
    "tool.name",
}
_LLM_OPERATIONS = {
    "chat",
    "create_agent",
    "embeddings",
    "generate_content",
    "invoke_agent",
    "text_completion",
}


class InvalidImport(ValueError):
    """Raised when an OTLP payload cannot be parsed."""


@dataclass(frozen=True, slots=True)
class _Turn:
    """One source trace within a multi-turn session."""

    trace_id: str
    inputs: Any = None
    outputs: Any = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


class _SpanRecord(BaseModel):
    """Decoded OTLP span with its resource and scope context."""

    model_config = ConfigDict(extra="forbid")

    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    name: str
    kind: Any = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    resource_attributes: dict[str, Any] = Field(default_factory=dict)
    scope_name: str | None = None
    scope_version: str | None = None
    scope_schema_url: str | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    link_count: int = 0
    dropped_attributes_count: int = 0
    dropped_events_count: int = 0
    dropped_links_count: int = 0
    status_code: Any = None
    status_message: str | None = None
    raw_digest: str


def _first(record: dict[str, Any], *names: str) -> Any:
    """Return the first present field."""
    for name in names:
        if name in record:
            return record[name]
    return None


def _canonical_digest(value: Any) -> str:
    """Hash one normalized JSON-compatible value."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _decode_json(value: Any) -> Any:
    """Decode structured JSON strings while preserving ordinary strings."""
    if not isinstance(value, str):
        return value
    candidate = value.strip()
    if not candidate or candidate[0] not in '[{"':
        return value
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return value


def _decode_any_value(value: Any) -> Any:
    """Decode an OTLP AnyValue JSON object."""
    if not isinstance(value, dict):
        return value
    for key in (
        "stringValue",
        "string_value",
        "boolValue",
        "bool_value",
        "intValue",
        "int_value",
        "doubleValue",
        "double_value",
        "bytesValue",
        "bytes_value",
    ):
        if key in value:
            decoded = value[key]
            if key in {"intValue", "int_value"}:
                try:
                    return int(decoded)
                except (TypeError, ValueError):
                    return decoded
            return _decode_json(decoded)
    array = _first(value, "arrayValue", "array_value")
    if isinstance(array, dict):
        values = array.get("values", [])
        return [_decode_any_value(item) for item in values]
    kvlist = _first(value, "kvlistValue", "kvlist_value")
    if isinstance(kvlist, dict):
        return _decode_attributes(kvlist.get("values", []))
    return None


def _decode_attributes(value: Any) -> dict[str, Any]:
    """Decode an OTLP KeyValue list."""
    if not isinstance(value, list):
        return {}
    attributes: dict[str, Any] = {}
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            continue
        attributes[item["key"]] = _decode_any_value(item.get("value"))
    return attributes


def _timestamp(value: Any) -> datetime | None:
    """Convert OTLP epoch nanoseconds to UTC."""
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1_000_000_000, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _datetime(value: Any) -> datetime | None:
    """Convert an OTLP timestamp or an ISO timestamp to UTC."""
    parsed = _timestamp(value)
    if parsed is not None:
        return parsed
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _integer(value: Any) -> int | None:
    """Parse an integer field."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal | None:
    """Parse a decimal field."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_requests(content: bytes) -> list[dict[str, Any]]:
    """Parse an OTLP JSON request, request array, or JSONL stream."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("OTLP import exceeds the 64 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("OTLP import must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("OTLP import contains no JSON records")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        requests: list[dict[str, Any]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise InvalidImport(
                    f"Line {line_number} is not valid OTLP JSON"
                ) from exc
            if not isinstance(item, dict):
                raise InvalidImport(
                    f"Line {line_number} must contain a JSON object"
                ) from None
            requests.append(item)
    else:
        if isinstance(decoded, dict):
            requests = [decoded]
        elif isinstance(decoded, list) and all(
            isinstance(item, dict) for item in decoded
        ):
            requests = decoded
        else:
            raise InvalidImport("OTLP JSON must contain an object or object array")
    if not requests:
        raise InvalidImport("OTLP import contains no JSON records")
    return requests


def _event(record: Any) -> dict[str, Any] | None:
    """Decode one OTLP span event."""
    if not isinstance(record, dict):
        return None
    name = record.get("name")
    if not isinstance(name, str):
        return None
    attributes = record.get("attributes")
    return {
        "name": name,
        "time": _datetime(
            _first(
                record,
                "timeUnixNano",
                "time_unix_nano",
                "timestamp",
                "time",
            )
        ),
        "attributes": (
            attributes
            if isinstance(attributes, dict)
            else _decode_attributes(attributes)
        ),
    }


def _span_record(
    span: Any,
    resource_attributes: dict[str, Any],
    scope: dict[str, Any],
    scope_schema_url: str | None,
) -> _SpanRecord:
    """Decode and validate one OTLP span."""
    if not isinstance(span, dict):
        raise InvalidImport("OTLP span must be a JSON object")
    trace_id = str(_first(span, "traceId", "trace_id") or "").lower()
    span_id = str(_first(span, "spanId", "span_id") or "").lower()
    parent = str(_first(span, "parentSpanId", "parent_span_id") or "").lower()
    if not _TRACE_ID.fullmatch(trace_id):
        raise InvalidImport(f"OTLP span has invalid trace id '{trace_id}'")
    if not _SPAN_ID.fullmatch(span_id):
        raise InvalidImport(f"Trace '{trace_id}' contains invalid span id '{span_id}'")
    if parent and not _SPAN_ID.fullmatch(parent):
        raise InvalidImport(
            f"Span '{span_id}' contains invalid parent span id '{parent}'"
        )
    status = span.get("status") if isinstance(span.get("status"), dict) else {}
    events = [decoded for item in span.get("events", []) if (decoded := _event(item))]
    links = span.get("links") if isinstance(span.get("links"), list) else []
    return _SpanRecord(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent or None,
        name=str(span.get("name") or "span"),
        kind=span.get("kind"),
        start_time=_timestamp(
            _first(span, "startTimeUnixNano", "start_time_unix_nano")
        ),
        end_time=_timestamp(_first(span, "endTimeUnixNano", "end_time_unix_nano")),
        attributes=_decode_attributes(span.get("attributes")),
        resource_attributes=resource_attributes,
        scope_name=str(scope["name"]) if scope.get("name") is not None else None,
        scope_version=(
            str(scope["version"]) if scope.get("version") is not None else None
        ),
        scope_schema_url=scope_schema_url,
        events=events,
        link_count=len(links),
        dropped_attributes_count=_integer(
            _first(span, "droppedAttributesCount", "dropped_attributes_count")
        )
        or 0,
        dropped_events_count=_integer(
            _first(span, "droppedEventsCount", "dropped_events_count")
        )
        or 0,
        dropped_links_count=_integer(
            _first(span, "droppedLinksCount", "dropped_links_count")
        )
        or 0,
        status_code=_first(status, "code"),
        status_message=(str(status["message"]) if status.get("message") else None),
        raw_digest=_canonical_digest(span),
    )


def _flat_span_record(record: dict[str, Any]) -> _SpanRecord:
    """Decode one flattened OTLP, Arize, or Logfire span record."""
    context_value = record.get("context")
    context = context_value if isinstance(context_value, dict) else {}
    attributes_value = record.get("attributes")
    attributes = (
        dict(attributes_value)
        if isinstance(attributes_value, dict)
        else _decode_attributes(attributes_value)
    )
    resource_value = _first(
        record,
        "resource_attributes",
        "otel_resource_attributes",
    )
    resource_attributes = (
        dict(resource_value) if isinstance(resource_value, dict) else {}
    )
    top_level_resource_keys = {
        "deployment_environment": "deployment.environment.name",
        "service_instance_id": "service.instance.id",
        "service_name": "service.name",
        "service_namespace": "service.namespace",
        "service_version": "service.version",
        "telemetry_sdk_language": "telemetry.sdk.language",
        "telemetry_sdk_name": "telemetry.sdk.name",
        "telemetry_sdk_version": "telemetry.sdk.version",
    }
    for source_key, target_key in top_level_resource_keys.items():
        value = record.get(source_key)
        if value not in (None, ""):
            resource_attributes.setdefault(target_key, value)
    events_value = _first(record, "events", "otel_events")
    events = [
        decoded for item in events_value or [] if (decoded := _event(item)) is not None
    ]
    exception_message = record.get("exception_message")
    if exception_message:
        events.append(
            {
                "name": "exception",
                "time": None,
                "attributes": {
                    "exception.message": exception_message,
                    "exception.type": record.get("exception_type"),
                    "exception.stacktrace": record.get("exception_stacktrace"),
                },
            }
        )
    flattened = {
        "traceId": record.get("trace_id") or context.get("trace_id"),
        "spanId": record.get("span_id") or context.get("span_id"),
        "parentSpanId": _first(record, "parent_span_id", "parent_id"),
        "name": _first(record, "name", "span_name"),
        "kind": _first(record, "kind", "span_kind"),
        "startTimeUnixNano": record.get("start_time_unix_nano"),
        "endTimeUnixNano": record.get("end_time_unix_nano"),
        "attributes": [],
    }
    base = _span_record(flattened, resource_attributes, {}, None)
    status_value = record.get("status")
    status: dict[str, Any] = status_value if isinstance(status_value, dict) else {}
    scope_attributes = record.get("otel_scope_attributes")
    scope_schema_url = None
    if isinstance(scope_attributes, dict):
        value = _first(scope_attributes, "schema_url", "schemaUrl")
        scope_schema_url = str(value) if value else None
    return base.model_copy(
        update={
            "start_time": _datetime(
                _first(
                    record,
                    "start_time_unix_nano",
                    "start_timestamp",
                    "start_time",
                )
            ),
            "end_time": _datetime(
                _first(
                    record,
                    "end_time_unix_nano",
                    "end_timestamp",
                    "end_time",
                )
            ),
            "attributes": attributes,
            "scope_name": _first(record, "scope_name", "otel_scope_name"),
            "scope_version": _first(record, "scope_version", "otel_scope_version"),
            "scope_schema_url": scope_schema_url,
            "events": events,
            "link_count": len(_first(record, "links", "otel_links") or []),
            "dropped_attributes_count": _integer(record.get("dropped_attributes_count"))
            or 0,
            "dropped_events_count": _integer(record.get("dropped_events_count")) or 0,
            "dropped_links_count": _integer(record.get("dropped_links_count")) or 0,
            "status_code": _first(record, "otel_status_code", "status_code")
            or status.get("code"),
            "status_message": _first(record, "otel_status_message", "status_message")
            or status.get("message"),
            "raw_digest": _canonical_digest(record),
        }
    )


def _decode_spans(
    requests: list[dict[str, Any]],
) -> tuple[list[_SpanRecord], list[ImportFailure]]:
    """Decode spans and isolate malformed span records."""
    spans: list[_SpanRecord] = []
    errors: list[ImportFailure] = []

    def add_error(message: str, source_id: str | None = None) -> None:
        """Record one isolated decoding failure."""
        errors.append(
            ImportFailure(
                line=len(errors) + 1,
                external_id=source_id,
                error=message,
            )
        )

    for request in requests:
        resource_spans = _first(request, "resourceSpans", "resource_spans")
        if resource_spans is None:
            context = request.get("context")
            has_context_ids = (
                isinstance(context, dict)
                and context.get("trace_id")
                and context.get("span_id")
            )
            if not has_context_ids and (
                not request.get("trace_id") or not request.get("span_id")
            ):
                raise InvalidImport(
                    "Each OTLP request must contain resourceSpans or a flattened "
                    "trace and span id"
                )
            try:
                spans.append(_flat_span_record(request))
            except InvalidImport as exc:
                source_id_value = request.get("trace_id") or (
                    context.get("trace_id") if isinstance(context, dict) else None
                )
                add_error(str(exc), str(source_id_value) if source_id_value else None)
            continue
        if not isinstance(resource_spans, list):
            raise InvalidImport("resourceSpans must be a JSON array")
        for resource_group in resource_spans:
            if not isinstance(resource_group, dict):
                add_error("Invalid resourceSpans item")
                continue
            resource = resource_group.get("resource")
            resource_attributes = _decode_attributes(
                resource.get("attributes") if isinstance(resource, dict) else []
            )
            scope_groups = _first(
                resource_group,
                "scopeSpans",
                "scope_spans",
                "instrumentationLibrarySpans",
            )
            if not isinstance(scope_groups, list):
                add_error("resourceSpans item has no scopeSpans")
                continue
            for scope_group in scope_groups:
                if not isinstance(scope_group, dict):
                    add_error("Invalid scopeSpans item")
                    continue
                scope = _first(scope_group, "scope", "instrumentationLibrary")
                if not isinstance(scope, dict):
                    scope = {}
                schema_url_value = _first(scope_group, "schemaUrl", "schema_url")
                schema_url = str(schema_url_value) if schema_url_value else None
                raw_spans = scope_group.get("spans")
                if not isinstance(raw_spans, list):
                    add_error("scopeSpans item has no spans array")
                    continue
                for raw_span in raw_spans:
                    try:
                        spans.append(
                            _span_record(
                                raw_span, resource_attributes, scope, schema_url
                            )
                        )
                    except InvalidImport as exc:
                        source_id = None
                        if isinstance(raw_span, dict):
                            source_id_value = _first(raw_span, "traceId", "trace_id")
                            source_id = (
                                str(source_id_value) if source_id_value else None
                            )
                        add_error(str(exc), source_id)
    return spans, errors


def _attribute(record: _SpanRecord, *keys: str) -> Any:
    """Return a span or resource attribute."""
    for key in keys:
        for source in (record.attributes, record.resource_attributes):
            value = source.get(key)
            if value not in (None, ""):
                return value
    return None


def _content(record: _SpanRecord, keys: tuple[str, ...]) -> Any:
    """Return content from span attributes or events."""
    for key in keys:
        value = record.attributes.get(key)
        if value is not None:
            return _decode_json(value)
    for event in record.events:
        for key in keys:
            value = event["attributes"].get(key)
            if value is not None:
                return _decode_json(value)
    return None


def _source_instance(record: _SpanRecord, params: dict[str, Any]) -> tuple[str, bool]:
    """Resolve a source instance and whether a fallback was needed."""
    selected_source = params.get("source_instance")
    if selected_source not in (None, ""):
        return str(selected_source), False
    service_name = _attribute(record, "service.name")
    namespace = _attribute(record, "service.namespace")
    if service_name:
        if namespace:
            return f"{namespace}/{service_name}", False
        return str(service_name), False
    filename = params.get("filename")
    if isinstance(filename, str):
        stem = Path(filename).stem.strip()
        if stem:
            return stem, True
    return "otlp", True


def _node_type(record: _SpanRecord) -> tuple[NodeType, str | None]:
    """Map semantic conventions to a Kitaru node type."""
    operation = str(_attribute(record, "gen_ai.operation.name") or "").lower()
    tool_name_value = _attribute(record, "gen_ai.tool.name", "tool.name")
    tool_name = str(tool_name_value) if tool_name_value else None
    if operation == "execute_tool" or tool_name:
        return NodeType.TOOL_CALL, tool_name
    if operation in _LLM_OPERATIONS or _attribute(
        record,
        "gen_ai.request.model",
        "gen_ai.response.model",
        "gen_ai.provider.name",
        "gen_ai.system",
    ):
        return NodeType.LLM_CALL, None
    return NodeType.SPAN, None


def _node_status(record: _SpanRecord) -> NodeStatus:
    """Map an OTLP status code to a Kitaru status."""
    code = record.status_code
    if code == 2 or str(code).upper() in {"2", "STATUS_CODE_ERROR", "ERROR"}:
        return NodeStatus.FAILED
    return NodeStatus.COMPLETED


def _error(record: _SpanRecord) -> str | None:
    """Return the OTLP status or exception message."""
    if record.status_message:
        return record.status_message
    for event in record.events:
        if event["name"] == "exception":
            message = event["attributes"].get("exception.message")
            if message:
                return str(message)
    return None


def _tokens(record: _SpanRecord) -> TokenUsage | None:
    """Map GenAI token usage attributes."""
    counts = (
        _integer(
            _attribute(
                record,
                "gen_ai.usage.input_tokens",
                "gen_ai.usage.prompt_tokens",
                "llm.usage.prompt_tokens",
            )
        ),
        _integer(
            _attribute(
                record,
                "gen_ai.usage.output_tokens",
                "gen_ai.usage.completion_tokens",
                "llm.usage.completion_tokens",
            )
        ),
        _integer(
            _attribute(
                record,
                "gen_ai.usage.cached_input_tokens",
                "gen_ai.usage.cache_read_input_tokens",
            )
        ),
        _integer(_attribute(record, "gen_ai.usage.reasoning_tokens")),
    )
    if all(value is None for value in counts):
        return None
    return TokenUsage(
        input_tokens=counts[0],
        output_tokens=counts[1],
        cached_input_tokens=counts[2],
        reasoning_tokens=counts[3],
    )


def _cost(record: _SpanRecord) -> Decimal | None:
    """Map a total cost attribute when present."""
    return _decimal(
        _attribute(
            record,
            "gen_ai.usage.cost",
            "gen_ai.cost.total",
            "llm.usage.total_cost",
            "cost.total",
        )
    )


def _model_params(record: _SpanRecord) -> dict[str, Any] | None:
    """Map bounded GenAI request parameters."""
    excluded = {"gen_ai.request.messages", "gen_ai.request.model"}
    selected = {
        key.removeprefix("gen_ai.request."): value
        for key, value in record.attributes.items()
        if key.startswith("gen_ai.request.") and key not in excluded
    }
    return selected or None


def _source_metadata(record: _SpanRecord) -> dict[str, Any]:
    """Return bounded operational metadata without arbitrary attributes."""
    selected: dict[str, Any] = {}
    for source in (record.resource_attributes, record.attributes):
        for key in _METADATA_KEYS:
            if key in source:
                selected[f"otlp.{key}"] = source[key]
    if record.scope_name:
        selected["otlp.scope.name"] = record.scope_name
    if record.scope_version:
        selected["otlp.scope.version"] = record.scope_version
    if record.scope_schema_url:
        selected["otlp.scope.schema_url"] = record.scope_schema_url
    selected.update(
        {
            "otlp.span.kind": record.kind,
            "otlp.event.names": [event["name"] for event in record.events],
            "otlp.link_count": record.link_count,
            "otlp.dropped_attributes_count": record.dropped_attributes_count,
            "otlp.dropped_events_count": record.dropped_events_count,
            "otlp.dropped_links_count": record.dropped_links_count,
        }
    )
    return {key: value for key, value in selected.items() if value is not None}


def _find_cycle(records: list[_SpanRecord]) -> bool:
    """Return whether parent references contain a cycle."""
    parents = {record.span_id: record.parent_span_id for record in records}
    for span_id in parents:
        seen: set[str] = set()
        current: str | None = span_id
        while current in parents:
            if current in seen:
                return True
            seen.add(current)
            current = parents[current]
    return False


def _build_node_tree(
    nodes_with_parents: list[tuple[ParsedNode, str | None]],
) -> list[ParsedNode]:
    """Build the parsed-node tree."""
    converted = {
        node.external_id: node
        for node, _ in nodes_with_parents
        if node.external_id is not None
    }
    if len(converted) != len(nodes_with_parents):
        raise InvalidImport("The parsed node graph contains duplicate external ids")
    roots: list[ParsedNode] = []
    for node, parent_external_id in nodes_with_parents:
        if parent_external_id in converted:
            converted[parent_external_id].children.append(node)
        else:
            roots.append(node)
    return roots


def _parse_session(
    source_id: str,
    traces: list[tuple[str, list[_SpanRecord]]],
    source_instance: str,
    fallback_source: bool,
) -> ParsedSession:
    """Parse one conversation or trace-derived session."""
    warnings: list[str] = []
    if fallback_source:
        warnings.append("Source instance was derived from the filename or OTLP")
    turns: list[_Turn] = []
    nodes_with_parents: list[tuple[ParsedNode, str | None]] = []
    roots_by_trace: dict[str, _SpanRecord] = {}
    for trace_id, records in traces:
        ids = {record.span_id for record in records}
        if _find_cycle(records):
            raise InvalidImport(f"Trace '{trace_id}' contains a parent cycle")
        ordered = sorted(
            records,
            key=lambda record: (
                record.start_time or datetime.min.replace(tzinfo=UTC),
                record.span_id,
            ),
        )
        roots = [record for record in ordered if record.parent_span_id not in ids]
        if not roots:
            raise InvalidImport(f"Trace '{trace_id}' contains no root span")
        if len(roots) != 1:
            warnings.append(f"Trace '{trace_id}' has {len(roots)} root spans")
        for record in roots:
            if record.parent_span_id:
                warnings.append(f"Span '{record.span_id}' references a missing parent")
        root = roots[0]
        roots_by_trace[trace_id] = root
        trace_start = min(
            (record.start_time for record in ordered if record.start_time), default=None
        )
        trace_end = max(
            (record.end_time for record in ordered if record.end_time), default=None
        )
        root_inputs = _content(root, _INPUT_KEYS)
        root_outputs = _content(root, _OUTPUT_KEYS)
        turns.append(
            _Turn(
                trace_id=trace_id,
                inputs=root_inputs,
                outputs=root_outputs,
                started_at=trace_start,
                ended_at=trace_end,
            )
        )
        for record in ordered:
            node_type, tool_name = _node_type(record)
            input_keys = _TOOL_INPUT_KEYS + _INPUT_KEYS if tool_name else _INPUT_KEYS
            output_keys = (
                _TOOL_OUTPUT_KEYS + _OUTPUT_KEYS if tool_name else _OUTPUT_KEYS
            )
            status = _node_status(record)
            parent_external_id = (
                f"{trace_id}:{record.parent_span_id}"
                if record.parent_span_id in ids
                else None
            )
            nodes_with_parents.append(
                (
                    ParsedNode(
                        external_id=f"{trace_id}:{record.span_id}",
                        trace_id=trace_id,
                        node_type=node_type,
                        name=record.name,
                        status=status,
                        error=_error(record) if status is NodeStatus.FAILED else None,
                        started_at=record.start_time,
                        ended_at=record.end_time,
                        inputs=_content(record, input_keys),
                        outputs=_content(record, output_keys),
                        requested_model=(
                            str(value)
                            if (value := _attribute(record, "gen_ai.request.model"))
                            else None
                        ),
                        model=(
                            str(value)
                            if (
                                value := _attribute(
                                    record,
                                    "gen_ai.response.model",
                                    "gen_ai.request.model",
                                )
                            )
                            else None
                        ),
                        provider=(
                            str(value)
                            if (
                                value := _attribute(
                                    record, "gen_ai.provider.name", "gen_ai.system"
                                )
                            )
                            else None
                        ),
                        tokens=_tokens(record),
                        cost=_cost(record),
                        model_params=_model_params(record),
                        tool_name=tool_name,
                        attributes={"otlp.span.kind": record.kind},
                        metadata=_source_metadata(record),
                        children=[],
                    ),
                    parent_external_id,
                )
            )
    turns.sort(
        key=lambda turn: (
            turn.started_at or datetime.min.replace(tzinfo=UTC),
            turn.trace_id,
        )
    )
    nodes_with_parents.sort(
        key=lambda item: (
            item[0].started_at or datetime.min.replace(tzinfo=UTC),
            item[0].external_id or "",
        )
    )
    nodes = [node for node, _ in nodes_with_parents]
    system_prompt = populate_node_display_fields(nodes)
    latest_turn = turns[-1]
    latest_root = roots_by_trace[latest_turn.trace_id]
    session_status = (
        SessionStatus.FAILED
        if _node_status(latest_root) is NodeStatus.FAILED
        else SessionStatus.COMPLETED
    )
    session_error = (
        _error(latest_root) if session_status is SessionStatus.FAILED else None
    )
    inputs = {
        "schema_version": 1,
        "turns": [
            {
                "source_trace_id": turn.trace_id,
                "inputs": turn.inputs,
                "outputs": turn.outputs,
            }
            for turn in turns
        ],
    }
    all_records = [record for _, records in traces for record in records]
    metadata_values: dict[str, list[str]] = {}
    for key in (
        "deployment.environment.name",
        "service.name",
        "telemetry.sdk.name",
        "telemetry.sdk.version",
    ):
        values = sorted(
            {
                str(value)
                for record in all_records
                if (value := _attribute(record, key)) is not None
            }
        )
        if values:
            metadata_values[f"otlp.{key}s"] = values
    metadata = {
        "otlp.session_id": source_id,
        "otlp.trace_ids": [turn.trace_id for turn in turns],
        "source_trace_count": len(turns),
        "source_completeness": "unknown",
        **metadata_values,
        "normalization_warnings": warnings,
    }
    framework = detect_framework(
        [
            {
                "attributes": record.attributes,
                "resource_attributes": record.resource_attributes,
                "scope_name": record.scope_name,
            }
            for record in all_records
        ]
    )
    return ParsedSession(
        external_id=f"{source_instance}:{source_id}",
        name=latest_root.name,
        status=session_status,
        system_prompt=system_prompt,
        inputs=inputs,
        outputs=latest_turn.outputs,
        error=session_error,
        started_at=min(
            (turn.started_at for turn in turns if turn.started_at), default=None
        ),
        ended_at=max((turn.ended_at for turn in turns if turn.ended_at), default=None),
        metadata=metadata,
        framework=framework,
        nodes=_build_node_tree(nodes_with_parents),
    )


class OTLPJSONImporter:
    """Parse OTLP ExportTraceServiceRequest JSON and JSONL."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> list[ParsedSession | ImportFailure]:
        """Parse OTLP JSON or JSONL into Kitaru sessions."""
        spans, errors = _decode_spans(_parse_requests(content))
        if not spans:
            if errors:
                return [*errors]
            raise InvalidImport("OTLP import contains no spans")
        traces: dict[str, list[_SpanRecord]] = defaultdict(list)
        conflicting_traces: set[str] = set()
        seen: dict[tuple[str, str], _SpanRecord] = {}
        for record in spans:
            key = (record.trace_id, record.span_id)
            existing = seen.get(key)
            if existing:
                if existing.raw_digest != record.raw_digest:
                    conflicting_traces.add(record.trace_id)
                continue
            seen[key] = record
            traces[record.trace_id].append(record)
        for trace_id in sorted(conflicting_traces):
            traces.pop(trace_id, None)
            errors.append(
                ImportFailure(
                    line=len(errors) + 1,
                    external_id=trace_id,
                    error=f"Trace '{trace_id}' contains conflicting duplicate spans",
                )
            )

        grouped: dict[tuple[str, str], list[tuple[str, list[_SpanRecord]]]] = (
            defaultdict(list)
        )
        fallback_sources: dict[tuple[str, str], bool] = {}
        for trace_id, records in traces.items():
            conversation_ids = {
                str(value)
                for record in records
                for key in _CONVERSATION_KEYS
                if (value := _attribute(record, key)) not in (None, "")
            }
            if len(conversation_ids) > 1:
                errors.append(
                    ImportFailure(
                        line=len(errors) + 1,
                        external_id=trace_id,
                        error=(
                            f"Trace '{trace_id}' contains conflicting conversation ids"
                        ),
                    )
                )
                continue
            source_id = next(iter(conversation_ids), trace_id)
            root = min(
                records,
                key=lambda record: (
                    record.start_time or datetime.min.replace(tzinfo=UTC),
                    record.span_id,
                ),
            )
            source_instance, fallback = _source_instance(root, params)
            key = (source_instance, source_id)
            grouped[key].append((trace_id, records))
            fallback_sources[key] = fallback_sources.get(key, False) or fallback

        sessions: list[ParsedSession] = []
        for (source_instance, source_id), grouped_traces in sorted(grouped.items()):
            try:
                sessions.append(
                    _parse_session(
                        source_id,
                        grouped_traces,
                        source_instance,
                        fallback_sources[(source_instance, source_id)],
                    )
                )
            except InvalidImport as exc:
                errors.append(
                    ImportFailure(
                        line=len(errors) + 1,
                        external_id=source_id,
                        error=str(exc),
                    )
                )
        return [*sessions, *errors]


def parse(
    content: bytes,
    params: dict[str, Any],
) -> Iterator[ParsedSession | ImportFailure]:
    """Parse OTLP JSON or JSONL through the unified importer contract."""
    yield from OTLPJSONImporter().parse(content, params)
