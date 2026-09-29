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
"""MLflow trace importer plugin."""

import base64
import binascii
import json
import re
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator
from contextlib import aclosing
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple

from pydantic_core import PydanticSerializationError

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession

MAX_PARENT_DEPTH = 64
_SESSION_METADATA_KEY = "mlflow.trace.session"
_DEFAULT_JOIN_PATHS = (
    f"info.trace_metadata.{_SESSION_METADATA_KEY}",
    f"info.request_metadata.{_SESSION_METADATA_KEY}",
)
_LLM_SPAN_TYPES = {"LLM", "CHAT_MODEL"}
_TOOL_SPAN_TYPE = "TOOL"
_SPAN_TYPE_KEY = "mlflow.spanType"
_INPUTS_KEY = "mlflow.spanInputs"
_OUTPUTS_KEY = "mlflow.spanOutputs"
_USAGE_KEY = "mlflow.chat.tokenUsage"
_COST_KEY = "mlflow.llm.cost"
_MODEL_KEY = "mlflow.llm.model"
_PROVIDER_KEY = "mlflow.llm.provider"
_MESSAGE_FORMAT_KEY = "mlflow.message.format"
_FUNCTION_NAME_KEY = "mlflow.spanFunctionName"
_USER_METADATA_KEY = "mlflow.trace.user"
_FRAMEWORK_PATTERNS = (
    (re.compile(r"pydantic[._ -]?ai", re.IGNORECASE), "pydantic-ai"),
    (re.compile(r"langgraph", re.IGNORECASE), "langgraph"),
    (re.compile(r"openai[._ -]?agents?", re.IGNORECASE), "openai-agents"),
    (re.compile(r"google[._ -]?adk", re.IGNORECASE), "google-adk"),
    (
        re.compile(r"claude[._ -]?agent[._ -]?sdk|claudeagentsdk", re.IGNORECASE),
        "claude-agent-sdk",
    ),
)


class InvalidImport(ValueError):
    """Raised when an MLflow payload cannot be normalized."""


@dataclass(frozen=True, slots=True)
class _TextMatch:
    """Text selected from a provider payload."""

    selector: str
    text: str


@dataclass(slots=True)
class _Span:
    """One decoded MLflow span."""

    span_id: str
    parent_id: str | None
    name: str
    started_at: datetime | None
    ended_at: datetime | None
    status_code: str
    status_message: str
    attributes: dict[str, Any]
    events: list[Any]
    children: list["_Span"] = field(default_factory=list)

    @property
    def span_type(self) -> str:
        """Return the MLflow span type, or UNKNOWN when absent."""
        value = self.attributes.get(_SPAN_TYPE_KEY)
        return value.upper() if isinstance(value, str) and value else "UNKNOWN"


@dataclass(frozen=True, slots=True)
class _Trace:
    """One MLflow trace with its decoded info and spans."""

    trace_id: str
    document: dict[str, Any]
    info: dict[str, Any]
    metadata: dict[str, Any]
    tags: dict[str, Any]
    state: str
    spans: list[_Span]


@dataclass(frozen=True, slots=True)
class _Turn:
    """One MLflow trace within a grouped session."""

    trace_id: str
    inputs: Any
    outputs: Any
    started_at: datetime | None
    ended_at: datetime | None


def _escape_failure_text(value: str) -> str:
    """Escape unencodable characters in failure diagnostics only."""
    return value.encode("utf-8", errors="backslashreplace").decode("utf-8")


def _decode_json(value: Any, *, scalars: bool = False) -> Any:
    """Decode a JSON-encoded string, returning other values unchanged.

    Without ``scalars``, only strings that look like an object, array, or
    string are decoded. MLflow JSON-encodes every span attribute, so span
    attributes decode scalars such as ``20`` and ``true`` too.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or (not scalars and stripped[0] not in '[{"'):
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value
    except (RecursionError, ValueError) as exc:
        raise InvalidImport("Embedded JSON exceeds decoding limits") from exc


def _dict(value: Any) -> dict[str, Any]:
    """Return a decoded dictionary or an empty dictionary."""
    decoded = _decode_json(value)
    return decoded if isinstance(decoded, dict) else {}


def _iso_datetime(value: Any) -> datetime | None:
    """Parse an ISO 8601 timestamp as an aware datetime."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _unix_datetime(value: Any, units_per_second: int) -> datetime | None:
    """Parse an integer Unix timestamp in the given unit as an aware datetime."""
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    seconds, remainder = divmod(number, units_per_second)
    try:
        return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(
            microseconds=remainder * 1_000_000 // units_per_second
        )
    except (OverflowError, OSError, ValueError):
        return None


def _parse_token_count(value: Any) -> int | None:
    """Parse a nonnegative token count."""
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise InvalidImport("Token count must be a nonnegative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidImport("Token count must be a nonnegative integer") from exc
    if parsed < 0 or (isinstance(value, float) and value != parsed):
        raise InvalidImport("Token count must be a nonnegative integer")
    return parsed


def _decimal(value: Any) -> Decimal | None:
    """Parse a finite nonnegative cost."""
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        raise InvalidImport("Cost must be finite and nonnegative")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise InvalidImport("Cost must be finite and nonnegative") from exc
    if not parsed.is_finite() or parsed < 0:
        raise InvalidImport("Cost must be finite and nonnegative")
    return parsed


def _normalize_id(value: Any) -> str | None:
    """Normalize a span id to lowercase hex.

    MLflow serializes span ids as base64 bytes, as bare hex in 3.5.0, and as
    0x-prefixed hex in the 2.x schema.
    """
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise InvalidImport("Span id must be a string")
    text = value.strip()
    if text.lower().startswith("0x"):
        text = text[2:]
    if re.fullmatch(r"[0-9a-fA-F]{16}|[0-9a-fA-F]{32}", text):
        return text.lower()
    try:
        return base64.b64decode(text, validate=True).hex()
    except (binascii.Error, ValueError) as exc:
        raise InvalidImport(f"Span id '{value}' is not hex or base64") from exc


def _path_parts(path: str) -> list[str]:
    """Parse a dotted path or RFC 6901 JSON Pointer."""
    if not path.strip():
        raise InvalidImport("join path must be non-empty")
    if not path.startswith("/"):
        return path.split(".")
    parts = path[1:].split("/")
    if any(re.search(r"~(?:[^01]|$)", part) for part in parts):
        raise InvalidImport("join path contains an invalid JSON Pointer escape")
    return [part.replace("~1", "/").replace("~0", "~") for part in parts]


def _path_value(document: dict[str, Any], path: str) -> Any:
    """Resolve a join path, including dotted metadata keys such as mlflow.trace.*."""
    parts = _path_parts(path)
    current: Any = document
    for index, part in enumerate(parts):
        current = _decode_json(current)
        if not isinstance(current, dict):
            return None
        if part in current:
            current = current[part]
            continue
        return current.get(".".join(parts[index:]))
    return _decode_json(current)


def get_default_join_value(document: dict[str, Any]) -> str | None:
    """Resolve the session join value at the default join paths for one trace.

    Args:
        document: MLflow trace dictionary with an ``info`` object.

    Returns:
        Value at the first default join path, None when none resolves.
    """
    match = _match_default_join(document)
    return match[0] if match is not None else None


def _match_default_join(document: dict[str, Any]) -> tuple[str, str] | None:
    """Return the value and path of the first default join path that resolves."""
    for path in _DEFAULT_JOIN_PATHS:
        value = _path_value(document, path)
        if value not in (None, ""):
            return str(value), path
    return None


def _join_value(trace: _Trace, params: dict[str, Any]) -> tuple[str, str, bool]:
    """Resolve the session join value, its path, and whether it fell back."""
    configured = params.get("join_on")
    if configured is not None:
        if not isinstance(configured, str):
            raise InvalidImport("join_on must be a dotted path or JSON pointer")
        value = _path_value(trace.document, configured)
        if value in (None, ""):
            raise InvalidImport(
                f"Trace '{trace.trace_id}' has no value at join path '{configured}'"
            )
        if isinstance(value, dict | list):
            raise InvalidImport(
                f"Trace '{trace.trace_id}' has a non-scalar value at join path "
                f"'{configured}'"
            )
        return str(value), configured, False
    match = _match_default_join(trace.document)
    if match is not None:
        return match[0], match[1], False
    return trace.trace_id, "trace_id", True


def _get_identity(value: Any, field_name: str) -> str | None:
    """Validate and normalize a source identity."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidImport(f"MLflow {field_name} must be a string")
    return value.strip() or None


def _get_embedded_experiment(trace: _Trace) -> str | None:
    """Return the experiment id a trace records in either trace info schema."""
    experiment = _dict(_dict(trace.info.get("trace_location")).get("mlflow_experiment"))
    return _get_identity(
        experiment["experiment_id"]
        if "experiment_id" in experiment
        else trace.info.get("experiment_id"),
        "experiment_id",
    )


def _get_source_instance(trace: _Trace, params: dict[str, Any]) -> str:
    """Resolve the source namespace that keeps session ids from colliding."""
    override = _get_identity(params.get("source_instance"), "source_instance")
    alias = _get_identity(params.get("experiment_id"), "experiment_id parameter")
    embedded = _get_embedded_experiment(trace)
    source_instance = override or alias or embedded
    if source_instance is None:
        raise InvalidImport(
            "No MLflow experiment identity supplied; retry with "
            '--params \'{"source_instance":"my-mlflow-experiment"}\''
        )
    return source_instance


def _parse_span(record: Any, index: int) -> _Span:
    """Decode one span in the MLflow 3 or 2.x span schema."""
    if not isinstance(record, dict):
        raise InvalidImport(f"Span {index} must be a JSON object")
    context = _dict(record.get("context"))
    span_id = _normalize_id(record.get("span_id") or context.get("span_id"))
    if span_id is None:
        raise InvalidImport(f"Span {index} lacks a span id")
    status = _dict(record.get("status"))
    status_code = str(status.get("code") or record.get("status_code") or "")
    status_message = str(status.get("message") or record.get("status_message") or "")
    raw_attributes = _dict(record.get("attributes"))
    events = record.get("events")
    return _Span(
        span_id=span_id,
        parent_id=_normalize_id(
            record.get("parent_span_id") or record.get("parent_id")
        ),
        name=str(record.get("name") or "span"),
        started_at=_unix_datetime(
            record.get("start_time_unix_nano", record.get("start_time")),
            1_000_000_000,
        ),
        ended_at=_unix_datetime(
            record.get("end_time_unix_nano", record.get("end_time")), 1_000_000_000
        ),
        status_code=status_code.upper().removeprefix("STATUS_CODE_"),
        status_message=status_message,
        attributes={
            str(key): _decode_json(value, scalars=True)
            for key, value in raw_attributes.items()
        },
        events=events if isinstance(events, list) else [],
    )


def _parse_trace(document: Any) -> _Trace:
    """Decode one MLflow trace dictionary."""
    if not isinstance(document, dict):
        raise InvalidImport("MLflow trace must be a JSON object")
    info = document.get("info")
    if not isinstance(info, dict):
        raise InvalidImport("MLflow trace lacks an info object")
    trace_id = info.get("trace_id") or info.get("request_id")
    if not isinstance(trace_id, str) or not trace_id.strip():
        raise InvalidImport("MLflow trace info lacks a trace_id")
    data = document.get("data")
    spans = data.get("spans") if isinstance(data, dict) else None
    if not isinstance(spans, list) or not spans:
        raise InvalidImport(
            f"Trace '{trace_id}' has no spans; export traces with spans included"
        )
    return _Trace(
        trace_id=trace_id.strip(),
        document=document,
        info=info,
        metadata=_dict(info.get("trace_metadata") or info.get("request_metadata")),
        tags=_dict(info.get("tags")),
        state=str(info.get("state") or info.get("status") or "").upper(),
        spans=[_parse_span(span, index) for index, span in enumerate(spans, 1)],
    )


def _parse_documents(content: bytes) -> tuple[list[Any], list[ImportFailure]]:
    """Split a payload into trace documents and isolated JSONL line failures.

    Accepts one trace, a list of traces, an ``mlflow traces search --output
    json`` page, or JSONL whose lines are any of those.
    """
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no JSON records")
    failures: list[ImportFailure] = []
    try:
        values: list[Any] = [json.loads(text)]
    except (ValueError, RecursionError):
        values = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                values.append(json.loads(line))
            except (ValueError, RecursionError):
                failures.append(
                    ImportFailure(
                        line=line_number, error=f"Line {line_number} is not valid JSON"
                    )
                )
        if not values:
            raise InvalidImport("Import file contains no valid JSON records") from None

    documents: list[Any] = []
    for value in values:
        if isinstance(value, dict) and isinstance(value.get("traces"), list):
            documents.extend(value["traces"])
        elif isinstance(value, list):
            documents.extend(value)
        else:
            documents.append(value)
    if not documents:
        raise InvalidImport("Import file contains no MLflow traces")
    return documents, failures


def _node_type(span: _Span, has_llm_descendant: bool) -> tuple[NodeType, str | None]:
    """Map an MLflow span type to a Kitaru node type and tool name."""
    if span.span_type in _LLM_SPAN_TYPES and not has_llm_descendant:
        return NodeType.LLM_CALL, None
    if span.span_type == _TOOL_SPAN_TYPE:
        tool_name = span.attributes.get(_FUNCTION_NAME_KEY) or span.name
        return NodeType.TOOL_CALL, str(tool_name)
    return NodeType.SPAN, None


def _node_status(span: _Span) -> NodeStatus:
    """Map an MLflow span status."""
    if span.status_code == "ERROR":
        return NodeStatus.FAILED
    if span.ended_at is None:
        return NodeStatus.IN_PROGRESS
    return NodeStatus.COMPLETED


def _span_error(span: _Span) -> str:
    """Return the error message of a failed span."""
    if span.status_message:
        return span.status_message
    for event in span.events:
        if not isinstance(event, dict) or event.get("name") != "exception":
            continue
        attributes = _dict(event.get("attributes"))
        message = attributes.get("exception.message")
        if message:
            kind = attributes.get("exception.type")
            return f"{kind}: {message}" if kind else str(message)
    return "MLflow span failed"


def _token_counts(span: _Span) -> dict[str, int]:
    """Map MLflow's normalized token usage to the token fields it records."""
    usage = _dict(span.attributes.get(_USAGE_KEY))
    counts = {
        field_name: _parse_token_count(usage.get(source_key))
        for field_name, source_key in (
            ("input_tokens", "input_tokens"),
            ("output_tokens", "output_tokens"),
            ("cached_input_tokens", "cache_read_input_tokens"),
        )
    }
    return {name: count for name, count in counts.items() if count is not None}


def _cost(span: _Span) -> Decimal | None:
    """Map MLflow's computed span cost in USD."""
    return _decimal(_dict(span.attributes.get(_COST_KEY)).get("total_cost"))


def _string_attribute(span: _Span, key: str) -> str | None:
    """Return a non-empty string attribute."""
    value = span.attributes.get(key)
    return str(value) if value not in (None, "") else None


def _child_selector(selector: str, key: str | int) -> str:
    """Append a child token to an RFC 6901 JSON Pointer."""
    token = str(key).replace("~", "~0").replace("/", "~1")
    return f"{selector}/{token}"


def _role(value: dict[str, Any]) -> str | None:
    """Return a normalized message role."""
    candidates = [value.get("role"), value.get("type")]
    identifier = value.get("id")
    if isinstance(identifier, list) and identifier:
        candidates.append(identifier[-1])
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        normalized = candidate.lower().replace("_", "-")
        if normalized in {"user", "human", "humanmessage"}:
            return "user"
        if normalized in {"system", "systemmessage"}:
            return "system"
        if normalized in {"assistant", "ai", "aimessage", "model"}:
            return "assistant"
    return None


def _content_match(
    value: Any,
    selector: str = "",
    depth: int = 0,
    *,
    visible_output_only: bool = False,
) -> _TextMatch | None:
    """Return one scalar text value and its selector."""
    if depth > 8:
        return None
    if isinstance(value, str):
        text = value.strip()
        return _TextMatch(selector, text) if text else None
    if isinstance(value, list):
        matches = [
            match
            for index, item in enumerate(value)
            if (
                match := _content_match(
                    item,
                    _child_selector(selector, index),
                    depth + 1,
                    visible_output_only=visible_output_only,
                )
            )
            is not None
        ]
        return matches[-1] if matches else None
    if not isinstance(value, dict):
        return None
    if visible_output_only:
        kind = value.get("type")
        normalized = kind.lower().replace("_", "-") if isinstance(kind, str) else None
        if normalized in {
            "reasoning",
            "redacted-thinking",
            "thinking",
            "tool-call",
            "tool-use",
            "function-call",
        }:
            return None
    for key in ("text", "content", "parts", "kwargs", "data"):
        if key in value and (
            match := _content_match(
                value[key],
                _child_selector(selector, key),
                depth + 1,
                visible_output_only=visible_output_only,
            )
        ):
            return match
    return None


def _message_matches(
    value: Any,
    target_role: str,
    selector: str = "",
    depth: int = 0,
    *,
    visible_output_only: bool = False,
) -> list[_TextMatch]:
    """Return text matches for one nested message role."""
    if depth > 12:
        return []
    if isinstance(value, list):
        return [
            match
            for index, item in enumerate(value)
            for match in _message_matches(
                item,
                target_role,
                _child_selector(selector, index),
                depth + 1,
                visible_output_only=visible_output_only,
            )
        ]
    if not isinstance(value, dict):
        return []
    if _role(value) == target_role:
        for key in ("content", "text", "parts", "kwargs", "data"):
            if key in value and (
                match := _content_match(
                    value[key],
                    _child_selector(selector, key),
                    depth + 1,
                    visible_output_only=visible_output_only,
                )
            ):
                return [match]
    matches: list[_TextMatch] = []
    for key, child in value.items():
        matches.extend(
            _message_matches(
                child,
                target_role,
                _child_selector(selector, key),
                depth + 1,
                visible_output_only=visible_output_only,
            )
        )
    return matches


def _input_text_selector(value: Any) -> str | None:
    """Return the primary user input selector."""
    messages = _message_matches(value, "user")
    if messages:
        return messages[-1].selector
    if isinstance(value, str):
        return "" if value.strip() else None
    if isinstance(value, dict):
        for key in ("prompt", "query", "question", "input", "user_input", "message"):
            if key in value and (
                match := _content_match(value[key], _child_selector("", key))
            ):
                return match.selector
    return None


def _output_text_selector(value: Any) -> str | None:
    """Return the primary assistant output selector."""
    messages = _message_matches(value, "assistant", visible_output_only=True)
    if messages:
        return messages[-1].selector
    if isinstance(value, str):
        return "" if value.strip() else None
    if isinstance(value, dict):
        for key in ("answer", "result", "response", "output", "text", "content"):
            if key in value and (
                match := _content_match(
                    value[key], _child_selector("", key), visible_output_only=True
                )
            ):
                return match.selector
    return None


def _system_prompt_selector(value: Any) -> str | None:
    """Return the latest system prompt selector."""
    messages = _message_matches(value, "system")
    if messages:
        return messages[-1].selector
    if isinstance(value, dict):
        for key in ("system", "system_prompt", "instructions"):
            if key in value and (
                match := _content_match(value[key], _child_selector("", key))
            ):
                return match.selector
    return None


def _reasoning_selectors(value: Any) -> list[str]:
    """Return the JSON Pointers selecting visible reasoning in a provider payload."""
    found: list[str] = []

    def _collect(item: Any, selector: str = "", depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                _collect(child, _child_selector(selector, index), depth + 1)
            return
        if not isinstance(item, dict):
            return
        kind_value = item.get("type")
        kind = str(kind_value).lower().replace("_", "-") if kind_value else ""
        if kind in {"reasoning", "thinking"}:
            for key in ("thinking", "text", "content", "summary"):
                if key in item and (
                    match := _content_match(
                        item[key], _child_selector(selector, key), depth + 1
                    )
                ):
                    found.append(match.selector)
                    break
            return
        for key in ("reasoning", "reasoning_content"):
            if key in item and (
                match := _content_match(
                    item[key], _child_selector(selector, key), depth + 1
                )
            ):
                found.append(match.selector)
        for key, child in item.items():
            _collect(child, _child_selector(selector, key), depth + 1)

    _collect(value)
    return found


def _link_spans(trace: _Trace, warnings: list[str]) -> list[_Span]:
    """Link one trace's spans into an acyclic tree and return its roots."""
    ordered = sorted(
        trace.spans,
        key=lambda span: (
            span.started_at or datetime.min.replace(tzinfo=UTC),
            span.span_id,
        ),
    )
    by_id: dict[str, _Span] = {}
    for span in ordered:
        if span.span_id in by_id:
            raise InvalidImport(
                f"Trace '{trace.trace_id}' contains duplicate span id '{span.span_id}'"
            )
        by_id[span.span_id] = span
    parents: dict[str, str] = {}
    for span in ordered:
        if span.parent_id is None:
            continue
        if span.parent_id in by_id:
            parents[span.span_id] = span.parent_id
        else:
            warnings.append(
                f"Span '{span.span_id}' references missing parent '{span.parent_id}'"
            )
    depths: dict[str, int] = {}
    for span_id in parents:
        path: list[str] = []
        seen: set[str] = set()
        current: str | None = span_id
        while current in parents and current not in depths:
            if current in seen:
                raise InvalidImport(
                    f"Trace '{trace.trace_id}' contains a span parent cycle"
                )
            seen.add(current)
            path.append(current)
            current = parents[current]
        depth = depths.get(current, 1) if current is not None else 1
        for ancestor in reversed(path):
            depth += 1
            if depth > MAX_PARENT_DEPTH:
                raise InvalidImport("The imported span graph exceeds 64 parent levels")
            depths[ancestor] = depth
    roots: list[_Span] = []
    for span in ordered:
        span.children = []
    for span in ordered:
        if span.span_id in parents:
            by_id[parents[span.span_id]].children.append(span)
        else:
            roots.append(span)
    if len(roots) != 1:
        warnings.append(f"Trace '{trace.trace_id}' has {len(roots)} root spans")
    return roots


class _Subtree(NamedTuple):
    """A built node subtree, whether it holds LLM spans, and the usage it counts."""

    node: ImportedNode
    has_llm: bool
    token_counts: dict[str, int]
    cost: Decimal | None


def _sum_token_counts(subtrees: list[_Subtree]) -> dict[str, int]:
    """Sum the token fields counted across subtrees."""
    totals: dict[str, int] = {}
    for subtree in subtrees:
        for name, count in subtree.token_counts.items():
            totals[name] = totals.get(name, 0) + count
    return totals


def _get_residual_tokens(own: dict[str, int], below: dict[str, int]) -> dict[str, int]:
    """Return the part of a span's token fields its descendants do not count."""
    residual: dict[str, int] = {}
    for name, count in own.items():
        if name not in below:
            residual[name] = count
        elif count > below[name]:
            residual[name] = count - below[name]
    return residual


def _get_residual_cost(own: Decimal | None, below: Decimal | None) -> Decimal | None:
    """Return the part of a span's cost its descendants do not count."""
    if own is None or below is None:
        return own
    return own - below if own > below else None


def _build_nodes(trace: _Trace, span: _Span) -> _Subtree:
    """Build one node subtree.

    A LangChain chat model span wraps the provider span that the OpenAI or
    Anthropic autolog records for the same request, and rollup integrations
    set cumulative usage on an agent span above per-call spans. Session
    totals sum every node, so descendants keep their own usage and a span
    above them keeps only the part they do not account for, per token field
    and for cost. An LLM span wrapping another LLM span becomes a plain span
    so the request is counted once as a call.
    """
    subtrees = [_build_nodes(trace, child) for child in span.children]
    has_llm_descendant = any(subtree.has_llm for subtree in subtrees)
    own_tokens = _token_counts(span)
    own_cost = _cost(span)
    below_tokens = _sum_token_counts(subtrees)
    below_costs = [subtree.cost for subtree in subtrees if subtree.cost is not None]
    below_cost = sum(below_costs, Decimal(0)) if below_costs else None
    kept_tokens = _get_residual_tokens(own_tokens, below_tokens)
    kept_cost = _get_residual_cost(own_cost, below_cost)

    node_type, tool_name = _node_type(span, has_llm_descendant)
    status = _node_status(span)
    inputs = span.attributes.get(_INPUTS_KEY)
    outputs = span.attributes.get(_OUTPUTS_KEY)
    message_format = _string_attribute(span, _MESSAGE_FORMAT_KEY)
    requested_model = (
        str(inputs["model"])
        if node_type is NodeType.LLM_CALL
        and isinstance(inputs, dict)
        and isinstance(inputs.get("model"), str)
        and inputs["model"]
        else None
    )
    model_params = span.attributes.get("invocation_params")
    metadata: dict[str, Any] = {
        "mlflow.span_id": span.span_id,
        "mlflow.span_type": span.span_type,
    }
    if message_format:
        metadata["mlflow.message_format"] = message_format
    if kept_tokens != own_tokens or kept_cost != own_cost:
        metadata["mlflow.usage_counted_on_descendants"] = True
    node = ImportedNode(
        external_id=f"{trace.trace_id}:{span.span_id}",
        trace_id=trace.trace_id,
        node_type=node_type,
        name=span.name,
        status=status,
        error=_span_error(span) if status is NodeStatus.FAILED else None,
        started_at=span.started_at,
        ended_at=span.ended_at,
        inputs=inputs,
        outputs=outputs,
        requested_model=requested_model,
        model=_string_attribute(span, _MODEL_KEY),
        model_provider=_string_attribute(span, _PROVIDER_KEY),
        tokens=TokenUsage(**kept_tokens) if kept_tokens else None,
        cost=kept_cost,
        model_params=model_params if isinstance(model_params, dict) else None,
        tool_name=tool_name,
        attributes={
            "mlflow.attributes": {
                key: value
                for key, value in span.attributes.items()
                if key not in {_INPUTS_KEY, _OUTPUTS_KEY}
            },
            "mlflow.events": span.events,
        },
        metadata=metadata,
        children=[subtree.node for subtree in subtrees],
    )
    node.input_text_selector = _input_text_selector(node.inputs)
    node.output_text_selector = _output_text_selector(node.outputs)
    if node_type is NodeType.LLM_CALL:
        node.system_prompt_selector = _system_prompt_selector(node.inputs)
        node.reasoning_selectors = _reasoning_selectors(node.outputs)
    return _Subtree(
        node=node,
        has_llm=span.span_type in _LLM_SPAN_TYPES or has_llm_descendant,
        token_counts={
            name: below_tokens.get(name, 0) + kept_tokens.get(name, 0)
            for name in below_tokens.keys() | kept_tokens.keys()
        },
        cost=None
        if below_cost is None and kept_cost is None
        else (below_cost or Decimal(0)) + (kept_cost or Decimal(0)),
    )


def _trace_window(trace: _Trace) -> tuple[datetime | None, datetime | None]:
    """Return a trace's start and end from its spans, falling back to its info."""
    spans = trace.spans
    started = min((span.started_at for span in spans if span.started_at), default=None)
    ended = max((span.ended_at for span in spans if span.ended_at), default=None)
    info_start = _iso_datetime(trace.info.get("request_time")) or _unix_datetime(
        trace.info.get("timestamp_ms"), 1_000
    )
    started = started or info_start
    if ended is None and info_start is not None:
        duration = trace.info.get(
            "execution_duration_ms", trace.info.get("execution_time_ms")
        )
        if isinstance(duration, int) and not isinstance(duration, bool):
            try:
                ended = info_start + timedelta(milliseconds=duration)
            except OverflowError:
                ended = None
    return started, ended


def _trace_payload(trace: _Trace, root: _Span | None, attribute: str, key: str) -> Any:
    """Return a trace's inputs or outputs from its root span or metadata."""
    if root is not None and root.attributes.get(attribute) is not None:
        return root.attributes[attribute]
    value = trace.metadata.get(key)
    return _decode_json(value) if value not in (None, "") else None


def _assessments(trace: _Trace) -> list[dict[str, Any]]:
    """Return a trace's valid MLflow assessments in a compact form."""
    items = trace.info.get("assessments")
    assessments: list[dict[str, Any]] = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or item.get("valid") is False:
            continue
        feedback = _dict(item.get("feedback"))
        expectation = _dict(item.get("expectation"))
        kind = "feedback" if feedback else "expectation" if expectation else None
        assessments.append(
            {
                "trace_id": trace.trace_id,
                "assessment_id": item.get("assessment_id"),
                "name": item.get("assessment_name"),
                "kind": kind,
                "value": (feedback or expectation).get("value"),
                "error": feedback.get("error"),
                "rationale": item.get("rationale"),
                "source": item.get("source"),
                "span_id": _normalize_id(item.get("span_id")),
            }
        )
    return assessments


def _detect_framework(traces: list[_Trace], configured: Any) -> str | None:
    """Detect one supported agent framework."""
    evidence = [str(configured or "")]
    for trace in traces:
        evidence.append(str(trace.tags.get("mlflow.traceName") or ""))
        for span in trace.spans:
            evidence.append(span.name)
            evidence.append(str(span.attributes.get(_MESSAGE_FORMAT_KEY) or ""))
    joined = "\n".join(evidence)
    matches = {
        framework
        for pattern, framework in _FRAMEWORK_PATTERNS
        if pattern.search(joined)
    }
    return next(iter(matches)) if len(matches) == 1 else None


def _is_unfinished(trace: _Trace) -> bool:
    """Return whether MLflow had not finished recording a trace."""
    return trace.state == "IN_PROGRESS" or any(
        span.parent_id is None and span.ended_at is None for span in trace.spans
    )


def _parse_unique_traces(
    documents: list[Any],
) -> tuple[list[tuple[int, _Trace]], list[ImportFailure]]:
    """Decode trace documents, dropping repeated trace ids.

    Overlapping export pages and repeated fetch ids repeat a trace verbatim,
    so identical copies collapse into the first. Differing copies of one
    trace id cannot both be right, so every copy is rejected, even when the
    copies would group into different sessions.

    Returns:
        Each kept trace with its 1-based document position, and failures.
    """
    failures: list[ImportFailure] = []
    first: dict[str, tuple[int, _Trace]] = {}
    conflicted: set[str] = set()
    for index, document in enumerate(documents, start=1):
        try:
            trace = _parse_trace(document)
        except InvalidImport as exc:
            failures.append(
                ImportFailure(line=index, error=_escape_failure_text(str(exc)))
            )
            continue
        _, existing = first.setdefault(trace.trace_id, (index, trace))
        if existing is not trace and existing.document != trace.document:
            conflicted.add(trace.trace_id)
    for trace_id in sorted(conflicted):
        failures.append(
            ImportFailure(
                line=first[trace_id][0],
                external_id=_escape_failure_text(trace_id),
                error=_escape_failure_text(
                    f"The import contains conflicting copies of trace '{trace_id}'"
                ),
            )
        )
    kept = [item for trace_id, item in first.items() if trace_id not in conflicted]
    return kept, failures


class MlflowTraceImporter:
    """Normalize MLflow traces into Kitaru sessions."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedSession | ImportFailure]:
        """Parse MLflow traces into sessions and isolated failures."""
        documents, failures = _parse_documents(content)
        traces, trace_failures = _parse_unique_traces(documents)
        failures.extend(trace_failures)
        grouped: dict[tuple[str, str], list[_Trace]] = defaultdict(list)
        join_paths: dict[tuple[str, str], set[str]] = defaultdict(set)
        fallback_sessions: set[tuple[str, str]] = set()
        for index, trace in traces:
            try:
                session_id, join_path, fallback = _join_value(trace, params)
                source_instance = _get_source_instance(trace, params)
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=index,
                        external_id=_escape_failure_text(trace.trace_id),
                        error=_escape_failure_text(str(exc)),
                    )
                )
                continue
            key = (source_instance, session_id)
            grouped[key].append(trace)
            join_paths[key].add(join_path)
            if fallback:
                fallback_sessions.add(key)

        # Insertion order follows each session's first trace in the payload,
        # so ingestion follows payload order.
        for key, traces in grouped.items():
            source_instance, session_id = key
            try:
                session = self._parse_session(
                    source_instance,
                    session_id,
                    traces,
                    framework=params.get("framework"),
                    join_paths=join_paths[key],
                    trace_fallback=key in fallback_sessions,
                )
                # Surface unserializable content, such as lone surrogates, as
                # this session's failure instead of a failed ingest request.
                session.model_dump_json()
                yield session
            except (InvalidImport, PydanticSerializationError) as exc:
                yield ImportFailure(
                    line=len(failures) + 1,
                    external_id=_escape_failure_text(session_id),
                    error=_escape_failure_text(str(exc)),
                )
        yield from failures

    def _parse_session(
        self,
        source_instance: str,
        session_id: str,
        traces: list[_Trace],
        *,
        framework: Any,
        join_paths: set[str],
        trace_fallback: bool,
    ) -> ImportedSession:
        """Normalize one grouped MLflow session."""
        # An imported session is never updated after creation and a re-import
        # is skipped as a duplicate, so a session with an unfinished trace is
        # deferred whole rather than stored without its last turn.
        unfinished = sorted(t.trace_id for t in traces if _is_unfinished(t))
        if unfinished:
            raise InvalidImport(
                f"Session '{session_id}' includes unfinished trace "
                f"'{unfinished[0]}'; re-import it after MLflow finishes the trace"
            )
        experiments = {
            experiment
            for trace in traces
            if (experiment := _get_embedded_experiment(trace)) is not None
        }
        if len(experiments) > 1:
            raise InvalidImport(
                f"Session '{session_id}' contains conflicting MLflow experiment ids"
            )
        warnings: list[str] = []
        if trace_fallback:
            warnings.append("No mlflow.trace.session metadata; grouped by trace id")

        turns: list[tuple[_Turn, _Trace, _Span | None]] = []
        nodes: list[ImportedNode] = []
        windowed = sorted(
            ((_trace_window(trace), trace) for trace in traces),
            key=lambda item: (
                item[0][0] or datetime.min.replace(tzinfo=UTC),
                item[1].trace_id,
            ),
        )
        for (started_at, ended_at), trace in windowed:
            roots = _link_spans(trace, warnings)
            root = roots[0] if roots else None
            turn = _Turn(
                trace_id=trace.trace_id,
                inputs=_trace_payload(trace, root, _INPUTS_KEY, "mlflow.traceInputs"),
                outputs=_trace_payload(
                    trace, root, _OUTPUTS_KEY, "mlflow.traceOutputs"
                ),
                started_at=started_at,
                ended_at=ended_at,
            )
            turns.append((turn, trace, root))
            nodes.extend(_build_nodes(trace, span).node for span in roots)

        nodes.sort(
            key=lambda node: (
                node.started_at or datetime.min.replace(tzinfo=UTC),
                node.external_id,
            )
        )
        latest_turn, latest_trace, latest_root = turns[-1]
        failed = latest_trace.state == "ERROR" or (
            latest_root is not None and _node_status(latest_root) is NodeStatus.FAILED
        )
        ordered_traces = [trace for _, trace, _ in turns]
        users = sorted(
            {
                str(user)
                for trace in ordered_traces
                if (user := trace.metadata.get(_USER_METADATA_KEY)) not in (None, "")
            }
        )
        metadata: dict[str, Any] = {
            "mlflow.session_id": None if trace_fallback else session_id,
            "mlflow.experiment_id": next(iter(experiments), None),
            "mlflow.trace_ids": [trace.trace_id for trace in ordered_traces],
            "mlflow.join_paths": sorted(join_paths),
            "mlflow.users": users,
            "mlflow.client_request_ids": [
                str(trace.info["client_request_id"])
                for trace in ordered_traces
                if trace.info.get("client_request_id")
            ],
            "mlflow.tags": {
                key: value
                for trace in ordered_traces
                for key, value in trace.tags.items()
                if not str(key).startswith("mlflow.")
            },
            "mlflow.assessments": [
                assessment
                for trace in ordered_traces
                for assessment in _assessments(trace)
            ],
            "source_trace_count": len(turns),
            "source_completeness": "full",
            "normalization_warnings": warnings,
        }
        return ImportedSession(
            external_id=f"{source_instance}:{session_id}",
            name=str(
                latest_trace.tags.get("mlflow.traceName")
                or (latest_root.name if latest_root else None)
                or session_id
            ),
            status=SessionStatus.FAILED if failed else SessionStatus.COMPLETED,
            inputs={
                "schema_version": 1,
                "turns": [
                    {
                        "source_trace_id": turn.trace_id,
                        "inputs": turn.inputs,
                        "outputs": turn.outputs,
                    }
                    for turn, _, _ in turns
                ],
            },
            outputs=latest_turn.outputs,
            error=(
                _span_error(latest_root)
                if failed and latest_root is not None
                else "MLflow trace failed"
                if failed
                else None
            ),
            started_at=min(
                (turn.started_at for turn, _, _ in turns if turn.started_at),
                default=None,
            ),
            ended_at=max(
                (turn.ended_at for turn, _, _ in turns if turn.ended_at), default=None
            ),
            metadata=metadata,
            framework=_detect_framework(ordered_traces, framework),
            nodes=nodes,
        )

    async def fetch(self, query: dict[str, Any]) -> AsyncIterator[bytes]:
        """Fetch parser payloads from an MLflow tracking server."""
        from .api import fetch

        async with aclosing(fetch(query)) as payloads:
            async for payload in payloads:
                yield payload


importer = MlflowTraceImporter()


def parse(
    content: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse MLflow traces through the importer contract."""
    yield from MlflowTraceImporter().parse(content, params)
