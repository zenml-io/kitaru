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
from typing import Any

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

    @property
    def has_usage(self) -> bool:
        """Return whether the span records token usage or cost."""
        return any(
            self.attributes.get(key) not in (None, "", {})
            for key in (_USAGE_KEY, _COST_KEY)
        )


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


def _get_source_instance(trace: _Trace, params: dict[str, Any]) -> str:
    """Resolve the source namespace that keeps session ids from colliding."""
    override = _get_identity(params.get("source_instance"), "source_instance")
    alias = _get_identity(params.get("experiment_id"), "experiment_id parameter")
    experiment = _dict(_dict(trace.info.get("trace_location")).get("mlflow_experiment"))
    embedded = _get_identity(
        experiment["experiment_id"]
        if "experiment_id" in experiment
        else trace.info.get("experiment_id"),
        "experiment_id",
    )
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


def _tokens(span: _Span) -> TokenUsage | None:
    """Map MLflow's normalized token usage."""
    usage = _dict(span.attributes.get(_USAGE_KEY))
    values = (
        _parse_token_count(usage.get("input_tokens")),
        _parse_token_count(usage.get("output_tokens")),
        _parse_token_count(usage.get("cache_read_input_tokens")),
    )
    if all(value is None for value in values):
        return None
    return TokenUsage(
        input_tokens=values[0], output_tokens=values[1], cached_input_tokens=values[2]
    )


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


def _build_nodes(trace: _Trace, span: _Span) -> tuple[ImportedNode, bool, bool]:
    """Build one node subtree, returning whether it holds LLM spans and usage.

    A LangChain chat model span wraps the provider span that the OpenAI or
    Anthropic autolog records for the same request, and rollup integrations
    set cumulative usage on an agent span above per-call spans. Session
    totals sum every node, so only the innermost span carrying usage keeps
    its tokens and cost, and an LLM span wrapping another LLM span becomes a
    plain span so the request is counted once.
    """
    children: list[ImportedNode] = []
    has_llm_descendant = False
    has_usage_descendant = False
    for child in span.children:
        node, child_has_llm, child_has_usage = _build_nodes(trace, child)
        children.append(node)
        has_llm_descendant = has_llm_descendant or child_has_llm
        has_usage_descendant = has_usage_descendant or child_has_usage

    node_type, tool_name = _node_type(span, has_llm_descendant)
    status = _node_status(span)
    inputs = span.attributes.get(_INPUTS_KEY)
    outputs = span.attributes.get(_OUTPUTS_KEY)
    keeps_usage = not has_usage_descendant
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
    if span.has_usage and not keeps_usage:
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
        tokens=_tokens(span) if keeps_usage else None,
        cost=_cost(span) if keeps_usage else None,
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
        children=children,
    )
    node.input_text_selector = _input_text_selector(node.inputs)
    node.output_text_selector = _output_text_selector(node.outputs)
    if node_type is NodeType.LLM_CALL:
        node.system_prompt_selector = _system_prompt_selector(node.inputs)
        node.reasoning_selectors = _reasoning_selectors(node.outputs)
    is_llm = span.span_type in _LLM_SPAN_TYPES
    return (
        node,
        is_llm or has_llm_descendant,
        span.has_usage or has_usage_descendant,
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


class MlflowTraceImporter:
    """Normalize MLflow traces into Kitaru sessions."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedSession | ImportFailure]:
        """Parse MLflow traces into sessions and isolated failures."""
        documents, failures = _parse_documents(content)
        grouped: dict[tuple[str, str], list[_Trace]] = defaultdict(list)
        join_paths: dict[tuple[str, str], set[str]] = defaultdict(set)
        fallback_sessions: set[tuple[str, str]] = set()
        for index, document in enumerate(documents, start=1):
            trace_id: str | None = None
            try:
                trace = _parse_trace(document)
                trace_id = trace.trace_id
                session_id, join_path, fallback = _join_value(trace, params)
                source_instance = _get_source_instance(trace, params)
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=index,
                        external_id=_escape_failure_text(trace_id)
                        if trace_id
                        else None,
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
        # Overlapping export pages and repeated fetch ids repeat a trace
        # verbatim, so identical copies collapse into one.
        unique: dict[str, _Trace] = {}
        for trace in traces:
            existing = unique.setdefault(trace.trace_id, trace)
            if existing.document != trace.document:
                raise InvalidImport(
                    f"Session '{session_id}' contains conflicting copies of "
                    f"trace '{trace.trace_id}'"
                )
        traces = list(unique.values())
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
            nodes.extend(_build_nodes(trace, span)[0] for span in roots)
            if trace.state == "IN_PROGRESS":
                warnings.append(f"Trace '{trace.trace_id}' was still in progress")

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
            "mlflow.experiment_id": source_instance,
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
            "source_completeness": "partial"
            if any(trace.state == "IN_PROGRESS" for trace in ordered_traces)
            else "full",
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
