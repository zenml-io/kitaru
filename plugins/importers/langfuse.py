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
"""Langfuse JSON and JSONL trace importer plugin."""

import json
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
)

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_TRACE_SHAPE = "trace"
_OBSERVATION_SHAPE = "observation"
_EVENT_SHAPE = "ingestion_event"
_METADATA_KEYS = {
    "agent_name",
    "deployment.environment.name",
    "environment",
    "gen_ai.agent.name",
    "gen_ai.operation.name",
    "gen_ai.provider.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.system",
    "name",
    "sdk_span_type",
    "service.name",
    "service.version",
}
_RESOURCE_METADATA_KEYS = {
    "deployment.environment.name",
    "service.name",
    "service.version",
}
_TRACE_CONTEXT_FIELDS = {
    "traceEnvironment": "environment",
    "traceHtmlPath": "html_path",
    "traceRelease": "release",
    "traceTags": "tags",
    "traceVersion": "version",
}


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


@dataclass(frozen=True, slots=True)
class _TextMatch:
    """Text selected from a provider payload."""

    selector: str
    text: str


def _child_selector(selector: str, key: str | int) -> str:
    """Append a child segment to an RFC 9535 JSONPath."""
    if isinstance(key, int):
        return f"{selector}[{key}]"
    return f"{selector}[{json.dumps(key, ensure_ascii=False)}]"


def _role(value: dict[str, Any]) -> str | None:
    """Return a normalized message role."""
    candidates = [
        value.get("role"),
        value.get("type"),
        value.get("part_kind"),
        value.get("kind"),
    ]
    event_name = value.get("event.name")
    if isinstance(event_name, str):
        candidates.append(event_name.removeprefix("gen_ai.").removesuffix(".message"))
    identifier = value.get("id")
    if isinstance(identifier, list) and identifier:
        candidates.append(identifier[-1])
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        normalized = candidate.lower().replace("_", "-")
        if normalized in {"user", "human", "humanmessage", "user-prompt"}:
            return "user"
        if normalized in {"system", "systemmessage", "system-prompt"}:
            return "system"
        if normalized in {
            "assistant",
            "ai",
            "aimessage",
            "model",
            "response",
            "model-response",
        }:
            return "assistant"
    return None


def _content_match(
    value: Any, selector: str = "$", depth: int = 0
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
                    item, _child_selector(selector, index), depth + 1
                )
            )
            is not None
        ]
        return matches[-1] if matches else None
    if not isinstance(value, dict):
        return None
    for key in ("text", "content", "parts", "kwargs", "data"):
        if key in value and (
            match := _content_match(
                value[key], _child_selector(selector, key), depth + 1
            )
        ):
            return match
    return None


def _message_matches(
    value: Any, target_role: str, selector: str = "$", depth: int = 0
) -> list[_TextMatch]:
    """Return text matches for one nested message role."""
    if depth > 12:
        return []
    if isinstance(value, list):
        return [
            match
            for index, item in enumerate(value)
            for match in _message_matches(
                item, target_role, _child_selector(selector, index), depth + 1
            )
        ]
    if not isinstance(value, dict):
        return []
    if _role(value) == target_role:
        for key in ("content", "text", "parts", "kwargs", "data"):
            if key in value and (
                match := _content_match(
                    value[key], _child_selector(selector, key), depth + 1
                )
            ):
                return [match]
    matches: list[_TextMatch] = []
    for key, child in value.items():
        matches.extend(
            _message_matches(
                child, target_role, _child_selector(selector, key), depth + 1
            )
        )
    return matches


def _input_text_selector(value: Any) -> str | None:
    """Return the primary user input selector."""
    messages = _message_matches(value, "user")
    if messages:
        return messages[-1].selector
    if isinstance(value, str):
        return "$" if value.strip() else None
    if isinstance(value, dict):
        for key in ("prompt", "query", "question", "user_input", "message"):
            if key in value and (
                match := _content_match(value[key], _child_selector("$", key))
            ):
                return match.selector
    return None


def _output_text_selector(value: Any) -> str | None:
    """Return the primary assistant output selector."""
    messages = _message_matches(value, "assistant")
    if messages:
        return messages[-1].selector
    if isinstance(value, str):
        return "$" if value.strip() else None
    if isinstance(value, dict):
        for key in ("answer", "result", "response", "output", "text", "content"):
            if key in value and (
                match := _content_match(value[key], _child_selector("$", key))
            ):
                return match.selector
    return None


def _system_prompt_match(value: Any) -> _TextMatch | None:
    """Return the latest system prompt and its selector."""
    messages = _message_matches(value, "system")
    if messages:
        return messages[-1]
    found: list[_TextMatch] = []

    def _collect(item: Any, selector: str = "$", depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(item, list):
            for index, child in enumerate(item):
                _collect(child, _child_selector(selector, index), depth + 1)
            return
        if not isinstance(item, dict):
            return
        for key in ("system_prompt", "system_instruction", "instructions"):
            if key in item and (
                match := _content_match(
                    item[key], _child_selector(selector, key), depth + 1
                )
            ):
                found.append(match)
        for key, child in item.items():
            _collect(child, _child_selector(selector, key), depth + 1)

    _collect(value)
    return found[-1] if found else None


def _reasoning(value: Any) -> str | None:
    """Return visible reasoning from a provider payload."""
    found: list[str] = []

    def _collect(item: Any, depth: int = 0) -> None:
        if depth > 12:
            return
        if isinstance(item, list):
            for child in item:
                _collect(child, depth + 1)
            return
        if not isinstance(item, dict):
            return
        kind_value = item.get("type") or item.get("part_kind")
        kind = str(kind_value).lower().replace("_", "-") if kind_value else ""
        if kind in {"reasoning", "reasoning-content", "thinking", "thought"}:
            for key in ("text", "content", "summary"):
                if key in item and (
                    match := _content_match(item[key], depth=depth + 1)
                ):
                    found.append(match.text)
        for key in ("reasoning", "reasoning_content", "thinking", "thought"):
            if key in item and (match := _content_match(item[key], depth=depth + 1)):
                found.append(match.text)
        for child in item.values():
            _collect(child, depth + 1)

    _collect(value)
    return found[-1] if found else None


def _detect_framework(value: Any) -> str | None:
    """Detect one supported framework from provider metadata."""
    evidence: list[str] = []

    def _collect(item: Any, depth: int = 0) -> None:
        if depth > 8 or len(evidence) >= 500:
            return
        if isinstance(item, dict):
            for key, child in item.items():
                evidence.append(str(key))
                _collect(child, depth + 1)
        elif isinstance(item, list):
            for child in item[:100]:
                _collect(child, depth + 1)
        elif isinstance(item, str):
            evidence.append(item)

    _collect(value)
    joined = "\n".join(evidence)
    matches = {
        framework
        for pattern, framework in _FRAMEWORK_PATTERNS
        if pattern.search(joined)
    }
    return next(iter(matches)) if len(matches) == 1 else None


def _populate_node_fields(nodes: list[ImportedNode]) -> str | None:
    """Populate normalized node fields and return the latest system prompt."""
    session_system_prompt = None
    for node in nodes:
        node.input_text_selector = _input_text_selector(node.inputs)
        node.output_text_selector = _output_text_selector(node.outputs)
        if node.node_type is NodeType.LLM_CALL:
            system_prompt = _system_prompt_match(node.inputs)
            node.system_prompt_selector = (
                system_prompt.selector if system_prompt is not None else None
            )
            node.reasoning = _reasoning(node.outputs) or _reasoning(node.inputs)
            if system_prompt is not None:
                session_system_prompt = system_prompt.text
    return session_system_prompt


class InvalidImport(ValueError):
    """Raised when a Langfuse payload cannot be parsed."""


@dataclass(frozen=True, slots=True)
class _Turn:
    """One source trace within a multi-turn session."""

    trace_id: str
    inputs: Any = None
    outputs: Any = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


def _first(record: dict[str, Any], *names: str) -> Any:
    """Return the first present field."""
    for name in names:
        if name in record:
            return record[name]
    return None


def _first_nonempty(record: dict[str, Any], *names: str) -> Any:
    """Return the first non-empty field."""
    for name in names:
        value = record.get(name)
        if value not in (None, ""):
            return value
    return None


def _decode_json(value: Any) -> Any:
    """Decode a JSON string while preserving ordinary strings."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in '[{"':
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def _datetime(value: Any) -> datetime | None:
    """Parse one provider timestamp."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if not isinstance(value, str):
        return None
    candidate = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _decimal(value: Any) -> Decimal | None:
    """Parse one provider decimal."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _integer(value: Any) -> int | None:
    """Parse one provider integer."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dict(value: Any) -> dict[str, Any]:
    """Return a decoded dictionary or an empty one."""
    decoded = _decode_json(value)
    return decoded if isinstance(decoded, dict) else {}


def _path_value(value: Any, path: str) -> Any:
    """Resolve a dotted path or JSON Pointer against one observation."""
    if not path.strip():
        raise InvalidImport("join path must be non-empty")
    if path.startswith("/"):
        parts = [
            part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")
        ]
    else:
        parts = path.split(".")
    current = value
    for part in parts:
        current = _decode_json(current)
        if isinstance(current, dict) and part in current:
            current = current[part]
            continue
        if isinstance(current, list):
            try:
                current = current[int(part)]
            except (IndexError, ValueError):
                return None
            continue
        return None
    return _decode_json(current)


def _join_value(
    observations: list[dict[str, Any]], params: dict[str, Any], trace_id: str
) -> tuple[str, str, bool]:
    """Resolve one trace's session grouping value and selector."""
    join_on = params.get("join_on")
    join_key = params.get("join_key")
    join_path = params.get("join_path")
    if join_on is not None and (join_key is not None or join_path is not None):
        raise InvalidImport("join_on cannot be combined with join_key or join_path")
    if join_on is not None:
        if not isinstance(join_on, str):
            raise InvalidImport("join_on must be a dotted path or JSON pointer")
        selector = join_on
        values = {
            str(value)
            for record in observations
            if (value := _path_value(record, join_on)) not in (None, "")
        }
    elif join_key is not None or join_path is not None:
        if not isinstance(join_key, str) or not join_key.strip():
            raise InvalidImport("join_key must be a non-empty string")
        if not isinstance(join_path, str) or not join_path.strip():
            raise InvalidImport("join_path must be a dotted path or JSON pointer")
        selector = (
            f"{join_path.rstrip('/')}/{join_key}"
            if join_path.startswith("/")
            else f"{join_path}.{join_key}"
        )
        values: set[str] = set()
        for record in observations:
            container = _path_value(record, join_path)
            if isinstance(container, dict):
                value = container.get(join_key)
                if value not in (None, ""):
                    values.add(str(value))
    else:
        session_ids = {
            str(value)
            for record in observations
            if (value := _first(record, "sessionId", "session_id"))
        }
        if len(session_ids) > 1:
            raise InvalidImport(
                f"Trace '{trace_id}' contains conflicting Langfuse session ids"
            )
        if session_ids:
            return next(iter(session_ids)), "sessionId", False
        return trace_id, "traceId", True
    if not values:
        raise InvalidImport(
            f"Trace '{trace_id}' has no value at join selector '{selector}'"
        )
    if len(values) > 1:
        raise InvalidImport(
            f"Trace '{trace_id}' has conflicting values at join selector '{selector}'"
        )
    return next(iter(values)), selector, False


def _metadata_value(record: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value from Langfuse metadata layers."""
    metadata = _dict(record.get("metadata"))
    attributes = _dict(metadata.get("attributes"))
    resource_attributes = _dict(metadata.get("resourceAttributes"))
    for key in keys:
        for source in (attributes, metadata, resource_attributes):
            value = source.get(key)
            if value not in (None, ""):
                return value
        for flattened_key in (f"attributes.{key}", f"resourceAttributes.{key}"):
            value = metadata.get(flattened_key)
            if value not in (None, ""):
                return value
    return None


def _source_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return bounded metadata useful for debugging and filtering."""
    metadata = _dict(record.get("metadata"))
    attributes = _dict(metadata.get("attributes"))
    resource_attributes = _dict(metadata.get("resourceAttributes"))
    selected: dict[str, Any] = {}
    for source in (metadata, attributes):
        for key in _METADATA_KEYS:
            if key in source:
                selected[f"langfuse.{key}"] = source[key]
            flattened_key = f"attributes.{key}"
            if flattened_key in metadata:
                selected[f"langfuse.{key}"] = metadata[flattened_key]
    for key in _RESOURCE_METADATA_KEYS:
        if key in resource_attributes:
            selected[f"langfuse.{key}"] = resource_attributes[key]
        flattened_key = f"resourceAttributes.{key}"
        if flattened_key in metadata:
            selected[f"langfuse.{key}"] = metadata[flattened_key]
    for source_field, target_field in _TRACE_CONTEXT_FIELDS.items():
        if source_field in record:
            selected[f"langfuse.trace.{target_field}"] = record[source_field]
    for source_field, target_field in (
        ("environment", "environment"),
        ("promptName", "prompt_name"),
        ("promptVersion", "prompt_version"),
        ("version", "version"),
    ):
        if source_field in record and record[source_field] not in (None, ""):
            selected[f"langfuse.{target_field}"] = record[source_field]
    return selected


def _detect_shape(record: dict[str, Any]) -> str:
    """Detect one supported Langfuse record shape."""
    if isinstance(record.get("observations"), list):
        return _TRACE_SHAPE
    event_type = str(record.get("type", "")).lower()
    if isinstance(record.get("body"), dict) and (
        event_type.endswith("-create")
        or event_type.endswith("-update")
        or event_type in {"trace", "span", "generation", "event"}
    ):
        return _EVENT_SHAPE
    if _first(record, "traceId", "trace_id") is not None:
        return _OBSERVATION_SHAPE
    raise InvalidImport("Could not detect the Langfuse record shape")


def _parse_records(content: bytes) -> list[dict[str, Any]]:
    """Parse non-empty JSON or JSONL records."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Langfuse import exceeds the 50 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no JSON records")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        records = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise InvalidImport(f"Line {line_number} is not valid JSON") from exc
            if not isinstance(value, dict):
                raise InvalidImport(
                    f"Line {line_number} must contain a JSON object"
                ) from None
            records.append(value)
    else:
        if isinstance(decoded, dict):
            records = [decoded]
        elif isinstance(decoded, list) and all(
            isinstance(item, dict) for item in decoded
        ):
            records = decoded
        else:
            raise InvalidImport("Langfuse JSON must contain an object or object array")
    if not records:
        raise InvalidImport("Import file contains no JSON records")
    shapes = {_detect_shape(record) for record in records}
    if len(shapes) != 1:
        raise InvalidImport("Import file mixes multiple Langfuse record shapes")
    return records


def _events_to_traces(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply legacy ingestion events into trace rows."""
    traces: dict[str, dict[str, Any]] = {}
    observations: dict[tuple[str, str], dict[str, Any]] = {}
    for event in records:
        event_type = str(event.get("type", "")).lower()
        body = dict(event["body"])
        if event_type.startswith("trace") or event_type == "trace":
            trace_id = str(_first(body, "id", "traceId", "trace_id") or "")
            if not trace_id:
                raise InvalidImport("A Langfuse trace event has no trace id")
            traces.setdefault(trace_id, {"id": trace_id, "observations": []}).update(
                body
            )
            continue
        observation_id = str(_first(body, "id", "observationId") or "")
        trace_id = str(_first(body, "traceId", "trace_id") or "")
        if observation_id and not trace_id:
            matches = [
                candidate_trace_id
                for candidate_trace_id, candidate_id in observations
                if candidate_id == observation_id
            ]
            if len(matches) == 1:
                trace_id = matches[0]
        if not observation_id or not trace_id:
            raise InvalidImport(
                "A Langfuse observation event lacks an id or resolvable trace id"
            )
        if "generation" in event_type:
            body.setdefault("type", "GENERATION")
        elif event_type.startswith("event"):
            body.setdefault("type", "EVENT")
        else:
            body.setdefault("type", "SPAN")
        key = (trace_id, observation_id)
        observations.setdefault(key, {}).update(body)
    for (trace_id, _), observation in observations.items():
        trace = traces.setdefault(trace_id, {"id": trace_id, "observations": []})
        trace["observations"].append(observation)
    return list(traces.values())


def _trace_rows_to_observations(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Flatten trace rows while carrying trace context onto observations."""
    observations: list[dict[str, Any]] = []
    for trace in records:
        trace_id = str(_first(trace, "id", "traceId", "trace_id") or "")
        if not trace_id:
            raise InvalidImport("A Langfuse trace row has no trace id")
        for raw in trace.get("observations", []):
            if not isinstance(raw, dict):
                raise InvalidImport(f"Trace '{trace_id}' has a non-object observation")
            observation = dict(raw)
            observation.setdefault("traceId", trace_id)
            for source, target in (
                ("sessionId", "sessionId"),
                ("session_id", "sessionId"),
                ("projectId", "projectId"),
                ("project_id", "projectId"),
                ("name", "traceName"),
                ("timestamp", "traceTimestamp"),
                ("input", "traceInput"),
                ("output", "traceOutput"),
                ("environment", "traceEnvironment"),
                ("htmlPath", "traceHtmlPath"),
                ("release", "traceRelease"),
                ("tags", "traceTags"),
                ("version", "traceVersion"),
            ):
                if source in trace and target not in observation:
                    observation[target] = trace[source]
            observations.append(observation)
    return observations


def _node_type(record: dict[str, Any]) -> tuple[NodeType, str | None]:
    """Map one observation to a conservative node type and tool name."""
    observation_type = str(_first(record, "type", "observationType") or "").upper()
    metadata = _dict(record.get("metadata"))
    attributes = _dict(metadata.get("attributes"))
    operation = str(
        _first(
            record,
            "operationName",
            "operation_name",
        )
        or attributes.get("gen_ai.operation.name")
        or metadata.get("gen_ai.operation.name")
        or ""
    ).lower()
    explicit_tool = observation_type == "TOOL" or operation in {
        "execute_tool",
        "tool",
        "tool_call",
    }
    tool_name = _first(record, "toolName", "tool_name")
    if tool_name is None:
        tool_name = attributes.get("gen_ai.tool.name") or metadata.get(
            "gen_ai.tool.name"
        )
    function_span = (
        observation_type == "SPAN"
        and str(record.get("name") or "").startswith("Function:")
        and attributes.get("name") not in (None, "")
    )
    if function_span:
        tool_name = attributes["name"]
    if explicit_tool or function_span:
        return NodeType.TOOL_CALL, str(tool_name or record.get("name") or "tool")
    if observation_type == "GENERATION":
        return NodeType.LLM_CALL, None
    return NodeType.SPAN, None


def _node_status(record: dict[str, Any]) -> NodeStatus:
    """Map provider status fields to a terminal node status."""
    level = str(_first(record, "level", "status") or "").upper()
    if level in {"ERROR", "FAILED", "FAILURE"}:
        return NodeStatus.FAILED
    return NodeStatus.COMPLETED


def _tokens(record: dict[str, Any]) -> TokenUsage | None:
    """Map Langfuse usage data."""
    usage = _dict(_first(record, "usageDetails", "usage_details"))
    counts = (
        _integer(
            _first(record, "inputUsage", "input_usage")
            or usage.get("input")
            or usage.get("input_tokens")
        ),
        _integer(
            _first(record, "outputUsage", "output_usage")
            or usage.get("output")
            or usage.get("output_tokens")
        ),
        _integer(
            usage.get("input_cached_tokens")
            or usage.get("cache_read_tokens")
            or usage.get("cached_input_tokens")
        ),
        _integer(usage.get("reasoning_tokens")),
    )
    if all(value is None for value in counts):
        return None
    return TokenUsage(
        input_tokens=counts[0],
        output_tokens=counts[1],
        cached_input_tokens=counts[2],
        reasoning_tokens=counts[3],
    )


def _build_node_tree(
    nodes_with_parents: list[tuple[ImportedNode, str | None]],
) -> list[ImportedNode]:
    """Build and validate the parsed-node tree."""
    external_ids = [node.external_id for node, _ in nodes_with_parents]
    if any(external_id is None for external_id in external_ids):
        raise InvalidImport("The imported node graph contains a missing external id")
    if len(external_ids) != len(set(external_ids)):
        raise InvalidImport("The imported node graph contains duplicate external ids")

    converted = {
        node.external_id: node
        for node, _ in nodes_with_parents
        if node.external_id is not None
    }
    parents = {
        node.external_id: parent_external_id
        for node, parent_external_id in nodes_with_parents
        if node.external_id is not None and parent_external_id in converted
    }
    for external_id in external_ids:
        seen: set[str] = set()
        current = external_id
        while current in parents:
            if current in seen:
                raise InvalidImport("The imported node graph contains a parent cycle")
            seen.add(current)
            current = parents[current]

    roots: list[ImportedNode] = []
    for node, parent_external_id in nodes_with_parents:
        if parent_external_id in converted:
            converted[parent_external_id].children.append(node)
        else:
            roots.append(node)
    if nodes_with_parents and not roots:
        raise InvalidImport("The imported node graph contains no root node")
    return roots


class LangfuseJSONLImporter:
    """Parse Langfuse trace, observation, and ingestion-event records."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> list[ImportedSession | ImportFailure]:
        """Parse a Langfuse JSON or JSONL upload.

        Args:
            content: Complete uploaded JSON or JSONL.
            params: User selections passed to the importer.

        Returns:
            Imported sessions and isolated import failures.
        """
        records = _parse_records(content)
        shape = _detect_shape(records[0])
        if shape == _EVENT_SHAPE:
            records = _events_to_traces(records)
            shape = _TRACE_SHAPE
        if shape == _TRACE_SHAPE:
            records = _trace_rows_to_observations(records)
        file_framework = _detect_framework(
            [record.get("metadata") for record in records]
        )

        trace_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            trace_id = str(_first(record, "traceId", "trace_id") or "")
            if not trace_id:
                raise InvalidImport("A Langfuse observation has no trace id")
            trace_records[trace_id].append(record)

        session_traces: dict[str, list[tuple[str, list[dict[str, Any]]]]] = defaultdict(
            list
        )
        join_paths: dict[str, set[str]] = defaultdict(set)
        fallback_sessions: set[str] = set()
        failures: list[ImportFailure] = []
        for trace_id, observations in trace_records.items():
            try:
                session_id, join_path, fallback = _join_value(
                    observations, params, trace_id
                )
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1,
                        external_id=trace_id,
                        error=str(exc),
                    )
                )
                continue
            session_traces[session_id].append((trace_id, observations))
            join_paths[session_id].add(join_path)
            if fallback:
                fallback_sessions.add(session_id)

        sessions: list[ImportedSession] = []
        for source_id, traces in sorted(session_traces.items()):
            try:
                sessions.append(
                    self._parse_session(
                        source_id,
                        traces,
                        params,
                        join_paths=join_paths[source_id],
                        trace_fallback=source_id in fallback_sessions,
                        file_framework=file_framework,
                    )
                )
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1,
                        external_id=source_id,
                        error=str(exc),
                    )
                )
        return [*sessions, *failures]

    def _parse_session(
        self,
        source_id: str,
        traces: list[tuple[str, list[dict[str, Any]]]],
        params: dict[str, Any],
        *,
        join_paths: set[str],
        trace_fallback: bool,
        file_framework: str | None,
    ) -> ImportedSession:
        """Parse one grouped Langfuse session."""
        project_ids = {
            str(value)
            for _, observations in traces
            for record in observations
            if (value := _first(record, "projectId", "project_id"))
        }
        if len(project_ids) > 1:
            raise InvalidImport(
                f"Session '{source_id}' contains conflicting Langfuse project ids"
            )
        selected_source = params.get("source_instance")
        filename = params.get("filename")
        source_instance = (
            str(selected_source) if selected_source not in (None, "") else None
        ) or next(iter(project_ids), None)
        if not source_instance and isinstance(filename, str):
            source_instance = Path(filename).stem.strip() or None
        if not source_instance:
            raise InvalidImport(
                f"Session '{source_id}' has no project id; provide source_instance"
            )

        warnings: list[str] = []
        if trace_fallback:
            warnings.append("No Langfuse session id found; grouped by trace id")
        raw_nodes: list[tuple[str, str | None, dict[str, Any]]] = []
        turns: list[_Turn] = []
        trace_names: list[str] = []
        root_by_trace: dict[str, dict[str, Any]] = {}
        for trace_id, observations in traces:
            ordered = sorted(
                observations,
                key=lambda record: (
                    _datetime(_first(record, "startTime", "start_time"))
                    or datetime.min.replace(tzinfo=UTC),
                    str(record.get("id", "")),
                ),
            )
            ids = {
                str(record.get("id"))
                for record in ordered
                if record.get("id") is not None
            }
            roots = [
                record
                for record in ordered
                if not _first(record, "parentObservationId", "parent_observation_id")
                or str(_first(record, "parentObservationId", "parent_observation_id"))
                not in ids
            ]
            if len(roots) != 1:
                warnings.append(
                    f"Trace '{trace_id}' has {len(roots)} root observations"
                )
            root = roots[0] if roots else (ordered[0] if ordered else {})
            root_by_trace[trace_id] = root
            turn_input = _decode_json(_first(root, "traceInput", "input"))
            turn_output = _decode_json(_first(root, "traceOutput", "output"))
            trace_start = min(
                (
                    value
                    for record in ordered
                    if (value := _datetime(_first(record, "startTime", "start_time")))
                ),
                default=None,
            )
            trace_end = max(
                (
                    value
                    for record in ordered
                    if (value := _datetime(_first(record, "endTime", "end_time")))
                ),
                default=None,
            )
            turns.append(
                _Turn(
                    trace_id=trace_id,
                    inputs=turn_input,
                    outputs=turn_output,
                    started_at=trace_start,
                    ended_at=trace_end,
                )
            )
            trace_name = _first(root, "traceName", "trace_name")
            if trace_name:
                trace_names.append(str(trace_name))
            for record in ordered:
                observation_id = str(record.get("id") or "")
                if not observation_id:
                    raise InvalidImport(
                        f"Trace '{trace_id}' contains an observation without an id"
                    )
                parent = _first(record, "parentObservationId", "parent_observation_id")
                parent_id = str(parent) if parent is not None else None
                parent_source_id = (
                    f"{trace_id}:{parent_id}" if parent_id in ids else None
                )
                if parent and parent_source_id is None:
                    warnings.append(
                        f"Observation '{observation_id}' references a missing parent"
                    )
                raw_nodes.append(
                    (f"{trace_id}:{observation_id}", parent_source_id, record)
                )

        turns.sort(
            key=lambda turn: (
                turn.started_at or datetime.min.replace(tzinfo=UTC),
                turn.trace_id,
            )
        )
        raw_nodes.sort(
            key=lambda item: (
                _datetime(_first(item[2], "startTime", "start_time"))
                or datetime.min.replace(tzinfo=UTC),
                item[0],
            )
        )
        nodes_with_parents: list[tuple[ImportedNode, str | None]] = []
        for source_node_id, parent_source_id, record in raw_nodes:
            node_type, tool_name = _node_type(record)
            status = _node_status(record)
            nodes_with_parents.append(
                (
                    ImportedNode(
                        external_id=source_node_id,
                        trace_id=str(_first(record, "traceId", "trace_id")),
                        node_type=node_type,
                        name=str(record.get("name") or record.get("type") or "span"),
                        status=status,
                        error=(
                            str(_first(record, "statusMessage", "status_message"))
                            if status is NodeStatus.FAILED
                            and _first(record, "statusMessage", "status_message")
                            else None
                        ),
                        started_at=_datetime(_first(record, "startTime", "start_time")),
                        ended_at=_datetime(_first(record, "endTime", "end_time")),
                        inputs=_decode_json(record.get("input")),
                        outputs=_decode_json(record.get("output")),
                        requested_model=(
                            _metadata_value(record, "gen_ai.request.model")
                            or _first_nonempty(
                                record,
                                "providedModelName",
                                "provided_model_name",
                                "model",
                            )
                        ),
                        model=(
                            _metadata_value(record, "gen_ai.response.model")
                            or _first_nonempty(record, "model", "modelId", "model_id")
                            or _metadata_value(record, "gen_ai.request.model")
                        ),
                        provider=(
                            _first_nonempty(record, "modelProvider", "model_provider")
                            or _metadata_value(
                                record,
                                "gen_ai.provider.name",
                                "gen_ai.system",
                            )
                        ),
                        tokens=_tokens(record),
                        cost=_decimal(
                            _first(record, "totalCost", "total_cost")
                            or _dict(_first(record, "costDetails", "cost_details")).get(
                                "total"
                            )
                        ),
                        model_params=_dict(
                            _first(record, "modelParameters", "model_parameters")
                        )
                        or None,
                        tool_name=tool_name,
                        attributes={
                            "langfuse.type": record.get("type"),
                            "langfuse.level": record.get("level"),
                        },
                        metadata=_source_metadata(record),
                        children=[],
                    ),
                    parent_source_id,
                )
            )

        nodes = [node for node, _ in nodes_with_parents]
        system_prompt = _populate_node_fields(nodes)
        latest_turn = turns[-1]
        latest_root = root_by_trace[latest_turn.trace_id]
        latest_root_status = _node_status(latest_root)
        session_status = (
            SessionStatus.FAILED
            if latest_root_status is NodeStatus.FAILED
            else SessionStatus.COMPLETED
        )
        session_error = (
            str(_first(latest_root, "statusMessage", "status_message"))
            if session_status is SessionStatus.FAILED
            and _first(latest_root, "statusMessage", "status_message")
            else None
        )
        started_at = min(
            (turn.started_at for turn in turns if turn.started_at), default=None
        )
        ended_at = max((turn.ended_at for turn in turns if turn.ended_at), default=None)
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
        metadata = {
            "langfuse.project_id": next(iter(project_ids), None),
            "langfuse.session_id": source_id,
            "langfuse.trace_ids": [turn.trace_id for turn in turns],
            "langfuse.join_paths": sorted(join_paths),
            "langfuse.environments": sorted(
                {
                    str(value)
                    for _, observations in traces
                    for record in observations
                    if (
                        value := _first(
                            record,
                            "traceEnvironment",
                            "environment",
                        )
                    )
                }
            ),
            "langfuse.releases": sorted(
                {
                    str(record["traceRelease"])
                    for _, observations in traces
                    for record in observations
                    if record.get("traceRelease") not in (None, "")
                }
            ),
            "langfuse.versions": sorted(
                {
                    str(value)
                    for _, observations in traces
                    for record in observations
                    if (
                        value := _first(
                            record,
                            "traceVersion",
                            "version",
                        )
                    )
                }
            ),
            "source_trace_count": len(turns),
            "source_completeness": "unknown",
            "normalization_warnings": warnings,
        }
        framework = (
            _detect_framework(params.get("framework"))
            or _detect_framework(
                [record.get("metadata") for _, rows in traces for record in rows]
            )
            or file_framework
        )
        return ImportedSession(
            external_id=f"{source_instance}:{source_id}",
            name=trace_names[-1] if trace_names else None,
            status=session_status,
            system_prompt=system_prompt,
            inputs=inputs,
            outputs=(
                turns[-1].outputs
                if turns and turns[-1].outputs is not None
                else next(
                    (
                        node.outputs
                        for node in reversed(nodes)
                        if node.outputs is not None
                    ),
                    None,
                )
            ),
            error=session_error,
            started_at=started_at,
            ended_at=ended_at,
            metadata=metadata,
            framework=framework,
            nodes=_build_node_tree(nodes_with_parents),
        )


def parse(
    content: bytes,
    params: dict[str, Any],
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse Langfuse JSON or JSONL through the unified importer contract."""
    yield from LangfuseJSONLImporter().parse(content, params)
