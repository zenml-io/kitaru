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
"""Braintrust project-log and UI JSON importer plugin."""

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

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
)

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_SESSION_FIELDS = (
    "session_id",
    "sessionId",
    "thread_id",
    "conversation_id",
    "gen_ai.conversation.id",
)
_ALLOWED_METADATA = {
    "conversation_id",
    "gen_ai.conversation.id",
    "gen_ai.provider.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "model",
    "provider",
    "session_id",
    "sessionId",
    "thread_id",
    "turn_index",
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
    """Append a child token to an RFC 6901 JSON Pointer."""
    token = str(key).replace("~", "~0").replace("/", "~1")
    return f"{selector}/{token}"


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
        kind = next(
            (
                candidate.lower().replace("_", "-")
                for key in ("type", "part_kind", "kind", "event.name")
                if isinstance((candidate := value.get(key)), str)
            ),
            None,
        )
        if kind in {
            "reasoning",
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
        for key in ("prompt", "query", "question", "user_input", "message"):
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
                    value[key],
                    _child_selector("", key),
                    visible_output_only=True,
                )
            ):
                return match.selector
    return None


def _system_prompt_match(value: Any) -> _TextMatch | None:
    """Return the latest system prompt and its selector."""
    messages = _message_matches(value, "system")
    if messages:
        return messages[-1]
    found: list[_TextMatch] = []

    def _collect(item: Any, selector: str = "", depth: int = 0) -> None:
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


def _populate_node_fields(nodes: list[ImportedNode]) -> None:
    """Populate normalized node fields."""
    for node in nodes:
        node.input_text_selector = _input_text_selector(node.inputs)
        node.output_text_selector = _output_text_selector(node.outputs)
        if node.node_type is NodeType.LLM_CALL:
            system_prompt = _system_prompt_match(node.inputs)
            node.system_prompt_selector = (
                system_prompt.selector if system_prompt is not None else None
            )
            node.reasoning = _reasoning(node.outputs) or _reasoning(node.inputs)


class InvalidImport(ValueError):
    """Raised when a Braintrust payload cannot be parsed."""


@dataclass(frozen=True, slots=True)
class _Turn:
    """One source trace within a multi-turn session."""

    trace_id: str
    inputs: Any = None
    outputs: Any = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


def _digest(value: Any) -> str:
    """Return a stable digest for normalized JSON-compatible data."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    """Return a dictionary or an empty dictionary."""
    return value if isinstance(value, dict) else {}


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


def _path_value(value: Any, path: str) -> Any:
    """Resolve a dotted path or JSON Pointer against one provider record."""
    if not path.strip():
        raise InvalidImport("join_on must be a non-empty path")
    if path.startswith("/"):
        for token in path[1:].split("/"):
            if re.search(r"~(?:[^01]|$)", token):
                raise InvalidImport("join_on contains an invalid JSON Pointer escape")
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


def _decimal(value: Any) -> Decimal | None:
    """Parse one provider decimal."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _contains_tool_activity(value: Any) -> bool:
    """Detect model output that references tool calls without explicit spans."""
    if isinstance(value, dict):
        if any(key in value for key in ("tool_calls", "toolCalls")):
            return True
        return any(_contains_tool_activity(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_tool_activity(item) for item in value)
    return False


def _parse_datetime(value: Any) -> datetime | None:
    """Parse an ISO timestamp or Unix timestamp."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_records(content: bytes) -> tuple[list[dict[str, Any]], bool]:
    """Parse Braintrust JSON, JSONL, or API fetch output."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Braintrust import exceeds the 50 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no records")

    value: Any
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise InvalidImport(f"Line {line_number} is not valid JSON") from exc
            if not isinstance(row, dict):
                raise InvalidImport(
                    f"Line {line_number} must contain a JSON object"
                ) from None
            records.append(row)
        if not records:
            raise InvalidImport("Import file contains no records") from None
        return records, _is_full_export(records)

    if isinstance(value, dict) and isinstance(value.get("events"), list):
        value = value["events"]
    elif isinstance(value, dict):
        value = [value]
    if not isinstance(value, list) or not all(
        isinstance(record, dict) for record in value
    ):
        raise InvalidImport("Braintrust export must contain JSON objects")
    records = list(value)
    if not records:
        raise InvalidImport("Import file contains no records")
    return records, _is_full_export(records)


def _is_full_export(records: list[dict[str, Any]]) -> bool:
    """Return whether records contain Braintrust span identity fields."""
    return any(
        record.get("span_id")
        or record.get("root_span_id")
        or isinstance(record.get("span_attributes"), dict)
        for record in records
    )


def _metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return allowlisted Braintrust metadata."""
    metadata = _dict(record.get("metadata"))
    return {key: metadata[key] for key in _ALLOWED_METADATA if key in metadata}


def _session_id(record: dict[str, Any]) -> str | None:
    """Extract the configured Braintrust session identifier."""
    metadata = _dict(record.get("metadata"))
    for field in _SESSION_FIELDS:
        value = metadata.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def _join_value(record: dict[str, Any], params: dict[str, Any], trace_id: str) -> str:
    """Resolve the session grouping value for one trace root."""
    selected = params.get("join_on")
    if selected is None:
        return _session_id(record) or trace_id
    if not isinstance(selected, str):
        raise InvalidImport("join_on must be a dotted path or JSON pointer")
    value = _path_value(record, selected)
    if value in (None, ""):
        raise InvalidImport(
            f"Trace '{trace_id}' has no value at join_on path '{selected}'"
        )
    if isinstance(value, dict | list):
        raise InvalidImport(
            f"Trace '{trace_id}' has a non-scalar value at join_on path '{selected}'"
        )
    return str(value)


def _source_instance(record: dict[str, Any], params: dict[str, Any]) -> str:
    """Resolve project identity with a stable filename fallback."""
    project_id = record.get("project_id")
    if project_id not in (None, ""):
        return str(project_id)
    selected_source = params.get("source_instance")
    if selected_source not in (None, ""):
        return str(selected_source)
    filename = params.get("filename")
    if isinstance(filename, str):
        stem = Path(filename).stem.strip()
        if stem:
            return stem
    raise InvalidImport("Braintrust export has no project id; provide source_instance")


def _metrics(record: dict[str, Any]) -> dict[str, Any]:
    """Return Braintrust metrics."""
    return _dict(record.get("metrics"))


def _started_at(record: dict[str, Any]) -> datetime | None:
    """Return the span start timestamp."""
    metrics = _metrics(record)
    return _parse_datetime(metrics.get("start")) or _parse_datetime(
        record.get("created")
    )


def _ended_at(record: dict[str, Any]) -> datetime | None:
    """Return the span end timestamp."""
    return _parse_datetime(_metrics(record).get("end"))


def _node_type(record: dict[str, Any], *, full_export: bool) -> NodeType:
    """Map a Braintrust span type conservatively."""
    span_type = str(_dict(record.get("span_attributes")).get("type") or "").lower()
    metadata = _dict(record.get("metadata"))
    metrics = _metrics(record)
    if span_type == "tool" or metadata.get("tool.name"):
        return NodeType.TOOL_CALL
    if span_type == "llm":
        openinference_kind = str(metadata.get("openinference.span.kind") or "").upper()
        if openinference_kind in {"", "LLM"}:
            return NodeType.LLM_CALL
    if not full_export and (
        metadata.get("model")
        or metrics.get("prompt_tokens") is not None
        or metrics.get("completion_tokens") is not None
    ):
        return NodeType.LLM_CALL
    return NodeType.SPAN


def _token_usage(record: dict[str, Any]) -> TokenUsage | None:
    """Map Braintrust token metrics."""
    metrics = _metrics(record)
    fields = ("prompt_tokens", "completion_tokens", "prompt_cached_tokens")
    values: list[int | None] = []
    for field in fields:
        value = metrics.get(field)
        if value is None:
            values.append(None)
            continue
        try:
            values.append(int(value))
        except (TypeError, ValueError) as exc:
            raise InvalidImport(
                f"Braintrust metric '{field}' must be an integer"
            ) from exc
    if all(value is None for value in values):
        return None
    try:
        return TokenUsage(
            input_tokens=values[0],
            output_tokens=values[1],
            cached_input_tokens=values[2],
            reasoning_tokens=None,
        )
    except ValueError as exc:
        raise InvalidImport("Braintrust token metrics are invalid") from exc


def _node_status(record: dict[str, Any]) -> NodeStatus:
    """Map a Braintrust error to node status."""
    if record.get("error") not in (None, ""):
        return NodeStatus.FAILED
    return NodeStatus.COMPLETED


def _trace_id(record: dict[str, Any]) -> str:
    """Return a stable trace identifier."""
    value = record.get("root_span_id") or record.get("span_id") or record.get("id")
    if value not in (None, ""):
        return str(value)
    metadata = _dict(record.get("metadata"))
    turn_index = metadata.get("turn_index")
    created = record.get("created")
    if turn_index is not None:
        return f"turn-{turn_index}"
    if created:
        return f"created-{created}"
    return f"row-{_digest(record)[:16]}"


def _root_record(records: list[dict[str, Any]], trace_id: str) -> dict[str, Any]:
    """Choose the root record for one trace."""
    for record in records:
        if str(record.get("span_id") or "") == trace_id:
            return record

    def duration(record: dict[str, Any]) -> float:
        metrics = _metrics(record)
        start = metrics.get("start")
        end = metrics.get("end")
        if isinstance(start, (int, float)) and isinstance(end, (int, float)):
            return end - start
        return -1

    return max(records, key=duration)


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


class BraintrustProjectLogImporter:
    """Parse Braintrust project logs and lower-fidelity UI exports."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> list[ImportedSession | ImportFailure]:
        """Parse a Braintrust project-log or UI export."""
        records, full_export = _parse_records(content)
        file_framework = _detect_framework(
            [
                {
                    "metadata": record.get("metadata"),
                    "span_attributes": record.get("span_attributes"),
                    "name": record.get("name"),
                }
                for record in records
            ]
        )
        trace_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            trace_records[_trace_id(record)].append(record)

        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        sessions: list[ImportedSession] = []
        failures: list[ImportFailure] = []
        for trace_id, rows in trace_records.items():
            root = _root_record(rows, trace_id)
            try:
                source_instance = _source_instance(root, params)
                session_id = _join_value(root, params, trace_id)
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1,
                        external_id=trace_id,
                        error=str(exc),
                    )
                )
                continue
            grouped[(source_instance, session_id)].extend(rows)

        for (source_instance, source_id), session_records in sorted(grouped.items()):
            try:
                sessions.append(
                    self._parse_session(
                        source_instance,
                        source_id,
                        session_records,
                        full_export=full_export,
                        file_framework=file_framework,
                        join_on=(
                            params.get("join_on")
                            if isinstance(params.get("join_on"), str)
                            else None
                        ),
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
        source_instance: str,
        source_id: str,
        records: list[dict[str, Any]],
        *,
        full_export: bool,
        file_framework: str | None,
        join_on: str | None,
    ) -> ImportedSession:
        """Parse one Braintrust session."""
        trace_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            trace_records[_trace_id(record)].append(record)

        warnings: list[str] = []
        if not full_export:
            warnings.append("Braintrust UI export omits span identity and hierarchy")

        turns: list[_Turn] = []
        nodes_with_parents: list[tuple[ImportedNode, str | None]] = []
        root_by_trace: dict[str, dict[str, Any]] = {}
        missing_parent = False
        for trace_id, rows in trace_records.items():
            ordered = sorted(
                rows,
                key=lambda row: (
                    _started_at(row) or datetime.min.replace(tzinfo=UTC),
                    str(row.get("span_id") or row.get("id") or row.get("name")),
                ),
            )
            root = _root_record(ordered, trace_id)
            root_by_trace[trace_id] = root
            turns.append(
                _Turn(
                    trace_id=trace_id,
                    inputs=(
                        root.get("input")
                        if root.get("input") is not None
                        else _decode_json(
                            _dict(root.get("metadata")).get("input.value")
                        )
                    ),
                    outputs=(
                        root.get("output")
                        if root.get("output") is not None
                        else _decode_json(
                            _dict(root.get("metadata")).get("output.value")
                        )
                    ),
                    started_at=min(
                        (value for row in ordered if (value := _started_at(row))),
                        default=None,
                    ),
                    ended_at=max(
                        (value for row in ordered if (value := _ended_at(row))),
                        default=None,
                    ),
                )
            )
            span_ids = {
                str(row.get("span_id"))
                for row in ordered
                if row.get("span_id") not in (None, "")
            }
            for row in ordered:
                span_id = str(row.get("span_id") or row.get("id") or _digest(row)[:16])
                parents = [str(value) for value in (row.get("span_parents") or [])]
                parent_span_id = parents[-1] if parents else None
                if parent_span_id and parent_span_id not in span_ids:
                    missing_parent = True
                parent_source_id = (
                    f"{trace_id}:{parent_span_id}"
                    if parent_span_id in span_ids
                    else None
                )
                node_type = _node_type(row, full_export=full_export)
                status = _node_status(row)
                attributes = _dict(row.get("span_attributes"))
                metadata = _dict(row.get("metadata"))
                nodes_with_parents.append(
                    (
                        ImportedNode(
                            external_id=f"{trace_id}:{span_id}",
                            trace_id=trace_id,
                            node_type=node_type,
                            name=str(
                                attributes.get("name") or row.get("name") or "span"
                            ),
                            status=status,
                            error=(
                                str(row.get("error"))
                                if status is NodeStatus.FAILED
                                else None
                            ),
                            started_at=_started_at(row),
                            ended_at=_ended_at(row),
                            inputs=(
                                row.get("input")
                                if row.get("input") is not None
                                else _decode_json(metadata.get("input.value"))
                            ),
                            outputs=(
                                row.get("output")
                                if row.get("output") is not None
                                else _decode_json(metadata.get("output.value"))
                            ),
                            requested_model=metadata.get("gen_ai.request.model")
                            or metadata.get("model"),
                            model=metadata.get("gen_ai.response.model")
                            or metadata.get("model"),
                            model_provider=metadata.get("gen_ai.provider.name")
                            or metadata.get("provider"),
                            tokens=_token_usage(row),
                            cost=_decimal(_metrics(row).get("estimated_cost")),
                            tool_name=(
                                str(
                                    metadata.get("tool.name")
                                    or attributes.get("name")
                                    or row.get("name")
                                    or "tool"
                                )
                                if node_type is NodeType.TOOL_CALL
                                else None
                            ),
                            attributes={
                                "braintrust.type": attributes.get("type"),
                            },
                            metadata=_metadata(row),
                            children=[],
                        ),
                        parent_source_id,
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
        _populate_node_fields(nodes)
        if missing_parent:
            warnings.append("One or more spans reference a missing parent")
        llm_nodes = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
        incomplete_llm_nodes = [
            node for node in llm_nodes if node.inputs is None or node.outputs is None
        ]
        implicit_tool_activity = any(
            _contains_tool_activity(node.outputs) for node in llm_nodes
        )
        if implicit_tool_activity and not any(
            node.node_type is NodeType.TOOL_CALL for node in nodes
        ):
            warnings.append(
                "Model output contains tool activity but no explicit tool spans"
            )
        if incomplete_llm_nodes:
            warnings.append("One or more LLM spans lack recorded input or output")
        latest_turn = turns[-1]
        latest_root = root_by_trace[latest_turn.trace_id]
        session_status = (
            SessionStatus.FAILED
            if _node_status(latest_root) is NodeStatus.FAILED
            else SessionStatus.COMPLETED
        )
        session_error = (
            str(latest_root["error"])
            if session_status is SessionStatus.FAILED
            and latest_root.get("error") not in (None, "")
            else None
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
        project_ids = sorted(
            {
                str(row["project_id"])
                for row in records
                if row.get("project_id") not in (None, "")
            }
        )
        root = _root_record(records, turns[-1].trace_id) if turns else {}
        metadata = {
            "braintrust.project_ids": project_ids,
            "braintrust.session_id": source_id,
            "braintrust.trace_ids": [turn.trace_id for turn in turns],
            "source_trace_count": len(turns),
            "source_completeness": "full" if full_export else "flat",
            "normalization_warnings": warnings,
        }
        if join_on is not None:
            metadata["braintrust.join_on"] = join_on
        framework = (
            _detect_framework(
                [
                    {
                        "metadata": row.get("metadata"),
                        "span_attributes": row.get("span_attributes"),
                        "name": row.get("name"),
                    }
                    for row in records
                ]
            )
            or file_framework
        )
        return ImportedSession(
            external_id=f"{source_instance}:{source_id}",
            name=str(
                _dict(root.get("span_attributes")).get("name")
                or root.get("name")
                or source_id
            ),
            status=session_status,
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
            started_at=min(
                (turn.started_at for turn in turns if turn.started_at),
                default=None,
            ),
            ended_at=max(
                (turn.ended_at for turn in turns if turn.ended_at),
                default=None,
            ),
            metadata=metadata,
            framework=framework,
            nodes=_build_node_tree(nodes_with_parents),
        )


def parse(
    content: bytes,
    params: dict[str, Any],
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse Braintrust JSON through the unified importer contract."""
    yield from BraintrustProjectLogImporter().parse(content, params)
