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
"""Logfire records-query importer plugin."""

import json
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
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
_DEFAULT_JOIN_PATHS = (
    "attributes.session.id",
    "attributes.session_id",
    "attributes.conversation_id",
    "attributes.thread_id",
    "attributes.gen_ai.conversation.id",
    "attributes.conversation.id",
)
_METADATA_COLUMNS = (
    "deployment_environment",
    "service_name",
    "service_namespace",
    "service_version",
    "otel_scope_name",
    "otel_scope_version",
    "tags",
)
_ATTRIBUTE_METADATA_KEYS = {
    "agent_name",
    "deployment.environment.name",
    "gen_ai.agent.name",
    "gen_ai.conversation.id",
    "gen_ai.operation.name",
    "gen_ai.provider.name",
    "gen_ai.request.model",
    "gen_ai.response.model",
    "gen_ai.system",
    "service.name",
    "service.version",
    "session.id",
    "session_id",
    "thread_id",
    "user.id",
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


class InvalidImport(ValueError):
    """Raised when a Logfire payload cannot be parsed."""


@dataclass(frozen=True, slots=True)
class _Turn:
    """One Logfire trace within a session."""

    trace_id: str
    inputs: Any
    outputs: Any
    started_at: datetime | None
    ended_at: datetime | None


def _escape_failure_text(value: str) -> str:
    """Escape unencodable characters in failure diagnostics only."""
    return value.encode("utf-8", errors="backslashreplace").decode("utf-8")


def _decode_json(value: Any) -> Any:
    """Decode JSON-encoded query columns while preserving ordinary strings."""
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


def _dict(value: Any) -> dict[str, Any]:
    """Return a decoded dictionary or an empty dictionary."""
    decoded = _decode_json(value)
    return decoded if isinstance(decoded, dict) else {}


def _datetime(value: Any) -> datetime | None:
    """Parse a Logfire timestamp."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
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


def _parse_records(content: bytes) -> list[dict[str, Any]]:
    """Parse Logfire JSON, JSONL, or streaming NDJSON output."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("Logfire import exceeds the 50 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no JSON records")
    try:
        decoded = json.loads(text)
    except (ValueError, RecursionError):
        values: list[Any] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                values.append(json.loads(line))
            except (ValueError, RecursionError) as exc:
                raise InvalidImport(f"Line {line_number} is not valid JSON") from exc
    else:
        if isinstance(decoded, dict) and isinstance(decoded.get("data"), list):
            values = decoded["data"]
        elif isinstance(decoded, list):
            values = decoded
        else:
            values = [decoded]

    records: list[dict[str, Any]] = []
    for line_number, value in enumerate(values, start=1):
        if not isinstance(value, dict):
            raise InvalidImport(f"Record {line_number} must be a JSON object")
        message_type = value.get("type")
        if message_type in {"schema", "explain", "end"}:
            continue
        if message_type == "error":
            raise InvalidImport(
                str(
                    value.get("message")
                    or value.get("error")
                    or "Logfire query export failed"
                )
            )
        if message_type == "data" and isinstance(value.get("rows"), list):
            rows = value["rows"]
            if not all(isinstance(row, dict) for row in rows):
                raise InvalidImport(
                    f"Record {line_number} contains a non-object Logfire row"
                )
            records.extend(rows)
        elif message_type == "data" and isinstance(value.get("data"), dict):
            records.append(value["data"])
        else:
            records.append(value)
    if not records:
        raise InvalidImport("Import file contains no Logfire data rows")
    return records


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


def _path_value(record: dict[str, Any], path: str) -> Any:
    """Resolve a grouping path, including dotted OpenTelemetry attribute keys."""
    parts = _path_parts(path)
    current: Any = record
    for index, part in enumerate(parts):
        current = _decode_json(current)
        if not isinstance(current, dict):
            return None
        if part in current:
            current = current[part]
            continue
        remainder = ".".join(parts[index:])
        return current.get(remainder)
    return _decode_json(current)


def _is_missing_identity(value: Any) -> bool:
    """Return whether a grouping value has no usable identity."""
    if value in (None, ""):
        return True
    if not isinstance(value, str):
        return False
    return value.strip().casefold() in {"[redacted]", "[scrubbed]"}


def _join_value(
    records: list[dict[str, Any]], params: dict[str, Any], trace_id: str
) -> tuple[str, str, bool]:
    """Select the stable session identity for one trace."""
    configured = params.get("join_on")
    if configured is not None and not isinstance(configured, str):
        raise InvalidImport("join_on must be a dotted path or JSON pointer")
    paths = (configured,) if configured else _DEFAULT_JOIN_PATHS
    for path in paths:
        values = {
            str(value)
            for record in records
            if not _is_missing_identity(value := _path_value(record, path))
        }
        if len(values) > 1:
            raise InvalidImport(
                f"Trace '{trace_id}' has conflicting values at join path '{path}'"
            )
        if values:
            return next(iter(values)), path, False
    if configured:
        raise InvalidImport(
            f"Trace '{trace_id}' has no value at join path '{configured}'"
        )
    return trace_id, "trace_id", True


def _attributes(record: dict[str, Any]) -> dict[str, Any]:
    """Return decoded span attributes."""
    return _dict(record.get("attributes"))


def _attribute(record: dict[str, Any], *names: str) -> Any:
    """Return the first non-empty span attribute."""
    attributes = _attributes(record)
    for name in names:
        value = attributes.get(name)
        if value not in (None, ""):
            return _decode_json(value)
    return None


def _node_type(record: dict[str, Any]) -> tuple[NodeType, str | None]:
    """Map OpenTelemetry GenAI semantics to a Kitaru node type."""
    operation = str(_attribute(record, "gen_ai.operation.name") or "").casefold()
    span_name = str(record.get("span_name") or "")
    tool_name = _attribute(
        record,
        "gen_ai.tool.name",
        "tool.name",
        "tool_name",
    )
    if operation in {"execute_tool", "tool", "tool_call"} or tool_name:
        return NodeType.TOOL_CALL, str(tool_name or span_name or "tool")
    if operation in {
        "chat",
        "completion",
        "embeddings",
        "generate_content",
        "text_completion",
    } or _attribute(
        record,
        "gen_ai.request.model",
        "gen_ai.response.model",
        "gen_ai.system",
    ):
        return NodeType.LLM_CALL, None
    return NodeType.SPAN, None


def _node_status(record: dict[str, Any]) -> NodeStatus:
    """Map Logfire status and level fields."""
    status = str(
        record.get("otel_status_code") or record.get("status_code") or ""
    ).casefold()
    level = record.get("level")
    try:
        level_number = int(level) if level is not None else None
    except (TypeError, ValueError, OverflowError):
        level_number = None
    if status == "error" or record.get("is_exception") is True:
        return NodeStatus.FAILED
    if isinstance(level, str) and level.casefold() in {"error", "fatal"}:
        return NodeStatus.FAILED
    if level_number is not None and level_number >= 17:
        return NodeStatus.FAILED
    return NodeStatus.COMPLETED


def _payload(record: dict[str, Any], *names: str) -> Any:
    """Return the first decoded payload attribute."""
    return _attribute(record, *names)


def _tokens(record: dict[str, Any]) -> TokenUsage | None:
    """Map OpenTelemetry GenAI token attributes."""
    values = (
        _parse_token_count(
            _attribute(
                record,
                "gen_ai.usage.input_tokens",
                "gen_ai.usage.prompt_tokens",
            )
        ),
        _parse_token_count(
            _attribute(
                record,
                "gen_ai.usage.output_tokens",
                "gen_ai.usage.completion_tokens",
            )
        ),
        _parse_token_count(
            _attribute(
                record,
                "gen_ai.usage.details.cache_read_tokens",
                "gen_ai.usage.cached_input_tokens",
            )
        ),
        _parse_token_count(_attribute(record, "gen_ai.usage.details.reasoning_tokens")),
    )
    if all(value is None for value in values):
        return None
    return TokenUsage(
        input_tokens=values[0],
        output_tokens=values[1],
        cached_input_tokens=values[2],
        reasoning_tokens=values[3],
    )


def _source_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Select bounded Logfire metadata for filtering and debugging."""
    metadata = {
        f"logfire.{column}": record[column]
        for column in _METADATA_COLUMNS
        if record.get(column) not in (None, "", [])
    }
    attributes = _attributes(record)
    for key in _ATTRIBUTE_METADATA_KEYS:
        if attributes.get(key) not in (None, ""):
            metadata[f"logfire.attributes.{key}"] = attributes[key]
    return metadata


def _detect_framework(records: list[dict[str, Any]], configured: Any) -> str | None:
    """Detect one supported agent framework."""
    evidence = [str(configured or "")]
    for record in records:
        evidence.extend(
            str(value)
            for value in (
                record.get("otel_scope_name"),
                record.get("span_name"),
                _attribute(record, "gen_ai.agent.name"),
            )
            if value
        )
    joined = "\n".join(evidence)
    matches = {
        framework
        for pattern, framework in _FRAMEWORK_PATTERNS
        if pattern.search(joined)
    }
    return next(iter(matches)) if len(matches) == 1 else None


def _build_node_tree(
    nodes_with_parents: list[tuple[ImportedNode, str | None]],
) -> list[ImportedNode]:
    """Build an acyclic node tree while preserving orphans as roots."""
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
            roots.append(node)
    return roots


class LogfireRecordsImporter:
    """Normalize exported rows from Logfire's records table."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedSession | ImportFailure]:
        """Parse Logfire records into sessions and isolated failures."""
        records = _parse_records(content)
        traces: dict[str, list[dict[str, Any]]] = defaultdict(list)
        failures: list[ImportFailure] = []
        for line_number, record in enumerate(records, start=1):
            trace_id = record.get("trace_id")
            span_id = record.get("span_id")
            if trace_id in (None, "") or span_id in (None, ""):
                failures.append(
                    ImportFailure(
                        line=line_number,
                        external_id=_escape_failure_text(str(trace_id))
                        if trace_id
                        else None,
                        error="Logfire row lacks trace_id or span_id",
                    )
                )
                continue
            traces[str(trace_id)].append(record)

        grouped: dict[tuple[str, str], list[tuple[str, list[dict[str, Any]]]]] = (
            defaultdict(list)
        )
        join_paths: dict[tuple[str, str], set[str]] = defaultdict(set)
        fallback_sessions: set[tuple[str, str]] = set()
        for trace_id, rows in traces.items():
            try:
                session_id, join_path, fallback = _join_value(rows, params, trace_id)
                project_ids = {
                    str(row["project_id"])
                    for row in rows
                    if row.get("project_id") not in (None, "")
                }
                if len(project_ids) > 1:
                    raise InvalidImport(
                        f"Trace '{trace_id}' contains conflicting Logfire project ids"
                    )
                source_instance_value = (
                    params.get("source_instance")
                    or params.get("project_id")
                    or next(iter(project_ids), None)
                )
                source_instance = (
                    str(source_instance_value).strip()
                    if source_instance_value
                    else "logfire"
                )
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1,
                        external_id=_escape_failure_text(trace_id),
                        error=_escape_failure_text(str(exc)),
                    )
                )
                continue
            key = (source_instance, session_id)
            grouped[key].append((trace_id, rows))
            join_paths[key].add(join_path)
            if fallback:
                fallback_sessions.add(key)

        for (source_instance, session_id), session_traces in sorted(grouped.items()):
            try:
                session = self._parse_session(
                    source_instance,
                    session_id,
                    session_traces,
                    framework=params.get("framework"),
                    join_paths=join_paths[(source_instance, session_id)],
                    trace_fallback=(source_instance, session_id) in fallback_sessions,
                )
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
        traces: list[tuple[str, list[dict[str, Any]]]],
        *,
        framework: Any,
        join_paths: set[str],
        trace_fallback: bool,
    ) -> ImportedSession:
        """Normalize one grouped Logfire session."""
        project_ids = {
            str(row["project_id"])
            for _, rows in traces
            for row in rows
            if row.get("project_id") not in (None, "")
        }
        if len(project_ids) > 1:
            raise InvalidImport(
                f"Session '{session_id}' contains conflicting Logfire project ids"
            )
        warnings: list[str] = []
        if not project_ids and source_instance == "logfire":
            warnings.append(
                "No Logfire project identity supplied; using source_instance 'logfire'"
            )
        if trace_fallback:
            warnings.append("No session attribute found; grouped by trace id")

        turns: list[_Turn] = []
        nodes_with_parents: list[tuple[ImportedNode, str | None]] = []
        all_records: list[dict[str, Any]] = []
        roots_by_trace: dict[str, dict[str, Any]] = {}
        for trace_id, rows in traces:
            ordered = sorted(
                rows,
                key=lambda row: (
                    _datetime(row.get("start_timestamp"))
                    or datetime.min.replace(tzinfo=UTC),
                    str(row.get("span_id")),
                ),
            )
            ids = {str(row["span_id"]) for row in ordered}
            roots = [
                row
                for row in ordered
                if row.get("parent_span_id") in (None, "")
                or str(row.get("parent_span_id")) not in ids
            ]
            if len(roots) != 1:
                warnings.append(f"Trace '{trace_id}' has {len(roots)} root records")
            root = roots[0] if roots else ordered[0]
            roots_by_trace[trace_id] = root
            trace_start = min(
                (
                    timestamp
                    for row in ordered
                    if (timestamp := _datetime(row.get("start_timestamp")))
                ),
                default=None,
            )
            trace_end = max(
                (
                    timestamp
                    for row in ordered
                    if (timestamp := _datetime(row.get("end_timestamp")))
                ),
                default=None,
            )
            turn_input = _payload(
                root,
                "input",
                "inputs",
                "raw_input",
                "pydantic_ai.all_messages",
                "gen_ai.input.messages",
                "gen_ai.prompt",
            )
            turn_output = _payload(
                root,
                "output",
                "outputs",
                "final_result",
                "gen_ai.output.messages",
                "gen_ai.completion",
            )
            turns.append(
                _Turn(trace_id, turn_input, turn_output, trace_start, trace_end)
            )
            for row in ordered:
                span_id = str(row["span_id"])
                parent_id = row.get("parent_span_id")
                parent_external_id = (
                    f"{trace_id}:{parent_id}"
                    if parent_id not in (None, "") and str(parent_id) in ids
                    else None
                )
                if parent_id not in (None, "") and parent_external_id is None:
                    warnings.append(
                        f"Span '{span_id}' references missing parent '{parent_id}'"
                    )
                node_type, tool_name = _node_type(row)
                status = _node_status(row)
                error = None
                if status is NodeStatus.FAILED:
                    error_value = (
                        row.get("exception_message")
                        or row.get("otel_status_message")
                        or row.get("message")
                    )
                    error = str(error_value) if error_value else "Logfire span failed"
                inputs = _payload(
                    row,
                    "input",
                    "inputs",
                    "raw_input",
                    "pydantic_ai.all_messages",
                    "gen_ai.input.messages",
                    "gen_ai.prompt",
                    "tool.arguments",
                    "gen_ai.tool.call.arguments",
                )
                outputs = _payload(
                    row,
                    "output",
                    "outputs",
                    "final_result",
                    "gen_ai.output.messages",
                    "gen_ai.completion",
                    "tool.result",
                    "gen_ai.tool.call.result",
                )
                nodes_with_parents.append(
                    (
                        ImportedNode(
                            external_id=f"{trace_id}:{span_id}",
                            trace_id=trace_id,
                            node_type=node_type,
                            name=str(
                                row.get("span_name")
                                or row.get("message")
                                or row.get("kind")
                                or "span"
                            ),
                            status=status,
                            error=error,
                            started_at=_datetime(row.get("start_timestamp")),
                            ended_at=_datetime(row.get("end_timestamp")),
                            inputs=inputs,
                            outputs=outputs,
                            requested_model=(
                                str(value)
                                if (value := _attribute(row, "gen_ai.request.model"))
                                else None
                            ),
                            model=(
                                str(value)
                                if (
                                    value := _attribute(
                                        row,
                                        "gen_ai.response.model",
                                        "gen_ai.request.model",
                                    )
                                )
                                else None
                            ),
                            model_provider=(
                                str(value)
                                if (
                                    value := _attribute(
                                        row,
                                        "gen_ai.provider.name",
                                        "gen_ai.system",
                                    )
                                )
                                else None
                            ),
                            tokens=_tokens(row),
                            cost=_decimal(
                                _attribute(
                                    row,
                                    "gen_ai.usage.cost",
                                    "gen_ai.cost.total",
                                    "operation.cost",
                                )
                            ),
                            model_params=_dict(
                                _attribute(
                                    row,
                                    "gen_ai.request.parameters",
                                    "model_parameters",
                                    "model_request_parameters",
                                    "model_settings",
                                )
                            )
                            or None,
                            tool_name=tool_name,
                            attributes={
                                "logfire.kind": row.get("kind"),
                                "logfire.level": row.get("level"),
                                "logfire.message": row.get("message"),
                                "logfire.attributes": _attributes(row),
                            },
                            metadata=_source_metadata(row),
                            children=[],
                        ),
                        parent_external_id,
                    )
                )
            all_records.extend(ordered)

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
        node_tree = _build_node_tree(nodes_with_parents)
        latest_turn = turns[-1]
        latest_root = roots_by_trace[latest_turn.trace_id]
        session_status = (
            SessionStatus.FAILED
            if _node_status(latest_root) is NodeStatus.FAILED
            else SessionStatus.COMPLETED
        )
        metadata: dict[str, Any] = {
            "logfire.session_id": session_id,
            "logfire.project_id": next(iter(project_ids), None),
            "logfire.trace_ids": [turn.trace_id for turn in turns],
            "logfire.join_paths": sorted(join_paths),
            "logfire.services": sorted(
                {
                    str(row["service_name"])
                    for row in all_records
                    if row.get("service_name") not in (None, "")
                }
            ),
            "logfire.environments": sorted(
                {
                    str(row["deployment_environment"])
                    for row in all_records
                    if row.get("deployment_environment") not in (None, "")
                }
            ),
            "source_trace_count": len(turns),
            "source_completeness": "query-dependent",
            "normalization_warnings": warnings,
        }
        latest_error = (
            latest_root.get("exception_message")
            or latest_root.get("otel_status_message")
            if session_status is SessionStatus.FAILED
            else None
        )
        return ImportedSession(
            external_id=f"{source_instance}:{session_id}",
            name=str(
                latest_root.get("span_name") or latest_root.get("message") or session_id
            ),
            status=session_status,
            inputs={
                "schema_version": 1,
                "turns": [
                    {
                        "source_trace_id": turn.trace_id,
                        "inputs": turn.inputs,
                        "outputs": turn.outputs,
                    }
                    for turn in turns
                ],
            },
            outputs=latest_turn.outputs,
            error=str(latest_error) if latest_error else None,
            started_at=min(
                (turn.started_at for turn in turns if turn.started_at), default=None
            ),
            ended_at=max(
                (turn.ended_at for turn in turns if turn.ended_at), default=None
            ),
            metadata=metadata,
            framework=_detect_framework(all_records, framework),
            nodes=node_tree,
        )


def parse(
    content: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse a Logfire records-query export through the importer contract."""
    yield from LogfireRecordsImporter().parse(content, params)
