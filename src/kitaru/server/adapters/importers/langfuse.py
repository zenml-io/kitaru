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
"""Langfuse JSONL trace importer."""

import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from kitaru.server.application.models.import_jobs import (
    ImportContext,
    ImporterDescriptor,
    NormalizationError,
    NormalizedImport,
    NormalizedNode,
    NormalizedSession,
    NormalizedTurn,
    ReplayReadiness,
)
from kitaru.server.domain.import_job import InvalidImport
from kitaru.server.domain.session import SessionStatus, TokenUsage
from kitaru.server.domain.session_node import NodeStatus, NodeType

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_TRACE_SHAPE = "trace"
_OBSERVATION_SHAPE = "observation"
_EVENT_SHAPE = "ingestion_event"


def _first(record: dict[str, Any], *names: str) -> Any:
    """Return the first present field."""
    for name in names:
        if name in record:
            return record[name]
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


def _detect_shape(record: dict[str, Any]) -> str:
    """Detect one supported Langfuse JSONL row shape."""
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
    raise InvalidImport("Could not detect the Langfuse JSONL row shape")


def _parse_lines(content: bytes) -> list[dict[str, Any]]:
    """Parse non-empty JSONL records."""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSONL") from exc
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise InvalidImport(f"Line {line_number} is not valid JSON") from exc
        if not isinstance(value, dict):
            raise InvalidImport(f"Line {line_number} must contain a JSON object")
        records.append(value)
    if not records:
        raise InvalidImport("Import file contains no JSONL records")
    shapes = {_detect_shape(record) for record in records}
    if len(shapes) != 1:
        raise InvalidImport("Import file mixes multiple Langfuse JSONL row shapes")
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
    if explicit_tool:
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
    return TokenUsage.from_counts(
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


def _canonical_digest(value: Any) -> str:
    """Hash one normalized JSON-compatible value."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class LangfuseJSONLImporter:
    """Normalize Langfuse trace, observation, and ingestion-event JSONL."""

    @property
    def descriptor(self) -> ImporterDescriptor:
        """Return importer metadata."""
        return ImporterDescriptor(
            id="langfuse",
            display_name="Langfuse JSONL",
            version="1",
            file_extensions=[".jsonl"],
            max_upload_bytes=MAX_UPLOAD_BYTES,
        )

    def parse(self, content: bytes, context: ImportContext) -> NormalizedImport:
        """Parse a Langfuse JSONL upload.

        Args:
            content: Complete uploaded JSONL.
            context: User selections.

        Returns:
            Multi-turn sessions and isolated normalization errors.
        """
        records = _parse_lines(content)
        shape = _detect_shape(records[0])
        if shape == _EVENT_SHAPE:
            records = _events_to_traces(records)
            shape = _TRACE_SHAPE
        if shape == _TRACE_SHAPE:
            records = _trace_rows_to_observations(records)

        trace_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            trace_id = str(_first(record, "traceId", "trace_id") or "")
            if not trace_id:
                raise InvalidImport("A Langfuse observation has no trace id")
            trace_records[trace_id].append(record)

        session_traces: dict[str, list[tuple[str, list[dict[str, Any]]]]] = defaultdict(
            list
        )
        for trace_id, observations in trace_records.items():
            session_ids = {
                str(value)
                for record in observations
                if (value := _first(record, "sessionId", "session_id"))
            }
            if len(session_ids) > 1:
                raise InvalidImport(
                    f"Trace '{trace_id}' contains conflicting Langfuse session ids"
                )
            session_id = next(iter(session_ids), trace_id)
            session_traces[session_id].append((trace_id, observations))

        sessions: list[NormalizedSession] = []
        errors: list[NormalizationError] = []
        for source_id, traces in sorted(session_traces.items()):
            try:
                sessions.append(self._normalize_session(source_id, traces, context))
            except InvalidImport as exc:
                errors.append(NormalizationError(source_id=source_id, message=str(exc)))
        return NormalizedImport(sessions=sessions, errors=errors)

    def _normalize_session(
        self,
        source_id: str,
        traces: list[tuple[str, list[dict[str, Any]]]],
        context: ImportContext,
    ) -> NormalizedSession:
        """Normalize one grouped Langfuse session."""
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
        source_instance = context.source_instance or next(iter(project_ids), None)
        if not source_instance:
            raise InvalidImport(
                f"Session '{source_id}' has no project id; provide source_instance"
            )

        warnings: list[str] = []
        raw_nodes: list[tuple[str, str | None, dict[str, Any]]] = []
        turns: list[NormalizedTurn] = []
        trace_names: list[str] = []
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
                NormalizedTurn(
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
        nodes: list[NormalizedNode] = []
        for source_node_id, parent_source_id, record in raw_nodes:
            node_type, tool_name = _node_type(record)
            status = _node_status(record)
            metadata = _dict(record.get("metadata"))
            nodes.append(
                NormalizedNode(
                    source_id=source_node_id,
                    parent_source_id=parent_source_id,
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
                    requested_model=_first(
                        record, "providedModelName", "provided_model_name"
                    ),
                    model=_first(record, "modelId", "model_id"),
                    provider=_first(record, "modelProvider", "model_provider"),
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
                    source_metadata=metadata,
                )
            )

        graph_complete = not any("missing parent" in warning for warning in warnings)
        tool_nodes = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
        replayable_tools = [
            node
            for node in tool_nodes
            if node.tool_name and node.inputs is not None and node.outputs is not None
        ]
        root_inputs_available = bool(turns) and all(
            turn.inputs is not None for turn in turns
        )
        reasons = list(warnings)
        if not root_inputs_available:
            reasons.append("One or more turns have no root input")
        if len(replayable_tools) != len(tool_nodes):
            reasons.append("One or more tool calls lack a name, input, or output")
        if not root_inputs_available:
            readiness_level = "unavailable"
        elif graph_complete and len(replayable_tools) == len(tool_nodes):
            readiness_level = "ready"
        else:
            readiness_level = "partial"
        readiness = ReplayReadiness(
            level=readiness_level,
            root_inputs_available=root_inputs_available,
            graph_complete=graph_complete,
            tool_call_count=len(tool_nodes),
            replayable_tool_call_count=len(replayable_tools),
            reasons=reasons,
        )
        failed_nodes = [node for node in nodes if node.status is NodeStatus.FAILED]
        session_status = (
            SessionStatus.FAILED if failed_nodes else SessionStatus.COMPLETED
        )
        started_at = min(
            (turn.started_at for turn in turns if turn.started_at), default=None
        )
        ended_at = max((turn.ended_at for turn in turns if turn.ended_at), default=None)
        inputs = {
            "schema_version": 1,
            "turns": [
                {"source_trace_id": turn.trace_id, "inputs": turn.inputs}
                for turn in turns
            ],
        }
        digest_payload = {
            "source_id": source_id,
            "source_instance": source_instance,
            "status": session_status,
            "turns": [turn.model_dump(mode="json") for turn in turns],
            "nodes": [node.model_dump(mode="json") for node in nodes],
        }
        return NormalizedSession(
            source_id=source_id,
            source_instance=source_instance,
            name=trace_names[-1] if trace_names else None,
            status=session_status,
            turns=turns,
            nodes=nodes,
            inputs=inputs,
            outputs=turns[-1].outputs if turns else None,
            error=failed_nodes[-1].error if failed_nodes else None,
            started_at=started_at,
            ended_at=ended_at,
            source_metadata={
                "langfuse.project_id": next(iter(project_ids), None),
                "langfuse.session_id": source_id,
                "langfuse.trace_ids": [turn.trace_id for turn in turns],
                "source_trace_count": len(turns),
                "source_completeness": "unknown",
            },
            warnings=warnings,
            readiness=readiness,
            content_digest=_canonical_digest(digest_payload),
        )
