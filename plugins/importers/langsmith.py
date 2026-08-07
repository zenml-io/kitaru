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
"""LangSmith run-export importer plugin."""

import json
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import (
    ImportedNode,
    ImportedSession,
)

MAX_UPLOAD_BYTES = 50 * 1024 * 1024
_DEFAULT_JOIN_PATHS = (
    "extra.metadata.thread_id",
    "extra.metadata.session_id",
    "extra.metadata.conversation_id",
    "metadata.thread_id",
    "metadata.session_id",
    "metadata.conversation_id",
)
_METADATA_KEYS = {
    "assistant_id",
    "conversation_id",
    "environment",
    "graph_id",
    "langgraph_checkpoint_ns",
    "langgraph_node",
    "revision_id",
    "session_id",
    "thread_id",
    "user_id",
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
    """Raised when a LangSmith payload cannot be normalized."""


@dataclass(frozen=True, slots=True)
class _Turn:
    """One LangSmith trace within a grouped session."""

    trace_id: str
    inputs: Any
    outputs: Any
    started_at: datetime | None
    ended_at: datetime | None


def _decode_json(value: Any) -> Any:
    """Decode JSON-encoded export fields while preserving ordinary strings."""
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or stripped[0] not in '[{"':
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def _dict(value: Any) -> dict[str, Any]:
    """Return a decoded dictionary or an empty dictionary."""
    decoded = _decode_json(value)
    return decoded if isinstance(decoded, dict) else {}


def _datetime(value: Any) -> datetime | None:
    """Parse an ISO or Unix timestamp as an aware datetime."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
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


def _integer(value: Any) -> int | None:
    """Parse one integer value."""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal | None:
    """Parse one decimal value."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _parse_records(content: bytes) -> list[dict[str, Any]]:
    """Parse JSON, JSONL, and LangSmith run-query envelopes."""
    if len(content) > MAX_UPLOAD_BYTES:
        raise InvalidImport("LangSmith import exceeds the 50 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidImport("Import file must be UTF-8 JSON or JSONL") from exc
    if not text.strip():
        raise InvalidImport("Import file contains no JSON records")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise InvalidImport(f"Line {line_number} is not valid JSON") from exc
            if not isinstance(item, dict):
                raise InvalidImport(
                    f"Line {line_number} must be a JSON object"
                ) from None
            records.append(item)
    else:
        if isinstance(decoded, list):
            records = decoded
        elif isinstance(decoded, dict):
            enclosed = decoded.get("runs", decoded.get("data"))
            records = enclosed if isinstance(enclosed, list) else [decoded]
        else:
            raise InvalidImport("LangSmith JSON must contain an object or object array")
        if not all(isinstance(item, dict) for item in records):
            raise InvalidImport("Every LangSmith run must be a JSON object")
    if not records:
        raise InvalidImport("Import file contains no LangSmith runs")
    return records


def _path_value(value: Any, path: str) -> Any:
    """Resolve a dotted path or JSON pointer against one run record."""
    if not path.strip():
        raise InvalidImport("join_on must be a non-empty path")
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


def _trace_id(record: dict[str, Any]) -> str:
    """Return the stable trace identifier for one run."""
    value = record.get("trace_id") or record.get("traceId")
    if value in (None, "") and record.get("is_root") is True:
        value = record.get("id")
    if value in (None, ""):
        raise InvalidImport("A LangSmith run has no trace id")
    return str(value)


def _run_id(record: dict[str, Any]) -> str:
    """Return the stable run identifier."""
    value = record.get("id") or record.get("run_id")
    if value in (None, ""):
        raise InvalidImport("A LangSmith run has no run id")
    return str(value)


def _source_instance(record: dict[str, Any], params: dict[str, Any]) -> str:
    """Resolve the LangSmith project identity used for deduplication."""
    selected = params.get("source_instance")
    if selected not in (None, ""):
        return str(selected)
    value = (
        record.get("session_id")
        or record.get("project_id")
        or record.get("session_name")
        or record.get("project_name")
    )
    if value in (None, ""):
        raise InvalidImport(
            "LangSmith export has no project identity; provide source_instance"
        )
    return str(value)


def _join_value(
    record: dict[str, Any], params: dict[str, Any], trace_id: str
) -> tuple[str, str, bool]:
    """Resolve the session grouping value, path, and fallback status."""
    selected = params.get("join_on")
    if selected is not None:
        if not isinstance(selected, str):
            raise InvalidImport("join_on must be a dotted path or JSON pointer")
        value = _path_value(record, selected)
        if value in (None, ""):
            raise InvalidImport(
                f"Trace '{trace_id}' has no value at join_on path '{selected}'"
            )
        return str(value), selected, False
    for path in _DEFAULT_JOIN_PATHS:
        value = _path_value(record, path)
        if value not in (None, ""):
            return str(value), path, False
    return trace_id, "trace_id", True


def _started_at(record: dict[str, Any]) -> datetime | None:
    """Return one run start time."""
    return _datetime(record.get("start_time") or record.get("startTime"))


def _ended_at(record: dict[str, Any]) -> datetime | None:
    """Return one run end time."""
    return _datetime(record.get("end_time") or record.get("endTime"))


def _node_type(record: dict[str, Any]) -> tuple[NodeType, str | None]:
    """Map a LangSmith run type to a Kitaru node type."""
    run_type = str(record.get("run_type") or record.get("runType") or "").lower()
    if run_type in {"llm", "chat_model"}:
        return NodeType.LLM_CALL, None
    if run_type == "tool":
        return NodeType.TOOL_CALL, str(record.get("name") or "tool")
    return NodeType.SPAN, None


def _node_status(record: dict[str, Any]) -> NodeStatus:
    """Map LangSmith status and error fields."""
    if record.get("error") not in (None, ""):
        return NodeStatus.FAILED
    status = str(record.get("status") or "").lower()
    if status in {"error", "failed", "failure"}:
        return NodeStatus.FAILED
    if status in {"pending", "running", "in_progress"}:
        return NodeStatus.IN_PROGRESS
    return NodeStatus.COMPLETED


def _invocation(record: dict[str, Any]) -> dict[str, Any]:
    """Return model invocation parameters from common LangSmith locations."""
    extra = _dict(record.get("extra"))
    invocation = _dict(extra.get("invocation_params"))
    if invocation:
        return invocation
    return _dict(_dict(record.get("serialized")).get("kwargs"))


def _run_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return decoded metadata stored under a LangSmith run's extra field."""
    extra = _dict(record.get("extra"))
    return _dict(extra.get("metadata")) or _dict(record.get("metadata"))


def _tokens(record: dict[str, Any]) -> TokenUsage | None:
    """Map token counts from bulk exports and SDK run payloads."""
    extra = _dict(record.get("extra"))
    outputs = _dict(record.get("outputs"))
    llm_output = _dict(outputs.get("llm_output"))
    usage = (
        _dict(extra.get("token_usage"))
        or _dict(llm_output.get("token_usage"))
        or _dict(outputs.get("usage_metadata"))
    )
    input_tokens = _integer(
        record.get("prompt_tokens")
        or usage.get("prompt_tokens")
        or usage.get("input_tokens")
    )
    output_tokens = _integer(
        record.get("completion_tokens")
        or usage.get("completion_tokens")
        or usage.get("output_tokens")
    )
    cached_tokens = _integer(
        usage.get("cache_read_input_tokens") or usage.get("cached_input_tokens")
    )
    if input_tokens is None and output_tokens is None and cached_tokens is None:
        return None
    return TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_tokens,
    )


def _model_fields(record: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Return requested model, resolved model, and provider."""
    invocation = _invocation(record)
    metadata = _run_metadata(record)
    requested = (
        invocation.get("model")
        or invocation.get("model_name")
        or metadata.get("ls_model_name")
    )
    model = metadata.get("ls_model_name") or requested
    provider = metadata.get("ls_provider") or invocation.get("provider")
    return (
        str(requested) if requested not in (None, "") else None,
        str(model) if model not in (None, "") else None,
        str(provider) if provider not in (None, "") else None,
    )


def _metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Return bounded provider metadata for filtering and debugging."""
    selected: dict[str, Any] = {}
    metadata = _run_metadata(record)
    for key in _METADATA_KEYS:
        if metadata.get(key) not in (None, ""):
            selected[f"langsmith.{key}"] = metadata[key]
    if record.get("reference_example_id") not in (None, ""):
        selected["langsmith.reference_example_id"] = record["reference_example_id"]
    return selected


def _contains_tool_activity(value: Any) -> bool:
    """Detect model output containing tool calls without explicit tool runs."""
    decoded = _decode_json(value)
    if isinstance(decoded, dict):
        if any(key in decoded for key in ("tool_calls", "toolCalls")):
            return True
        return any(_contains_tool_activity(item) for item in decoded.values())
    if isinstance(decoded, list):
        return any(_contains_tool_activity(item) for item in decoded)
    return False


def _build_node_tree(
    nodes_with_parents: list[tuple[ImportedNode, str | None]],
) -> list[ImportedNode]:
    """Build and validate one imported node forest."""
    external_ids = [node.external_id for node, _ in nodes_with_parents]
    if any(value is None for value in external_ids):
        raise InvalidImport("The imported node graph contains a missing external id")
    if len(external_ids) != len(set(external_ids)):
        raise InvalidImport("The imported node graph contains duplicate external ids")
    converted = {
        node.external_id: node
        for node, _ in nodes_with_parents
        if node.external_id is not None
    }
    parents = {
        node.external_id: parent
        for node, parent in nodes_with_parents
        if node.external_id is not None and parent in converted
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
    for node, parent in nodes_with_parents:
        if parent in converted:
            converted[parent].children.append(node)
        else:
            roots.append(node)
    if nodes_with_parents and not roots:
        raise InvalidImport("The imported node graph contains no root node")
    return roots


class LangSmithRunImporter:
    """Parse LangSmith run-query and bulk-export JSON records."""

    def parse(
        self, content: bytes, params: dict[str, Any]
    ) -> list[ImportedSession | ImportFailure]:
        """Parse one LangSmith JSON or JSONL payload."""
        records = _parse_records(content)
        file_framework = _detect_framework(
            [
                {
                    "extra": record.get("extra"),
                    "name": record.get("name"),
                    "run_type": record.get("run_type"),
                    "tags": record.get("tags"),
                }
                for record in records
            ]
        )
        trace_records: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        failures: list[ImportFailure] = []
        for line, record in enumerate(records, start=1):
            try:
                trace_id = _trace_id(record)
                run_id = _run_id(record)
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(line=line, external_id=None, error=str(exc))
                )
                continue
            trace_records[trace_id].setdefault(run_id, {}).update(record)

        grouped: dict[tuple[str, str], list[tuple[str, list[dict[str, Any]]]]] = (
            defaultdict(list)
        )
        join_paths: dict[tuple[str, str], set[str]] = defaultdict(set)
        fallback_groups: set[tuple[str, str]] = set()
        for trace_id, by_run_id in sorted(trace_records.items()):
            rows = list(by_run_id.values())
            try:
                roots = self._get_roots(rows, trace_id)
                source_instances = {_source_instance(row, params) for row in rows}
                if len(source_instances) != 1:
                    raise InvalidImport(
                        f"Trace '{trace_id}' contains conflicting project identities"
                    )
                source_instance = next(iter(source_instances))
                join_values = {_join_value(row, params, trace_id) for row in roots}
                values = {value for value, _, _ in join_values}
                if len(values) != 1:
                    raise InvalidImport(
                        f"Trace '{trace_id}' contains conflicting session identifiers"
                    )
                source_id = next(iter(values))
                key = (source_instance, source_id)
                grouped[key].append((trace_id, rows))
                join_paths[key].update(path for _, path, _ in join_values)
                if any(fallback for _, _, fallback in join_values):
                    fallback_groups.add(key)
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1, external_id=trace_id, error=str(exc)
                    )
                )

        sessions: list[ImportedSession] = []
        for key, traces in sorted(grouped.items()):
            try:
                sessions.append(
                    self._parse_session(
                        key[0],
                        key[1],
                        traces,
                        join_paths=join_paths[key],
                        trace_fallback=key in fallback_groups,
                        file_framework=file_framework,
                    )
                )
            except InvalidImport as exc:
                failures.append(
                    ImportFailure(
                        line=len(failures) + 1,
                        external_id=key[1],
                        error=str(exc),
                    )
                )
        return [*sessions, *failures]

    def _get_roots(
        self, records: list[dict[str, Any]], trace_id: str
    ) -> list[dict[str, Any]]:
        """Return root runs in stable order."""
        ids = {_run_id(record) for record in records}
        roots = [
            record
            for record in records
            if record.get("is_root") is True
            or record.get("parent_run_id") in (None, "")
            or str(record.get("parent_run_id")) not in ids
        ]
        if not roots:
            raise InvalidImport(f"Trace '{trace_id}' contains no root run")
        return sorted(
            roots,
            key=lambda record: (
                _started_at(record) or datetime.min.replace(tzinfo=UTC),
                _run_id(record),
            ),
        )

    def _parse_session(
        self,
        source_instance: str,
        source_id: str,
        traces: list[tuple[str, list[dict[str, Any]]]],
        *,
        join_paths: set[str],
        trace_fallback: bool,
        file_framework: str | None,
    ) -> ImportedSession:
        """Normalize one grouped LangSmith session."""
        warnings: list[str] = []
        if trace_fallback:
            warnings.append("No LangSmith thread metadata found; grouped by trace id")
        turns: list[_Turn] = []
        roots_by_trace: dict[str, dict[str, Any]] = {}
        nodes_with_parents: list[tuple[ImportedNode, str | None]] = []
        graph_complete = True
        tags: set[str] = set()
        users: set[str] = set()
        for trace_id, records in traces:
            roots = self._get_roots(records, trace_id)
            if len(roots) != 1:
                warnings.append(f"Trace '{trace_id}' has {len(roots)} root runs")
                graph_complete = False
            root = roots[0]
            roots_by_trace[trace_id] = root
            turns.append(
                _Turn(
                    trace_id=trace_id,
                    inputs=_decode_json(root.get("inputs")),
                    outputs=_decode_json(root.get("outputs")),
                    started_at=min(
                        (value for row in records if (value := _started_at(row))),
                        default=None,
                    ),
                    ended_at=max(
                        (value for row in records if (value := _ended_at(row))),
                        default=None,
                    ),
                )
            )
            ids = {_run_id(record) for record in records}
            ordered = sorted(
                records,
                key=lambda record: (
                    str(record.get("dotted_order") or ""),
                    _started_at(record) or datetime.min.replace(tzinfo=UTC),
                    _run_id(record),
                ),
            )
            for record in ordered:
                run_id = _run_id(record)
                parent = record.get("parent_run_id")
                parent_id = str(parent) if parent not in (None, "") else None
                if parent_id and parent_id not in ids:
                    warnings.append(f"Run '{run_id}' references a missing parent")
                    graph_complete = False
                parent_external_id = (
                    f"{trace_id}:{parent_id}" if parent_id in ids else None
                )
                node_type, tool_name = _node_type(record)
                status = _node_status(record)
                requested_model, model, provider = _model_fields(record)
                metadata = _run_metadata(record)
                if metadata.get("user_id") not in (None, ""):
                    users.add(str(metadata["user_id"]))
                raw_tags = record.get("tags")
                if isinstance(raw_tags, list):
                    tags.update(str(tag) for tag in raw_tags)
                nodes_with_parents.append(
                    (
                        ImportedNode(
                            external_id=f"{trace_id}:{run_id}",
                            trace_id=trace_id,
                            node_type=node_type,
                            name=str(
                                record.get("name") or record.get("run_type") or "run"
                            ),
                            status=status,
                            error=(
                                str(record.get("error"))
                                if status is NodeStatus.FAILED
                                and record.get("error") not in (None, "")
                                else None
                            ),
                            started_at=_started_at(record),
                            ended_at=_ended_at(record),
                            inputs=_decode_json(record.get("inputs")),
                            outputs=_decode_json(record.get("outputs")),
                            requested_model=requested_model,
                            model=model,
                            provider=provider,
                            tokens=_tokens(record),
                            cost=_decimal(record.get("total_cost")),
                            model_params=_invocation(record) or None,
                            tool_name=tool_name,
                            attributes={
                                "langsmith.run_type": record.get("run_type"),
                                "langsmith.status": record.get("status"),
                                "langsmith.tags": raw_tags
                                if isinstance(raw_tags, list)
                                else [],
                            },
                            metadata=_metadata(record),
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
        system_prompt = _populate_node_fields(nodes)
        llm_nodes = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
        tool_nodes = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
        if (
            any(_contains_tool_activity(node.outputs) for node in llm_nodes)
            and not tool_nodes
        ):
            warnings.append(
                "Model output contains tool calls but no explicit tool runs"
            )
            graph_complete = False
        latest_turn = turns[-1]
        latest_root = roots_by_trace[latest_turn.trace_id]
        root_status = _node_status(latest_root)
        session_status = (
            SessionStatus.FAILED
            if root_status is NodeStatus.FAILED
            else SessionStatus.COMPLETED
        )
        session_error = (
            str(latest_root.get("error"))
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
        metadata = {
            "langsmith.project_id": source_instance,
            "langsmith.thread_id": source_id,
            "langsmith.trace_ids": [turn.trace_id for turn in turns],
            "langsmith.join_paths": sorted(join_paths),
            "langsmith.tags": sorted(tags),
            "langsmith.user_ids": sorted(users),
            "source_trace_count": len(turns),
            "source_completeness": "full" if graph_complete else "partial",
            "normalization_warnings": warnings,
        }
        framework = (
            _detect_framework(
                [
                    {
                        "extra": record.get("extra"),
                        "name": record.get("name"),
                        "run_type": record.get("run_type"),
                        "tags": record.get("tags"),
                    }
                    for _, rows in traces
                    for record in rows
                ]
            )
            or file_framework
        )
        return ImportedSession(
            external_id=f"{source_instance}:{source_id}",
            name=str(latest_root.get("name") or source_id),
            status=session_status,
            system_prompt=system_prompt,
            inputs=inputs,
            outputs=(
                latest_turn.outputs
                if latest_turn.outputs is not None
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
                (turn.started_at for turn in turns if turn.started_at), default=None
            ),
            ended_at=max(
                (turn.ended_at for turn in turns if turn.ended_at), default=None
            ),
            metadata=metadata,
            framework=framework,
            nodes=_build_node_tree(nodes_with_parents),
        )


def parse(
    content: bytes,
    params: dict[str, Any],
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse LangSmith JSON through the worker importer contract."""
    yield from LangSmithRunImporter().parse(content, params)
