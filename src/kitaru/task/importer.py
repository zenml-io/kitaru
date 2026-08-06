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
"""Importer plugin contract and the import flow."""

import json
import re
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from kitaru.api_models.v1.imports import MAX_IMPORT_FAILURES, ImportFailure, ImportStats
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.task import ImportTaskDetails, ScriptPluginSpec
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.task.plugins import PluginLoadError, load_plugin_entrypoint, load_source_ref
from kitaru.task.task_io import get_required_env, write_task_result

__all__ = [
    "MAX_IMPORT_FAILURES",
    "NODE_BATCH_SIZE",
    "ImportFailure",
    "ImportStats",
    "ParsedItem",
    "ParsedNode",
    "ParsedSession",
    "Parser",
    "SessionImportError",
    "call_parser",
    "detect_framework",
    "flatten_nodes",
    "populate_node_display_fields",
    "run",
    "session_request",
]

NODE_BATCH_SIZE = 200

_LABEL = "Importer"

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


class SessionImportError(Exception):
    """Raised when a parser fails while starting or advancing."""


class ParsedNode(BaseModel):
    """Parsed session node."""

    model_config = ConfigDict(extra="forbid")

    index: int | None = None
    parent_index: int | None = None
    secondary_parent_indexes: list[int] = Field(default_factory=list)
    external_id: str | None = None
    trace_id: str | None = None
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    input_text: str | None = None
    output_text: str | None = None
    system_prompt: str | None = None
    reasoning: str | None = None
    inputs: Any
    outputs: Any
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: str | None = None
    attributes: Any
    metadata: dict[str, Any] = Field(default_factory=dict)
    children: list["ParsedNode"] = Field(default_factory=list)


ParsedNode.model_rebuild()


class ParsedSession(BaseModel):
    """Parsed session."""

    model_config = ConfigDict(extra="forbid")

    status: SessionStatus
    name: str | None = None
    system_prompt: str | None = None
    inputs: Any
    outputs: Any
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    external_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    framework: str | None = None
    nodes: list[ParsedNode]


ParsedItem = ParsedSession | ImportFailure

Parser = Callable[[bytes, dict[str, Any]], Iterator[ParsedItem]]


def _role(value: dict[str, Any]) -> str | None:
    """Return a normalized role from common message encodings."""
    candidates = [value.get("role"), value.get("type"), value.get("part_kind")]
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
        if normalized in {"assistant", "ai", "aimessage", "model"}:
            return "assistant"
    return None


def _content_text(value: Any, depth: int = 0) -> str | None:
    """Return displayable text from a common content value."""
    if depth > 8:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, list):
        parts = [
            text
            for item in value
            if (text := _content_text(item, depth + 1)) is not None
        ]
        return "\n".join(parts) or None
    if not isinstance(value, dict):
        return None
    for key in ("text", "content"):
        if key in value and (text := _content_text(value[key], depth + 1)):
            return text
    for key in ("parts", "kwargs", "data"):
        if key in value and (text := _content_text(value[key], depth + 1)):
            return text
    return None


def _message_texts(value: Any, target_role: str, depth: int = 0) -> list[str]:
    """Collect text for one role from nested provider message data."""
    if depth > 12:
        return []
    if isinstance(value, list):
        return [
            text
            for item in value
            for text in _message_texts(item, target_role, depth + 1)
        ]
    if not isinstance(value, dict):
        return []
    role = _role(value)
    if role == target_role:
        for key in ("content", "text", "parts", "kwargs", "data"):
            if key in value and (text := _content_text(value[key], depth + 1)):
                return [text]
    texts: list[str] = []
    for child in value.values():
        texts.extend(_message_texts(child, target_role, depth + 1))
    return texts


def _json_text(value: Any) -> str | None:
    """Serialize a structured tool value for display."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _extract_input_text(value: Any) -> str | None:
    """Extract the latest user input from provider data."""
    messages = _message_texts(value, "user")
    if messages:
        return messages[-1]
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("prompt", "query", "question", "user_input", "message"):
            if key in value and (text := _content_text(value[key])):
                return text
    return None


def _extract_output_text(value: Any) -> str | None:
    """Extract the latest assistant output from provider data."""
    messages = _message_texts(value, "assistant")
    if messages:
        return messages[-1]
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for key in ("answer", "result", "response", "output", "text", "content"):
            if key in value and (text := _content_text(value[key])):
                return text
    return None


def _extract_system_prompt(value: Any) -> str | None:
    """Extract the latest system prompt from provider data."""
    messages = _message_texts(value, "system")
    if messages:
        return messages[-1]
    if isinstance(value, dict):
        for key in ("system_prompt", "system_instruction", "instructions"):
            if key in value and (text := _content_text(value[key])):
                return text
    return None


def _extract_reasoning(value: Any) -> str | None:
    """Extract visible reasoning text from provider data."""
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
                if key in item and (text := _content_text(item[key], depth + 1)):
                    found.append(text)
        for key in ("reasoning", "reasoning_content", "thinking", "thought"):
            if key in item and (text := _content_text(item[key], depth + 1)):
                found.append(text)
        for child in item.values():
            _collect(child, depth + 1)

    _collect(value)
    return found[-1] if found else None


def detect_framework(value: Any) -> str | None:
    """Detect one supported framework from provider metadata.

    Args:
        value: Provider metadata or attributes.

    Returns:
        Canonical framework name when the evidence identifies exactly one.
    """
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


def populate_node_display_fields(nodes: list[ParsedNode]) -> str | None:
    """Populate node text projections and return the latest system prompt.

    Args:
        nodes: Nodes ordered by source time.

    Returns:
        System prompt from the latest model call that contains one.
    """
    session_system_prompt = None
    for node in nodes:
        if node.node_type is NodeType.TOOL_CALL:
            node.input_text = _extract_input_text(node.inputs) or _json_text(
                node.inputs
            )
            node.output_text = _extract_output_text(node.outputs) or _json_text(
                node.outputs
            )
        else:
            node.input_text = _extract_input_text(node.inputs)
            node.output_text = _extract_output_text(node.outputs)
        if node.node_type is NodeType.LLM_CALL:
            node.system_prompt = _extract_system_prompt(node.inputs)
            node.reasoning = _extract_reasoning(node.outputs) or _extract_reasoning(
                node.inputs
            )
            session_system_prompt = node.system_prompt or session_system_prompt
    return session_system_prompt


def call_parser(
    parser: Parser, payload: bytes, params: dict[str, Any]
) -> Iterator[ParsedItem]:
    """Advance a parser one item at a time, wrapping any failure.

    Wrapping only the parser call would protect nothing, since a generator
    function runs no code until iterated. This wraps every step of the
    iteration instead.

    Args:
        parser: Parser callable.
        payload: Raw payload bytes.
        params: Parameters passed to the parser.

    Raises:
        SessionImportError: The parser raised while starting or advancing, or
            yielded an item that is not a ParsedSession or ImportFailure.

    Yields:
        Parsed items.
    """
    try:
        iterator = iter(parser(payload, params))
    except Exception as exc:
        raise SessionImportError(f"Parser raised an error: {exc}") from exc
    while True:
        try:
            item = next(iterator)
        except StopIteration:
            return
        except Exception as exc:
            raise SessionImportError(f"Parser raised an error: {exc}") from exc
        if not isinstance(item, ParsedSession | ImportFailure):
            raise SessionImportError(
                f"Parser yielded an item that is not a ParsedSession or "
                f"ImportFailure: {item!r}"
            )
        yield item


def session_request(
    importer: ImportTaskDetails, parsed: ParsedSession
) -> SessionCreateRequest:
    """Build a session create request for one parsed import item.

    Args:
        importer: Importer task details.
        parsed: Parsed session.

    Returns:
        Session create request.
    """
    return SessionCreateRequest(
        agent_id=importer.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=parsed.status,
        name=parsed.name,
        system_prompt=parsed.system_prompt,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        external_id=parsed.external_id,
        metadata=parsed.metadata,
        imported_from=importer.provider,
        framework=parsed.framework,
    )


def _node_request(
    node: ParsedNode, *, index: int, parent_index: int | None
) -> SessionNodeCreateRequest:
    """Convert a parsed node to an ingest request."""
    return SessionNodeCreateRequest(
        index=index,
        parent_index=parent_index,
        secondary_parent_indexes=node.secondary_parent_indexes,
        external_id=node.external_id,
        trace_id=node.trace_id,
        node_type=node.node_type,
        name=node.name,
        status=node.status,
        error=node.error,
        started_at=node.started_at,
        ended_at=node.ended_at,
        input_text=node.input_text,
        output_text=node.output_text,
        system_prompt=node.system_prompt,
        reasoning=node.reasoning,
        inputs=node.inputs,
        outputs=node.outputs,
        requested_model=node.requested_model,
        model=node.model,
        provider=node.provider,
        tokens=node.tokens,
        cost=node.cost,
        model_params=node.model_params,
        tool_name=node.tool_name,
        subagent_id=node.subagent_id,
        attributes=node.attributes,
        metadata=node.metadata,
    )


def flatten_nodes(nodes: list[ParsedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten a parsed node tree into indexed ingest requests, depth-first.

    Args:
        nodes: Top-level parsed nodes.

    Returns:
        Flat session node create requests in depth-first order.
    """
    explicit_indexes = [node.index is not None for node in nodes]
    if any(explicit_indexes):
        if not all(explicit_indexes) or any(node.children for node in nodes):
            raise SessionImportError(
                "Indexed parsed nodes must all have indexes and cannot have children"
            )
        indexed_nodes = sorted(
            nodes, key=lambda node: node.index if node.index is not None else -1
        )
        direct = [
            _node_request(
                node,
                index=node.index,
                parent_index=node.parent_index,
            )
            for node in indexed_nodes
            if node.index is not None
        ]
        return SessionNodeBatchRequest(nodes=direct).nodes

    flattened: list[SessionNodeCreateRequest] = []

    def _walk(node: ParsedNode, parent_index: int | None) -> None:
        index = len(flattened)
        flattened.append(_node_request(node, index=index, parent_index=parent_index))
        for child in node.children:
            _walk(child, index)

    for node in nodes:
        _walk(node, None)
    return flattened


def _resolve_parser(details: ImportTaskDetails) -> Parser:
    """Load the parser callable named by a task's plugin spec.

    Args:
        details: Import task details.

    Raises:
        SessionImportError: The plugin file or module fails to import, or
            the entrypoint is missing or not callable.

    Returns:
        Parser callable.
    """
    try:
        if isinstance(details.plugin, ScriptPluginSpec):
            path = Path(get_required_env("KITARU_TASK_PLUGIN_PATH"))
            return load_plugin_entrypoint(path, details.plugin.entrypoint, _LABEL)
        return load_source_ref(details.plugin.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the import flow: parse the payload and ingest sessions and nodes.

    Args:
        client: API client.
        task_id: Id of the importer task.

    Raises:
        SessionImportError: The task is not an importer task, the plugin
            fails to load, or the parser crashes mid-stream.
    """
    task_uuid = uuid.UUID(task_id)
    spec = await client.tasks.get_spec(task_uuid)
    details = spec.details
    if not isinstance(details, ImportTaskDetails):
        raise SessionImportError(f"Task {task_id} is not an importer task")
    parser = _resolve_parser(details)
    payload = Path(get_required_env("KITARU_TASK_PAYLOAD_PATH")).read_bytes()

    created = 0
    skipped = 0
    failed = 0
    failures: list[ImportFailure] = []
    line = 0

    def _record_failure(failure: ImportFailure) -> None:
        nonlocal failed
        failed += 1
        if len(failures) < MAX_IMPORT_FAILURES:
            failures.append(failure)

    def _stats() -> ImportStats:
        return ImportStats(
            created=created, skipped=skipped, failed=failed, failures=failures
        )

    try:
        for item in call_parser(parser, payload, details.params):
            line += 1
            if isinstance(item, ImportFailure):
                _record_failure(item)
                continue
            request = session_request(details, item)
            try:
                session = await client.sessions.create(request)
            except APIError as exc:
                if exc.status_code == httpx.codes.CONFLICT:
                    skipped += 1
                else:
                    _record_failure(
                        ImportFailure(
                            line=line, external_id=item.external_id, error=str(exc)
                        )
                    )
                continue
            nodes = flatten_nodes(item.nodes)
            try:
                for start in range(0, len(nodes), NODE_BATCH_SIZE):
                    batch = nodes[start : start + NODE_BATCH_SIZE]
                    await client.sessions.ingest_nodes(
                        session.id, SessionNodeBatchRequest(nodes=batch)
                    )
            except APIError as exc:
                _record_failure(
                    ImportFailure(
                        line=line, external_id=item.external_id, error=str(exc)
                    )
                )
                continue
            created += 1
    except SessionImportError as exc:
        _record_failure(ImportFailure(line=line + 1, external_id=None, error=str(exc)))
        write_task_result(_stats())
        raise

    write_task_result(_stats())
