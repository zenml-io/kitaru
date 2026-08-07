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
    SessionUpdateRequest,
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
    "ImportedItem",
    "ImportedNode",
    "ImportedSession",
    "Parser",
    "SessionImportError",
    "call_parser",
    "flatten_nodes",
    "run",
    "session_request",
]

NODE_BATCH_SIZE = 200

_LABEL = "Importer"


class SessionImportError(Exception):
    """Raised when a parser fails while starting or advancing."""


class ImportedNode(BaseModel):
    """Provider data normalized for node ingestion."""

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
    input_text_selector: str | None = None
    output_text_selector: str | None = None
    system_prompt_selector: str | None = None
    reasoning: str | None = None
    inputs: Any
    outputs: Any
    requested_model: str | None = None
    model: str | None = None
    model_provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: str | None = None
    attributes: Any
    metadata: dict[str, Any] = Field(default_factory=dict)
    children: list["ImportedNode"] = Field(default_factory=list)


ImportedNode.model_rebuild()


class ImportedSession(BaseModel):
    """Provider data normalized for session ingestion."""

    model_config = ConfigDict(extra="forbid")

    status: SessionStatus
    name: str | None = None
    inputs: Any
    outputs: Any
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    external_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    framework: str | None = None
    nodes: list[ImportedNode]


ImportedItem = ImportedSession | ImportFailure

Parser = Callable[[bytes, dict[str, Any]], Iterator[ImportedItem]]


def call_parser(
    parser: Parser, payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedItem]:
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
            yielded an item that is not an ImportedSession or ImportFailure.

    Yields:
        Imported items.
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
        if not isinstance(item, ImportedSession | ImportFailure):
            raise SessionImportError(
                f"Parser yielded an item that is not an ImportedSession or "
                f"ImportFailure: {item!r}"
            )
        yield item


def session_request(
    importer: ImportTaskDetails, parsed: ImportedSession
) -> SessionCreateRequest:
    """Build a session create request for one parsed import item.

    Args:
        importer: Importer task details.
        parsed: Imported session.

    Returns:
        Session create request.
    """
    return SessionCreateRequest(
        agent_id=importer.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=parsed.status,
        name=parsed.name,
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
    node: ImportedNode, *, index: int, parent_index: int | None
) -> SessionNodeCreateRequest:
    """Convert an imported node to an ingest request."""
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
        input_text_selector=node.input_text_selector,
        output_text_selector=node.output_text_selector,
        system_prompt_selector=node.system_prompt_selector,
        reasoning=node.reasoning,
        inputs=node.inputs,
        outputs=node.outputs,
        requested_model=node.requested_model,
        model=node.model,
        model_provider=node.model_provider,
        tokens=node.tokens,
        cost=node.cost,
        model_params=node.model_params,
        tool_name=node.tool_name,
        subagent_id=node.subagent_id,
        attributes=node.attributes,
        metadata=node.metadata,
    )


def flatten_nodes(nodes: list[ImportedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten an imported node tree into indexed ingest requests, depth-first.

    Args:
        nodes: Top-level imported nodes.

    Returns:
        Flat session node create requests in depth-first order.
    """
    explicit_indexes = [node.index is not None for node in nodes]
    if any(explicit_indexes):
        if not all(explicit_indexes) or any(node.children for node in nodes):
            raise SessionImportError(
                "Indexed imported nodes must all have indexes and cannot have children"
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

    def _walk(node: ImportedNode, parent_index: int | None) -> None:
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
            if session.status is SessionStatus.PENDING_IMPORT:
                # Adoption copied outputs, error, and timestamps at create time
                # and holds the session open until every node batch lands, so
                # only the terminal status remains to be written here.
                try:
                    await client.sessions.update(
                        session.id, SessionUpdateRequest(status=item.status)
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
