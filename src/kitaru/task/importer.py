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
"""Importer plugin contract, translation, and task flow."""

import uuid
from collections.abc import Callable, Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from kitaru.api_models.v1.imports import (
    MAX_IMPORT_FAILURES,
    ImportFailure,
    ImportStats,
)
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
from kitaru.api_models.v1.task import ImportTaskDetails
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.task.plugins import (
    PluginLoadError,
    load_plugin_entrypoint,
    load_source_ref,
)
from kitaru.task.task_io import get_required_env, write_task_result

NODE_BATCH_SIZE = 200


class ParsedNode(BaseModel):
    """A parsed session node with nested children."""

    external_id: str | None = None
    trace_id: str | None = None
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    subagent_id: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    children: list["ParsedNode"] = Field(default_factory=list)


class ParsedSession(BaseModel):
    """One session produced by an importer parser."""

    status: SessionStatus
    name: str | None
    inputs: Any
    outputs: Any
    expected: Any
    error: str | None
    started_at: datetime | None
    ended_at: datetime | None
    external_id: str
    metadata: dict[str, Any]
    nodes: list[ParsedNode]


ParsedItem = ParsedSession | ImportFailure
Parser = Callable[[bytes, dict[str, Any]], Iterator[ParsedItem]]


class SessionImportError(Exception):
    """An importer task failed."""


def call_parser(
    parser: Parser,
    payload: bytes,
    params: dict[str, Any],
) -> Iterator[ParsedItem]:
    """Call and lazily advance an importer parser.

    Args:
        parser: Importer parser callable.
        payload: Raw import payload.
        params: Importer parameters.

    Yields:
        Parsed sessions and parser-reported failures.

    Raises:
        SessionImportError: Creating or advancing the parser failed, or it
            yielded an unsupported item.
    """
    try:
        items = iter(parser(payload, params))
    except Exception as exc:
        raise SessionImportError(f"Importer parser failed: {exc}") from exc

    while True:
        try:
            item = next(items)
        except StopIteration:
            return
        except Exception as exc:
            raise SessionImportError(f"Importer parser failed: {exc}") from exc
        if not isinstance(item, ParsedSession | ImportFailure):
            raise SessionImportError(
                "Importer parser yielded a value that is not a ParsedSession "
                "or ImportFailure."
            )
        yield item


def session_request(
    importer: ImportTaskDetails,
    parsed: ParsedSession,
) -> SessionCreateRequest:
    """Translate a parsed session to its API create request.

    Args:
        importer: Import task details.
        parsed: Parsed session.

    Returns:
        Imported session create request linked to the current task.
    """
    return SessionCreateRequest(
        agent_id=importer.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=parsed.status,
        name=parsed.name,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        expected=parsed.expected,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        external_id=parsed.external_id,
        metadata=parsed.metadata,
        provider=importer.provider,
        task_id=get_required_env("KITARU_TASK_ID"),
    )


def flatten_nodes(nodes: list[ParsedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten a parsed node tree in parent-first depth-first order.

    Args:
        nodes: Root nodes to flatten.

    Returns:
        Indexed node create requests.
    """
    flattened: list[SessionNodeCreateRequest] = []

    def append(node: ParsedNode, parent_index: int | None) -> None:
        index = len(flattened)
        flattened.append(
            SessionNodeCreateRequest(
                index=index,
                parent_index=parent_index,
                secondary_parent_indexes=[],
                **node.model_dump(exclude={"children"}),
            )
        )
        for child in node.children:
            append(child, index)

    for root in nodes:
        append(root, None)
    return flattened


def _add_failure(
    failures: list[ImportFailure],
    *,
    error: str,
    external_id: str | None = None,
) -> None:
    """Keep one failure sample while the bounded sample has room."""
    if len(failures) < MAX_IMPORT_FAILURES:
        failures.append(ImportFailure(line=None, external_id=external_id, error=error))


def _stats(
    *,
    created: int,
    skipped: int,
    failed: int,
    failures: list[ImportFailure],
) -> ImportStats:
    """Build the current import statistics."""
    return ImportStats(
        created=created,
        skipped=skipped,
        failed=failed,
        failures=failures,
    )


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run an importer task.

    Args:
        client: API client for spec reads and session writes.
        task_id: Importer task id.

    Raises:
        SessionImportError: The spec is not for an importer, plugin loading
            fails, or its parser crashes.
    """
    spec = await client.tasks.get_spec(uuid.UUID(task_id))
    details = spec.details
    if not isinstance(details, ImportTaskDetails):
        raise SessionImportError(f"Task {task_id!r} is not an importer task.")

    try:
        if details.plugin.type == "script":
            parser = load_plugin_entrypoint(
                Path(get_required_env("KITARU_TASK_PLUGIN_PATH")),
                details.plugin.entrypoint,
                "Importer",
            )
        else:
            parser = load_source_ref(
                details.plugin.entrypoint,
                "Importer",
            )
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc

    payload = Path(get_required_env("KITARU_TASK_PAYLOAD_PATH")).read_bytes()
    created = 0
    skipped = 0
    failed = 0
    failures: list[ImportFailure] = []

    try:
        for item in call_parser(parser, payload, details.params):
            if isinstance(item, ImportFailure):
                failed += 1
                if len(failures) < MAX_IMPORT_FAILURES:
                    failures.append(item)
                continue

            try:
                session = await client.sessions.create(session_request(details, item))
            except APIError as exc:
                if exc.status_code == 409:
                    skipped += 1
                else:
                    failed += 1
                    _add_failure(
                        failures,
                        external_id=item.external_id,
                        error=str(exc),
                    )
                continue

            try:
                flat_nodes = flatten_nodes(item.nodes)
                for start in range(0, len(flat_nodes), NODE_BATCH_SIZE):
                    await client.sessions.ingest_nodes(
                        session.id,
                        SessionNodeBatchRequest(
                            nodes=flat_nodes[start : start + NODE_BATCH_SIZE]
                        ),
                    )
            except APIError as exc:
                failed += 1
                _add_failure(
                    failures,
                    external_id=item.external_id,
                    error=str(exc),
                )
                continue
            created += 1
    except SessionImportError as exc:
        failed += 1
        _add_failure(failures, error=str(exc))
        write_task_result(
            _stats(
                created=created,
                skipped=skipped,
                failed=failed,
                failures=failures,
            )
        )
        raise

    write_task_result(
        _stats(
            created=created,
            skipped=skipped,
            failed=failed,
            failures=failures,
        )
    )


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
    "flatten_nodes",
    "run",
    "session_request",
]
