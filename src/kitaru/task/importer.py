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

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Self, TypeVar

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.imports import MAX_IMPORT_FAILURES, ImportFailure, ImportStats
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.task import (
    ApiSourceSpec,
    BlobSourceSpec,
    ImportTaskDetails,
    ScriptPluginSpec,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.task.plugins import PluginLoadError, load_plugin_entrypoint, load_source_ref
from kitaru.task.task_io import get_required_env, write_task_result

__all__ = [
    "MAX_IMPORT_FAILURES",
    "NODE_BATCH_SIZE",
    "FetchQuery",
    "Fetcher",
    "ImportFailure",
    "ImportStats",
    "ImportedItem",
    "ImportedNode",
    "ImportedSession",
    "Parser",
    "SessionImportError",
    "call_fetcher",
    "call_parser",
    "flatten_nodes",
    "gather_bounded",
    "ingest_session",
    "retry_rate_limited",
    "run",
    "session_request",
]

NODE_BATCH_SIZE = 200
DEFAULT_FETCH_CONCURRENCY = 4
MAX_RATE_LIMIT_RETRIES = 10

T = TypeVar("T")

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
    input_text_selector: str | None = None
    output_text_selector: str | None = None
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

Fetcher = Callable[[dict[str, Any]], AsyncIterator[bytes]]


class FetchQuery(BaseModel):
    """Fetch query."""

    model_config = ConfigDict(extra="forbid")

    trace_ids: list[str] | None = None
    since: AwareDatetime | None = None
    until: AwareDatetime | None = None
    concurrency: int = Field(default=DEFAULT_FETCH_CONCURRENCY, ge=1)

    @model_validator(mode="after")
    def _check_window(self) -> Self:
        """Require since without trace ids and reject an inverted window.

        Raises:
            ValueError: Neither trace_ids nor since is set, or until is
                before since.

        Returns:
            The validated query.
        """
        if self.trace_ids is None and self.since is None:
            raise ValueError("since is required when trace_ids is absent")
        if (
            self.since is not None
            and self.until is not None
            and (self.until < self.since)
        ):
            raise ValueError("until must not be before since")
        return self

    def get_window(self) -> tuple[datetime, datetime]:
        """Return the time window, with until defaulting to now.

        Returns:
            Window bounds.
        """
        assert self.since is not None
        return self.since, self.until or datetime.now(UTC)


async def gather_bounded(
    awaitables: Iterable[Awaitable[T]], concurrency: int
) -> list[T]:
    """Await every awaitable with at most concurrency in flight, in input order.

    Args:
        awaitables: Awaitables to run.
        concurrency: Maximum number in flight at once.

    Returns:
        Results in input order.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def _run(awaitable: Awaitable[T]) -> T:
        async with semaphore:
            return await awaitable

    return list(await asyncio.gather(*(_run(item) for item in awaitables)))


async def retry_rate_limited(
    call: Callable[[], Awaitable[T]],
    get_retry_after: Callable[[Exception], float | None],
    max_retries: int = MAX_RATE_LIMIT_RETRIES,
) -> T:
    """Await a call, sleeping and retrying while it reports a rate limit.

    Args:
        call: Factory of the awaitable to run.
        get_retry_after: Seconds to wait when the exception is a rate limit,
            None when it is not.
        max_retries: Retries before the rate limit error propagates.

    Returns:
        Result of the call.
    """
    retries = 0
    while True:
        try:
            return await call()
        except Exception as exc:
            retry_after = get_retry_after(exc)
            if retry_after is None or retries >= max_retries:
                raise
            retries += 1
            await asyncio.sleep(retry_after)


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
        raise SessionImportError(
            f"Parser raised an error: {type(exc).__name__}: {exc}"
        ) from exc
    while True:
        try:
            item = next(iterator)
        except StopIteration:
            return
        except Exception as exc:
            raise SessionImportError(
                f"Parser raised an error: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(item, ImportedSession | ImportFailure):
            raise SessionImportError(
                f"Parser yielded an item that is not an ImportedSession or "
                f"ImportFailure: {item!r}"
            )
        yield item


async def call_fetcher(fetcher: Fetcher, query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Advance a fetcher one payload at a time, wrapping any failure.

    Wrapping only the fetcher call would protect nothing, since an async
    generator function runs no code until iterated. This wraps every step of
    the iteration instead.

    Args:
        fetcher: Fetcher callable.
        query: Importer-defined selection of what to fetch.

    Raises:
        SessionImportError: The fetcher raised while starting or advancing,
            or yielded an item that is not bytes.

    Yields:
        Fetched payloads.
    """
    try:
        iterator = fetcher(query)
    except Exception as exc:
        raise SessionImportError(
            f"Fetcher raised an error: {type(exc).__name__}: {exc}"
        ) from exc
    while True:
        try:
            payload = await anext(iterator)
        except StopAsyncIteration:
            return
        except Exception as exc:
            raise SessionImportError(
                f"Fetcher raised an error: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(payload, bytes):
            raise SessionImportError(
                f"Fetcher yielded an item that is not bytes: {payload!r}"
            )
        yield payload


def session_request(
    parsed: ImportedSession,
    agent_id: uuid.UUID | None,
    provider: str | None,
    origin: SessionOrigin = SessionOrigin.IMPORTED,
) -> SessionCreateRequest:
    """Build a session create request for one parsed import item.

    Args:
        parsed: Imported session.
        agent_id: Agent the session is created under, None resolves it from
            the task.
        provider: Source system named on the import.
        origin: Session origin.

    Returns:
        Session create request.
    """
    return SessionCreateRequest(
        agent_id=agent_id,
        origin=origin,
        status=parsed.status,
        name=parsed.name,
        input_text_selector=parsed.input_text_selector,
        output_text_selector=parsed.output_text_selector,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        external_id=parsed.external_id,
        metadata=parsed.metadata,
        imported_from=provider,
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

    active: set[int] = set()
    stack: list[tuple[ImportedNode, int | None, bool]] = [
        (node, None, False) for node in reversed(nodes)
    ]
    while stack:
        node, parent_index, exiting = stack.pop()
        if exiting:
            active.remove(id(node))
            continue
        if id(node) in active:
            raise SessionImportError("Imported node tree contains a cycle")
        active.add(id(node))
        index = len(flattened)
        flattened.append(_node_request(node, index=index, parent_index=parent_index))
        stack.append((node, parent_index, True))
        stack.extend((child, index, False) for child in reversed(node.children))
    return flattened


async def ingest_session(
    client: KitaruAPIClient,
    parsed: ImportedSession,
    agent_id: uuid.UUID | None,
    provider: str | None,
    origin: SessionOrigin = SessionOrigin.IMPORTED,
) -> SessionResponse | None:
    """Create a session for one parsed import item and ingest its nodes.

    Args:
        client: API client.
        parsed: Imported session.
        agent_id: Agent the session is created under, None resolves it from
            the task.
        provider: Source system named on the import.
        origin: Session origin.

    Raises:
        APIError: Session creation or node ingestion failed.
        SessionImportError: The imported node tree is invalid.

    Returns:
        Created session, None when a session with the external id already
        exists.
    """
    request = session_request(parsed, agent_id, provider, origin)
    try:
        session = await client.sessions.create(request)
    except APIError as exc:
        if exc.status_code == httpx.codes.CONFLICT:
            return None
        raise
    nodes = flatten_nodes(parsed.nodes)
    for start in range(0, len(nodes), NODE_BATCH_SIZE):
        batch = nodes[start : start + NODE_BATCH_SIZE]
        await client.sessions.ingest_nodes(
            session.id, SessionNodeBatchRequest(nodes=batch)
        )
    return session


def _resolve_entrypoint(details: ImportTaskDetails, entrypoint: str) -> Any:
    """Load a callable named in the form of the task's plugin spec entrypoint.

    Args:
        details: Import task details.
        entrypoint: Attribute name for a script plugin, module:attribute for
            a package plugin.

    Raises:
        SessionImportError: The plugin file or module fails to import, or
            the entrypoint is missing or not callable.

    Returns:
        Loaded callable.
    """
    try:
        if isinstance(details.plugin, ScriptPluginSpec):
            path = Path(get_required_env("KITARU_TASK_PLUGIN_PATH"))
            return load_plugin_entrypoint(path, entrypoint, _LABEL)
        return load_source_ref(entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc


async def _iter_payloads(details: ImportTaskDetails) -> AsyncIterator[bytes]:
    """Yield the payloads to parse for a blob or API import source.

    Args:
        details: Import task details.

    Raises:
        SessionImportError: The fetcher raised while starting or advancing,
            or yielded an item that is not bytes.

    Yields:
        Raw payload bytes.
    """
    if isinstance(details.source, BlobSourceSpec):
        yield Path(get_required_env("KITARU_TASK_PAYLOAD_PATH")).read_bytes()
        return
    assert isinstance(details.source, ApiSourceSpec)
    fetcher = _resolve_entrypoint(details, details.source.entrypoint)
    async for payload in call_fetcher(fetcher, details.source.query):
        yield payload


async def run(client: KitaruAPIClient, task_id: str) -> None:
    """Run the import flow: fetch, parse, and ingest sessions and nodes.

    Args:
        client: API client.
        task_id: Id of the importer task.

    Raises:
        SessionImportError: The task is not an importer task, the plugin
            fails to load, or the fetcher or parser crashes mid-stream.
    """
    task_uuid = uuid.UUID(task_id)
    spec = await client.tasks.get_spec(task_uuid)
    details = spec.details
    if not isinstance(details, ImportTaskDetails):
        raise SessionImportError(f"Task {task_id} is not an importer task")
    parser = _resolve_entrypoint(details, details.plugin.entrypoint)

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
        async for payload in _iter_payloads(details):
            for item in call_parser(parser, payload, details.params):
                line += 1
                if isinstance(item, ImportFailure):
                    _record_failure(item)
                    continue
                try:
                    session = await ingest_session(
                        client, item, details.agent_id, details.provider
                    )
                except APIError as exc:
                    _record_failure(
                        ImportFailure(
                            line=line, external_id=item.external_id, error=str(exc)
                        )
                    )
                    continue
                if session is None:
                    skipped += 1
                else:
                    created += 1
    except SessionImportError as exc:
        _record_failure(ImportFailure(line=line + 1, external_id=None, error=str(exc)))
        write_task_result(_stats())
        raise

    write_task_result(_stats())
