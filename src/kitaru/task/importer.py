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
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, TypeVar, runtime_checkable

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

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
    ApiImportSourceSpec,
    BlobImportSourceSpec,
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
    "Fetcher",
    "FetchingImporter",
    "ImportFailure",
    "ImportStats",
    "ImportedItem",
    "ImportedNode",
    "ImportedSession",
    "Importer",
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

Parser = Callable[
    [bytes, dict[str, Any]], Iterator[ImportedItem] | AsyncIterator[ImportedItem]
]

Fetcher = Callable[[dict[str, Any]], Iterator[bytes] | AsyncIterator[bytes]]


@runtime_checkable
class Importer(Protocol):
    """Importer object."""

    def parse(
        self, payload: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedItem] | AsyncIterator[ImportedItem]:
        """Parse one payload into imported items, sync or async."""
        ...


@runtime_checkable
class FetchingImporter(Importer, Protocol):
    """Importer object that also fetches payloads from a provider API."""

    def fetch(self, query: dict[str, Any]) -> Iterator[bytes] | AsyncIterator[bytes]:
        """Fetch payloads matching a query, sync or async."""
        ...


# TODO: Move gather_bounded and retry_rate_limited into a module importer
# implementations import, separate from the runtime in this module that calls
# them.
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


async def _advance(iterator: Iterator[T] | AsyncIterator[T]) -> T:
    """Advance a sync or async iterator by one item.

    Raises:
        StopAsyncIteration: The iterator is exhausted.

    Returns:
        The next item.
    """
    if isinstance(iterator, AsyncIterator):
        return await anext(iterator)
    try:
        return next(iterator)
    except StopIteration:
        raise StopAsyncIteration from None


async def call_parser(
    parser: Parser, payload: bytes, params: dict[str, Any]
) -> AsyncIterator[ImportedItem]:
    """Advance a parser one item at a time, wrapping any failure.

    Wrapping only the parser call would protect nothing, since a generator
    function runs no code until iterated. This wraps every step of the
    iteration instead.

    Args:
        parser: Parser callable, sync or async.
        payload: Raw payload bytes.
        params: Parameters passed to the parser.

    Raises:
        SessionImportError: The parser raised while starting or advancing, or
            yielded an item that is not an ImportedSession or ImportFailure.

    Yields:
        Imported items.
    """
    try:
        result = parser(payload, params)
        iterator = result if isinstance(result, AsyncIterator) else iter(result)
    except Exception as exc:
        raise SessionImportError(
            f"Parser raised an error: {type(exc).__name__}: {exc}"
        ) from exc
    while True:
        try:
            item = await _advance(iterator)
        except StopAsyncIteration:
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

    Wrapping only the fetcher call would protect nothing, since a generator
    function runs no code until iterated. This wraps every step of the
    iteration instead.

    Args:
        fetcher: Fetcher callable, sync or async.
        query: Importer-defined selection of what to fetch.

    Raises:
        SessionImportError: The fetcher raised while starting or advancing,
            or yielded an item that is not bytes.

    Yields:
        Fetched payloads.
    """
    try:
        result = fetcher(query)
        iterator = result if isinstance(result, AsyncIterator) else iter(result)
    except Exception as exc:
        raise SessionImportError(
            f"Fetcher raised an error: {type(exc).__name__}: {exc}"
        ) from exc
    while True:
        try:
            payload = await _advance(iterator)
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
    node: ImportedNode,
    external_id: str,
    parent_external_id: str | None,
    secondary_parent_external_ids: list[str],
) -> SessionNodeCreateRequest:
    """Convert an imported node to an ingest request."""
    return SessionNodeCreateRequest(
        external_id=external_id,
        parent_external_id=parent_external_id,
        secondary_parent_external_ids=secondary_parent_external_ids,
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


def _reject_duplicate_external_ids(external_ids: Iterable[str]) -> None:
    """Raise SessionImportError when an external id repeats within a session."""
    seen: set[str] = set()
    for external_id in external_ids:
        if external_id in seen:
            raise SessionImportError(
                f"Imported node external id '{external_id}' is not unique "
                "within the session"
            )
        seen.add(external_id)


def flatten_nodes(nodes: list[ImportedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten an imported node tree into ingest requests, depth-first.

    A node without an external id gets one minted from its position, its
    explicit index in the indexed representation or its depth-first position
    in the tree representation.

    Args:
        nodes: Top-level imported nodes.

    Raises:
        SessionImportError: The node tree contains a cycle, or an external id
            repeats within the session.

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
        external_ids = {
            node.index: node.external_id or f"node-{node.index}"
            for node in indexed_nodes
            if node.index is not None
        }
        _reject_duplicate_external_ids(external_ids.values())
        direct = [
            _node_request(
                node,
                external_ids[node.index],
                external_ids.get(node.parent_index),
                [external_ids[i] for i in node.secondary_parent_indexes],
            )
            for node in indexed_nodes
            if node.index is not None
        ]
        return SessionNodeBatchRequest(nodes=direct).nodes

    flattened: list[SessionNodeCreateRequest] = []
    external_ids_by_position: list[str] = []

    active: set[int] = set()
    stack: list[tuple[ImportedNode, str | None, bool]] = [
        (node, None, False) for node in reversed(nodes)
    ]
    while stack:
        node, parent_external_id, exiting = stack.pop()
        if exiting:
            active.remove(id(node))
            continue
        if id(node) in active:
            raise SessionImportError("Imported node tree contains a cycle")
        active.add(id(node))
        external_id = node.external_id or f"node-{len(flattened)}"
        external_ids_by_position.append(external_id)
        flattened.append(
            _node_request(
                node,
                external_id,
                parent_external_id,
                [external_ids_by_position[i] for i in node.secondary_parent_indexes],
            )
        )
        stack.append((node, parent_external_id, True))
        stack.extend((child, external_id, False) for child in reversed(node.children))
    _reject_duplicate_external_ids(external_ids_by_position)
    return flattened


async def ingest_session(
    client: KitaruAPIClient,
    parsed: ImportedSession,
    agent_id: uuid.UUID | None,
    provider: str | None,
    origin: SessionOrigin = SessionOrigin.IMPORTED,
) -> SessionResponse | None:
    """Create a session for one parsed import item and ingest its nodes.

    A session the calling task already registered under the same
    imported_from and external id pair is reused and its nodes are ingested
    the same way as for a new session. A pair another caller registered
    skips the item.

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
        Session the nodes were ingested into, None when another caller
        already registered the external id.
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


def _resolve_importer(details: ImportTaskDetails) -> tuple[Parser, Fetcher | None]:
    """Load the parser and optional fetcher named by a task's plugin spec.

    The entrypoint is either a parse callable or an importer object exposing
    parse and, when it supports API imports, fetch.

    Args:
        details: Import task details.

    Raises:
        SessionImportError: The plugin file or module fails to import, the
            entrypoint is missing, or it is neither callable nor an importer.

    Returns:
        Parser and fetcher, None when the importer only parses uploads.
    """
    try:
        if isinstance(details.plugin, ScriptPluginSpec):
            path = Path(get_required_env("KITARU_TASK_PLUGIN_PATH"))
            entrypoint = load_plugin_entrypoint(path, details.plugin.entrypoint, _LABEL)
        else:
            entrypoint = load_source_ref(details.plugin.entrypoint, _LABEL)
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc
    if isinstance(entrypoint, FetchingImporter):
        return entrypoint.parse, entrypoint.fetch
    if isinstance(entrypoint, Importer):
        return entrypoint.parse, None
    if callable(entrypoint):
        return entrypoint, None
    raise SessionImportError(
        f"{_LABEL} entrypoint '{details.plugin.entrypoint}' is neither callable "
        "nor an importer"
    )


async def _iter_payloads(
    details: ImportTaskDetails, fetcher: Fetcher | None
) -> AsyncIterator[bytes]:
    """Yield the payloads to parse for a blob or API import source.

    Args:
        details: Import task details.
        fetcher: Importer fetcher, None when it only parses uploads.

    Raises:
        SessionImportError: The source is an API but the importer has no
            fetcher, or the fetcher raised while starting or advancing, or
            yielded an item that is not bytes.

    Yields:
        Raw payload bytes.
    """
    if isinstance(details.source, BlobImportSourceSpec):
        yield Path(get_required_env("KITARU_TASK_PAYLOAD_PATH")).read_bytes()
        return
    assert isinstance(details.source, ApiImportSourceSpec)
    if fetcher is None:
        raise SessionImportError(
            f"{_LABEL} entrypoint '{details.plugin.entrypoint}' does not fetch "
            "from an API"
        )
    query = details.source.query.model_dump(mode="json")
    async for payload in call_fetcher(fetcher, query):
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
    parser, fetcher = _resolve_importer(details)

    created = 0
    skipped = 0
    ingested: set[str] = set()
    failed = 0
    limit_reached = False
    failures: list[ImportFailure] = []
    line = 0

    def _record_failure(failure: ImportFailure) -> None:
        nonlocal failed
        failed += 1
        if len(failures) < MAX_IMPORT_FAILURES:
            failures.append(failure)

    def _stats() -> ImportStats:
        return ImportStats(
            created=created,
            skipped=skipped,
            failed=failed,
            failures=failures,
            limit_reached=limit_reached,
        )

    try:
        async for payload in _iter_payloads(details, fetcher):
            async for item in call_parser(parser, payload, details.params):
                line += 1
                if isinstance(item, ImportFailure):
                    _record_failure(item)
                    continue
                if details.max_sessions is not None and created >= details.max_sessions:
                    limit_reached = True
                    break
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
                # A session this run already ingested was updated in place,
                # and one another caller registered was left untouched.
                if session is None or item.external_id in ingested:
                    skipped += 1
                else:
                    created += 1
                    ingested.add(item.external_id)
            if limit_reached:
                break
    except SessionImportError as exc:
        _record_failure(ImportFailure(line=line + 1, external_id=None, error=str(exc)))
        write_task_result(_stats())
        raise

    write_task_result(_stats())
