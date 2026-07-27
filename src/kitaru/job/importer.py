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
"""Importer contract, request builders, and the import flow."""

import os
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.jobs import (
    MAX_IMPORT_FAILURES,
    ImportFailure,
    ImportStats,
    JobSpecImporter,
)
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, ConflictError
from kitaru.ids import uuid7
from kitaru.job.plugins import PluginLoadError, get_module_attribute, load_plugin_module

PLUGIN_MODULE_NAME = "kitaru_importer_plugin"
IMPORTER_LABEL = "Importer"
NODE_BATCH_SIZE = 20


class SessionImportError(Exception):
    """Raised when an importer cannot be loaded or does not produce parsed items."""


class ParsedNode(BaseModel):
    """Parsed session node."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    node_type: NodeType
    name: str
    status: NodeStatus = NodeStatus.COMPLETED
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    inputs: JsonValue = None
    outputs: JsonValue = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, JsonValue] | None = None
    tool_name: str | None = None
    external_id: str | None = None
    attributes: dict[str, JsonValue] = Field(default_factory=dict)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    children: list["ParsedNode"] = Field(default_factory=list)


class ParsedSession(BaseModel):
    """Parsed session."""

    model_config = ConfigDict(extra="forbid")

    status: SessionStatus = SessionStatus.COMPLETED
    name: str | None = None
    inputs: JsonValue = None
    outputs: JsonValue = None
    expected: JsonValue = None
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    external_id: str
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    nodes: list[ParsedNode] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        """Validate that the session is terminal.

        Raises:
            ValueError: The session is in progress.

        Returns:
            Validated session.
        """
        if self.status is SessionStatus.IN_PROGRESS:
            raise ValueError("Imported sessions cannot be in progress")
        return self


class ParseFailure(BaseModel):
    """Parse failure."""

    model_config = ConfigDict(extra="forbid")

    line: int = Field(ge=0)
    external_id: str | None = None
    error: str


ParsedItem = ParsedSession | ParseFailure
Parser = Callable[[bytes, dict[str, Any]], Iterator[ParsedItem]]


def _required_env(name: str) -> str:
    """Read an environment variable of the import job contract.

    Args:
        name: Name of the variable.

    Raises:
        SessionImportError: The variable is not set.

    Returns:
        Value of the variable.
    """
    value = os.environ.get(name)
    if not value:
        raise SessionImportError(f"{name} is not set")
    return value


def call_parser(
    parser: Parser, payload: bytes, params: dict[str, Any]
) -> Iterator[ParsedItem]:
    """Call a parser on a payload and iterate what it yields.

    An importer is called as ``parse(payload: bytes, params: dict)`` and
    yields parsed sessions and parse failures.

    Args:
        parser: Parser function.
        payload: Payload content.
        params: Parameters for the parser.

    Raises:
        SessionImportError: The parser rejected the call.

    Returns:
        Iterator over the parsed items.
    """
    try:
        return iter(parser(payload, params))
    except Exception as exc:
        raise SessionImportError(
            f"Importer raised {type(exc).__name__}: {exc}"
        ) from exc


def session_request(
    importer: JobSpecImporter, parsed: ParsedSession
) -> SessionCreateRequest:
    """Build the create request of a parsed session.

    Args:
        importer: Importer of the job spec.
        parsed: Parsed session.

    Returns:
        Session create request.
    """
    return SessionCreateRequest(
        agent_id=importer.agent_id,
        origin=SessionOrigin.IMPORTED,
        provider=importer.provider,
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
    )


def _node_request(
    parsed: ParsedNode,
    node_id: uuid.UUID,
    parent_id: uuid.UUID | None,
    sequence: int,
) -> SessionNodeCreateRequest:
    """Build the ingest request of a parsed node.

    Args:
        parsed: Parsed node.
        node_id: Id assigned to the node.
        parent_id: Id of the primary parent.
        sequence: Order within the session.

    Returns:
        Session node create request.
    """
    return SessionNodeCreateRequest(
        id=node_id,
        parent_id=parent_id,
        sequence=sequence,
        external_id=parsed.external_id,
        node_type=parsed.node_type,
        name=parsed.name,
        status=parsed.status,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        requested_model=parsed.requested_model,
        model=parsed.model,
        provider=parsed.provider,
        tokens=parsed.tokens,
        cost=parsed.cost,
        model_params=parsed.model_params,
        tool_name=parsed.tool_name,
        attributes=parsed.attributes,
        metadata=parsed.metadata,
    )


def flatten_nodes(nodes: list[ParsedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten a parsed node tree into ingest requests.

    The walk assigns ids, parent links, and sequence numbers depth first,
    so every node arrives after its parent.

    Args:
        nodes: Root nodes of the tree.

    Returns:
        Ingest requests in walk order.
    """
    requests: list[SessionNodeCreateRequest] = []

    def walk(node: ParsedNode, parent_id: uuid.UUID | None) -> None:
        node_id = uuid7()
        requests.append(_node_request(node, node_id, parent_id, len(requests)))
        for child in node.children:
            walk(child, node_id)

    for root in nodes:
        walk(root, None)
    return requests


async def _ingest_nodes(
    client: KitaruAPIClient,
    session_id: uuid.UUID,
    nodes: list[ParsedNode],
    batch_size: int = NODE_BATCH_SIZE,
) -> None:
    """Send the node tree of an imported session in batches.

    Args:
        client: API client.
        session_id: Id of the session.
        nodes: Root nodes of the tree.
        batch_size: Maximum nodes per request.

    Raises:
        APIError: An ingest request failed.
    """
    requests = flatten_nodes(nodes)
    for start in range(0, len(requests), batch_size):
        await client.session_nodes.upsert(
            session_id,
            SessionNodeBatchRequest(nodes=requests[start : start + batch_size]),
        )


class _StatsBuilder:
    """Running import stats."""

    def __init__(self) -> None:
        """Initialize the builder."""
        self.created = 0
        self.skipped = 0
        self.failed = 0
        self._failures: list[ImportFailure] = []

    def fail(self, line: int, external_id: str | None, error: str) -> None:
        """Count a failure and keep it while the sample has room.

        Args:
            line: Line the failure occurred on.
            external_id: External id of the failed session.
            error: Error message.
        """
        self.failed += 1
        if len(self._failures) < MAX_IMPORT_FAILURES:
            self._failures.append(
                ImportFailure(line=line, external_id=external_id, error=error)
            )

    def build(self) -> ImportStats:
        """Return the recorded stats.

        Returns:
            Import stats.
        """
        return ImportStats(
            created=self.created,
            skipped=self.skipped,
            failed=self.failed,
            failures=list(self._failures),
        )


async def _import_sessions(
    client: KitaruAPIClient,
    importer: JobSpecImporter,
    parsed: Iterator[ParsedItem],
) -> ImportStats:
    """Ingest a stream of parsed items and report what landed.

    Items are consumed one at a time. A session the server already holds
    counts as skipped, a session whose creation or node ingest failed
    counts as failed alongside the parse failures the importer reported.

    Args:
        client: API client.
        importer: Importer of the job spec.
        parsed: Stream of parsed sessions and parse failures.

    Returns:
        Import stats.
    """
    stats = _StatsBuilder()
    for position, item in enumerate(parsed, start=1):
        if isinstance(item, ParseFailure):
            stats.fail(item.line, item.external_id, item.error)
            continue
        try:
            session = await client.sessions.create(session_request(importer, item))
        except ConflictError:
            stats.skipped += 1
            continue
        except APIError as exc:
            stats.fail(position, item.external_id, str(exc))
            continue
        try:
            await _ingest_nodes(client, session.id, item.nodes)
        except APIError as exc:
            stats.fail(position, item.external_id, str(exc))
            continue
        stats.created += 1
    return stats.build()


async def run(client: KitaruAPIClient, job_id: uuid.UUID) -> None:
    """Import the payload of an import job and write the result.

    Args:
        client: API client.
        job_id: Id of the job.

    Raises:
        SessionImportError: The job is not an import job, its importer
            does not load, or its payload does not read.
        APIError: The spec read failed.
    """
    spec = await client.jobs.get_spec(job_id)
    if spec.importer is None:
        raise SessionImportError(f"Job {job_id} is not an import job")
    path = Path(_required_env("KITARU_JOB_PLUGIN_PATH"))
    try:
        module = load_plugin_module(PLUGIN_MODULE_NAME, path)
    except PluginLoadError as exc:
        raise SessionImportError(
            f"Failed to import importer code from {path}: {exc}"
        ) from exc
    try:
        parser = get_module_attribute(
            module, spec.importer.plugin.entrypoint, IMPORTER_LABEL
        )
    except PluginLoadError as exc:
        raise SessionImportError(str(exc)) from exc
    payload_path = Path(_required_env("KITARU_JOB_PAYLOAD_PATH"))
    try:
        payload = payload_path.read_bytes()
    except OSError as exc:
        raise SessionImportError(
            f"Failed to read the payload from {payload_path}"
        ) from exc
    stats = await _import_sessions(
        client, spec.importer, call_parser(parser, payload, spec.importer.params)
    )
    Path(_required_env("KITARU_JOB_RESULT_PATH")).write_text(stats.model_dump_json())
