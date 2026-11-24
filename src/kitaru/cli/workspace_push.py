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
"""Copying of workspace content from one Kitaru server to another."""

import asyncio
import json
import uuid
from collections import defaultdict
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from pydantic import ValidationError

from kitaru.api_models.v1.agent import (
    AgentCreateRequest,
    AgentListParams,
    AgentResponse,
)
from kitaru.api_models.v1.agent_version import (
    AgentVersionCreateRequest,
    AgentVersionListParams,
    AgentVersionResponse,
)
from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionResponse,
)
from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.insight import (
    MAX_INSIGHT_BATCH_SIZE,
    InsightListParams,
    InsightResponse,
)
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
    SessionOrigin,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    SessionIngestRequest,
    SessionNodeCreateRequest,
    SessionNodeListParams,
    SessionWithNodesResponse,
)
from kitaru.cli.output import CLIError, describe_validation_error
from kitaru.cli.workspace_push_destination import build_login_hint
from kitaru.cli.workspace_push_references import (
    ReferenceMap,
    build_pushed_insight,
    find_uuids,
    get_pushed_insight_source_id,
)
from kitaru.cli.workspace_push_report import PushReport
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, InvalidServerResponseError

PUSH_IMPORTED_FROM = "kitaru-workspace-push"
_PAGE_SIZE = 200
_DUPLICATE_SESSION_CODE = "duplicate_session_external_id"
_ID_PAGE_SIZE = 1000
_MAX_ERROR_LENGTH = 300
_CONCURRENCY = 8


class _SessionCopyError(Exception):
    """Raised when a session copy fails for a reason the API did not state."""


_RECORD_ERRORS = (
    APIError,
    httpx.TransportError,
    InvalidServerResponseError,
    ValidationError,
    _SessionCopyError,
)
"""Failures that end the copy of one record and leave the others running."""


@dataclass(slots=True)
class _AgentPair:
    """A selected source agent and its destination counterpart."""

    source: AgentResponse
    destination: AgentResponse | None
    """Destination agent, None when it failed or a dry run would create it."""
    created: bool = False
    failed: bool = False

    @property
    def has_earlier_records(self) -> bool:
        """Whether the destination agent existed before this run."""
        return self.destination is not None and not self.created


@dataclass(slots=True)
class _Push:
    """Clients and accumulated state of one push."""

    source: KitaruAPIClient
    destination: KitaruAPIClient
    references: ReferenceMap
    report: PushReport
    sessions: dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)
    """Source session id to destination session id."""
    unsettled: dict[str, tuple[str, bool]] = field(default_factory=dict)
    """Source id that failed in this run to why, and whether a rerun can fix it."""

    def fail(
        self,
        kind: str,
        record: str,
        error: Exception,
        source_id: uuid.UUID | None = None,
    ) -> None:
        """Report one record that failed to copy.

        Args:
            kind: Kind of the record.
            record: Name or id of the record.
            error: Failure that ended its copy.
            source_id: Source id of the record, kept so that insights
                referencing it wait for a run that copies it.

        Raises:
            APIError: The destination rejected the credential, which would
                fail every remaining record too.
        """
        if isinstance(error, APIError) and _is_auth_failure(error):
            raise error
        message, retryable = _describe_failure(error)
        self.report.fail(kind, record, message, retryable=retryable)
        if source_id is not None:
            self.references.add_unmapped(source_id, kind.removesuffix("s"))
            self.unsettled[str(source_id)] = (
                f"{kind.removesuffix('s').replace('_', ' ')} {record}, which "
                "failed to copy",
                retryable,
            )


async def push_workspace(
    source: KitaruAPIClient,
    destination: KitaruAPIClient,
    *,
    source_url: str,
    destination_url: str,
    agent_names: list[str] | None,
    dry_run: bool,
) -> PushReport:
    """Copy agents, connections, sessions, and insights between servers.

    Records are matched before they are written, so a repeated push creates
    nothing twice. Agents and connections match by name, agent versions by
    content, sessions by ``imported_from`` and ``external_id`` within their
    agent, and insights by the source id recorded in their metadata.

    Args:
        source: Client for the server to copy from.
        destination: Client for the server to copy to.
        source_url: Source server URL.
        destination_url: Destination server URL.
        agent_names: Names of the agents to copy, None for every agent.
        dry_run: Whether to report without writing.

    Raises:
        CLIError: A named agent does not exist on the source, or the
            destination rejects its stored credential.

    Returns:
        Outcome of the push by kind of record.
    """
    push = _Push(
        source=source,
        destination=destination,
        references=ReferenceMap(source_url, destination_url),
        report=PushReport(source_url, destination_url, dry_run),
    )
    selected = await _select_agents(push, agent_names)
    destination_agents = await _list_destination_agents(push)
    pairs = [await _push_agent(push, agent, destination_agents) for agent in selected]
    for pair in pairs:
        await _push_agent_versions(push, pair)
    await _push_connections(push)
    for pair in pairs:
        await _push_sessions(push, pair)
    await _push_insights(push, pairs)
    return push.report


def get_session_identity(session: SessionResponse) -> tuple[str, str]:
    """Return the ``imported_from`` and ``external_id`` of a session's copy.

    A complete source identity is kept, so the copy also deduplicates
    against a later import of the same trace. A partial one is replaced by
    the push itself and the source session id: the database treats a NULL
    as distinct, so several source sessions can share the other half.

    Args:
        session: Source session.

    Returns:
        ``imported_from`` and ``external_id`` the destination session carries.
    """
    if session.imported_from is not None and session.external_id is not None:
        return session.imported_from, session.external_id
    return PUSH_IMPORTED_FROM, str(session.id)


def build_version_request(version: AgentVersionResponse) -> AgentVersionCreateRequest:
    """Build the destination copy of a source agent version.

    Secret ids are dropped from the run spec, because they name source
    records the destination rejects.

    Args:
        version: Source agent version.

    Returns:
        Request creating the copy.
    """
    run_spec = version.run_spec
    if run_spec is not None and run_spec.secret_ids:
        run_spec = run_spec.model_copy(update={"secret_ids": []})
    return AgentVersionCreateRequest(
        display_version=version.display_version,
        description=version.description,
        run_spec=run_spec,
        capabilities=version.capabilities,
    )


def get_version_content(version: AgentVersionResponse) -> str:
    """Return the content a version copy repeats, as a comparable key."""
    request = build_version_request(version)
    # Sort keys because a server may store JSON maps in another key order.
    return json.dumps(request.model_dump(mode="json"), sort_keys=True)


def build_ingest_request(
    full: SessionWithNodesResponse,
    agent_id: uuid.UUID,
    agent_version_id: uuid.UUID | None,
) -> SessionIngestRequest:
    """Build the destination ingest request for a source session.

    Every request field is read from the source record of the same name, so
    a field added to both models is copied without a change here.

    Args:
        full: Source session with every node and payload.
        agent_id: Destination agent.
        agent_version_id: Destination agent version, when one was mapped.

    Returns:
        Request creating the session and its nodes in one transaction.
    """
    imported_from, external_id = get_session_identity(full.session)
    session = SessionCreateRequest.model_validate(
        full.session, from_attributes=True
    ).model_copy(
        update={
            "agent_id": agent_id,
            "agent_version_id": agent_version_id,
            "imported_from": imported_from,
            "external_id": external_id,
        }
    )
    return SessionIngestRequest(
        session=session,
        nodes=[
            SessionNodeCreateRequest.model_validate(node, from_attributes=True)
            for node in full.nodes
        ],
    )


def _filter_by_agent(agent_id: uuid.UUID) -> FilterCondition:
    """Build the list filter selecting one agent's records."""
    return FilterCondition(field="agent_id", op=FilterOp.EQ, value=str(agent_id))


def _describe_error(error: APIError) -> str:
    """Describe an API failure in bounded text."""
    detail = error.detail.strip()[:_MAX_ERROR_LENGTH]
    return f"HTTP {error.status_code}: {detail}" if detail else str(error.status_code)


def _is_auth_failure(error: APIError) -> bool:
    """Return whether a server rejected the credential of a request."""
    return error.status_code in {httpx.codes.UNAUTHORIZED, httpx.codes.FORBIDDEN}


def _is_identity_conflict(error: APIError) -> bool:
    """Return whether a conflict reports a record identity that already exists."""
    # Servers before the conflict code send none, so an uncoded conflict is
    # still treated as an identity conflict and settled by listing.
    return error.status_code == httpx.codes.CONFLICT and error.code in (
        None,
        _DUPLICATE_SESSION_CODE,
    )


def _describe_failure(error: Exception) -> tuple[str, bool]:
    """Describe a record failure in bounded text and whether a rerun can fix it."""
    if isinstance(error, APIError):
        retryable = (
            _is_identity_conflict(error)
            or error.status_code == httpx.codes.TOO_MANY_REQUESTS
            or error.status_code >= httpx.codes.INTERNAL_SERVER_ERROR
        )
        return _describe_error(error), retryable
    if isinstance(error, ValidationError):
        location, problem = describe_validation_error(error)
        return f"The source record has invalid {location}: {problem}.", False
    if isinstance(error, _SessionCopyError):
        return str(error), True
    name = type(error).__name__
    detail = str(error).strip()[:_MAX_ERROR_LENGTH]
    message = f"{name}: {detail}" if detail else name
    # A dropped connection can succeed on a rerun, while a response the
    # client cannot read would fail the same way again.
    return message, isinstance(error, httpx.TransportError)


@asynccontextmanager
async def _open_task_group() -> AsyncGenerator[asyncio.TaskGroup]:
    """Open a task group that raises its first failure in place of the group.

    The tasks of a group run the same copy, so the first failure describes
    the others, and a bare exception keeps its stable CLI error kind.
    """
    try:
        async with asyncio.TaskGroup() as group:
            yield group
    except BaseExceptionGroup as failures:
        error: BaseException = failures
        while isinstance(error, BaseExceptionGroup):
            error = error.exceptions[0]
        raise error from None


async def _select_agents(
    push: _Push, agent_names: list[str] | None
) -> list[AgentResponse]:
    """List the source agents to copy, oldest first."""
    agents = [
        agent
        async for agent in push.source.agents.iter(
            AgentListParams(sort="created:asc", size=_PAGE_SIZE)
        )
    ]
    if agent_names is None:
        return agents
    wanted = set(agent_names)
    missing = sorted(wanted - {agent.name for agent in agents})
    if missing:
        raise CLIError(
            "not_found",
            f"No agent named {', '.join(missing)} on {push.references.source_url}.",
        )
    for agent in agents:
        if agent.name not in wanted:
            push.references.add_unmapped(agent.id, "agent")
    return [agent for agent in agents if agent.name in wanted]


async def _list_destination_agents(push: _Push) -> dict[str, AgentResponse]:
    """List the destination agents by name, checking its credential."""
    try:
        return {
            agent.name: agent
            async for agent in push.destination.agents.iter(
                AgentListParams(size=_PAGE_SIZE)
            )
        }
    except APIError as error:
        if not _is_auth_failure(error):
            raise
        destination = push.references.destination_url
        raise CLIError(
            "authentication_failed",
            f"The destination {destination} rejected its stored credential.",
            details={"status_code": error.status_code},
            hint=build_login_hint(destination),
        ) from error


async def _push_agent(
    push: _Push, agent: AgentResponse, destination_agents: dict[str, AgentResponse]
) -> _AgentPair:
    """Match one source agent by name or create it on the destination."""
    existing = destination_agents.get(agent.name)
    if existing is not None:
        push.references.add(agent.id, existing.id)
        push.report.count("agents", "already_present")
        return _AgentPair(agent, existing)
    if push.report.dry_run:
        push.report.count("agents", "copied")
        return _AgentPair(agent, None)
    try:
        created = await push.destination.agents.create(
            AgentCreateRequest(name=agent.name, description=agent.description)
        )
    except _RECORD_ERRORS as error:
        push.fail("agents", agent.name, error, agent.id)
        return _AgentPair(agent, None, failed=True)
    push.references.add(agent.id, created.id)
    push.report.count("agents", "copied")
    return _AgentPair(agent, created, created=True)


async def _push_agent_versions(push: _Push, pair: _AgentPair) -> None:
    """Copy the versions of one agent, matching them by content.

    A server numbers versions itself and never reuses a deleted number, so
    numbers cannot match a copy to its source. Each destination version
    stands in for at most one source version, in number order, which keeps
    two versions of the same content apart.
    """
    versions = sorted(
        [
            version
            async for version in push.source.agents.iter_versions(
                pair.source.id, AgentVersionListParams(size=_PAGE_SIZE)
            )
        ],
        key=lambda version: version.version,
    )
    if not versions:
        return
    agent_name = pair.source.name
    if pair.failed:
        for version in versions:
            push.references.add_unmapped(version.id, "agent_version")
            push.report.skip(
                "agent_versions",
                f"{agent_name}@{version.version}",
                "Its agent was not copied.",
            )
        return
    available: defaultdict[str, list[AgentVersionResponse]] = defaultdict(list)
    if pair.destination is not None and pair.has_earlier_records:
        existing = [
            version
            async for version in push.destination.agents.iter_versions(
                pair.destination.id, AgentVersionListParams(size=_PAGE_SIZE)
            )
        ]
        for version in sorted(existing, key=lambda version: version.version):
            available[get_version_content(version)].append(version)
    dropped_secrets = False
    renumbered = False
    for version in versions:
        record = f"{agent_name}@{version.version}"
        candidates = available.get(get_version_content(version))
        if candidates:
            match = candidates.pop(0)
            push.references.add(version.id, match.id)
            push.report.count("agent_versions", "already_present")
            renumbered |= match.version != version.version
            continue
        if pair.destination is None or push.report.dry_run:
            push.report.count("agent_versions", "copied")
            continue
        request = build_version_request(version)
        try:
            created = await push.destination.agents.create_version(
                pair.destination.id, request
            )
        except _RECORD_ERRORS as error:
            push.fail("agent_versions", record, error, version.id)
            continue
        push.references.add(version.id, created.id)
        push.report.count("agent_versions", "copied")
        renumbered |= created.version != version.version
        dropped_secrets |= request.run_spec != version.run_spec
    if renumbered:
        push.report.warnings.append(
            f"Some versions of agent {agent_name!r} carry other numbers on "
            f"{push.references.destination_url} than on the source, because "
            "a server numbers versions itself. Copied sessions and insights "
            "point at the matching copies."
        )
    if dropped_secrets:
        push.report.warnings.append(
            f"Agent {agent_name!r} has versions whose run spec names secrets. "
            "The copies name none, so attach destination secrets before "
            "running them."
        )


async def _push_connections(push: _Push) -> None:
    """Copy provider connections without their secret values, by name.

    A server never returns the secret values of a connection, so a copy
    holds the non-secret values and every secret key the destination lacks
    is reported, also for a connection copied by an earlier run.
    """
    existing: dict[str, ConnectionResponse] = {}
    default_providers: set[str] = set()
    async for connection in push.destination.connections.iter(
        ConnectionListParams(size=_PAGE_SIZE)
    ):
        existing[connection.name] = connection
        if connection.default:
            default_providers.add(connection.provider)
    async for connection in push.source.connections.iter(
        ConnectionListParams(sort="created:asc", size=_PAGE_SIZE)
    ):
        match = existing.get(connection.name)
        if match is not None and match.provider != connection.provider:
            push.report.fail(
                "connections",
                connection.name,
                f"The destination connection of this name addresses provider "
                f"{match.provider!r}, not {connection.provider!r}. Rename one "
                "of them, then rerun.",
                retryable=False,
            )
            continue
        stored_keys = set(match.secret_keys) if match is not None else set()
        missing_keys = set(connection.secret_keys) - stored_keys
        # A default without its secret values would be selected for jobs on
        # the destination and unset the missing keys from their environment,
        # so it becomes the default only once it is complete.
        wants_default = (
            connection.default and connection.provider not in default_providers
        )
        if match is not None:
            push.report.count("connections", "already_present")
            if match.env != connection.env:
                push.report.warnings.append(
                    f"Connection {connection.name!r} on "
                    f"{push.references.destination_url} has other non-secret "
                    "values than on the source. The push left them unchanged."
                )
        elif push.report.dry_run:
            push.report.count("connections", "copied")
        else:
            try:
                await push.destination.connections.create(
                    ConnectionCreateRequest(
                        name=connection.name,
                        provider=connection.provider,
                        env=connection.env,
                        default=wants_default and not missing_keys,
                    )
                )
            except _RECORD_ERRORS as error:
                push.fail("connections", connection.name, error)
                continue
            push.report.count("connections", "copied")
        if wants_default:
            default_providers.add(connection.provider)
        if missing_keys:
            push.report.add_incomplete_connection(
                connection.name,
                missing_keys,
                stored_keys,
                make_default=wants_default,
            )


async def _push_sessions(push: _Push, pair: _AgentPair) -> None:
    """Copy the settled sessions of one agent with their nodes.

    Copies run concurrently while the source listing streams. The listing
    waits for a free slot before it starts a copy, which bounds both the
    open requests and the sessions held in memory.
    """
    existing: dict[tuple[str, str], uuid.UUID] = {}
    if pair.destination is not None and pair.has_earlier_records:
        async for session in push.destination.sessions.iter(
            SessionListParams(
                filter=_filter_by_agent(pair.destination.id), size=_ID_PAGE_SIZE
            )
        ):
            if session.imported_from is not None and session.external_id is not None:
                existing[(session.imported_from, session.external_id)] = session.id
    slots = asyncio.Semaphore(_CONCURRENCY)
    without_version: list[uuid.UUID] = []

    async def copy(session: SessionResponse, agent_id: uuid.UUID) -> None:
        try:
            await _copy_session(push, session, agent_id, without_version)
        finally:
            slots.release()

    async with _open_task_group() as copies:
        async for session in push.source.sessions.iter(
            SessionListParams(
                filter=_filter_by_agent(pair.source.id),
                sort="created:asc",
                size=_ID_PAGE_SIZE,
            )
        ):
            if session.import_id is not None:
                push.references.add_unmapped(session.import_id, "import")
            reason = _get_session_skip_reason(session, pair)
            if reason is not None:
                push.references.add_unmapped(session.id, "session")
                push.report.skip("sessions", str(session.id), reason)
                if pair.failed:
                    push.unsettled[str(session.id)] = (
                        f"session {session.id}, whose agent failed to copy",
                        True,
                    )
                continue
            match = existing.get(get_session_identity(session))
            if match is not None:
                _record_session(push, session.id, match)
                push.report.count("sessions", "already_present")
            elif pair.destination is None or push.report.dry_run:
                push.report.count("sessions", "copied")
            else:
                await slots.acquire()
                copies.create_task(copy(session, pair.destination.id))
    if without_version:
        push.report.warnings.append(
            f"{len(without_version)} session(s) of agent {pair.source.name!r} "
            "were copied without an agent version because their version was "
            "not copied."
        )


def _get_session_skip_reason(session: SessionResponse, pair: _AgentPair) -> str | None:
    """Return why a source session is not copied, None when it is."""
    if pair.failed:
        return "Its agent was not copied."
    if session.status == SessionStatus.IN_PROGRESS:
        return "The session is still in progress on the source."
    if session.origin == SessionOrigin.REPLAY:
        return "Replay result sessions belong to replays, which are not copied."
    return None


def _record_session(
    push: _Push, source_id: uuid.UUID, destination_id: uuid.UUID
) -> None:
    """Record the destination copy of a source session."""
    push.sessions[source_id] = destination_id
    push.references.add(source_id, destination_id)


async def _copy_session(
    push: _Push,
    session: SessionResponse,
    agent_id: uuid.UUID,
    without_version: list[uuid.UUID],
) -> None:
    """Copy one session and record how the copy ended."""
    agent_version_id = None
    if session.agent_version_id is not None:
        agent_version_id = push.references.get(session.agent_version_id)
    try:
        destination_id, copied = await _ingest_session(
            push, session, agent_id, agent_version_id
        )
    except _RECORD_ERRORS as error:
        push.fail("sessions", str(session.id), error, session.id)
        return
    _record_session(push, session.id, destination_id)
    push.report.count("sessions", "copied" if copied else "already_present")
    if copied and session.agent_version_id is not None and agent_version_id is None:
        without_version.append(session.id)


async def _ingest_session(
    push: _Push,
    session: SessionResponse,
    agent_id: uuid.UUID,
    agent_version_id: uuid.UUID | None,
) -> tuple[uuid.UUID, bool]:
    """Create one session with its nodes on the destination.

    Raises:
        _SessionCopyError: The destination reports the session as present
            but lists no session with its identity.

    Returns:
        Destination session id and whether this call created it.
    """
    full = await push.source.sessions.get_with_nodes(session.id)
    request = build_ingest_request(full, agent_id, agent_version_id)
    try:
        created = await push.destination.sessions.ingest(request)
    except APIError as error:
        if not _is_identity_conflict(error):
            raise
    else:
        return created.id, True
    # Another writer created the same identity after the destination listing.
    imported_from, external_id = get_session_identity(session)
    page = await push.destination.sessions.list(
        SessionListParams(
            filter=AndFilter.model_validate(
                {
                    "and": [
                        _filter_by_agent(agent_id),
                        FilterCondition(
                            field="imported_from", op=FilterOp.EQ, value=imported_from
                        ),
                        FilterCondition(
                            field="external_id", op=FilterOp.EQ, value=external_id
                        ),
                    ]
                }
            ),
            size=1,
        )
    )
    if not page.items:
        raise _SessionCopyError(
            "The destination reported the session as present but did not list it."
        )
    return page.items[0].id, False


async def _push_insights(push: _Push, pairs: list[_AgentPair]) -> None:
    """Copy the insights of every agent with their references rewritten.

    An insight that references a record which failed to copy in this run is
    not written, because a written copy is never rewritten again and would
    keep pointing at the source.
    """
    pending = [(pair, await _list_pending_insights(push, pair)) for pair in pairs]
    references = {
        insight.id: find_uuids(insight.model_dump_json())
        for _, insights in pending
        for insight in insights
    }
    if not push.report.dry_run:
        await _map_referenced_nodes(push, set().union(*references.values()))
    for pair, insights in pending:
        if pair.destination is None or push.report.dry_run:
            push.report.count("insights", "copied", len(insights))
            continue
        ready = [
            insight
            for insight in insights
            if not _defer_insight(push, insight, references[insight.id])
        ]
        for start in range(0, len(ready), MAX_INSIGHT_BATCH_SIZE):
            await _create_insights(
                push,
                pair.destination.id,
                ready[start : start + MAX_INSIGHT_BATCH_SIZE],
            )


def _defer_insight(push: _Push, insight: InsightResponse, references: set[str]) -> bool:
    """Fail an insight that references a record this run failed to copy."""
    unsettled = sorted(references & push.unsettled.keys())
    if not unsettled:
        return False
    reason, retryable = push.unsettled[unsettled[0]]
    push.report.fail(
        "insights",
        str(insight.id),
        f"Not copied, because it references {reason}.",
        retryable=retryable,
    )
    return True


async def _list_pending_insights(
    push: _Push, pair: _AgentPair
) -> list[InsightResponse]:
    """List the source insights of one agent the destination lacks."""
    existing: set[str] = set()
    if pair.destination is not None and pair.has_earlier_records:
        async for insight in push.destination.insights.iter(
            InsightListParams(
                filter=_filter_by_agent(pair.destination.id), size=_PAGE_SIZE
            )
        ):
            source_id = get_pushed_insight_source_id(insight.metadata)
            if source_id is not None:
                existing.add(source_id)
    pending: list[InsightResponse] = []
    async for insight in push.source.insights.iter(
        InsightListParams(
            filter=_filter_by_agent(pair.source.id),
            sort="created:asc",
            size=_PAGE_SIZE,
        )
    ):
        if str(insight.id) in existing:
            push.report.count("insights", "already_present")
        elif pair.failed:
            push.report.skip("insights", str(insight.id), "Its agent was not copied.")
        else:
            pending.append(insight)
    return pending


async def _create_insights(
    push: _Push, agent_id: uuid.UUID, insights: list[InsightResponse]
) -> None:
    """Create one batch of rewritten insights on the destination."""
    for insight in insights:
        if insight.import_id is not None:
            push.references.add_unmapped(insight.import_id, "import")
        if insight.analyzer_version_id is not None:
            push.references.add_unmapped(
                insight.analyzer_version_id, "analyzer_version"
            )
    built = [build_pushed_insight(insight, push.references) for insight in insights]
    try:
        await push.destination.insights.create(
            agent_id, [pushed for pushed, _ in built]
        )
    except _RECORD_ERRORS as error:
        for insight in insights:
            push.fail("insights", str(insight.id), error)
        return
    push.report.count("insights", "copied", len(insights))
    for insight, (_, unmapped) in zip(insights, built, strict=True):
        if unmapped:
            push.report.add_unmapped(insight, unmapped)


async def _map_referenced_nodes(push: _Push, referenced: set[str]) -> None:
    """Map the nodes of every copied session the insights reference.

    Nodes get new ids on the destination and match their source by
    ``external_id``. Only referenced sessions are read, because most sessions
    are never cited by an insight. A session whose nodes cannot be listed
    holds back the insights that reference it.
    """
    slots = asyncio.Semaphore(_CONCURRENCY)

    async def list_nodes(
        client: KitaruAPIClient, session_id: uuid.UUID
    ) -> dict[str, uuid.UUID]:
        return {
            node.external_id: node.id
            async for node in client.sessions.iter_nodes(
                session_id, SessionNodeListParams(size=_ID_PAGE_SIZE)
            )
        }

    async def map_nodes(source_id: uuid.UUID, destination_id: uuid.UUID) -> None:
        try:
            async with slots:
                source_nodes, destination_nodes = await asyncio.gather(
                    list_nodes(push.source, source_id),
                    list_nodes(push.destination, destination_id),
                )
        except _RECORD_ERRORS as error:
            if isinstance(error, APIError) and _is_auth_failure(error):
                raise
            message, retryable = _describe_failure(error)
            push.unsettled[str(source_id)] = (
                f"session {source_id}, whose nodes could not be listed: {message}",
                retryable,
            )
            return
        for external_id, node_id in source_nodes.items():
            match = destination_nodes.get(external_id)
            if match is None:
                push.references.add_unmapped(node_id, "session_node")
            else:
                push.references.add(node_id, match)

    async with _open_task_group() as listings:
        for source_id, destination_id in push.sessions.items():
            if str(source_id) in referenced:
                listings.create_task(map_nodes(source_id, destination_id))
