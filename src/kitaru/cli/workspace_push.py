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
import uuid
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
from kitaru.cli.config import (
    ResolvedCredential,
    build_api_client,
    validate_server_url,
)
from kitaru.cli.output import CLIError, describe_validation_error
from kitaru.cli.workspace_push_references import (
    ReferenceMap,
    build_pushed_insight,
    find_uuids,
    get_pushed_insight_source_id,
)
from kitaru.cli.workspace_push_report import PushReport
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import APIError

PUSH_IMPORTED_FROM = "kitaru-workspace-push"
_PAGE_SIZE = 200
_ID_PAGE_SIZE = 1000
_MAX_ERROR_LENGTH = 300
_CONCURRENCY = 8


class _SessionCopyError(Exception):
    """Raised when a session copy fails for a reason the API did not state."""


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


@asynccontextmanager
async def open_destination_client(
    destination: str, credential_store: CredentialStore, timeout: float
) -> AsyncGenerator[KitaruAPIClient]:
    """Open an SDK client for the destination with its stored credential.

    ``KITARU_API_KEY`` is ignored here because it authenticates the source
    server of the same invocation.

    Args:
        destination: Destination server URL.
        credential_store: Store holding the destination's credential.
        timeout: HTTP timeout in seconds.

    Raises:
        CLIError: No credential is stored for the destination.

    Yields:
        Client authenticated against the destination.
    """
    stored = credential_store.get(destination)
    if stored is None:
        raise CLIError(
            "authentication_failed",
            f"No stored credential for the destination {destination}.",
            hint=_build_login_hint(destination),
        )
    client = build_api_client(
        destination,
        ResolvedCredential("stored", stored=stored),
        credential_store,
        timeout,
    )
    try:
        yield client
    finally:
        await client.close()


def validate_destination(destination: str, source_url: str) -> str:
    """Normalize the destination URL and require it to differ from the source."""
    destination_url = validate_server_url(destination)
    if destination_url == source_url:
        raise CLIError(
            "invalid_arguments",
            f"The destination {destination_url} is the source server.",
            hint="Select the source with --server and name another server in --to.",
        )
    return destination_url


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
    version number, sessions by ``imported_from`` and ``external_id`` within
    their agent, and insights by the source id recorded in their metadata.

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

    A source identity is kept, so the copy also deduplicates against a later
    import of the same trace. A missing half is filled from the push itself
    and the source session id, which makes the pair unique and stable across
    reruns.

    Args:
        session: Source session.

    Returns:
        ``imported_from`` and ``external_id`` the destination session carries.
    """
    return (
        session.imported_from or PUSH_IMPORTED_FROM,
        session.external_id or str(session.id),
    )


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


def _build_login_hint(destination: str) -> str:
    """Build the hint for a destination that needs a fresh credential."""
    return (
        f"Run `kitaru login {destination}`, then rerun the push with `--server` "
        "naming the source."
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


def _reraise_fatal(error: APIError) -> None:
    """Raise failures that would repeat for every remaining record."""
    if _is_auth_failure(error):
        raise error


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
            hint=_build_login_hint(destination),
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
    except APIError as error:
        _reraise_fatal(error)
        push.references.add_unmapped(agent.id, "agent")
        push.report.fail("agents", agent.name, _describe_error(error))
        return _AgentPair(agent, None, failed=True)
    push.references.add(agent.id, created.id)
    push.report.count("agents", "copied")
    return _AgentPair(agent, created, created=True)


async def _push_agent_versions(push: _Push, pair: _AgentPair) -> None:
    """Copy the versions of one agent, matching them by version number.

    A version is created only while it would receive its source number,
    which keeps the number-based match valid for the next run.
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
    existing: dict[int, AgentVersionResponse] = {}
    next_number = 1
    if pair.destination is not None:
        next_number = pair.destination.latest_version + 1
        if pair.has_earlier_records:
            existing = {
                version.version: version
                async for version in push.destination.agents.iter_versions(
                    pair.destination.id, AgentVersionListParams(size=_PAGE_SIZE)
                )
            }
    dropped_secrets = False
    for version in versions:
        record = f"{agent_name}@{version.version}"
        match = existing.get(version.version)
        if match is not None:
            push.references.add(version.id, match.id)
            push.report.count("agent_versions", "already_present")
            continue
        if version.version != next_number:
            push.references.add_unmapped(version.id, "agent_version")
            push.report.skip(
                "agent_versions",
                record,
                "The destination agent's version numbers differ from the source.",
            )
            continue
        if pair.destination is None or push.report.dry_run:
            push.report.count("agent_versions", "copied")
            next_number += 1
            continue
        run_spec = version.run_spec
        if run_spec is not None and run_spec.secret_ids:
            # Secret ids name source records, which the destination rejects.
            run_spec = run_spec.model_copy(update={"secret_ids": []})
            dropped_secrets = True
        try:
            created = await push.destination.agents.create_version(
                pair.destination.id,
                AgentVersionCreateRequest(
                    display_version=version.display_version,
                    description=version.description,
                    run_spec=run_spec,
                    capabilities=version.capabilities,
                ),
            )
        except APIError as error:
            _reraise_fatal(error)
            push.references.add_unmapped(version.id, "agent_version")
            push.report.fail("agent_versions", record, _describe_error(error))
            continue
        push.references.add(version.id, created.id)
        push.report.count("agent_versions", "copied")
        next_number = created.version + 1
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
        stored_keys = set(match.secret_keys) if match is not None else set()
        if match is not None:
            push.report.count("connections", "already_present")
        elif push.report.dry_run:
            push.report.count("connections", "copied")
        else:
            # Keep the default the destination already has, because a
            # provider allows one and a second is rejected.
            default = (
                connection.default and connection.provider not in default_providers
            )
            try:
                await push.destination.connections.create(
                    ConnectionCreateRequest(
                        name=connection.name,
                        provider=connection.provider,
                        env=connection.env,
                        default=default,
                    )
                )
            except APIError as error:
                _reraise_fatal(error)
                push.report.fail("connections", connection.name, _describe_error(error))
                continue
            if default:
                default_providers.add(connection.provider)
            push.report.count("connections", "copied")
        missing_keys = set(connection.secret_keys) - stored_keys
        if missing_keys:
            push.report.add_incomplete_connection(
                connection.name, missing_keys, stored_keys
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
    except (APIError, ValidationError, _SessionCopyError) as error:
        if isinstance(error, APIError):
            _reraise_fatal(error)
            message = _describe_error(error)
        elif isinstance(error, ValidationError):
            location, problem = describe_validation_error(error)
            message = f"The source session has invalid {location}: {problem}."
        else:
            message = str(error)
        push.references.add_unmapped(session.id, "session")
        push.report.fail("sessions", str(session.id), message)
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
        if error.status_code != httpx.codes.CONFLICT:
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
    """Copy the insights of every agent with their references rewritten."""
    pending = [(pair, await _list_pending_insights(push, pair)) for pair in pairs]
    if not push.report.dry_run:
        await _map_referenced_nodes(
            push, [insight for _, insights in pending for insight in insights]
        )
    for pair, insights in pending:
        if pair.destination is None or push.report.dry_run:
            push.report.count("insights", "copied", len(insights))
            continue
        for start in range(0, len(insights), MAX_INSIGHT_BATCH_SIZE):
            await _create_insights(
                push,
                pair.destination.id,
                insights[start : start + MAX_INSIGHT_BATCH_SIZE],
            )


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
    except APIError as error:
        _reraise_fatal(error)
        for insight in insights:
            push.report.fail("insights", str(insight.id), _describe_error(error))
        return
    push.report.count("insights", "copied", len(insights))
    for insight, (_, unmapped) in zip(insights, built, strict=True):
        if unmapped:
            push.report.add_unmapped(insight, unmapped)


async def _map_referenced_nodes(push: _Push, insights: list[InsightResponse]) -> None:
    """Map the nodes of every copied session the insights reference.

    Nodes get new ids on the destination and match their source by
    ``external_id``. Only referenced sessions are read, because most sessions
    are never cited by an insight.
    """
    referenced: set[str] = set()
    for insight in insights:
        referenced |= find_uuids(insight.model_dump_json())
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
        async with slots:
            source_nodes, destination_nodes = await asyncio.gather(
                list_nodes(push.source, source_id),
                list_nodes(push.destination, destination_id),
            )
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
