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

import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from pydantic import SecretStr, ValidationError

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
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.cli.config import (
    ResolvedCredential,
    build_api_client,
    validate_server_url,
)
from kitaru.cli.output import CLIError
from kitaru.cli.redaction import redact
from kitaru.cli.workspace_push_report import PushReport
from kitaru.cli.workspace_references import (
    ReferenceMap,
    build_pushed_insight,
    find_uuids,
    get_pushed_insight_source_id,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.credential_store import CredentialStore
from kitaru.client.exceptions import APIError, NotFoundError

PUSH_IMPORTED_FROM = "kitaru-workspace-push"
_PAGE_SIZE = 200
_MAX_ERROR_LENGTH = 300


@dataclass(slots=True)
class _AgentPair:
    """A selected source agent and its destination counterpart."""

    source: AgentResponse
    destination: AgentResponse | None
    """Destination agent, None when it was not created."""
    created: bool = False
    """Whether this run created the destination agent, which is then empty."""


@dataclass(slots=True)
class _Push:
    """Clients, options, and accumulated state of one push."""

    source: KitaruAPIClient
    destination: KitaruAPIClient
    references: ReferenceMap
    report: PushReport
    dry_run: bool
    sessions: dict[uuid.UUID, uuid.UUID] = field(default_factory=dict)
    """Source session id to destination session id."""
    node_mapped_sessions: set[uuid.UUID] = field(default_factory=set)


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
            hint=f"Run `kitaru login {destination}`, then rerun the push with "
            "`--server` naming the source.",
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
    copy_secrets: bool,
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
        copy_secrets: Whether connection secret values may leave the source.
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
        dry_run=dry_run,
    )
    selected = await _select_agents(push, agent_names)
    destination_agents = await _list_destination_agents(push)
    pairs = [await _push_agent(push, agent, destination_agents) for agent in selected]
    for pair in pairs:
        await _push_agent_versions(push, pair)
    await _push_connections(push, copy_secrets=copy_secrets)
    for pair in pairs:
        await _push_sessions(push, pair)
    for pair in pairs:
        await _push_insights(push, pair)
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

    Args:
        full: Source session with every node and payload.
        agent_id: Destination agent.
        agent_version_id: Destination agent version, when one was mapped.

    Returns:
        Request creating the session and its nodes in one transaction.
    """
    session = full.session
    imported_from, external_id = get_session_identity(session)
    return SessionIngestRequest(
        session=SessionCreateRequest(
            agent_id=agent_id,
            agent_version_id=agent_version_id,
            origin=session.origin,
            status=session.status,
            name=session.name,
            input_text_selector=session.input_text_selector,
            output_text_selector=session.output_text_selector,
            inputs=session.inputs,
            outputs=session.outputs,
            error=session.error,
            started_at=session.started_at,
            ended_at=session.ended_at,
            external_id=external_id,
            metadata=session.metadata,
            imported_from=imported_from,
            framework=session.framework,
            adapter_version=session.adapter_version,
        ),
        nodes=[_build_node_request(node) for node in full.nodes],
    )


def _build_node_request(node: SessionNodeResponse) -> SessionNodeCreateRequest:
    """Build the create request repeating one source node."""
    return SessionNodeCreateRequest(
        external_id=node.external_id,
        parent_external_id=node.parent_external_id,
        links=node.links,
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
        reasoning_selectors=node.reasoning_selectors,
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


def _filter_by_agent(agent_id: uuid.UUID) -> FilterCondition:
    """Build the list filter selecting one agent's records."""
    return FilterCondition(field="agent_id", op=FilterOp.EQ, value=str(agent_id))


def _describe_error(error: APIError) -> str:
    """Describe an API failure without credentials or unbounded text."""
    detail = redact(error.detail.strip())[:_MAX_ERROR_LENGTH]
    return f"HTTP {error.status_code}: {detail}" if detail else str(error.status_code)


def _reraise_fatal(error: APIError) -> None:
    """Raise failures that would repeat for every remaining record."""
    if error.status_code in {httpx.codes.UNAUTHORIZED, httpx.codes.FORBIDDEN}:
        raise error


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
        if error.status_code in {httpx.codes.UNAUTHORIZED, httpx.codes.FORBIDDEN}:
            destination = push.references.destination_url
            raise CLIError(
                "authentication_failed",
                f"The destination {destination} rejected its stored credential.",
                details={"status_code": error.status_code},
                hint=f"Run `kitaru login {destination}`, then rerun the push "
                "with `--server` naming the source.",
            ) from error
        raise


async def _push_agent(
    push: _Push, agent: AgentResponse, destination_agents: dict[str, AgentResponse]
) -> _AgentPair:
    """Match one source agent by name or create it on the destination."""
    existing = destination_agents.get(agent.name)
    if existing is not None:
        push.references.add(agent.id, existing.id)
        push.report.count("agents", "already_present")
        return _AgentPair(agent, existing)
    if push.dry_run:
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
        return _AgentPair(agent, None)
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
    if pair.destination is None and not push.dry_run:
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
        if not pair.created:
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
        if push.dry_run or pair.destination is None:
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


async def _push_connections(push: _Push, *, copy_secrets: bool) -> None:
    """Copy provider connections, matching them by name.

    A connection whose secret values cannot be read is copied with its
    non-secret values and reported with the keys to set on the destination.
    """
    existing: set[str] = set()
    default_providers: set[str] = set()
    async for connection in push.destination.connections.iter(
        ConnectionListParams(size=_PAGE_SIZE)
    ):
        existing.add(connection.name)
        if connection.default:
            default_providers.add(connection.provider)
    hidden_values = False
    async for connection in push.source.connections.iter(
        ConnectionListParams(sort="created:asc", size=_PAGE_SIZE)
    ):
        if connection.name in existing:
            push.report.count("connections", "already_present")
            continue
        if connection.secret_keys and not copy_secrets:
            push.report.skip(
                "connections",
                connection.name,
                "Copying a connection sends its secret values to the "
                "destination, which requires --copy-secrets.",
            )
            continue
        if push.dry_run:
            push.report.count("connections", "copied")
            continue
        # Keep the default the destination already has, because a provider
        # allows one and a second is rejected.
        default = connection.default and connection.provider not in default_providers
        try:
            secrets = await _read_connection_secrets(push, connection)
            await push.destination.connections.create(
                ConnectionCreateRequest(
                    name=connection.name,
                    provider=connection.provider,
                    env=connection.env,
                    secrets=secrets or {},
                    default=default,
                )
            )
        except APIError as error:
            _reraise_fatal(error)
            # A rejected request can echo its body, which holds the values.
            push.report.fail(
                "connections", connection.name, f"HTTP {error.status_code}"
            )
            continue
        if default:
            default_providers.add(connection.provider)
        push.report.count("connections", "copied")
        if secrets is None:
            hidden_values = True
            push.report.add_incomplete_connection(
                connection.name, connection.secret_keys
            )
    if hidden_values:
        push.report.warnings.append(
            f"{push.references.source_url} does not return the secret values "
            "of connections, so the copies hold only the non-secret values. "
            "Set the keys listed under `incomplete_connections` on the "
            "destination."
        )


async def _read_connection_secrets(
    push: _Push, connection: ConnectionResponse
) -> dict[str, SecretStr] | None:
    """Read the secret values of a source connection.

    Returns:
        Secret values, empty when the connection has none, or None when the
        source keeps the connection's secret hidden.
    """
    if not connection.secret_keys:
        return {}
    try:
        secret = await push.source.secrets.get(connection.secret_id, True)
    except NotFoundError:
        return None
    return secret.values


async def _push_sessions(push: _Push, pair: _AgentPair) -> None:
    """Copy the settled sessions of one agent with their nodes."""
    existing: dict[tuple[str, str], uuid.UUID] = {}
    if pair.destination is not None and not pair.created:
        async for session in push.destination.sessions.iter(
            SessionListParams(
                filter=_filter_by_agent(pair.destination.id), size=_PAGE_SIZE
            )
        ):
            if session.imported_from is not None and session.external_id is not None:
                existing[(session.imported_from, session.external_id)] = session.id
    missing_versions = 0
    async for session in push.source.sessions.iter(
        SessionListParams(
            filter=_filter_by_agent(pair.source.id),
            sort="created:asc",
            size=_PAGE_SIZE,
        )
    ):
        record = str(session.id)
        if session.import_id is not None:
            push.references.add_unmapped(session.import_id, "import")
        reason = _get_session_skip_reason(session, pair, dry_run=push.dry_run)
        if reason is not None:
            push.references.add_unmapped(session.id, "session")
            push.report.skip("sessions", record, reason)
            continue
        match = existing.get(get_session_identity(session))
        if match is not None:
            _record_session(push, session.id, match)
            push.report.count("sessions", "already_present")
            continue
        if push.dry_run or pair.destination is None:
            push.report.count("sessions", "copied")
            continue
        agent_version_id = None
        if session.agent_version_id is not None:
            agent_version_id = push.references.get(session.agent_version_id)
            missing_versions += agent_version_id is None
        try:
            created = await _ingest_session(
                push, session, pair.destination.id, agent_version_id
            )
        except APIError as error:
            _reraise_fatal(error)
            push.references.add_unmapped(session.id, "session")
            push.report.fail("sessions", record, _describe_error(error))
            continue
        except ValidationError as error:
            push.references.add_unmapped(session.id, "session")
            push.report.fail(
                "sessions",
                record,
                f"The source session is not a valid ingest request "
                f"({error.error_count()} validation error(s)).",
            )
            continue
        if created is None:
            push.references.add_unmapped(session.id, "session")
            push.report.fail(
                "sessions",
                record,
                "The destination reported the session as present but did not list it.",
            )
            continue
        session_id, copied = created
        _record_session(push, session.id, session_id)
        push.report.count("sessions", "copied" if copied else "already_present")
    if missing_versions:
        push.report.warnings.append(
            f"{missing_versions} session(s) of agent {pair.source.name!r} were "
            "copied without an agent version because their version was not "
            "copied."
        )


def _get_session_skip_reason(
    session: SessionResponse, pair: _AgentPair, *, dry_run: bool
) -> str | None:
    """Return why a source session is not copied, None when it is."""
    if pair.destination is None and not dry_run:
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


async def _ingest_session(
    push: _Push,
    session: SessionResponse,
    agent_id: uuid.UUID,
    agent_version_id: uuid.UUID | None,
) -> tuple[uuid.UUID, bool] | None:
    """Create one session with its nodes on the destination.

    Returns:
        Destination session id and whether this call created it, None when
        the destination reports a conflict but lists no matching session.
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
    return (page.items[0].id, False) if page.items else None


async def _push_insights(push: _Push, pair: _AgentPair) -> None:
    """Copy the insights of one agent with their references rewritten."""
    existing: set[str] = set()
    if pair.destination is not None and not pair.created:
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
        elif pair.destination is None and not push.dry_run:
            push.report.skip("insights", str(insight.id), "Its agent was not copied.")
        else:
            pending.append(insight)
    if push.dry_run or pair.destination is None:
        push.report.count("insights", "copied", len(pending))
        return
    await _map_referenced_nodes(push, pending)
    for insight in pending:
        if insight.import_id is not None:
            push.references.add_unmapped(insight.import_id, "import")
        if insight.analyzer_version_id is not None:
            push.references.add_unmapped(
                insight.analyzer_version_id, "analyzer_version"
            )
    for start in range(0, len(pending), MAX_INSIGHT_BATCH_SIZE):
        batch = pending[start : start + MAX_INSIGHT_BATCH_SIZE]
        built = [build_pushed_insight(insight, push.references) for insight in batch]
        try:
            await push.destination.insights.create(
                pair.destination.id, [insight for insight, _ in built]
            )
        except APIError as error:
            _reraise_fatal(error)
            for insight in batch:
                push.report.fail("insights", str(insight.id), _describe_error(error))
            continue
        push.report.count("insights", "copied", len(batch))
        for insight, (_, unmapped) in zip(batch, built, strict=True):
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
        referenced |= find_uuids(insight.model_dump(mode="json"))
    for source_id, destination_id in list(push.sessions.items()):
        if str(source_id) not in referenced or source_id in push.node_mapped_sessions:
            continue
        push.node_mapped_sessions.add(source_id)
        destination_nodes = {
            node.external_id: node.id
            async for node in push.destination.sessions.iter_nodes(
                destination_id, SessionNodeListParams(size=_PAGE_SIZE)
            )
        }
        async for node in push.source.sessions.iter_nodes(
            source_id, SessionNodeListParams(size=_PAGE_SIZE)
        ):
            match = destination_nodes.get(node.external_id)
            if match is None:
                push.references.add_unmapped(node.id, "session_node")
            else:
                push.references.add(node.id, match)
