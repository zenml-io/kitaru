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
"""In-memory fakes for secrets, connections, agents, and agent versions."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import SecretStr

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.application.models.connection import ConnectionFilter
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.domain.agent import Agent, AgentNotFound, DuplicateAgentName
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.connection import (
    Connection,
    ConnectionNotFound,
    DuplicateConnectionName,
)
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretInUse,
    SecretNotFound,
)
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)

# Named only in string annotations; a runtime import could be circular.
if TYPE_CHECKING:
    from kitaru_test_support.evaluations import FakeExperimentRunRepository
    from kitaru_test_support.sessions import FakeSessionRepository, FakeTagRepository


class FakeSecretRepository:
    """In-memory secret repository."""

    def __init__(
        self,
        agent_versions: "FakeAgentVersionRepository | None" = None,
        connections: "FakeConnectionRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            agent_versions: Fake agent version repository, consulted by
                delete to check for an in-use secret.
            connections: Fake connection repository, consulted by delete to
                check for an in-use secret.
        """
        self._secrets: dict[uuid.UUID, Secret] = {}
        self._agent_versions = agent_versions
        self._connections = connections

    def _check_duplicate_name(self, secret: Secret) -> None:
        for other in self._secrets.values():
            if other.id != secret.id and other.name == secret.name:
                raise DuplicateSecretName(secret.name)

    async def create(self, secret: Secret) -> Secret:
        """Persist a new secret.

        Args:
            secret: Secret to store.

        Raises:
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with timestamps set.
        """
        self._check_duplicate_name(secret)
        now = datetime.now(UTC)
        stored = secret.model_copy(update={"created": now, "updated": now})
        self._secrets[stored.id] = stored
        return stored.model_copy()

    async def get(self, secret_id: uuid.UUID) -> Secret:
        """Load a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.

        Returns:
            Stored secret.
        """
        secret = self._secrets.get(secret_id)
        if secret is None:
            raise SecretNotFound(secret_id)
        return secret.model_copy()

    async def get_many(
        self, secret_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, Secret]:
        """Bulk-load secrets by id, keyed by id, missing ids omitted.

        Args:
            secret_ids: Ids of the secrets to load.

        Returns:
            Stored secrets keyed by id.
        """
        wanted = set(secret_ids)
        return {
            secret_id: secret.model_copy(deep=True)
            for secret_id, secret in self._secrets.items()
            if secret_id in wanted
        }

    async def query(
        self, secret_filter: SecretFilter
    ) -> tuple[list[Secret], str | None]:
        """Query secrets matching a filter.

        Args:
            secret_filter: Filter and pagination parameters.

        Returns:
            Page of matching secrets and the next cursor.
        """
        secrets = list(self._secrets.values())
        if secret_filter.owner_id is not None:
            secrets = [
                secret
                for secret in secrets
                if secret.owner_id == secret_filter.owner_id
            ]
        if secret_filter.internal is not None:
            secrets = [
                secret
                for secret in secrets
                if secret.internal == secret_filter.internal
            ]
        if secret_filter.expression is not None:
            secrets = [
                secret
                for secret in secrets
                if _evaluate_filter_expression(secret, secret_filter.expression)
            ]
        page, next_cursor = _paginate_fake(secrets, secret_filter)
        return [secret.model_copy() for secret in page], next_cursor

    async def update(self, secret: Secret) -> Secret:
        """Persist changes to an existing secret.

        Args:
            secret: Secret with modified fields.

        Raises:
            SecretNotFound: No secret has this id.
            DuplicateSecretName: The secret name is already registered.

        Returns:
            Stored secret with the updated timestamp renewed.
        """
        stored = self._secrets.get(secret.id)
        if stored is None:
            raise SecretNotFound(secret.id)
        self._check_duplicate_name(secret)
        now = _renewed_timestamp(stored.updated)
        updated = secret.model_copy(update={"created": stored.created, "updated": now})
        self._secrets[secret.id] = updated
        return updated.model_copy()

    async def delete(self, secret_id: uuid.UUID) -> None:
        """Delete a secret by id.

        Args:
            secret_id: Id of the secret.

        Raises:
            SecretNotFound: No secret has this id.
            SecretInUse: An agent version or a connection references the
                secret.
        """
        if secret_id not in self._secrets:
            raise SecretNotFound(secret_id)
        if self._agent_versions is not None and any(
            version.run_spec is not None and secret_id in version.run_spec.secret_ids
            for version in self._agent_versions._versions.values()
        ):
            raise SecretInUse(secret_id)
        if self._connections is not None and any(
            connection.secret_id == secret_id
            for connection in self._connections._connections.values()
        ):
            raise SecretInUse(secret_id)
        del self._secrets[secret_id]


async def create_secret(
    repository: FakeSecretRepository,
    owner_id: uuid.UUID,
    name: str = "db",
    internal: bool = False,
    values: dict[str, SecretStr] | None = None,
) -> Secret:
    """Store a secret in the fake repository.

    Args:
        repository: Fake secret repository.
        owner_id: Id of the owning account.
        name: Secret name.
        internal: Internal state.
        values: Secret values.

    Returns:
        Stored secret.
    """
    if values is None:
        values = {"password": SecretStr("hunter2")}
    return await repository.create(
        Secret(owner_id=owner_id, name=name, internal=internal, values=values)
    )


class FakeConnectionRepository:
    """In-memory connection repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._connections: dict[uuid.UUID, Connection] = {}

    def _check_duplicate_name(self, connection: Connection) -> None:
        for other in self._connections.values():
            if other.id != connection.id and other.name == connection.name:
                raise DuplicateConnectionName(connection.name)

    def _clear_default(self, connection: Connection) -> None:
        for other_id, other in self._connections.items():
            if (
                other_id != connection.id
                and other.provider == connection.provider
                and other.default
            ):
                self._connections[other_id] = other.model_copy(
                    update={"default": False}
                )

    async def create(self, connection: Connection) -> Connection:
        """Persist a new connection, clearing the provider's previous default.

        Args:
            connection: Connection to store.

        Raises:
            DuplicateConnectionName: The connection name is already
                registered.

        Returns:
            Stored connection with timestamps set.
        """
        self._check_duplicate_name(connection)
        if connection.default:
            self._clear_default(connection)
        now = datetime.now(UTC)
        stored = connection.model_copy(update={"created": now, "updated": now})
        self._connections[stored.id] = stored
        return stored.model_copy()

    async def get(self, connection_id: uuid.UUID) -> Connection:
        """Load a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.

        Returns:
            Stored connection.
        """
        connection = self._connections.get(connection_id)
        if connection is None:
            raise ConnectionNotFound(connection_id)
        return connection.model_copy()

    async def get_default(self, provider: str) -> Connection | None:
        """Load the default connection of a provider, if any.

        Args:
            provider: Provider the connection addresses.

        Returns:
            Stored connection, or ``None`` when the provider has no default.
        """
        for connection in self._connections.values():
            if connection.provider == provider and connection.default:
                return connection.model_copy()
        return None

    async def query(
        self, connection_filter: ConnectionFilter
    ) -> tuple[list[Connection], str | None]:
        """Query connections matching a filter.

        Args:
            connection_filter: Filter and pagination parameters.

        Returns:
            Page of matching connections and the next cursor.
        """
        connections = list(self._connections.values())
        if connection_filter.expression is not None:
            connections = [
                connection
                for connection in connections
                if _evaluate_filter_expression(connection, connection_filter.expression)
            ]
        page, next_cursor = _paginate_fake(connections, connection_filter)
        return [connection.model_copy() for connection in page], next_cursor

    async def update(self, connection: Connection) -> Connection:
        """Persist changes to an existing connection.

        Args:
            connection: Connection with modified fields.

        Raises:
            ConnectionNotFound: No connection has this id.
            DuplicateConnectionName: The connection name is already
                registered.

        Returns:
            Stored connection with the updated timestamp renewed.
        """
        stored = self._connections.get(connection.id)
        if stored is None:
            raise ConnectionNotFound(connection.id)
        self._check_duplicate_name(connection)
        if connection.default:
            self._clear_default(connection)
        now = _renewed_timestamp(stored.updated)
        updated = connection.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._connections[connection.id] = updated
        return updated.model_copy()

    async def delete(self, connection_id: uuid.UUID) -> None:
        """Delete a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.
        """
        if connection_id not in self._connections:
            raise ConnectionNotFound(connection_id)
        del self._connections[connection_id]


async def create_connection(
    repository: FakeConnectionRepository,
    owner_id: uuid.UUID,
    secret_id: uuid.UUID,
    name: str = "langfuse-prod",
    provider: str = "langfuse",
    env: dict[str, str] | None = None,
    default: bool = False,
) -> Connection:
    """Store a connection in the fake repository.

    Args:
        repository: Fake connection repository.
        owner_id: Id of the owning account.
        secret_id: Secret holding the sensitive values.
        name: Connection name.
        provider: Provider the connection addresses.
        env: Non-secret values.
        default: Whether the connection is the provider's default.

    Returns:
        Stored connection.
    """
    return await repository.create(
        Connection(
            owner_id=owner_id,
            name=name,
            provider=provider,
            env=env if env is not None else {},
            secret_id=secret_id,
            default=default,
        )
    )


class FakeAgentRepository:
    """In-memory agent repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._agents: dict[uuid.UUID, Agent] = {}

    def _check_duplicate_name(self, agent: Agent) -> None:
        for other in self._agents.values():
            if (
                other.id != agent.id
                and other.deleted_at is None
                and other.name == agent.name
            ):
                raise DuplicateAgentName(agent.name)

    def exists(self, agent_id: uuid.UUID) -> bool:
        """Report whether an agent has this id.

        Args:
            agent_id: Id of the agent.

        Returns:
            Whether an agent has this id.
        """
        return agent_id in self._agents

    def increment_latest_version(self, agent_id: uuid.UUID) -> int:
        """Bump and return the agent's version counter.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            agent_id: Id of the agent to bump.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            New version number.
        """
        agent = self._agents.get(agent_id)
        if agent is None or agent.deleted_at is not None:
            raise AgentNotFound(agent_id)
        agent.latest_version += 1
        return agent.latest_version

    async def create(self, agent: Agent) -> Agent:
        """Persist a new agent.

        Args:
            agent: Agent to store.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with timestamps set.
        """
        self._check_duplicate_name(agent)
        now = datetime.now(UTC)
        stored = agent.model_copy(update={"created": now, "updated": now})
        self._agents[stored.id] = stored
        return stored.model_copy()

    async def get(self, agent_id: uuid.UUID) -> Agent:
        """Load an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored agent.
        """
        agent = self._agents.get(agent_id)
        if agent is None or agent.deleted_at is not None:
            raise AgentNotFound(agent_id)
        return agent.model_copy()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], str | None]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the next cursor.
        """
        agents = [agent for agent in self._agents.values() if agent.deleted_at is None]
        if agent_filter.expression is not None:
            agents = [
                agent
                for agent in agents
                if _evaluate_filter_expression(agent, agent_filter.expression)
            ]
        page, next_cursor = _paginate_fake(agents, agent_filter)
        return [agent.model_copy() for agent in page], next_cursor

    async def update(self, agent: Agent) -> Agent:
        """Persist changes to an existing agent.

        Args:
            agent: Agent with modified fields.

        Raises:
            AgentNotFound: No agent has this id.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with the updated timestamp renewed.
        """
        stored = self._agents.get(agent.id)
        if stored is None or stored.deleted_at is not None:
            raise AgentNotFound(agent.id)
        self._check_duplicate_name(agent)
        now = _renewed_timestamp(stored.updated)
        updated = agent.model_copy(update={"created": stored.created, "updated": now})
        self._agents[agent.id] = updated
        return updated.model_copy()

    async def mark_deleted(self, agent_id: uuid.UUID) -> None:
        """Mark an agent deleted, hiding it from every read.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
        """
        agent = self._agents.get(agent_id)
        if agent is None or agent.deleted_at is not None:
            raise AgentNotFound(agent_id)
        agent.deleted_at = datetime.now(UTC)


async def create_agent(
    repository: FakeAgentRepository,
    owner_id: uuid.UUID,
    name: str = "assistant",
    description: str | None = None,
) -> Agent:
    """Store an agent in the fake repository.

    Args:
        repository: Fake agent repository.
        owner_id: Id of the owning account.
        name: Agent name.
        description: Agent description.

    Returns:
        Stored agent.
    """
    return await repository.create(
        Agent(owner_id=owner_id, name=name, description=description)
    )


class FakeAgentVersionRepository:
    """In-memory agent version repository."""

    def __init__(
        self,
        agents: FakeAgentRepository,
        tags: "FakeTagRepository | None" = None,
        experiment_runs: "FakeExperimentRunRepository | None" = None,
        sessions: "FakeSessionRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            agents: Fake agent repository sharing the version counter.
            tags: Fake tag repository, consulted by the ``tag`` filter.
            experiment_runs: Fake experiment run repository, consulted by
                delete to check for an in-use version.
            sessions: Fake session repository, whose agent version pointers
                are cleared on delete.
        """
        self._agents = agents
        self._versions: dict[uuid.UUID, AgentVersion] = {}
        self._tags = tags
        self._experiment_runs = experiment_runs
        self._sessions = sessions

    async def create(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        Args:
            agent_version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the given agent id.

        Returns:
            Stored agent version with its assigned version number and
            timestamps set.
        """
        version_number = self._agents.increment_latest_version(agent_version.agent_id)
        now = datetime.now(UTC)
        stored = agent_version.model_copy(
            update={"version": version_number, "created": now, "updated": now}
        )
        self._versions[stored.id] = stored
        return stored.model_copy()

    async def get(self, agent_version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        agent_version = self._versions.get(agent_version_id)
        if agent_version is None:
            raise AgentVersionNotFound(agent_version_id)
        return agent_version.model_copy()

    async def get_runnable(self, agent_version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version whose agent is not deleted.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentNotFound: The version's agent is deleted.

        Returns:
            Stored agent version.
        """
        agent_version = await self.get(agent_version_id)
        agent = self._agents._agents.get(agent_version.agent_id)
        if agent is None or agent.deleted_at is not None:
            raise AgentNotFound(agent_version.agent_id)
        return agent_version

    async def get_agent_id(self, agent_version_id: uuid.UUID) -> uuid.UUID:
        """Load the id of the agent a version belongs to.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Id of the owning agent.
        """
        agent_version = self._versions.get(agent_version_id)
        if agent_version is None:
            raise AgentVersionNotFound(agent_version_id)
        return agent_version.agent_id

    def _agent_version_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of agent versions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of agent versions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id
            and link.resource_type == TagResourceType.AGENT_VERSION
        }

    async def query(
        self, agent_version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], str | None]:
        """Query agent versions matching a filter.

        Args:
            agent_version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the next cursor.
        """
        versions = list(self._versions.values())
        if agent_version_filter.agent_id is not None:
            versions = [
                version
                for version in versions
                if version.agent_id == agent_version_filter.agent_id
            ]
        if agent_version_filter.expression is not None:
            resolvers = {"tag": self._evaluate_tag_condition}
            versions = [
                version
                for version in versions
                if _evaluate_filter_expression(
                    version, agent_version_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(versions, agent_version_filter)
        return [version.model_copy() for version in page], next_cursor

    def _evaluate_tag_condition(
        self, agent_version: AgentVersion, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against an agent version.

        Args:
            agent_version: Agent version to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the agent version has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            agent_version.id in self._agent_version_ids_tagged(name) for name in names
        )

    async def update(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Args:
            agent_version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        stored = self._versions.get(agent_version.id)
        if stored is None:
            raise AgentVersionNotFound(agent_version.id)
        now = _renewed_timestamp(stored.updated)
        updated = agent_version.model_copy(
            update={"created": stored.created, "updated": now}
        )
        self._versions[agent_version.id] = updated
        return updated.model_copy()

    async def delete(self, agent_version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentVersionInUse: An experiment run references this version.
        """
        if agent_version_id not in self._versions:
            raise AgentVersionNotFound(agent_version_id)
        if self._experiment_runs is not None and any(
            run.agent_version_id == agent_version_id
            for run in self._experiment_runs._runs.values()
        ):
            raise AgentVersionInUse(agent_version_id)
        del self._versions[agent_version_id]
        if self._sessions is not None:
            self._sessions._null_agent_version(agent_version_id)


async def create_agent_version(
    repository: FakeAgentVersionRepository,
    agent_id: uuid.UUID,
    owner_id: uuid.UUID,
    display_version: str | None = None,
    description: str | None = None,
    run_spec: RunSpec | None = None,
    capabilities: AgentCapabilities | None = None,
) -> AgentVersion:
    """Store an agent version in the fake repository.

    Args:
        repository: Fake agent version repository.
        agent_id: Id of the agent this version belongs to.
        owner_id: Id of the owning account.
        display_version: Human-readable designator.
        description: Version description.
        run_spec: Run spec.
        capabilities: Agent capabilities, empty when omitted.

    Returns:
        Stored agent version.
    """
    return await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version=display_version,
            description=description,
            run_spec=run_spec,
            capabilities=capabilities
            if capabilities is not None
            else AgentCapabilities(),
        )
    )
