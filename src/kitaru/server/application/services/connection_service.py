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
"""Connection use cases."""

import uuid

from pydantic import SecretStr

from kitaru.server.application.interfaces.connection_repository import (
    ConnectionRepository,
)
from kitaru.server.application.interfaces.secret_repository import SecretRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.connection import (
    ConnectionCreate,
    ConnectionFilter,
)
from kitaru.server.domain.connection import Connection
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.secret import Secret

# Internal secret names carry the connection id rather than the connection
# name, so a user-created secret can never take the name first.
INTERNAL_SECRET_NAME_PREFIX = "connection-"


def build_internal_secret_name(connection_id: uuid.UUID) -> str:
    """Build the name of the secret holding a connection's sensitive values.

    Args:
        connection_id: Id of the connection.

    Returns:
        Secret name.
    """
    return f"{INTERNAL_SECRET_NAME_PREFIX}{connection_id.hex}"


class ConnectionService:
    """Connection use cases."""

    def __init__(
        self, repository: ConnectionRepository, secret_repository: SecretRepository
    ) -> None:
        """Initialize the service.

        Args:
            repository: Connection repository.
            secret_repository: Secret repository, for the connection's
                internal secret.
        """
        self._repository = repository
        self._secrets = secret_repository

    async def create_connection(
        self, command: ConnectionCreate, actor: AuthContext
    ) -> Connection:
        """Create a connection and the internal secret holding its values.

        Args:
            command: Fields for the new connection.
            actor: Caller context.

        Raises:
            DuplicateConnectionName: The connection name is already
                registered.
            InvalidConnectionValues: A key uses the reserved prefix or is set
                as both an env value and a secret.

        Returns:
            Created connection.
        """
        owner_id = actor.account.id
        secret_id = uuid7()
        connection = Connection(
            owner_id=owner_id,
            name=command.name,
            provider=command.provider,
            env=command.env,
            secret_id=secret_id,
            default=command.default,
        )
        connection.check_secret_keys(command.secrets)
        await self._secrets.create(
            Secret(
                id=secret_id,
                owner_id=owner_id,
                name=build_internal_secret_name(connection.id),
                internal=True,
                values=command.secrets,
            )
        )
        return await self._repository.create(connection)

    async def get_connection(
        self, connection_id: uuid.UUID, actor: AuthContext
    ) -> Connection:
        """Get a connection by id.

        Args:
            connection_id: Id of the connection.
            actor: Caller context.

        Raises:
            ConnectionNotFound: No connection has this id.

        Returns:
            Stored connection.
        """
        _ = actor
        return await self._repository.get(connection_id)

    async def list_connections(
        self, connection_filter: ConnectionFilter, actor: AuthContext
    ) -> tuple[list[Connection], str | None]:
        """List connections matching a filter.

        Args:
            connection_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching connections and the next cursor.
        """
        _ = actor
        return await self._repository.query(connection_filter)

    async def update_connection(
        self,
        connection_id: uuid.UUID,
        env: dict[str, str] | None,
        secrets: dict[str, SecretStr] | None,
        default: bool | None,
        actor: AuthContext,
    ) -> Connection:
        """Partially update a connection, merging env and secrets by key.

        Args:
            connection_id: Id of the connection.
            env: Env entries to upsert, unchanged when ``None``.
            secrets: Secret entries to upsert, unchanged when ``None``.
            default: New default state, unchanged when ``None``.
            actor: Caller context.

        Raises:
            ConnectionNotFound: No connection has this id.
            SecretNotFound: The connection's internal secret is gone.
            InvalidConnectionValues: A key uses the reserved prefix or is set
                as both an env value and a secret.

        Returns:
            Updated connection.
        """
        _ = actor
        connection = await self._repository.get(connection_id)
        secret = await self._secrets.get(connection.secret_id)
        if env is not None:
            connection.update_env({**connection.env, **env})
        if secrets is not None:
            secret.update_values({**secret.values, **secrets})
        if default is not None:
            connection.update_default(default)
        connection.check_secret_keys(secret.values)
        if secrets is not None:
            await self._secrets.update(secret)
        return await self._repository.update(connection)

    async def delete_connection(
        self, connection_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete a connection and the internal secret holding its values.

        Args:
            connection_id: Id of the connection.
            actor: Caller context.

        Raises:
            ConnectionNotFound: No connection has this id.
        """
        _ = actor
        connection = await self._repository.get(connection_id)
        await self._repository.delete(connection.id)
        await self._secrets.delete(connection.secret_id)
