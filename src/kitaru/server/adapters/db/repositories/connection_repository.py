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
"""SQL connection repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import select, update

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.connection import (
    CONNECTION_NAME_UNIQUE_CONSTRAINT,
    ConnectionORM,
)
from kitaru.server.adapters.db.pagination import IdOrder, paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.connection import ConnectionFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.connection import (
    Connection,
    ConnectionNotFound,
    DuplicateConnectionName,
)

CONNECTION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": ConnectionORM.id,
    "name": ConnectionORM.name,
    "provider": ConnectionORM.provider,
    "default": ConnectionORM.default,
}


class SQLConnectionRepository(BaseSQLRepository[ConnectionORM]):
    """Connection repository backed by the application database."""

    orm_class = ConnectionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ConnectionNotFound(entity_id)

    async def _clear_default(self, connection: Connection) -> None:
        """Clear the default flag on the provider's previous default.

        Args:
            connection: Connection taking over as the provider's default.
        """
        statement = (
            update(ConnectionORM)
            .where(
                ConnectionORM.provider == connection.provider,
                ConnectionORM.default.is_(True),
                ConnectionORM.id != connection.id,
            )
            .values(default=False)
            .execution_options(synchronize_session="fetch")
        )
        await self._session.execute(statement)

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
        if connection.default:
            await self._clear_default(connection)
        row = ConnectionORM.from_domain(connection)
        await self._add(
            row,
            {
                CONNECTION_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateConnectionName(
                    connection.name
                )
            },
        )
        return row.to_domain()

    async def get(self, connection_id: uuid.UUID) -> Connection:
        """Load a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.

        Returns:
            Stored connection.
        """
        row = await self._get_row(connection_id)
        return row.to_domain()

    async def get_default(self, provider: str) -> Connection | None:
        """Load the default connection of a provider, if any.

        Args:
            provider: Provider the connection addresses.

        Returns:
            Stored connection, or ``None`` when the provider has no default.
        """
        statement = select(ConnectionORM).where(
            ConnectionORM.provider == provider, ConnectionORM.default.is_(True)
        )
        row = (await self._session.scalars(statement)).one_or_none()
        return row.to_domain() if row is not None else None

    async def query(
        self, connection_filter: ConnectionFilter
    ) -> tuple[list[Connection], str | None]:
        """Query connections matching a filter.

        Args:
            connection_filter: Filter and pagination parameters.

        Returns:
            Page of matching connections and the next cursor.
        """
        statement = select(ConnectionORM)
        if connection_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    connection_filter.expression, CONNECTION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session,
            statement,
            connection_filter,
            IdOrder(ConnectionORM.id, connection_filter.sort),
        )
        return [row.to_domain() for row in rows], next_cursor

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
        row = await self._get_row(connection.id)
        if connection.default:
            await self._clear_default(connection)
        row.name = connection.name
        row.provider = connection.provider
        row.env = connection.env
        row.secret_id = connection.secret_id
        row.default = connection.default
        await self._flush(
            {
                CONNECTION_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateConnectionName(
                    connection.name
                )
            }
        )
        return row.to_domain()

    async def delete(self, connection_id: uuid.UUID) -> None:
        """Delete a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.
        """
        await self._delete_row(connection_id)
