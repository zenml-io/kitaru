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
"""Connection repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.connection import ConnectionFilter
from kitaru.server.domain.connection import Connection


class ConnectionRepository(Protocol):
    """Connection persistence operations."""

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
        ...

    async def get(self, connection_id: uuid.UUID) -> Connection:
        """Load a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.

        Returns:
            Stored connection.
        """
        ...

    async def get_default(self, provider: str) -> Connection | None:
        """Load the default connection of a provider, if any.

        Args:
            provider: Provider the connection addresses.

        Returns:
            Stored connection, or ``None`` when the provider has no default.
        """
        ...

    async def query(
        self, connection_filter: ConnectionFilter
    ) -> tuple[list[Connection], str | None]:
        """Query connections matching a filter.

        Args:
            connection_filter: Filter and pagination parameters.

        Returns:
            Page of matching connections and the next cursor.
        """
        ...

    async def update(self, connection: Connection) -> Connection:
        """Persist changes to an existing connection, clearing the previous default.

        Args:
            connection: Connection with modified fields.

        Raises:
            ConnectionNotFound: No connection has this id.
            DuplicateConnectionName: The connection name is already
                registered.

        Returns:
            Stored connection with the updated timestamp renewed.
        """
        ...

    async def delete(self, connection_id: uuid.UUID) -> None:
        """Delete a connection by id.

        Args:
            connection_id: Id of the connection.

        Raises:
            ConnectionNotFound: No connection has this id.
        """
        ...
