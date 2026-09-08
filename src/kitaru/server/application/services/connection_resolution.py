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
"""Connection resolution."""

import uuid

from kitaru.server.application.interfaces.connection_repository import (
    ConnectionRepository,
)


async def resolve_connection_id(
    connection_id: uuid.UUID | None,
    provider: str | None,
    connection_repository: ConnectionRepository,
) -> uuid.UUID | None:
    """Resolve a named connection or the provider's default.

    Args:
        connection_id: Named connection.
        provider: Plugin provider.
        connection_repository: Connection repository.

    Raises:
        ConnectionNotFound: No connection has the named id.

    Returns:
        Resolved connection id.
    """
    if connection_id is not None:
        return (await connection_repository.get(connection_id)).id
    if provider is None:
        return None
    default = await connection_repository.get_default(provider)
    return None if default is None else default.id
