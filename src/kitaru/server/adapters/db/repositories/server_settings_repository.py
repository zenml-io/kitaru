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
"""SQL server settings repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.orm.server_settings import ServerSettingsORM


class SQLServerSettingsRepository:
    """Server settings repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def ensure_server_id(self, server_id: uuid.UUID) -> uuid.UUID:
        """Persist the server id when none is stored yet.

        A concurrent first startup resolves through the primary key conflict,
        so every process reads the same stored id.

        Args:
            server_id: Server id stored when the table is empty.

        Returns:
            Stored server id.
        """
        statement = (
            insert(ServerSettingsORM)
            .values(server_id=server_id)
            .on_conflict_do_nothing()
        )
        await self._session.execute(statement)
        return (
            await self._session.execute(select(ServerSettingsORM.server_id))
        ).scalar_one()
