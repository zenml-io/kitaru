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
from datetime import datetime

from sqlalchemy import or_, select, update
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

    async def get_last_user_activity(self) -> datetime | None:
        """Load the time of the last recorded user activity.

        Returns:
            Stored timestamp, or ``None`` when none was recorded yet.
        """
        return (
            await self._session.execute(select(ServerSettingsORM.last_user_activity))
        ).scalar_one_or_none()

    async def record_user_activity(self, at: datetime) -> None:
        """Store a user activity time unless a later one is already stored.

        The guard lives in the statement, so replicas writing out of order
        can never move the stored time backward.

        Args:
            at: Time of the activity.
        """
        await self._session.execute(
            update(ServerSettingsORM)
            .where(
                or_(
                    ServerSettingsORM.last_user_activity.is_(None),
                    ServerSettingsORM.last_user_activity < at,
                )
            )
            .values(last_user_activity=at)
        )
