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
"""SQL replay config repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.schemas.experiment import (
    EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.schemas.replay import (
    REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.schemas.replay_config import ReplayConfigSchema
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayConfigNotFound,
)


class SQLReplayConfigRepository:
    """Replay config repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        row = ReplayConfigSchema.from_domain(config)
        self._session.add(row)
        await self._session.flush()
        return row.to_domain()

    async def get(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        row = await self._session.get(ReplayConfigSchema, config_id)
        if row is None:
            raise ReplayConfigNotFound(config_id)
        return row.to_domain()

    async def get_many(
        self, config_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Stored replay configs keyed by id, missing ids omitted.
        """
        if not config_ids:
            return {}
        statement = select(ReplayConfigSchema).where(
            col(ReplayConfigSchema.id).in_(config_ids)
        )
        rows = (await self._session.scalars(statement)).all()
        return {row.id: row.to_domain() for row in rows}

    async def delete_if_unreferenced(self, config_id: uuid.UUID) -> bool:
        """Delete a replay config unless something still references it.

        Args:
            config_id: Id of the replay config.

        Returns:
            ``True`` when the config was deleted.
        """
        row = await self._session.get(ReplayConfigSchema, config_id)
        if row is None:
            return False
        try:
            async with self._session.begin_nested():
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint in (
                EXPERIMENT_REPLAY_CONFIG_ID_FOREIGN_KEY,
                REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY,
            ):
                return False
            raise
        return True
