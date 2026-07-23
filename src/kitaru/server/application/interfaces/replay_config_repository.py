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
"""Replay config repository interface."""

import uuid
from typing import Protocol

from kitaru.server.domain.replay_config import ReplayConfig


class ReplayConfigRepository(Protocol):
    """Replay config persistence operations."""

    async def create(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        ...

    async def get(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        ...

    async def get_many(
        self, config_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Stored replay configs keyed by id, missing ids omitted.
        """
        ...

    async def delete_if_unreferenced(self, config_id: uuid.UUID) -> bool:
        """Delete a replay config unless something still references it.

        Args:
            config_id: Id of the replay config.

        Returns:
            ``True`` when the config was deleted.
        """
        ...
