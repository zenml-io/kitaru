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
"""Agent version repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.domain.agent_version import AgentVersion


class AgentVersionRepository(Protocol):
    """Agent version persistence operations."""

    async def create(self, version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        Args:
            version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the version's agent id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with timestamps set.
        """
        ...

    async def get(self, version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        ...

    async def query(
        self, version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], int]:
        """Query agent versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the total match count.
        """
        ...

    async def update(self, version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Args:
            version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        ...

    async def delete(self, version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
        """
        ...
