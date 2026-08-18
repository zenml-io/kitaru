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

from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.domain.agent_version import AgentVersion


class AgentVersionRepository(Protocol):
    """Agent version persistence operations."""

    async def create(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        The version number is server-assigned from the owning agent's
        version counter, in the same transaction as the insert.

        Args:
            agent_version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the given agent id.

        Returns:
            Stored agent version with its assigned version number and
            timestamps set.
        """
        ...

    async def get(self, agent_version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        ...

    async def get_agent_id(self, agent_version_id: uuid.UUID) -> uuid.UUID:
        """Load the id of the agent a version belongs to.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Id of the owning agent.
        """
        ...

    async def query(
        self, agent_version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], str | None]:
        """Query agent versions matching a filter.

        Args:
            agent_version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the next cursor.
        """
        ...

    async def update(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Replacing the run spec replaces the secret link rows to match its
        secret ids.

        Args:
            agent_version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        ...

    async def delete(self, agent_version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentVersionInUse: The version is referenced by an experiment run.
        """
        ...
