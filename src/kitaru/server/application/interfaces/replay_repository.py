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
"""Replay repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.replay import Replay, ReplayStatus


class ReplayRepository(Protocol):
    """Replay persistence operations."""

    async def create(self, replay: Replay) -> Replay:
        """Persist a new replay.

        Args:
            replay: Replay to store.

        Raises:
            ExperimentRunNotFound: No experiment run has the replay's
                experiment run id.
            ReplayConfigNotFound: No replay config has the replay's replay
                config id.
            AgentVersionNotFound: No agent version has the replay's agent
                version id.
            SessionNotFound: No session has the replay's original session
                id.
            DuplicateReplaySession: The run already replays the original
                session.

        Returns:
            Stored replay with timestamps set.
        """
        ...

    async def get(self, replay_id: uuid.UUID) -> Replay:
        """Load a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            ReplayNotFound: No replay has this id.

        Returns:
            Stored replay.
        """
        ...

    async def query(self, replay_filter: ReplayFilter) -> tuple[list[Replay], int]:
        """Query replays matching a filter.

        Args:
            replay_filter: Filter and pagination parameters.

        Returns:
            Page of matching replays and the total match count.
        """
        ...

    async def count_by_status(
        self, run_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, dict[ReplayStatus, int]]:
        """Count replays by status for a set of experiment runs.

        Args:
            run_ids: Ids of the experiment runs.

        Returns:
            Replay counts by status, keyed by experiment run id.
        """
        ...

    async def references_agent_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored replay references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored replay references the version.
        """
        ...
