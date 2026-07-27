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
"""Agent version resolution helpers."""

import uuid

from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotRunnable,
    NoRunnableAgentVersion,
)
from kitaru.server.domain.job import InvalidJob


async def resolve_agent_version(
    repository: AgentVersionRepository,
    agent_id: uuid.UUID,
    version_id: uuid.UUID | None,
) -> AgentVersion:
    """Resolve the agent version a job executes.

    Args:
        repository: Agent version repository.
        agent_id: Id of the agent the job runs for.
        version_id: Explicit version id, ``None`` resolves the latest
            runnable version.

    Raises:
        NoRunnableAgentVersion: The agent has no runnable version.
        AgentVersionNotFound: No agent version has the explicit id.
        InvalidJob: The explicit version belongs to another agent.
        AgentVersionNotRunnable: The explicit version has no run spec.

    Returns:
        Resolved agent version.
    """
    if version_id is None:
        version = await repository.get_latest_runnable(agent_id)
        if version is None:
            raise NoRunnableAgentVersion(agent_id)
        return version
    version = await repository.get(version_id)
    if version.agent_id != agent_id:
        raise InvalidJob(
            f"Agent version {version_id} does not belong to agent {agent_id}"
        )
    if version.run_spec is None:
        raise AgentVersionNotRunnable(version_id)
    return version
