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
"""Agent version resolution for task-creating callers."""

import uuid

from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionWithoutRunSpec,
)


async def resolve_agent_version(
    agent_version_id: uuid.UUID, repository: AgentVersionRepository
) -> AgentVersion:
    """Load an agent version and require it to carry a run spec.

    Args:
        agent_version_id: Id of the agent version.
        repository: Agent version repository.

    Raises:
        AgentVersionNotFound: No agent version has this id.
        AgentVersionWithoutRunSpec: The version carries no run spec, so no
            worker could ever execute a task for it.

    Returns:
        Agent version carrying a run spec.
    """
    agent_version = await repository.get(agent_version_id)
    if agent_version.run_spec is None:
        raise AgentVersionWithoutRunSpec(agent_version_id)
    return agent_version
