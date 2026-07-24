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
"""Agent DTO conversions."""

from kitaru.api_models.v1.agents import AgentResponse, AgentUpdateRequest
from kitaru.server.adapters.rest.mapping.partial import set_fields
from kitaru.server.application.models.agents import AgentUpdate
from kitaru.server.domain.agent import Agent


def agent_update_to_command(body: AgentUpdateRequest) -> AgentUpdate:
    """Convert an agent update request to its command.

    Only fields set on the request are set on the command, so an absent
    field stays distinguishable from an explicit null.

    Args:
        body: Agent update request.

    Returns:
        Agent update command.
    """
    return AgentUpdate(**set_fields(body))


def agent_to_response(agent: Agent) -> AgentResponse:
    """Convert an agent entity to its response DTO.

    Args:
        agent: Stored agent.

    Returns:
        Agent response.
    """
    assert agent.created is not None
    assert agent.updated is not None
    return AgentResponse(
        id=agent.id,
        owner_id=agent.owner_id,
        name=agent.name,
        description=agent.description,
        created=agent.created,
        updated=agent.updated,
    )
