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
"""Agent routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from kitaru.api_models.v1.agent import (
    AgentCreateRequest,
    AgentListParams,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.api_models.v1.agent_version import (
    AgentVersionCreateRequest,
    AgentVersionListParams,
    AgentVersionResponse,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_list_params_to_filter,
    agent_version_to_response,
    capabilities_to_domain,
    run_spec_to_domain,
)
from kitaru.server.adapters.rest.mapping.agents import (
    agent_list_params_to_filter,
    agent_to_response,
    agent_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_agent(
    body: AgentCreateRequest,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentResponse:
    """Create an agent.

    Clients observe HTTP 201 on success, 409 when the name is already
    registered, and 422 on invalid input.

    Args:
        body: Agent create request.
        service: Agent service.
        actor: Caller context.

    Returns:
        Created agent.
    """
    agent = await service.create_agent(
        name=body.name, description=body.description, actor=actor
    )
    return agent_to_response(agent)


@router.get("")
async def list_agents(
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AgentListParams, Query()],
) -> Page[AgentResponse]:
    """List agents.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Agent service.
        actor: Caller context.
        params: Agent list params.

    Returns:
        Page of agents.
    """
    agent_filter = agent_list_params_to_filter(params)
    agents, next_cursor = await service.list_agents(agent_filter, actor=actor)
    return Page[AgentResponse](
        items=[agent_to_response(agent) for agent in agents],
        next_cursor=next_cursor,
    )


@router.get("/{agent_id}")
async def get_agent(
    agent_id: uuid.UUID,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentResponse:
    """Get an agent by id.

    Clients observe HTTP 200 on success and 404 when no agent has this id.

    Args:
        agent_id: Id of the agent.
        service: Agent service.
        actor: Caller context.

    Returns:
        Stored agent.
    """
    agent = await service.get_agent(agent_id, actor=actor)
    return agent_to_response(agent)


@router.patch("/{agent_id}")
async def update_agent(
    agent_id: uuid.UUID,
    body: AgentUpdateRequest,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentResponse:
    """Update an agent.

    Clients observe HTTP 200 on success, 404 when no agent has this id, and
    422 on invalid input, including an attempt to clear the name.

    Args:
        agent_id: Id of the agent.
        body: Agent update request.
        service: Agent service.
        actor: Caller context.

    Returns:
        Updated agent.
    """
    command = agent_update_to_command(body)
    agent = await service.update_agent(agent_id, command, actor=actor)
    return agent_to_response(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: uuid.UUID,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent.

    Clients observe HTTP 204 on success, 404 when no agent has this id, and
    409 when the agent has versions.

    Args:
        agent_id: Id of the agent.
        service: Agent service.
        actor: Caller context.
    """
    await service.delete_agent(agent_id, actor=actor)


@router.post("/{agent_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_agent_version(
    agent_id: uuid.UUID,
    body: AgentVersionCreateRequest,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Create a new version of an agent.

    Clients observe HTTP 201 on success, 404 when no agent has this id, and
    422 on invalid input.

    Args:
        agent_id: Id of the agent.
        body: Agent version create request.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Created agent version.
    """
    agent_version = await service.create_version(
        agent_id=agent_id,
        display_version=body.display_version,
        description=body.description,
        run_spec=run_spec_to_domain(body.run_spec)
        if body.run_spec is not None
        else None,
        capabilities=(
            capabilities_to_domain(body.capabilities)
            if body.capabilities is not None
            else None
        ),
        actor=actor,
    )
    return agent_version_to_response(agent_version)


@router.get("/{agent_id}/versions")
async def list_agent_versions(
    agent_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AgentVersionListParams, Query()],
) -> Page[AgentVersionResponse]:
    """List the versions of an agent.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        agent_id: Id of the agent.
        service: Agent version service.
        actor: Caller context.
        params: Agent version list params.

    Returns:
        Page of agent versions.
    """
    agent_version_filter = agent_version_list_params_to_filter(agent_id, params)
    versions, next_cursor = await service.list_versions(
        agent_version_filter, actor=actor
    )
    return Page[AgentVersionResponse](
        items=[agent_version_to_response(version) for version in versions],
        next_cursor=next_cursor,
    )
