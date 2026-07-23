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

from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    AgentVersionResponse,
)
from kitaru.api_models.v1.agents import (
    AgentCreateRequest,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.api_models.v1.base import Page
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_to_response,
    capabilities_to_domain,
    run_spec_to_domain,
)
from kitaru.server.adapters.rest.mapping.agents import agent_to_response
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)

router = APIRouter()


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
    name: str | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[AgentResponse]:
    """List agents.

    Clients observe HTTP 200 on success and 422 on invalid pagination
    parameters.

    Args:
        service: Agent service.
        actor: Caller context.
        name: Filter on agent name.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of agents.
    """
    agent_filter = AgentFilter(name=name, page=page, page_size=page_size)
    agents, total = await service.list_agents(agent_filter, actor=actor)
    return Page[AgentResponse](
        items=[agent_to_response(agent) for agent in agents],
        total=total,
        page=page,
        page_size=page_size,
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

    Clients observe HTTP 200 on success, 404 when no agent has this id,
    409 when the new name is already registered, and 422 on invalid input.

    Args:
        agent_id: Id of the agent.
        body: Agent update request.
        service: Agent service.
        actor: Caller context.

    Returns:
        Updated agent.
    """
    agent = await service.update_agent(
        agent_id, name=body.name, description=body.description, actor=actor
    )
    return agent_to_response(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: uuid.UUID,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent.

    Clients observe HTTP 204 on success, 404 when no agent has this id,
    and 409 while the agent still has versions.

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
    """Create an agent version.

    Clients observe HTTP 201 on success, 404 when no agent has this id or
    a referenced secret does not exist, 409 when the version is already
    registered for the agent, and 422 on invalid input.

    Args:
        agent_id: Id of the agent.
        body: Agent version create request.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Created agent version.
    """
    version = await service.create_version(
        agent_id,
        version=body.version,
        description=body.description,
        run_spec=run_spec_to_domain(body.run_spec),
        capabilities=capabilities_to_domain(body.capabilities),
        actor=actor,
    )
    return agent_version_to_response(version)


@router.get("/{agent_id}/versions")
async def list_agent_versions(
    agent_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=1000)] = 20,
) -> Page[AgentVersionResponse]:
    """List the versions of an agent.

    Clients observe HTTP 200 on success, 404 when no agent has this id,
    and 422 on invalid pagination parameters.

    Args:
        agent_id: Id of the agent.
        service: Agent version service.
        actor: Caller context.
        page: Page number.
        page_size: Page size.

    Returns:
        Page of agent versions.
    """
    version_filter = AgentVersionFilter(
        agent_id=agent_id, page=page, page_size=page_size
    )
    versions, total = await service.list_versions(version_filter, actor=actor)
    return Page[AgentVersionResponse](
        items=[agent_version_to_response(version) for version in versions],
        total=total,
        page=page,
        page_size=page_size,
    )
