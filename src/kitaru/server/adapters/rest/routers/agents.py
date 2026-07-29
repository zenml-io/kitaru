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
    AgentVersionResponse,
)
from kitaru.api_models.v1.base import ListParams, Page
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_service,
    get_agent_version_service,
)
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_create_values,
    agent_version_list_filter,
    agent_version_to_response,
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
    """Create an agent; clients observe 201, 409, or 422."""
    return agent_to_response(
        await service.create_agent(body.name, body.description, actor=actor)
    )


@router.get("")
async def list_agents(
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[AgentListParams, Query()],
) -> Page[AgentResponse]:
    """List agents; clients observe 200 or 422."""
    items, cursor = await service.list_agents(
        agent_list_params_to_filter(params), actor=actor
    )
    return Page[AgentResponse](
        items=[agent_to_response(item) for item in items], next_cursor=cursor
    )


@router.get("/{agent_id}")
async def get_agent(
    agent_id: uuid.UUID,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentResponse:
    """Get an agent; clients observe 200 or 404."""
    return agent_to_response(await service.get_agent(agent_id, actor=actor))


@router.patch("/{agent_id}")
async def update_agent(
    agent_id: uuid.UUID,
    body: AgentUpdateRequest,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentResponse:
    """Update an agent; clients observe 200, 404, 409, or 422."""
    return agent_to_response(
        await service.update_agent(agent_id, agent_update_to_command(body), actor=actor)
    )


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: uuid.UUID,
    service: Annotated[AgentService, Depends(get_agent_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent; clients observe 204, 404, or 409."""
    await service.delete_agent(agent_id, actor=actor)


@router.post("/{agent_id}/versions", status_code=status.HTTP_201_CREATED)
async def create_agent_version(
    agent_id: uuid.UUID,
    body: AgentVersionCreateRequest,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Create an agent version; clients observe 201, 404, or 422."""
    display_version, description, run_spec, capabilities = agent_version_create_values(
        body
    )
    version = await service.create_version(
        agent_id,
        display_version=display_version,
        description=description,
        run_spec=run_spec,
        capabilities=capabilities,
        actor=actor,
    )
    return agent_version_to_response(version)


@router.get("/{agent_id}/versions")
async def list_agent_versions(
    agent_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    params: Annotated[ListParams, Query()],
) -> Page[AgentVersionResponse]:
    """List agent versions; clients observe 200, 404, or 422."""
    items, cursor = await service.list_versions(
        agent_version_list_filter(agent_id, params), actor=actor
    )
    return Page[AgentVersionResponse](
        items=[agent_version_to_response(item) for item in items],
        next_cursor=cursor,
    )
