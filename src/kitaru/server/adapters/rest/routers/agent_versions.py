"""Agent version routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.agent_version import (
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_version_service,
)
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_to_response,
    agent_version_update_to_command,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)

router = APIRouter(route_class=CommitRoute)


@router.get("/{version_id}")
async def get_agent_version(
    version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Get an agent version; clients observe 200 or 404."""
    return agent_version_to_response(await service.get_version(version_id, actor=actor))


@router.patch("/{version_id}")
async def update_agent_version(
    version_id: uuid.UUID,
    body: AgentVersionUpdateRequest,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Update an agent version; clients observe 200, 404, or 409."""
    return agent_version_to_response(
        await service.update_version(
            version_id, agent_version_update_to_command(body), actor=actor
        )
    )


@router.delete("/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent_version(
    version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent version; clients observe 204, 404, or 409."""
    await service.delete_version(version_id, actor=actor)
