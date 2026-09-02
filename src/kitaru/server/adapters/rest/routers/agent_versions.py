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
"""Agent version routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status

from kitaru.api_models.v1.agent_version import (
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_agent_version_service,
)
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_to_response,
    agent_version_update_to_command,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)

router = APIRouter(route_class=KitaruAPIRoute)


@router.get("/{agent_version_id}", responses=error_responses(404))
async def get_agent_version(
    agent_version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Get an agent version by id.

    Clients observe HTTP 200 on success and 404 when no agent version has
    this id.

    Args:
        agent_version_id: Id of the agent version.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Stored agent version.
    """
    agent_version = await service.get_version(agent_version_id, actor=actor)
    return agent_version_to_response(agent_version)


@router.patch("/{agent_version_id}", responses=error_responses(404))
async def update_agent_version(
    agent_version_id: uuid.UUID,
    body: AgentVersionUpdateRequest,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Update an agent version.

    Clients observe HTTP 200 on success, 404 when no agent version has this
    id, and 422 on invalid input.

    Args:
        agent_version_id: Id of the agent version.
        body: Agent version update request.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Updated agent version.
    """
    command = agent_version_update_to_command(body)
    agent_version = await service.update_version(agent_version_id, command, actor=actor)
    return agent_version_to_response(agent_version)


@router.delete(
    "/{agent_version_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=error_responses(404, 409),
)
async def delete_agent_version(
    agent_version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent version.

    Clients observe HTTP 204 on success, 404 when no agent version has
    this id, and 409 when an experiment run references it.

    Args:
        agent_version_id: Id of the agent version.
        service: Agent version service.
        actor: Caller context.
    """
    await service.delete_version(agent_version_id, actor=actor)
