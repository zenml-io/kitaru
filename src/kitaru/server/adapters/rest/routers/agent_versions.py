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

from kitaru.api_models.v1.agent_versions import (
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
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)

router = APIRouter()


@router.get("/{version_id}")
async def get_agent_version(
    version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Get an agent version by id.

    Clients observe HTTP 200 on success and 404 when no agent version has
    this id.

    Args:
        version_id: Id of the agent version.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Stored agent version.
    """
    version = await service.get_version(version_id, actor=actor)
    return agent_version_to_response(version)


@router.patch("/{version_id}")
async def update_agent_version(
    version_id: uuid.UUID,
    body: AgentVersionUpdateRequest,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> AgentVersionResponse:
    """Update an agent version.

    Clients observe HTTP 200 on success, 404 when no agent version has
    this id or a referenced secret does not exist, 409 when a run spec or
    capability change hits a version referenced by a job, and 422 on
    invalid input.

    Args:
        version_id: Id of the agent version.
        body: Agent version update request.
        service: Agent version service.
        actor: Caller context.

    Returns:
        Updated agent version.
    """
    version = await service.update_version(
        version_id, agent_version_update_to_command(body), actor=actor
    )
    return agent_version_to_response(version)


@router.delete("/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent_version(
    version_id: uuid.UUID,
    service: Annotated[AgentVersionService, Depends(get_agent_version_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete an agent version.

    Clients observe HTTP 204 on success, 404 when no agent version has
    this id, and 409 while the version is referenced by a session, an
    experiment run, or a job.

    Args:
        version_id: Id of the agent version.
        service: Agent version service.
        actor: Caller context.
    """
    await service.delete_version(version_id, actor=actor)
