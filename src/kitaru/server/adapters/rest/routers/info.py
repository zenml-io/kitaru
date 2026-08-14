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
"""Server info routes."""

import uuid
from importlib.metadata import version
from typing import Annotated

from fastapi import APIRouter, Depends

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.server.adapters.rest.dependencies import (
    get_app_settings,
    get_server_id_state,
    get_ui_version_state,
)
from kitaru.server.api.config import APISettings

router = APIRouter()

KITARU_VERSION = version("kitaru")


@router.get("")
async def get_info(
    settings: Annotated[APISettings, Depends(get_app_settings)],
    server_id: Annotated[uuid.UUID | None, Depends(get_server_id_state)],
    ui_version: Annotated[str | None, Depends(get_ui_version_state)],
) -> ServerInfoResponse:
    """Report how this server identifies itself and authenticates its callers.

    The endpoint is unauthenticated, because a client has to read it before it
    can know which credential to present.

    Args:
        settings: Service settings governing auth behavior.
        server_id: Persisted server id.
        ui_version: UI version served by this process.

    Returns:
        Server info.
    """
    control_plane_api_url = None
    if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
        control_plane_api_url = settings.CONTROL_PLANE_API_URL.rstrip("/")
    return ServerInfoResponse(
        id=server_id,
        version=KITARU_VERSION,
        ui_version=ui_version,
        auth_scheme=settings.AUTH_SCHEME,
        server_url=settings.SERVER_URL.rstrip("/") or None,
        dashboard_url=settings.DASHBOARD_URL.rstrip("/") or None,
        control_plane_api_url=control_plane_api_url,
    )
