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
"""Server info API models."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel


class AuthScheme(StrEnum):
    """Authentication scheme."""

    NONE = "none"
    LOCAL = "local"
    CONTROL_PLANE = "control_plane"


class ServerInfoResponse(ResponseModel):
    """Server info response."""

    id: uuid.UUID | None = Field(
        default=None,
        description="Server ID.",
    )
    version: str = Field(description="Kitaru version the server runs.")
    auth_scheme: AuthScheme = Field(description="Scheme used to authenticate requests.")
    server_url: str | None = Field(
        default=None,
        description="URL the server API is reachable at.",
    )
    dashboard_url: str | None = Field(
        default=None,
        description="URL the dashboard is reachable at.",
    )
    control_plane_api_url: str | None = Field(
        default=None,
        description="Control plane API the server accepts credentials from.",
    )
    features: list[str] = Field(
        default_factory=list,
        description="Backwards-compatible server capability identifiers.",
    )
