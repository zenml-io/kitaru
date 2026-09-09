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
"""Connection API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    OwnedResponseModel,
    PlainSerializedSecretStr,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams


class ConnectionCreateRequest(RequestModel):
    """Connection create request."""

    name: str = Field(description="Connection name.")
    provider: str = Field(description="Provider the connection addresses.")
    env: dict[str, str] = Field(
        default_factory=dict, description="Non-secret environment values."
    )
    secrets: dict[str, PlainSerializedSecretStr] = Field(
        default_factory=dict, description="Sensitive environment values."
    )
    default: bool = Field(
        default=False, description="Whether this is the provider's default connection."
    )


class ConnectionUpdateRequest(RequestModel):
    """Connection update request."""

    env: dict[str, str] | None = Field(
        default=None, description="New non-secret environment values."
    )
    secrets: dict[str, PlainSerializedSecretStr] | None = Field(
        default=None, description="New sensitive environment values."
    )
    default: bool | None = Field(
        default=None,
        description="Whether this is the provider's default connection.",
    )


class ConnectionListParams(FilterableListParams):
    """Connection list params."""


class ConnectionResponse(OwnedResponseModel):
    """Connection response."""

    id: uuid.UUID = Field(description="Connection id.")
    name: str = Field(description="Connection name.")
    provider: str = Field(description="Provider the connection addresses.")
    env: dict[str, str] = Field(description="Non-secret environment values.")
    secret_id: uuid.UUID = Field(description="Secret holding the sensitive values.")
    secret_keys: list[str] = Field(description="Key names of the sensitive values.")
    default: bool = Field(
        description="Whether this is the provider's default connection."
    )
