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
"""API key API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import OwnedResponseModel, RequestModel
from kitaru.api_models.v1.filter import FilterableListParams


class ApiKeyCreateRequest(RequestModel):
    """API key create request."""

    name: str = Field(description="API key name.")


class ApiKeyUpdateRequest(RequestModel):
    """API key update request."""

    active: bool = Field(description="New active state.")


class ApiKeyListParams(FilterableListParams):
    """API key list params."""


class ApiKeyResponse(OwnedResponseModel):
    """API key response."""

    id: uuid.UUID = Field(description="API key id.")
    name: str = Field(description="API key name.")
    active: bool = Field(description="Whether the key can authenticate.")
    last_used: datetime | None = Field(
        description="Time of the last use for authentication."
    )


class ApiKeyIssuedResponse(ApiKeyResponse):
    """API key response carrying newly issued key material."""

    key: str = Field(description="Plaintext key, shown once.")
