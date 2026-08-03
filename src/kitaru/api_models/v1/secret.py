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
"""Secret API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    OwnedResponseModel,
    PlainSerializedSecretStr,
    RequestModel,
)
from kitaru.api_models.v1.filter import FilterableListParams


class SecretCreateRequest(RequestModel):
    """Secret create request."""

    name: str = Field(description="Secret name.")
    type: str | None = Field(default=None, description="Secret type.")
    values: dict[str, PlainSerializedSecretStr] = Field(description="Secret values.")


class SecretUpdateRequest(RequestModel):
    """Secret update request."""

    type: str | None = Field(default=None, description="New secret type.")
    values: dict[str, PlainSerializedSecretStr] | None = Field(
        default=None, description="New secret values."
    )


class SecretListParams(FilterableListParams):
    """Secret list params."""


class SecretResponse(OwnedResponseModel):
    """Secret response."""

    id: uuid.UUID = Field(description="Secret id.")
    name: str = Field(description="Secret name.")
    type: str | None = Field(description="Secret type.")


class SecretWithValuesResponse(SecretResponse):
    """Secret response carrying the secret values."""

    values: dict[str, PlainSerializedSecretStr] = Field(description="Secret values.")
