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
"""Account API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, TimestampedResponseModel


class AccountCreateRequest(RequestModel):
    """Account create request."""

    name: str = Field(description="Account name.")
    email: str | None = Field(default=None, description="Contact email.")
    password: str | None = Field(default=None, description="Login password.")


class AccountUpdateRequest(RequestModel):
    """Account update request."""

    active: bool | None = Field(default=None, description="New active state.")
    password: str | None = Field(default=None, description="New login password.")


class AccountResponse(TimestampedResponseModel):
    """Account response."""

    id: uuid.UUID = Field(description="Account id.")
    name: str = Field(description="Account name.")
    email: str | None = Field(description="Contact email.")
    is_service_account: bool = Field(description="Whether this is a service account.")
    active: bool = Field(description="Whether the account can authenticate.")
