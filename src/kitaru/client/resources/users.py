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
"""Users resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.account import (
    AccountResponse,
    UserActivateRequest,
    UserActivationTokenResponse,
    UserCreateRequest,
    UserUpdateRequest,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class UsersResource:
    """User API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: UserCreateRequest) -> AccountResponse:
        """Create a user.

        Args:
            request: User create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created account.
        """
        response = await self._client.request(
            "POST",
            "/v1/users",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())

    async def update(
        self, account_id: uuid.UUID, request: UserUpdateRequest
    ) -> AccountResponse:
        """Partially update a user.

        Args:
            account_id: Id of the account.
            request: User update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing user.

        Returns:
            Updated account.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/users/{account_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())

    async def activate(
        self, account_id: uuid.UUID, request: UserActivateRequest
    ) -> AccountResponse:
        """Activate a user with its activation token and a new password.

        Args:
            account_id: Id of the account.
            request: User activate request.

        Raises:
            APIError: The request failed, including 403 for a token mismatch.

        Returns:
            Activated account.
        """
        response = await self._client.request(
            "POST",
            f"/v1/users/{account_id}/activate",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())

    async def deactivate(self, account_id: uuid.UUID) -> UserActivationTokenResponse:
        """Deactivate a user and read back its activation token.

        Args:
            account_id: Id of the account.

        Raises:
            APIError: The request failed, including 403 for the calling
                account and 404 for a missing user.

        Returns:
            Deactivated account carrying its activation token.
        """
        response = await self._client.request(
            "POST", f"/v1/users/{account_id}/deactivate"
        )
        return UserActivationTokenResponse.model_validate(response.json())
