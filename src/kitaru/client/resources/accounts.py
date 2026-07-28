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
"""Accounts resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.account import (
    AccountCreateRequest,
    AccountListParams,
    AccountResponse,
    AccountUpdateRequest,
)
from kitaru.api_models.v1.base import Page

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class AccountsResource:
    """Account API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: AccountCreateRequest) -> AccountResponse:
        """Create an account.

        Args:
            request: Account create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created account.
        """
        response = await self._client.request(
            "POST",
            "/v1/accounts",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())

    async def get(self, account_id: uuid.UUID) -> AccountResponse:
        """Get an account by id.

        Args:
            account_id: Id of the account.

        Raises:
            APIError: The request failed, including 404 for a missing account.

        Returns:
            Stored account.
        """
        response = await self._client.request("GET", f"/v1/accounts/{account_id}")
        return AccountResponse.model_validate(response.json())

    async def list(
        self,
        params: AccountListParams | None = None,
    ) -> Page[AccountResponse]:
        """List accounts.

        Args:
            params: Account list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of accounts.
        """
        params = params or AccountListParams()
        response = await self._client.request(
            "GET",
            "/v1/accounts",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[AccountResponse].model_validate(response.json())

    async def update(
        self, account_id: uuid.UUID, request: AccountUpdateRequest
    ) -> AccountResponse:
        """Partially update an account.

        Args:
            account_id: Id of the account.
            request: Account update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing account.

        Returns:
            Updated account.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/accounts/{account_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())
