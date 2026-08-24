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
"""Service accounts resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.account import (
    AccountResponse,
    ServiceAccountCreateRequest,
    ServiceAccountUpdateRequest,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ServiceAccountsResource:
    """Service account API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ServiceAccountCreateRequest, idempotency_key: str | None = None
    ) -> AccountResponse:
        """Create a service account.

        Args:
            request: Service account create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created account.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/service-accounts",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return AccountResponse.model_validate(response.json())

    async def update(
        self, account_id: uuid.UUID, request: ServiceAccountUpdateRequest
    ) -> AccountResponse:
        """Partially update a service account.

        Args:
            account_id: Id of the account.
            request: Service account update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing service
                account.

        Returns:
            Updated account.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/service-accounts/{account_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return AccountResponse.model_validate(response.json())
