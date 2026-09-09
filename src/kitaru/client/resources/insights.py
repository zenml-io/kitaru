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
"""Insights resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.insight import (
    InsightBatchCreateRequest,
    InsightInput,
    InsightListParams,
    InsightResponse,
    InsightUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class InsightsResource:
    """Insight API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self,
        agent_id: uuid.UUID,
        insights: list[InsightInput],
        idempotency_key: str | None = None,
    ) -> list[InsightResponse]:
        """Create a batch of insights for one agent in one shot.

        Args:
            agent_id: Agent the insights belong to.
            insights: Insights to create, in input order.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 when the agent does
                not exist.

        Returns:
            Created insights in input order.
        """
        request = InsightBatchCreateRequest(agent_id=agent_id, insights=insights)
        response = await self._client.request(
            "POST",
            "/api/v1/insights",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return [InsightResponse.model_validate(item) for item in response.json()]

    async def get(self, insight_id: uuid.UUID) -> InsightResponse:
        """Get an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            APIError: The request failed, including 404 for a missing
                insight.

        Returns:
            Stored insight.
        """
        response = await self._client.request("GET", f"/api/v1/insights/{insight_id}")
        return InsightResponse.model_validate(response.json())

    async def list(
        self,
        params: InsightListParams | None = None,
    ) -> Page[InsightResponse]:
        """List insights.

        Args:
            params: Insight list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of insights.
        """
        params = params or InsightListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/insights",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[InsightResponse].model_validate(response.json())

    async def iter(
        self,
        params: InsightListParams | None = None,
    ) -> AsyncIterator[InsightResponse]:
        """Iterate over all insights.

        Args:
            params: Insight list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every insight.
        """
        async for item in iterate_pages(params or InsightListParams(), self.list):
            yield item

    async def update(
        self, insight_id: uuid.UUID, request: InsightUpdateRequest
    ) -> InsightResponse:
        """Update an insight's title and description.

        Args:
            insight_id: Id of the insight.
            request: Insight update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                insight.

        Returns:
            Updated insight.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/insights/{insight_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return InsightResponse.model_validate(response.json())

    async def delete(self, insight_id: uuid.UUID) -> None:
        """Delete an insight.

        Args:
            insight_id: Id of the insight.

        Raises:
            APIError: The request failed, including 404 for a missing
                insight.
        """
        await self._client.request("DELETE", f"/api/v1/insights/{insight_id}")
