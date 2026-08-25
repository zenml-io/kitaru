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
"""Replays resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.replay import (
    ReplayCreateRequest,
    ReplayListParams,
    ReplayResponse,
    ToolLookupRequest,
    ToolLookupResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ReplaysResource:
    """Replay API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: ReplayCreateRequest, idempotency_key: str | None = None
    ) -> ReplayResponse:
        """Create a standalone replay of a recorded or imported session.

        Args:
            request: Replay create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 404 for a missing
                baseline session, agent version, or evaluator, and 422 for
                a missing agent version resolution or an invalid tool
                policy.

        Returns:
            Created replay.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/replays",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return ReplayResponse.model_validate(response.json())

    async def get(self, replay_id: uuid.UUID) -> ReplayResponse:
        """Get a replay by id.

        Args:
            replay_id: Id of the replay.

        Raises:
            APIError: The request failed, including 404 for a missing
                replay.

        Returns:
            Stored replay.
        """
        response = await self._client.request("GET", f"/api/v1/replays/{replay_id}")
        return ReplayResponse.model_validate(response.json())

    async def list(
        self, params: ReplayListParams | None = None
    ) -> Page[ReplayResponse]:
        """List replays.

        Args:
            params: Replay list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of replays.
        """
        params = params or ReplayListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/replays",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[ReplayResponse].model_validate(response.json())

    async def iter(
        self, params: ReplayListParams | None = None
    ) -> AsyncIterator[ReplayResponse]:
        """Iterate over all replays.

        Args:
            params: Replay list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every replay.
        """
        async for item in iterate_pages(params or ReplayListParams(), self.list):
            yield item

    async def tool_lookup(
        self, replay_id: uuid.UUID, request: ToolLookupRequest
    ) -> ToolLookupResponse:
        """Search recorded tool-call history for a cached result.

        Args:
            replay_id: Id of the replay.
            request: Tool lookup request.

        Raises:
            APIError: The request failed, including 404 for a missing
                replay and 422 for a tool not configured for history or an
                occurrence given for a non-baseline history scope.

        Returns:
            Matching recorded tool call, unset on a miss.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/replays/{replay_id}/tool-lookup",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ToolLookupResponse.model_validate(response.json())
