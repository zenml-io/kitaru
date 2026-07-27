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
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.replays import (
    ReplayCreateRequest,
    ReplayDiffResponse,
    ReplayResponse,
)

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

    async def create(self, request: ReplayCreateRequest) -> ReplayResponse:
        """Create a standalone replay of one session with an inline config.

        Args:
            request: Replay create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                session or agent version, 409 when no runnable agent
                version resolves, and 422 for an in-progress session or an
                invalid config.

        Returns:
            Created replay.
        """
        response = await self._client.request(
            "POST",
            "/v1/replays",
            json=request.model_dump(mode="json", exclude_unset=True),
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
        response = await self._client.request("GET", f"/v1/replays/{replay_id}")
        return ReplayResponse.model_validate(response.json())

    async def list(
        self,
        experiment_run_id: uuid.UUID | None = None,
        input_session_id: uuid.UUID | None = None,
        passed: bool | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ReplayResponse]:
        """List replays.

        Args:
            experiment_run_id: Filter on experiment run id.
            input_session_id: Filter on the session replayed.
            passed: Filter on scoring outcome.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of replays.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if experiment_run_id is not None:
            params["experiment_run_id"] = str(experiment_run_id)
        if input_session_id is not None:
            params["input_session_id"] = str(input_session_id)
        if passed is not None:
            params["passed"] = passed
        response = await self._client.request("GET", "/v1/replays", params=params)
        return Page[ReplayResponse].model_validate(response.json())

    async def get_diff(self, replay_id: uuid.UUID) -> ReplayDiffResponse:
        """Compute the full diff between a replay's sessions.

        Args:
            replay_id: Id of the replay.

        Raises:
            APIError: The request failed, including 404 for a missing
                replay and 409 when the replay has no result session yet.

        Returns:
            Computed replay diff.
        """
        response = await self._client.request("GET", f"/v1/replays/{replay_id}/diff")
        return ReplayDiffResponse.model_validate(response.json())
