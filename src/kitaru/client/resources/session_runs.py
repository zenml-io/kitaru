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
"""Session runs resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.session_run import SessionRunCreateRequest

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SessionRunsResource:
    """Session run API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: SessionRunCreateRequest, idempotency_key: str | None = None
    ) -> JobResponse:
        """Run an agent version once.

        Args:
            request: Session run create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 422 when the agent
                version carries no run spec.

        Returns:
            Created job.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/session-runs",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return JobResponse.model_validate(response.json())
