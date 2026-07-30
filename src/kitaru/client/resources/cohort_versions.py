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
"""Cohort versions resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.cohort_version import (
    CohortVersionResponse,
    CohortVersionUpdateRequest,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class CohortVersionsResource:
    """Cohort version API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def get(self, cohort_version_id: uuid.UUID) -> CohortVersionResponse:
        """Get a cohort version by id.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            APIError: The request failed, including 404 for a missing cohort
                version.

        Returns:
            Stored cohort version.
        """
        response = await self._client.request(
            "GET", f"/v1/cohort-versions/{cohort_version_id}"
        )
        return CohortVersionResponse.model_validate(response.json())

    async def update(
        self, cohort_version_id: uuid.UUID, request: CohortVersionUpdateRequest
    ) -> CohortVersionResponse:
        """Update a cohort version.

        Args:
            cohort_version_id: Id of the cohort version.
            request: Cohort version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing cohort
                version.

        Returns:
            Updated cohort version.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/cohort-versions/{cohort_version_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortVersionResponse.model_validate(response.json())

    async def delete(self, cohort_version_id: uuid.UUID) -> None:
        """Delete a cohort version.

        Args:
            cohort_version_id: Id of the cohort version.

        Raises:
            APIError: The request failed, including 404 for a missing cohort
                version.
        """
        await self._client.request("DELETE", f"/v1/cohort-versions/{cohort_version_id}")
