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
"""Cohorts resource."""

import uuid
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.cohorts import (
    CohortCreateRequest,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.sessions import SessionResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class CohortsResource:
    """Cohort API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: CohortCreateRequest) -> CohortResponse:
        """Create a cohort from explicit session ids or a session filter.

        Args:
            request: Cohort create request.

        Raises:
            APIError: The request failed, including 404 for a missing agent
                or session, 409 for a duplicate name, and 422 for an empty
                or invalid membership.

        Returns:
            Created cohort.
        """
        response = await self._client.request(
            "POST",
            "/v1/cohorts",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortResponse.model_validate(response.json())

    async def list(
        self,
        name: str | None = None,
        tag: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[CohortResponse]:
        """List cohorts.

        Args:
            name: Filter on cohort name.
            tag: Filter on attached tag name.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of cohorts.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name is not None:
            params["name"] = name
        if tag is not None:
            params["tag"] = tag
        response = await self._client.request("GET", "/v1/cohorts", params=params)
        return Page[CohortResponse].model_validate(response.json())

    async def get(self, cohort_id: uuid.UUID) -> CohortResponse:
        """Get a cohort by id.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.

        Returns:
            Stored cohort.
        """
        response = await self._client.request("GET", f"/v1/cohorts/{cohort_id}")
        return CohortResponse.model_validate(response.json())

    async def list_sessions(
        self,
        cohort_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[SessionResponse]:
        """List the member sessions of a cohort ordered by position.

        Args:
            cohort_id: Id of the cohort.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.

        Returns:
            Page of member sessions.
        """
        response = await self._client.request(
            "GET",
            f"/v1/cohorts/{cohort_id}/sessions",
            params={"page": page, "page_size": page_size},
        )
        return Page[SessionResponse].model_validate(response.json())

    async def update(
        self, cohort_id: uuid.UUID, request: CohortUpdateRequest
    ) -> CohortResponse:
        """Update a cohort's name and description.

        Args:
            cohort_id: Id of the cohort.
            request: Cohort update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort and 409 for a duplicate name.

        Returns:
            Updated cohort.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/cohorts/{cohort_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortResponse.model_validate(response.json())

    async def delete(self, cohort_id: uuid.UUID) -> None:
        """Delete a cohort, including its membership and tag links.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.
        """
        await self._client.request("DELETE", f"/v1/cohorts/{cohort_id}")
