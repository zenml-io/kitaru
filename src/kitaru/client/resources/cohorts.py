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
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortListParams,
    CohortResponse,
    CohortSessionsListParams,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.session import SessionResponse

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
        """Create a cohort.

        Args:
            request: Cohort create request.

        Raises:
            APIError: The request failed, including 404 when the agent does
                not exist, 409 for a duplicate name, and 422 for an invalid
                or mismatched member list.

        Returns:
            Created cohort.
        """
        response = await self._client.request(
            "POST",
            "/v1/cohorts",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return CohortResponse.model_validate(response.json())

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

    async def list(
        self,
        params: CohortListParams | None = None,
    ) -> Page[CohortResponse]:
        """List cohorts.

        Args:
            params: Cohort list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of cohorts.
        """
        params = params or CohortListParams()
        response = await self._client.request(
            "GET",
            "/v1/cohorts",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[CohortResponse].model_validate(response.json())

    async def iter(
        self,
        params: CohortListParams | None = None,
    ) -> AsyncIterator[CohortResponse]:
        """Iterate over all cohorts.

        Args:
            params: Cohort list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every cohort.
        """
        params = params or CohortListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def list_sessions(
        self,
        cohort_id: uuid.UUID,
        params: CohortSessionsListParams | None = None,
    ) -> Page[SessionResponse]:
        """List a cohort's member sessions in cohort order.

        Args:
            cohort_id: Id of the cohort.
            params: Cohort sessions list params.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.

        Returns:
            Page of member sessions, in cohort order.
        """
        params = params or CohortSessionsListParams()
        response = await self._client.request(
            "GET",
            f"/v1/cohorts/{cohort_id}/sessions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SessionResponse].model_validate(response.json())

    async def iter_sessions(
        self,
        cohort_id: uuid.UUID,
        params: CohortSessionsListParams | None = None,
    ) -> AsyncIterator[SessionResponse]:
        """Iterate over every member session of a cohort, in cohort order.

        Args:
            cohort_id: Id of the cohort.
            params: Cohort sessions list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every member session of the cohort.
        """
        params = params or CohortSessionsListParams()
        while True:
            page = await self.list_sessions(cohort_id, params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def update(
        self, cohort_id: uuid.UUID, request: CohortUpdateRequest
    ) -> CohortResponse:
        """Update a cohort's name and description.

        Args:
            cohort_id: Id of the cohort.
            request: Cohort update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.

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
        """Delete a cohort.

        Args:
            cohort_id: Id of the cohort.

        Raises:
            APIError: The request failed, including 404 for a missing
                cohort.
        """
        await self._client.request("DELETE", f"/v1/cohorts/{cohort_id}")
