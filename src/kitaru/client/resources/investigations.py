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
"""Investigations resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationResponse,
    InvestigationSessionResponse,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationUpdateRequest,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class InvestigationsResource:
    """Investigation API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: InvestigationCreateRequest
    ) -> InvestigationResponse:
        """Create an investigation with its sessions in one shot.

        Args:
            request: Investigation create request.

        Raises:
            APIError: The request failed, including 404 when the agent or a
                session does not exist.

        Returns:
            Created investigation.
        """
        response = await self._client.request(
            "POST",
            "/v1/investigations",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return InvestigationResponse.model_validate(response.json())

    async def get(self, investigation_id: uuid.UUID) -> InvestigationResponse:
        """Get an investigation by id.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation.

        Returns:
            Stored investigation.
        """
        response = await self._client.request(
            "GET", f"/v1/investigations/{investigation_id}"
        )
        return InvestigationResponse.model_validate(response.json())

    async def list(
        self,
        params: InvestigationListParams | None = None,
    ) -> Page[InvestigationResponse]:
        """List investigations.

        Args:
            params: Investigation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of investigations.
        """
        params = params or InvestigationListParams()
        response = await self._client.request(
            "GET",
            "/v1/investigations",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[InvestigationResponse].model_validate(response.json())

    async def iter(
        self,
        params: InvestigationListParams | None = None,
    ) -> AsyncIterator[InvestigationResponse]:
        """Iterate over all investigations.

        Args:
            params: Investigation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every investigation.
        """
        async for item in iterate_pages(params or InvestigationListParams(), self.list):
            yield item

    async def update(
        self, investigation_id: uuid.UUID, request: InvestigationUpdateRequest
    ) -> InvestigationResponse:
        """Update an investigation's name, description, and status.

        Args:
            investigation_id: Id of the investigation.
            request: Investigation update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation.

        Returns:
            Updated investigation.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/investigations/{investigation_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return InvestigationResponse.model_validate(response.json())

    async def delete(self, investigation_id: uuid.UUID) -> None:
        """Delete an investigation, cascading its sessions and answers.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation.
        """
        await self._client.request("DELETE", f"/v1/investigations/{investigation_id}")

    async def list_sessions(
        self,
        investigation_id: uuid.UUID,
        params: InvestigationSessionsListParams | None = None,
    ) -> Page[InvestigationSessionResponse]:
        """List the sessions of an investigation, ordered by position.

        Args:
            investigation_id: Id of the investigation.
            params: Investigation sessions list params.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation.

        Returns:
            Page of investigation sessions, ordered by position.
        """
        params = params or InvestigationSessionsListParams()
        response = await self._client.request(
            "GET",
            f"/v1/investigations/{investigation_id}/sessions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[InvestigationSessionResponse].model_validate(response.json())

    async def iter_sessions(
        self,
        investigation_id: uuid.UUID,
        params: InvestigationSessionsListParams | None = None,
    ) -> AsyncIterator[InvestigationSessionResponse]:
        """Iterate over all sessions of an investigation, ordered by position.

        Args:
            investigation_id: Id of the investigation.
            params: Investigation sessions list params.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation.

        Returns:
            Async iterator over every session of the investigation, ordered
            by position.
        """
        async for item in iterate_pages(
            params or InvestigationSessionsListParams(),
            lambda page_params: self.list_sessions(investigation_id, page_params),
        ):
            yield item

    async def update_session(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        request: InvestigationSessionUpdateRequest,
    ) -> InvestigationSessionResponse:
        """Set or clear an investigation session's verdict.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the session.
            request: Investigation session update request.

        Raises:
            APIError: The request failed, including 404 for a missing
                investigation session.

        Returns:
            Updated investigation session.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/investigations/{investigation_id}/sessions/{session_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return InvestigationSessionResponse.model_validate(response.json())
