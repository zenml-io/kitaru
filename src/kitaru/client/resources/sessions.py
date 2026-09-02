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
"""Sessions resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionDetailResponse,
    SessionEvaluationsRequest,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session_node import (
    SessionNodeBatchRequest,
    SessionNodeListParams,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.client.resources.pagination import iterate_pages

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SessionsResource:
    """Session API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(
        self, request: SessionCreateRequest, idempotency_key: str | None = None
    ) -> SessionResponse:
        """Create a session.

        Args:
            request: Session create request.
            idempotency_key: Idempotency key overriding the transport's
                random default.

        Raises:
            APIError: The request failed, including 409 for a duplicate
                imported_from and external id pair.

        Returns:
            Created session.
        """
        response = await self._client.request(
            "POST",
            "/api/v1/sessions",
            json=request.model_dump(mode="json", exclude_unset=True),
            idempotency_key=idempotency_key,
        )
        return SessionResponse.model_validate(response.json())

    async def get(self, session_id: uuid.UUID) -> SessionDetailResponse:
        """Get a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing
                session.

        Returns:
            Stored session.
        """
        response = await self._client.request("GET", f"/api/v1/sessions/{session_id}")
        return SessionDetailResponse.model_validate(response.json())

    async def get_with_nodes(self, session_id: uuid.UUID) -> SessionWithNodesResponse:
        """Get a session together with every one of its nodes.

        The node list is not paginated, so one call carries a whole session.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing
                session.

        Returns:
            Session with every node, ordered by index.
        """
        response = await self._client.request(
            "GET", f"/api/v1/sessions/{session_id}/full"
        )
        return SessionWithNodesResponse.model_validate(response.json())

    async def ingest_nodes(
        self, session_id: uuid.UUID, batch: SessionNodeBatchRequest
    ) -> list[SessionNodeResponse]:
        """Ingest a batch of session nodes.

        An index already stored is replaced whole.

        Args:
            session_id: Id of the session to ingest into.
            batch: Session node batch request.

        Raises:
            APIError: The request failed, including 409 when the session
                does not currently accept node ingestion.

        Returns:
            Stored nodes in batch order.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/sessions/{session_id}/nodes",
            json=batch.model_dump(mode="json", exclude_unset=True),
        )
        return [SessionNodeResponse.model_validate(item) for item in response.json()]

    async def create_evaluations(
        self, session_id: uuid.UUID, request: SessionEvaluationsRequest
    ) -> list[EvaluationResponse]:
        """Create manual evaluations on a session.

        Args:
            session_id: Id of the session to create evaluations on.
            request: Session evaluations request.

        Raises:
            APIError: The request failed, including 404 for a missing
                session, 409 when an evaluation name already exists for the
                session, and 422 when the request names the same evaluation
                twice.

        Returns:
            Stored evaluations in request order.
        """
        response = await self._client.request(
            "POST",
            f"/api/v1/sessions/{session_id}/evaluations",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return [EvaluationResponse.model_validate(item) for item in response.json()]

    async def list(
        self,
        params: SessionListParams | None = None,
    ) -> Page[SessionResponse] | Page[SessionDetailResponse]:
        """List sessions.

        Args:
            params: Session list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of sessions, with payloads when include_payloads is set.
        """
        params = params or SessionListParams()
        response = await self._client.request(
            "GET",
            "/api/v1/sessions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        if params.include_payloads:
            return Page[SessionDetailResponse].model_validate(response.json())
        return Page[SessionResponse].model_validate(response.json())

    async def iter(
        self,
        params: SessionListParams | None = None,
    ) -> AsyncIterator[SessionResponse]:
        """Iterate over all sessions.

        Args:
            params: Session list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every session.
        """
        async for item in iterate_pages(params or SessionListParams(), self.list):
            yield item

    async def update(
        self, session_id: uuid.UUID, request: SessionUpdateRequest
    ) -> SessionResponse:
        """Update a session.

        Args:
            session_id: Id of the session.
            request: Session update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                session and 409 for an illegal status transition.

        Returns:
            Updated session.
        """
        response = await self._client.request(
            "PATCH",
            f"/api/v1/sessions/{session_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing
                session and 409 when the session is referenced by a
                cohort version, investigation, or replay.
        """
        await self._client.request("DELETE", f"/api/v1/sessions/{session_id}")

    async def list_nodes(
        self,
        session_id: uuid.UUID,
        params: SessionNodeListParams | None = None,
    ) -> Page[SessionNodeResponse]:
        """List the nodes of a session, ordered by index ascending.

        Args:
            session_id: Id of the session.
            params: Session node list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of session nodes, ordered by index.
        """
        params = params or SessionNodeListParams()
        response = await self._client.request(
            "GET",
            f"/api/v1/sessions/{session_id}/nodes",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[SessionNodeResponse].model_validate(response.json())

    async def iter_nodes(
        self,
        session_id: uuid.UUID,
        params: SessionNodeListParams | None = None,
    ) -> AsyncIterator[SessionNodeResponse]:
        """Iterate over every node of a session, ordered by index ascending.

        Args:
            session_id: Id of the session.
            params: Session node list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every node of the session.
        """
        async for item in iterate_pages(
            params or SessionNodeListParams(),
            lambda page_params: self.list_nodes(session_id, page_params),
        ):
            yield item
