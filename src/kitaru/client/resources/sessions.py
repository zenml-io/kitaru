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
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionProvider,
    SessionResponse,
    SessionScoresRequest,
    SessionStatus,
    SessionUpdateRequest,
)

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

    async def create(self, request: SessionCreateRequest) -> SessionResponse:
        """Create a session.

        Args:
            request: Session create request.

        Raises:
            APIError: The request failed, including 404 for a missing agent
                or agent version and 409 for a duplicate provider and
                external id pair.

        Returns:
            Created session.
        """
        response = await self._client.request(
            "POST",
            "/v1/sessions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def list(
        self,
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        origin: SessionOrigin | None = None,
        status: SessionStatus | None = None,
        provider: SessionProvider | None = None,
        external_id: str | None = None,
        name: str | None = None,
        tag: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        ended_after: datetime | None = None,
        ended_before: datetime | None = None,
        has_score: bool | None = None,
        min_cost: Decimal | None = None,
        max_cost: Decimal | None = None,
        min_total_tokens: int | None = None,
        max_total_tokens: int | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[SessionResponse]:
        """List sessions.

        Args:
            agent_id: Filter on agent id.
            agent_version_id: Filter on agent version id.
            origin: Filter on session origin.
            status: Filter on session status.
            provider: Filter on session provider.
            external_id: Filter on external id.
            name: Filter on session name.
            tag: Filter on attached tag name.
            started_after: Earliest start time.
            started_before: Latest start time.
            ended_after: Earliest end time.
            ended_before: Latest end time.
            has_score: Filter on the presence of scores.
            min_cost: Lowest cost.
            max_cost: Highest cost.
            min_total_tokens: Lowest total token count.
            max_total_tokens: Highest total token count.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                agent.

        Returns:
            Page of sessions.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        optional: dict[str, Any] = {
            "agent_id": str(agent_id) if agent_id is not None else None,
            "agent_version_id": str(agent_version_id)
            if agent_version_id is not None
            else None,
            "origin": origin.value if origin is not None else None,
            "status": status.value if status is not None else None,
            "provider": provider.value if provider is not None else None,
            "external_id": external_id,
            "name": name,
            "tag": tag,
            "started_after": started_after.isoformat()
            if started_after is not None
            else None,
            "started_before": started_before.isoformat()
            if started_before is not None
            else None,
            "ended_after": ended_after.isoformat() if ended_after is not None else None,
            "ended_before": ended_before.isoformat()
            if ended_before is not None
            else None,
            "has_score": has_score,
            "min_cost": str(min_cost) if min_cost is not None else None,
            "max_cost": str(max_cost) if max_cost is not None else None,
            "min_total_tokens": min_total_tokens,
            "max_total_tokens": max_total_tokens,
        }
        params.update(
            {name: value for name, value in optional.items() if value is not None}
        )
        response = await self._client.request("GET", "/v1/sessions", params=params)
        return Page[SessionResponse].model_validate(response.json())

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Get a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing
                session.

        Returns:
            Stored session.
        """
        response = await self._client.request("GET", f"/v1/sessions/{session_id}")
        return SessionResponse.model_validate(response.json())

    async def update(
        self, session_id: uuid.UUID, request: SessionUpdateRequest
    ) -> SessionResponse:
        """Update a session, finishing it when a status is set.

        Args:
            session_id: Id of the session.
            request: Session update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                session and 409 when a status is set but the session is not
                in progress.

        Returns:
            Updated session.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/sessions/{session_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def merge_scores(
        self, session_id: uuid.UUID, request: SessionScoresRequest
    ) -> SessionResponse:
        """Merge values into a session's scores map, latest wins per name.

        Args:
            session_id: Id of the session.
            request: Session scores request.

        Raises:
            APIError: The request failed, including 404 for a missing
                session.

        Returns:
            Updated session.
        """
        response = await self._client.request(
            "POST",
            f"/v1/sessions/{session_id}/scores",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return SessionResponse.model_validate(response.json())

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session, including its nodes and tag links.

        Args:
            session_id: Id of the session.

        Raises:
            APIError: The request failed, including 404 for a missing
                session and 409 while the session is a member of a cohort
                or referenced by a job.
        """
        await self._client.request("DELETE", f"/v1/sessions/{session_id}")
