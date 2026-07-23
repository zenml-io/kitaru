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
"""Session nodes resource."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.session_nodes import (
    SessionNodeBatchRequest,
    SessionNodeResponse,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class SessionNodesResource:
    """Session node API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def upsert(
        self, session_id: uuid.UUID, request: SessionNodeBatchRequest
    ) -> list[SessionNodeResponse]:
        """Upsert a batch of session nodes.

        Nodes upsert on their client-generated id, so retries are
        idempotent. Nodes must arrive parent-before-child within and across
        batches.

        Args:
            session_id: Id of the session.
            request: Session node batch request.

        Raises:
            APIError: The request failed, including 404 for a missing
                session, 409 when the session does not accept node ingest,
                and 422 for an unknown parent.

        Returns:
            Stored nodes in batch order.
        """
        response = await self._client.request(
            "POST",
            f"/v1/sessions/{session_id}/nodes",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return [SessionNodeResponse.model_validate(item) for item in response.json()]

    async def list(
        self, session_id: uuid.UUID, include_payloads: bool = False
    ) -> list[SessionNodeResponse]:
        """List the nodes of a session ordered by sequence.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to include inputs, outputs, and
                attributes.

        Raises:
            APIError: The request failed, including 404 for a missing
                session.

        Returns:
            Nodes ordered by sequence.
        """
        response = await self._client.request(
            "GET",
            f"/v1/sessions/{session_id}/nodes",
            params={"include_payloads": include_payloads},
        )
        return [SessionNodeResponse.model_validate(item) for item in response.json()]
