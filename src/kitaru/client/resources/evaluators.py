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
"""Evaluators resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionResponse,
    EvaluatorVersionUpdateRequest,
)

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class EvaluatorsResource:
    """Evaluator API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: EvaluatorCreateRequest) -> EvaluatorResponse:
        """Create an evaluator.

        Args:
            request: Evaluator create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created evaluator.
        """
        response = await self._client.request(
            "POST",
            "/v1/evaluators",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorResponse.model_validate(response.json())

    async def get(self, evaluator_id: uuid.UUID) -> EvaluatorResponse:
        """Get an evaluator by id.

        Args:
            evaluator_id: Id of the evaluator.

        Raises:
            APIError: The request failed, including 404 for a missing
                evaluator.

        Returns:
            Stored evaluator.
        """
        response = await self._client.request("GET", f"/v1/evaluators/{evaluator_id}")
        return EvaluatorResponse.model_validate(response.json())

    async def list(
        self, params: EvaluatorListParams | None = None
    ) -> Page[EvaluatorResponse]:
        """List evaluators.

        Args:
            params: Evaluator list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of evaluators.
        """
        params = params or EvaluatorListParams()
        response = await self._client.request(
            "GET",
            "/v1/evaluators",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluatorResponse].model_validate(response.json())

    async def iter(
        self, params: EvaluatorListParams | None = None
    ) -> AsyncIterator[EvaluatorResponse]:
        """Iterate over all evaluators.

        Args:
            params: Evaluator list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every evaluator.
        """
        params = params or EvaluatorListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def update(
        self, evaluator_id: uuid.UUID, request: EvaluatorUpdateRequest
    ) -> EvaluatorResponse:
        """Update an evaluator.

        Args:
            evaluator_id: Id of the evaluator.
            request: Evaluator update request, unset fields stay unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                evaluator.

        Returns:
            Updated evaluator.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/evaluators/{evaluator_id}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorResponse.model_validate(response.json())

    async def delete(self, evaluator_id: uuid.UUID) -> None:
        """Delete an evaluator, cascading its versions.

        Args:
            evaluator_id: Id of the evaluator.

        Raises:
            APIError: The request failed, including 404 for a missing
                evaluator.
        """
        await self._client.request("DELETE", f"/v1/evaluators/{evaluator_id}")

    async def create_version(
        self, evaluator_id: uuid.UUID, request: EvaluatorVersionCreateRequest
    ) -> EvaluatorVersionResponse:
        """Create an evaluator version.

        Args:
            evaluator_id: Id of the evaluator.
            request: Evaluator version create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                evaluator or blob.

        Returns:
            Created evaluator version.
        """
        response = await self._client.request(
            "POST",
            f"/v1/evaluators/{evaluator_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorVersionResponse.model_validate(response.json())

    async def list_versions(
        self, evaluator_id: uuid.UUID, params: ListParams | None = None
    ) -> Page[EvaluatorVersionResponse]:
        """List an evaluator's versions.

        Args:
            evaluator_id: Id of the evaluator.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of evaluator versions.
        """
        params = params or ListParams()
        response = await self._client.request(
            "GET",
            f"/v1/evaluators/{evaluator_id}/versions",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluatorVersionResponse].model_validate(response.json())

    async def iter_versions(
        self, evaluator_id: uuid.UUID, params: ListParams | None = None
    ) -> AsyncIterator[EvaluatorVersionResponse]:
        """Iterate over all of an evaluator's versions.

        Args:
            evaluator_id: Id of the evaluator.
            params: List params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every evaluator version.
        """
        params = params or ListParams()
        while True:
            page = await self.list_versions(evaluator_id, params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})

    async def get_version(
        self, evaluator_id: uuid.UUID, version: int
    ) -> EvaluatorVersionResponse:
        """Get an evaluator version by version number.

        Args:
            evaluator_id: Id of the evaluator.
            version: Version number.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Stored evaluator version.
        """
        response = await self._client.request(
            "GET", f"/v1/evaluators/{evaluator_id}/versions/{version}"
        )
        return EvaluatorVersionResponse.model_validate(response.json())

    async def update_version(
        self,
        evaluator_id: uuid.UUID,
        version: int,
        request: EvaluatorVersionUpdateRequest,
    ) -> EvaluatorVersionResponse:
        """Update an evaluator version's display version.

        Args:
            evaluator_id: Id of the evaluator.
            version: Version number.
            request: Evaluator version update request, unset fields stay
                unchanged.

        Raises:
            APIError: The request failed, including 404 for a missing
                version.

        Returns:
            Updated evaluator version.
        """
        response = await self._client.request(
            "PATCH",
            f"/v1/evaluators/{evaluator_id}/versions/{version}",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return EvaluatorVersionResponse.model_validate(response.json())
