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
"""Evaluations resource."""

import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.evaluation import EvaluationListParams, EvaluationResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class EvaluationsResource:
    """Evaluation API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationResponse:
        """Get an evaluation by id.

        Args:
            evaluation_id: Id of the evaluation.

        Raises:
            APIError: The request failed, including 404 for a missing
                evaluation.

        Returns:
            Stored evaluation.
        """
        response = await self._client.request("GET", f"/v1/evaluations/{evaluation_id}")
        return EvaluationResponse.model_validate(response.json())

    async def list(
        self,
        params: EvaluationListParams | None = None,
    ) -> Page[EvaluationResponse]:
        """List evaluations.

        Args:
            params: Evaluation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Page of evaluations.
        """
        params = params or EvaluationListParams()
        response = await self._client.request(
            "GET",
            "/v1/evaluations",
            params=params.model_dump(mode="json", exclude_unset=True),
        )
        return Page[EvaluationResponse].model_validate(response.json())

    async def iter(
        self,
        params: EvaluationListParams | None = None,
    ) -> AsyncIterator[EvaluationResponse]:
        """Iterate over all evaluations.

        Args:
            params: Evaluation list params.

        Raises:
            APIError: The request failed.

        Returns:
            Async iterator over every evaluation.
        """
        params = params or EvaluationListParams()
        while True:
            page = await self.list(params)
            for item in page.items:
                yield item
            if page.next_cursor is None:
                break
            params = params.model_copy(update={"cursor": page.next_cursor})
