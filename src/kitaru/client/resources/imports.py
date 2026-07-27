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
"""Imports resource."""

from typing import TYPE_CHECKING

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.jobs import JobResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ImportsResource:
    """Import API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: ImportCreateRequest) -> JobResponse:
        """Create an import of one payload blob.

        Args:
            request: Import create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                importer, importer version, or payload blob.

        Returns:
            Created job.
        """
        response = await self._client.request(
            "POST",
            "/v1/imports",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return JobResponse.model_validate(response.json())
