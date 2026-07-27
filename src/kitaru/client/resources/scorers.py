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
"""Scorers resource."""

import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.scorers import (
    ScorerCreateRequest,
    ScorerResponse,
    ScorerVersionCreateRequest,
    ScorerVersionResponse,
)
from kitaru.client.resources.plugin_registration import resolve_or_create

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient

SCORER_MEDIA_TYPE = "text/x-python"


class ScorersResource:
    """Scorer API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource.

        Args:
            client: API client used to send requests.
        """
        self._client = client

    async def create(self, request: ScorerCreateRequest) -> ScorerResponse:
        """Create a scorer.

        Args:
            request: Scorer create request.

        Raises:
            APIError: The request failed, including 409 for a duplicate name.

        Returns:
            Created scorer.
        """
        response = await self._client.request(
            "POST",
            "/v1/scorers",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ScorerResponse.model_validate(response.json())

    async def get(self, scorer_id: uuid.UUID) -> ScorerResponse:
        """Get a scorer by id.

        Args:
            scorer_id: Id of the scorer.

        Raises:
            APIError: The request failed, including 404 for a missing
                scorer.

        Returns:
            Stored scorer.
        """
        response = await self._client.request("GET", f"/v1/scorers/{scorer_id}")
        return ScorerResponse.model_validate(response.json())

    async def list(
        self,
        name: str | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Page[ScorerResponse]:
        """List scorers.

        Args:
            name: Filter on scorer name.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed.

        Returns:
            Page of scorers.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if name is not None:
            params["name"] = name
        response = await self._client.request("GET", "/v1/scorers", params=params)
        return Page[ScorerResponse].model_validate(response.json())

    async def delete(self, scorer_id: uuid.UUID) -> None:
        """Delete a scorer and its versions.

        Args:
            scorer_id: Id of the scorer.

        Raises:
            APIError: The request failed, including 404 for a missing
                scorer.
        """
        await self._client.request("DELETE", f"/v1/scorers/{scorer_id}")

    async def create_version(
        self, scorer_id: uuid.UUID, request: ScorerVersionCreateRequest
    ) -> ScorerVersionResponse:
        """Create a scorer version.

        Args:
            scorer_id: Id of the scorer.
            request: Scorer version create request.

        Raises:
            APIError: The request failed, including 404 for a missing
                scorer or code blob.

        Returns:
            Created scorer version.
        """
        response = await self._client.request(
            "POST",
            f"/v1/scorers/{scorer_id}/versions",
            json=request.model_dump(mode="json", exclude_unset=True),
        )
        return ScorerVersionResponse.model_validate(response.json())

    async def get_version(
        self, scorer_id: uuid.UUID, version: int
    ) -> ScorerVersionResponse:
        """Get a scorer version by version number.

        Args:
            scorer_id: Id of the scorer.
            version: Version number.

        Raises:
            APIError: The request failed, including 404 for a missing
                scorer or version.

        Returns:
            Stored scorer version.
        """
        response = await self._client.request(
            "GET", f"/v1/scorers/{scorer_id}/versions/{version}"
        )
        return ScorerVersionResponse.model_validate(response.json())

    async def list_versions(
        self, scorer_id: uuid.UUID, page: int = 1, page_size: int = 20
    ) -> Page[ScorerVersionResponse]:
        """List the versions of a scorer.

        Args:
            scorer_id: Id of the scorer.
            page: Page number.
            page_size: Page size.

        Raises:
            APIError: The request failed, including 404 for a missing
                scorer.

        Returns:
            Page of scorer versions.
        """
        response = await self._client.request(
            "GET",
            f"/v1/scorers/{scorer_id}/versions",
            params={"page": page, "page_size": page_size},
        )
        return Page[ScorerVersionResponse].model_validate(response.json())

    async def register(
        self, name: str, file: Path | str, entrypoint: str
    ) -> ScorerVersionResponse:
        """Register a new version of a scorer from a source file.

        Uploads the file, creates the scorer when it does not exist yet,
        and creates a version pointing at the uploaded code.

        Args:
            name: Scorer name.
            file: Path of the source file holding the scorer.
            entrypoint: Attribute implementing the scorer.

        Raises:
            APIError: A request failed.

        Returns:
            Created scorer version.
        """
        blob = await self._client.blobs.upload(
            Path(file).read_bytes(), SCORER_MEDIA_TYPE
        )
        scorer = await resolve_or_create(
            lambda: self.create(ScorerCreateRequest(name=name)),
            lambda: self.list(name=name, page_size=1),
        )
        return await self.create_version(
            scorer.id,
            ScorerVersionCreateRequest(blob_id=blob.id, entrypoint=entrypoint),
        )
