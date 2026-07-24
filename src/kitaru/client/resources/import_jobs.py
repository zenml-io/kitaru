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
"""Trace importer and import job client methods."""

import uuid
from typing import TYPE_CHECKING

from kitaru.api_models.v1.import_jobs import ImporterResponse, ImportJobResponse

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ImportJobsResource:
    """Trace import API methods."""

    def __init__(self, client: "KitaruAPIClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def list_importers(self) -> list[ImporterResponse]:
        """List importers available in the server deployment."""
        response = await self._client.request("GET", "/v1/importers")
        return [ImporterResponse.model_validate(item) for item in response.json()]

    async def create(
        self,
        content: bytes,
        filename: str,
        importer_id: str,
        agent_version_id: uuid.UUID,
        source_instance: str | None = None,
    ) -> ImportJobResponse:
        """Upload JSONL and create an import job."""
        data = {
            "importer_id": importer_id,
            "agent_version_id": str(agent_version_id),
        }
        if source_instance is not None:
            data["source_instance"] = source_instance
        response = await self._client.request(
            "POST",
            "/v1/import-jobs",
            data=data,
            files={"file": (filename, content, "application/x-ndjson")},
        )
        return ImportJobResponse.model_validate(response.json())

    async def get(self, job_id: uuid.UUID) -> ImportJobResponse:
        """Get an import job."""
        response = await self._client.request("GET", f"/v1/import-jobs/{job_id}")
        return ImportJobResponse.model_validate(response.json())
