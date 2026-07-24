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
"""Trace importer and import job API models."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import ResponseModel


class ImportJobStatus(StrEnum):
    """Import job lifecycle status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"


class ImporterResponse(ResponseModel):
    """Available trace importer."""

    id: str = Field(description="Stable importer id.")
    display_name: str = Field(description="Importer display name.")
    version: str = Field(description="Installed importer version.")
    file_extensions: list[str] = Field(description="Accepted filename extensions.")
    max_upload_bytes: int = Field(description="Maximum upload size.")


class ImportJobErrorResponse(ResponseModel):
    """One source session that failed to import."""

    source_id: str | None = Field(description="Provider session id, when known.")
    message: str = Field(description="Failure message.")


class ImportJobResponse(ResponseModel):
    """Background trace import job."""

    id: uuid.UUID = Field(description="Import job id.")
    owner_id: uuid.UUID = Field(description="Owning account id.")
    agent_version_id: uuid.UUID = Field(description="Target agent version id.")
    importer_id: str = Field(description="Importer id.")
    importer_version: str = Field(description="Importer version.")
    source_instance: str | None = Field(description="Source project or instance.")
    filename: str = Field(description="Uploaded filename.")
    status: ImportJobStatus = Field(description="Job status.")
    started_at: datetime | None = Field(description="Processing start time.")
    ended_at: datetime | None = Field(description="Processing end time.")
    source_session_count: int = Field(description="Normalized source session count.")
    imported_count: int = Field(description="New sessions created.")
    deduplicated_count: int = Field(description="Exact sessions reused.")
    failed_count: int = Field(description="Source sessions that failed.")
    session_ids: list[uuid.UUID] = Field(description="Created and reused session ids.")
    errors: list[ImportJobErrorResponse] = Field(description="Per-session failures.")
    error: str | None = Field(description="Whole-job failure.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
