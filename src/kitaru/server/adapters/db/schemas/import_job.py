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
"""Import job ORM table."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKeyConstraint, Index, LargeBinary, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from kitaru.server.adapters.db.schemas.base import (
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.schemas.schema_utils import foreign_key_name, index_name
from kitaru.server.domain.import_job import ImportJob, ImportJobError, ImportJobStatus

IMPORT_JOB_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "import_job", ["agent_version_id"]
)
IMPORT_JOB_STATUS_CREATED_INDEX = index_name("import_job", ["status", "created"])
IMPORT_JOB_OWNER_ID_INDEX = index_name("import_job", ["owner_id"])

MAX_IMPORTER_ID_LENGTH = 255
MAX_VERSION_LENGTH = 64
MAX_STATUS_LENGTH = 32
MAX_FILENAME_LENGTH = 255
MAX_WORKER_ID_LENGTH = 255


class ImportJobSchema(UUIDPrimaryKeyMixin, TimestampMixin, table=True):
    """Import job table."""

    __tablename__ = "import_job"
    __table_args__ = (
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=IMPORT_JOB_AGENT_VERSION_ID_FOREIGN_KEY,
        ),
        Index(IMPORT_JOB_STATUS_CREATED_INDEX, "status", "created"),
        Index(IMPORT_JOB_OWNER_ID_INDEX, "owner_id"),
    )

    owner_id: uuid.UUID = Field(foreign_key="account.id", nullable=False)
    agent_version_id: uuid.UUID = Field(nullable=False)
    importer_id: str = Field(max_length=MAX_IMPORTER_ID_LENGTH, nullable=False)
    importer_version: str = Field(max_length=MAX_VERSION_LENGTH, nullable=False)
    source_instance: str | None = Field(default=None, max_length=MAX_IMPORTER_ID_LENGTH)
    filename: str = Field(max_length=MAX_FILENAME_LENGTH, nullable=False)
    content: bytes | None = Field(default=None, sa_type=LargeBinary)
    status: str = Field(max_length=MAX_STATUS_LENGTH, nullable=False)
    worker_id: str | None = Field(default=None, max_length=MAX_WORKER_ID_LENGTH)
    started_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    ended_at: datetime | None = Field(
        default=None,
        sa_type=DateTime(timezone=True),  # ty: ignore[invalid-argument-type]
    )
    source_session_count: int = Field(default=0, nullable=False)
    imported_count: int = Field(default=0, nullable=False)
    deduplicated_count: int = Field(default=0, nullable=False)
    failed_count: int = Field(default=0, nullable=False)
    session_ids: list[str] = Field(default_factory=list, sa_type=JSONB, nullable=False)
    errors: list[dict[str, Any]] = Field(
        default_factory=list, sa_type=JSONB, nullable=False
    )
    error: str | None = Field(default=None, sa_type=Text)

    @classmethod
    def from_domain(cls, job: ImportJob) -> "ImportJobSchema":
        """Build a row from an import job."""
        return cls(
            id=job.id,
            owner_id=job.owner_id,
            agent_version_id=job.agent_version_id,
            importer_id=job.importer_id,
            importer_version=job.importer_version,
            source_instance=job.source_instance,
            filename=job.filename,
            content=job.content,
            status=job.status.value,
            worker_id=job.worker_id,
            started_at=job.started_at,
            ended_at=job.ended_at,
            source_session_count=job.source_session_count,
            imported_count=job.imported_count,
            deduplicated_count=job.deduplicated_count,
            failed_count=job.failed_count,
            session_ids=[str(session_id) for session_id in job.session_ids],
            errors=[error.model_dump(mode="json") for error in job.errors],
            error=job.error,
        )

    def to_domain(self) -> ImportJob:
        """Build a domain import job."""
        return ImportJob(
            id=self.id,
            owner_id=self.owner_id,
            agent_version_id=self.agent_version_id,
            importer_id=self.importer_id,
            importer_version=self.importer_version,
            source_instance=self.source_instance,
            filename=self.filename,
            content=self.content,
            status=ImportJobStatus(self.status),
            worker_id=self.worker_id,
            started_at=self.started_at,
            ended_at=self.ended_at,
            source_session_count=self.source_session_count,
            imported_count=self.imported_count,
            deduplicated_count=self.deduplicated_count,
            failed_count=self.failed_count,
            session_ids=[uuid.UUID(session_id) for session_id in self.session_ids],
            errors=[ImportJobError.model_validate(error) for error in self.errors],
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
