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
"""Import ORM table."""

import uuid
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.imports import ImportStats
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import (
    foreign_key_name,
    index_name,
    unique_constraint_name,
)
from kitaru.server.domain.imports import Import
from kitaru.server.domain.replay_config import AnalyzerConfig, EvaluatorConfig

IMPORT_OWNER_ID_FOREIGN_KEY = foreign_key_name("import", ["owner_id"])
IMPORT_JOB_ID_FOREIGN_KEY = foreign_key_name("import", ["job_id"])
IMPORT_AGENT_ID_FOREIGN_KEY = foreign_key_name("import", ["agent_id"])
IMPORT_AGENT_VERSION_ID_FOREIGN_KEY = foreign_key_name("import", ["agent_version_id"])
IMPORT_IMPORTER_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "import", ["importer_version_id"]
)
IMPORT_PAYLOAD_BLOB_ID_FOREIGN_KEY = foreign_key_name("import", ["payload_blob_id"])
IMPORT_JOB_ID_UNIQUE_CONSTRAINT = unique_constraint_name("import", ["job_id"])
IMPORT_AGENT_ID_INDEX = index_name("import", ["agent_id"])


class ImportORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Import table."""

    __tablename__ = "import"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=IMPORT_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["job_id"], ["job.id"], name=IMPORT_JOB_ID_FOREIGN_KEY, ondelete="SET NULL"
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=IMPORT_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["agent_version_id"],
            ["agent_version.id"],
            name=IMPORT_AGENT_VERSION_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["importer_version_id"],
            ["plugin_version.id"],
            name=IMPORT_IMPORTER_VERSION_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["payload_blob_id"], ["blob.id"], name=IMPORT_PAYLOAD_BLOB_ID_FOREIGN_KEY
        ),
        UniqueConstraint("job_id", name=IMPORT_JOB_ID_UNIQUE_CONSTRAINT),
        Index(IMPORT_AGENT_ID_INDEX, "agent_id"),
    )

    owner_id: Mapped[uuid.UUID]
    job_id: Mapped[uuid.UUID | None]
    agent_id: Mapped[uuid.UUID]
    agent_version_id: Mapped[uuid.UUID | None]
    importer_version_id: Mapped[uuid.UUID | None]
    payload_blob_id: Mapped[uuid.UUID]
    params: Mapped[dict[str, Any]] = mapped_column(JSONB)
    evaluators: Mapped[list[Any]] = mapped_column(JSONB)
    analyzers: Mapped[list[Any]] = mapped_column(JSONB)
    stats: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[str | None] = mapped_column(Text)

    @classmethod
    def from_domain(cls, import_: Import) -> "ImportORM":
        """Build a row from a domain import.

        Args:
            import_: Import to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=import_.id,
            owner_id=import_.owner_id,
            job_id=import_.job_id,
            agent_id=import_.agent_id,
            agent_version_id=import_.agent_version_id,
            importer_version_id=import_.importer_version_id,
            payload_blob_id=import_.payload_blob_id,
            params=import_.params,
            evaluators=[
                evaluator.model_dump(mode="json") for evaluator in import_.evaluators
            ],
            analyzers=[
                analyzer.model_dump(mode="json") for analyzer in import_.analyzers
            ],
            stats=(
                import_.stats.model_dump(mode="json")
                if import_.stats is not None
                else None
            ),
            error=import_.error,
        )

    def apply(self, import_: Import) -> None:
        """Copy a domain import's mutable fields onto this row.

        Args:
            import_: Import with modified fields.
        """
        self.stats = (
            import_.stats.model_dump(mode="json") if import_.stats is not None else None
        )
        self.error = import_.error

    def to_domain(self) -> Import:
        """Build a domain import from this row.

        Returns:
            Import with timestamps set.
        """
        return Import(
            id=self.id,
            owner_id=self.owner_id,
            job_id=self.job_id,
            agent_id=self.agent_id,
            agent_version_id=self.agent_version_id,
            importer_version_id=self.importer_version_id,
            payload_blob_id=self.payload_blob_id,
            params=self.params,
            evaluators=[
                EvaluatorConfig.model_validate(evaluator)
                for evaluator in self.evaluators
            ],
            analyzers=[
                AnalyzerConfig.model_validate(analyzer) for analyzer in self.analyzers
            ],
            stats=(
                ImportStats.model_validate(self.stats)
                if self.stats is not None
                else None
            ),
            error=self.error,
            created=self.created,
            updated=self.updated,
        )
