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
"""Insight ORM table."""

import uuid
from typing import Any, cast

from pydantic import TypeAdapter
from sqlalchemy import ForeignKeyConstraint, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from kitaru.api_models.v1.insight import (
    InsightAction,
    InsightCoverage,
    InsightFinding,
    InsightHighlight,
    InsightStatus,
    SuggestedQuestion,
)
from kitaru.server.adapters.db.orm.base import (
    Base,
    TimestampMixin,
    UUIDPrimaryKeyMixin,
)
from kitaru.server.adapters.db.orm.orm_utils import foreign_key_name, index_name
from kitaru.server.domain.insight import Insight
from kitaru.server.domain.names import MAX_NAME_LENGTH

_FINDINGS_ADAPTER: TypeAdapter[list[InsightFinding]] = TypeAdapter(list[InsightFinding])
_QUESTIONS_ADAPTER: TypeAdapter[list[SuggestedQuestion]] = TypeAdapter(
    list[SuggestedQuestion]
)

INSIGHT_OWNER_ID_FOREIGN_KEY = foreign_key_name("insight", ["owner_id"])
INSIGHT_AGENT_ID_FOREIGN_KEY = foreign_key_name("insight", ["agent_id"])
INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY = foreign_key_name(
    "insight", ["analyzer_version_id"]
)
INSIGHT_TASK_ID_FOREIGN_KEY = foreign_key_name("insight", ["task_id"])
INSIGHT_IMPORT_ID_FOREIGN_KEY = foreign_key_name("insight", ["import_id"])
INSIGHT_OWNER_ID_INDEX = index_name("insight", ["owner_id"])
INSIGHT_AGENT_ID_INDEX = index_name("insight", ["agent_id"])
INSIGHT_ANALYZER_VERSION_ID_INDEX = index_name("insight", ["analyzer_version_id"])
INSIGHT_TASK_ID_INDEX = index_name("insight", ["task_id"])
INSIGHT_IMPORT_ID_INDEX = index_name("insight", ["import_id"])

STATUS_LENGTH = 32
SOURCE_PROVIDER_LENGTH = 80


class InsightORM(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    """Insight table."""

    __tablename__ = "insight"
    __table_args__ = (
        ForeignKeyConstraint(
            ["owner_id"], ["account.id"], name=INSIGHT_OWNER_ID_FOREIGN_KEY
        ),
        ForeignKeyConstraint(
            ["agent_id"],
            ["agent.id"],
            name=INSIGHT_AGENT_ID_FOREIGN_KEY,
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["analyzer_version_id"],
            ["plugin_version.id"],
            name=INSIGHT_ANALYZER_VERSION_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["import_id"],
            ["import.id"],
            name=INSIGHT_IMPORT_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        ForeignKeyConstraint(
            ["task_id"],
            ["task.id"],
            name=INSIGHT_TASK_ID_FOREIGN_KEY,
            ondelete="SET NULL",
        ),
        Index(INSIGHT_OWNER_ID_INDEX, "owner_id"),
        Index(INSIGHT_AGENT_ID_INDEX, "agent_id"),
        Index(INSIGHT_ANALYZER_VERSION_ID_INDEX, "analyzer_version_id"),
        Index(INSIGHT_TASK_ID_INDEX, "task_id"),
        Index(INSIGHT_IMPORT_ID_INDEX, "import_id"),
    )

    owner_id: Mapped[uuid.UUID]
    agent_id: Mapped[uuid.UUID]
    analyzer_version_id: Mapped[uuid.UUID | None]
    task_id: Mapped[uuid.UUID | None]
    import_id: Mapped[uuid.UUID | None]
    # No foreign key, identifies the analyzer invocation that produced this row
    # alongside its siblings.
    invocation_id: Mapped[uuid.UUID | None]
    source_provider: Mapped[str | None] = mapped_column(String(SOURCE_PROVIDER_LENGTH))
    name: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    title: Mapped[str] = mapped_column(String(MAX_NAME_LENGTH))
    summary: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(STATUS_LENGTH))
    coverage: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    highlight: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    next_step: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    findings: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    suggested_questions: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    analyzer_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True)
    )
    params_hash: Mapped[str | None] = mapped_column(String(64))

    @classmethod
    def from_domain(cls, insight: Insight) -> "InsightORM":
        """Build a row from a domain insight.

        Args:
            insight: Insight to store.

        Returns:
            Row without timestamps set.
        """
        return cls(
            id=insight.id,
            owner_id=insight.owner_id,
            agent_id=insight.agent_id,
            analyzer_version_id=insight.analyzer_version_id,
            task_id=insight.task_id,
            import_id=insight.import_id,
            invocation_id=insight.invocation_id,
            source_provider=insight.source_provider,
            name=insight.name,
            title=insight.title,
            summary=insight.summary,
            status=insight.status,
            coverage=(
                insight.coverage.model_dump(mode="json")
                if insight.coverage is not None
                else None
            ),
            highlight=(
                insight.highlight.model_dump(mode="json")
                if insight.highlight is not None
                else None
            ),
            next_step=(
                insight.next_step.model_dump(mode="json")
                if insight.next_step is not None
                else None
            ),
            findings=[finding.model_dump(mode="json") for finding in insight.findings],
            suggested_questions=[
                question.model_dump(mode="json")
                for question in insight.suggested_questions
            ],
            metadata_=insight.metadata,
            analyzer_params=insight.analyzer_params,
            params_hash=insight.params_hash,
        )

    def to_domain(self) -> Insight:
        """Build a domain insight from this row.

        Returns:
            Insight with timestamps set.
        """
        return Insight(
            id=self.id,
            owner_id=self.owner_id,
            agent_id=self.agent_id,
            analyzer_version_id=self.analyzer_version_id,
            task_id=self.task_id,
            import_id=self.import_id,
            invocation_id=self.invocation_id,
            source_provider=self.source_provider,
            name=self.name,
            title=self.title,
            summary=self.summary,
            status=cast(InsightStatus, self.status),
            coverage=(
                InsightCoverage.model_validate(self.coverage)
                if self.coverage is not None
                else None
            ),
            highlight=(
                InsightHighlight.model_validate(self.highlight)
                if self.highlight is not None
                else None
            ),
            next_step=(
                InsightAction.model_validate(self.next_step)
                if self.next_step is not None
                else None
            ),
            findings=_FINDINGS_ADAPTER.validate_python(self.findings),
            suggested_questions=_QUESTIONS_ADAPTER.validate_python(
                self.suggested_questions
            ),
            metadata=self.metadata_,
            analyzer_params=self.analyzer_params,
            params_hash=self.params_hash,
            created=self.created,
            updated=self.updated,
        )
