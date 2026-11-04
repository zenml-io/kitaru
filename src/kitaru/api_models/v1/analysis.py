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
"""Analysis API models."""

import uuid

from pydantic import AwareDatetime, Field, field_validator

from kitaru.api_models.v1.base import RequestModel
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.plugin import AnalyzerConfig


class AnalysisCreateRequest(RequestModel):
    """Analysis create request.

    Every scope field that is set must match, so they combine as AND. With
    none set, every finished session of the agent is analyzed.
    """

    agent_id: uuid.UUID = Field(description="Agent whose sessions are analyzed.")
    analyzers: list[AnalyzerConfig] = Field(
        min_length=1, description="Analyzers run across the selected sessions."
    )
    session_ids: list[uuid.UUID] | None = Field(
        default=None, min_length=1, description="Only these sessions."
    )
    tag: str | None = Field(
        default=None, min_length=1, description="Only sessions with this tag name."
    )
    cohort_version_id: uuid.UUID | None = Field(
        default=None, description="Only sessions in this cohort version."
    )
    import_id: uuid.UUID | None = Field(
        default=None,
        description="Only sessions this import created. Insights also record it.",
    )
    started_after: AwareDatetime | None = Field(
        default=None, description="Only sessions started at or after this time."
    )
    started_before: AwareDatetime | None = Field(
        default=None, description="Only sessions started before this time."
    )
    filter: Filter | None = Field(
        default=None, description="Only sessions matching this session filter."
    )

    @field_validator("session_ids")
    @classmethod
    def _check_unique(cls, value: list[uuid.UUID] | None) -> list[uuid.UUID] | None:
        """Reject duplicate session ids.

        Args:
            value: Session ids to check.

        Raises:
            ValueError: A session id appears more than once.

        Returns:
            The unchanged session ids.
        """
        if value is not None and len(set(value)) != len(value):
            raise ValueError("session_ids must be unique")
        return value
