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
"""Investigation and investigation session entities, and errors."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from kitaru.api_models.v1.investigation import (
    InvestigationSessionVerdict,
    InvestigationSessionView,
    InvestigationStatus,
    QuestionItem,
)
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class InvestigationNotFound(NotFoundError):
    """Raised when an investigation lookup does not resolve."""

    def __init__(self, investigation_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            investigation_id: Id of the missing investigation.
        """
        super().__init__(f"Investigation {investigation_id} was not found")


class IllegalInvestigationStatusTransition(ConflictError):
    """Raised when an investigation status transition is not allowed."""

    def __init__(
        self,
        investigation_id: uuid.UUID,
        current: InvestigationStatus,
        target: InvestigationStatus,
    ) -> None:
        """Initialize the error.

        Args:
            investigation_id: Id of the investigation.
            current: Current investigation status.
            target: Target investigation status.
        """
        super().__init__(
            f"Investigation {investigation_id} cannot transition from "
            f"{current} to {target}"
        )


class DuplicateQuestionKey(ValidationError):
    """Raised when an investigation's questions contain a key more than once."""

    def __init__(self, key: str) -> None:
        """Initialize the error.

        Args:
            key: Key that appears more than once.
        """
        super().__init__(f"Question key '{key}' appears more than once")


class UnknownQuestionKey(ValidationError):
    """Raised when a key does not name one of an investigation's questions."""

    def __init__(self, investigation_id: uuid.UUID, key: str) -> None:
        """Initialize the error.

        Args:
            investigation_id: Id of the investigation.
            key: Key that does not match a question.
        """
        super().__init__(
            f"Investigation {investigation_id} has no question with key '{key}'"
        )


class InvestigationSessionNotFound(NotFoundError):
    """Raised when an investigation session lookup does not resolve."""

    def __init__(self, investigation_session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            investigation_session_id: Id of the missing investigation session.
        """
        super().__init__(
            f"Investigation session {investigation_session_id} was not found"
        )


class Investigation(DomainModel):
    """Investigation."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    name: Name
    description: str | None = None
    status: InvestigationStatus = InvestigationStatus.PENDING
    questions: list[QuestionItem]
    started_at: datetime | None = None
    ended_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    total_sessions: int
    completed_sessions: int
    created: datetime | None = None
    updated: datetime | None = None

    @field_validator("questions")
    @classmethod
    def _check_unique_keys(cls, value: list[QuestionItem]) -> list[QuestionItem]:
        """Reject duplicate question keys.

        Args:
            value: Questions to check.

        Raises:
            DuplicateQuestionKey: A key appears more than once.

        Returns:
            Validated questions.
        """
        seen: set[str] = set()
        for item in value:
            if item.key in seen:
                raise DuplicateQuestionKey(item.key)
            seen.add(item.key)
        return value

    def check_question_key(self, question_key: str) -> None:
        """Require a key to name one of the investigation's questions.

        Args:
            question_key: Key to check.

        Raises:
            UnknownQuestionKey: No question has this key.
        """
        if question_key not in {item.key for item in self.questions}:
            raise UnknownQuestionKey(self.id, question_key)

    def update_name(self, name: str) -> None:
        """Set a new investigation name.

        Args:
            name: New name.
        """
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set a new investigation description.

        Args:
            description: New description.
        """
        self.description = description

    def update_status(self, status: InvestigationStatus, now: datetime) -> None:
        """Set a new investigation status.

        Args:
            status: New status.
            now: Current time.

        Raises:
            IllegalInvestigationStatusTransition: The status moves backwards.
        """
        if status is self.status:
            return
        if status is InvestigationStatus.IN_PROGRESS:
            self.start(now)
        elif status is InvestigationStatus.COMPLETED:
            self.complete(now)
        else:
            raise IllegalInvestigationStatusTransition(self.id, self.status, status)

    def start(self, now: datetime) -> None:
        """Move a pending investigation to in_progress on its first answer.

        Args:
            now: Current time.

        Raises:
            IllegalInvestigationStatusTransition: The investigation is not
                pending.
        """
        if self.status is not InvestigationStatus.PENDING:
            raise IllegalInvestigationStatusTransition(
                self.id, self.status, InvestigationStatus.IN_PROGRESS
            )
        self.status = InvestigationStatus.IN_PROGRESS
        self.started_at = now

    def complete(self, now: datetime) -> None:
        """Move a pending or in-progress investigation to completed.

        Args:
            now: Current time.

        Raises:
            IllegalInvestigationStatusTransition: The investigation is
                already completed.
        """
        if self.status is InvestigationStatus.COMPLETED:
            raise IllegalInvestigationStatusTransition(
                self.id, self.status, InvestigationStatus.COMPLETED
            )
        self.status = InvestigationStatus.COMPLETED
        self.ended_at = now


class InvestigationSession(DomainModel):
    """Investigation session."""

    id: uuid.UUID = Field(default_factory=uuid7)
    investigation_id: uuid.UUID
    session_id: uuid.UUID
    position: int
    verdict: InvestigationSessionVerdict | None = None
    view: InvestigationSessionView | None = None
    created: datetime | None = None
    updated: datetime | None = None

    def update_verdict(self, verdict: InvestigationSessionVerdict | None) -> None:
        """Set a new session verdict.

        Args:
            verdict: New verdict, None clears it.
        """
        self.verdict = verdict
