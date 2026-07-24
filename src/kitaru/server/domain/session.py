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
"""Session entity, value objects, and errors."""

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, model_validator

from kitaru.server.base import FrozenModel
from kitaru.server.domain.base import (
    ConflictError,
    DomainModel,
    NotFoundError,
    ValidationError,
)
from kitaru.server.domain.ids import uuid7


class SessionOrigin(StrEnum):
    """Session origin."""

    IMPORTED = "imported"
    RECORDED = "recorded"
    REPLAY = "replay"


class SessionStatus(StrEnum):
    """Session status."""

    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class SessionProvider(StrEnum):
    """Session provider."""

    LANGFUSE = "langfuse"
    BRAINTRUST = "braintrust"
    OTLP = "otlp"


class SessionNotFound(NotFoundError):
    """Raised when a session lookup does not resolve."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the missing session.
        """
        super().__init__(f"Session {session_id} was not found")


class DuplicateSessionExternalId(ConflictError):
    """Raised when a provider session is already registered."""

    def __init__(self, provider: str, external_id: str) -> None:
        """Initialize the error.

        Args:
            provider: Provider of the session.
            external_id: External id that is already registered.
        """
        super().__init__(
            f"Session external id '{external_id}' is already registered "
            f"for provider '{provider}'"
        )


class SessionInUse(ConflictError):
    """Raised when a session deletion is blocked by existing references."""

    def __init__(self, session_id: uuid.UUID, referrer: str) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the referenced session.
            referrer: Kind of resource referencing the session.
        """
        super().__init__(f"Session {session_id} is referenced by {referrer}")


class SessionNotInProgress(ConflictError):
    """Raised when an operation requires an in-progress session."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the session.
        """
        super().__init__(f"Session {session_id} is not in progress")


class ImportedSessionImmutable(ConflictError):
    """Raised when imported execution evidence would be changed."""

    def __init__(self, session_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            session_id: Id of the imported session.
        """
        super().__init__(f"Imported session {session_id} evidence is immutable")


class InvalidSession(ValidationError):
    """Raised when a session violates its shape rules."""


class TokenUsage(FrozenModel):
    """Token usage."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None

    @classmethod
    def from_counts(
        cls,
        input_tokens: int | None,
        output_tokens: int | None,
        cached_input_tokens: int | None,
        reasoning_tokens: int | None,
    ) -> "TokenUsage | None":
        """Build a token usage from counts.

        Args:
            input_tokens: Input token count.
            output_tokens: Output token count.
            cached_input_tokens: Cached input token count.
            reasoning_tokens: Reasoning token count.

        Returns:
            Token usage, ``None`` when every count is ``None``.
        """
        if (
            input_tokens is None
            and output_tokens is None
            and cached_input_tokens is None
            and reasoning_tokens is None
        ):
            return None
        return cls(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_input_tokens=cached_input_tokens,
            reasoning_tokens=reasoning_tokens,
        )


class SessionRollups(FrozenModel):
    """Session rollups."""

    cost: Decimal | None = None
    tokens: TokenUsage | None = None
    llm_call_count: int = 0
    tool_call_count: int = 0


class Session(DomainModel):
    """Session."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    origin: SessionOrigin
    status: SessionStatus = SessionStatus.IN_PROGRESS
    name: str | None = None
    inputs: Any = None
    outputs: Any = None
    expected: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    external_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    provider: SessionProvider | str | None = None
    source_instance: str | None = None
    source_revision: int | None = None
    source_digest: str | None = None
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    replay_readiness: dict[str, Any] | None = None
    normalization_warnings: list[str] = Field(default_factory=list)
    import_job_id: uuid.UUID | None = None
    supersedes_session_id: uuid.UUID | None = None
    framework: str | None = None
    adapter_version: str | None = None
    log_uri: str | None = None
    scores: dict[str, float] = Field(default_factory=dict)
    cost: Decimal | None = None
    tokens: TokenUsage | None = None
    llm_call_count: int = 0
    tool_call_count: int = 0
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def validate_origin_fields(self) -> Self:
        """Validate origin-specific field requirements.

        Raises:
            InvalidSession: The fields violate the origin rules.

        Returns:
            The validated session.
        """
        if self.origin is SessionOrigin.IMPORTED:
            if self.provider is None or self.external_id is None:
                raise InvalidSession(
                    "Imported sessions require a provider and an external id"
                )
            if self.status is SessionStatus.IN_PROGRESS:
                raise InvalidSession("Imported sessions cannot be in progress")
        elif self.provider is not None:
            raise InvalidSession("Only imported sessions carry a provider")
        return self

    def update_name(self, name: str) -> None:
        """Set a new session name.

        Args:
            name: New name.
        """
        self.name = name

    def update_expected(self, expected: Any) -> None:
        """Set new expected outputs.

        Args:
            expected: New expected outputs.
        """
        self.expected = expected

    def update_metadata(self, metadata: dict[str, Any]) -> None:
        """Set new user metadata.

        Args:
            metadata: New metadata.
        """
        self.metadata = metadata

    def merge_scores(self, scores: dict[str, float]) -> None:
        """Merge values into the scores map, latest wins per scorer name.

        Args:
            scores: Score values by scorer name.
        """
        self.scores = {**self.scores, **scores}

    def check_node_ingest(self) -> None:
        """Check that the session accepts node ingest.

        Imported session evidence is immutable. Other sessions accept nodes
        only while in progress.

        Raises:
            ImportedSessionImmutable: The session is imported.
            SessionNotInProgress: The session does not accept node ingest.
        """
        if self.origin is SessionOrigin.IMPORTED:
            raise ImportedSessionImmutable(self.id)
        if self.status is not SessionStatus.IN_PROGRESS:
            raise SessionNotInProgress(self.id)

    def set_import_rollups(self, rollups: SessionRollups) -> None:
        """Set rollups computed while atomically importing the session.

        Args:
            rollups: Rollups computed from normalized nodes.

        Raises:
            InvalidSession: The session is not imported.
        """
        if self.origin is not SessionOrigin.IMPORTED:
            raise InvalidSession("Import rollups require an imported session")
        self.cost = rollups.cost
        self.tokens = rollups.tokens
        self.llm_call_count = rollups.llm_call_count
        self.tool_call_count = rollups.tool_call_count

    def finish(
        self,
        status: SessionStatus,
        outputs: Any,
        error: str | None,
        ended_at: datetime | None,
        log_uri: str | None,
        rollups: SessionRollups,
    ) -> None:
        """Finish the session and record its rollups.

        Args:
            status: Terminal status.
            outputs: Final agent outputs.
            error: Error message.
            ended_at: Execution end time.
            log_uri: Log location.
            rollups: Rollups computed from the session's nodes.

        Raises:
            SessionNotInProgress: The session is not in progress.
            InvalidSession: ``status`` is not terminal.
        """
        if self.status is not SessionStatus.IN_PROGRESS:
            raise SessionNotInProgress(self.id)
        if status is SessionStatus.IN_PROGRESS:
            raise InvalidSession("Session finish requires a terminal status")
        self.status = status
        self.outputs = outputs
        self.error = error
        self.ended_at = ended_at
        self.log_uri = log_uri
        self.cost = rollups.cost
        self.tokens = rollups.tokens
        self.llm_call_count = rollups.llm_call_count
        self.tool_call_count = rollups.tool_call_count
