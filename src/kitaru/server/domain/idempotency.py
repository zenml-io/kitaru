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
"""Idempotency record entity and errors."""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import Field, model_validator

from kitaru.server.domain.base import DomainError, DomainModel
from kitaru.server.domain.ids import uuid7


class IdempotencyState(StrEnum):
    """Persistence state of an idempotency record."""

    PENDING = "pending"
    COMPLETED = "completed"


class IdempotencyMismatch(DomainError):
    """Raised when a scoped key is reused for a different request."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__(
            "The idempotency key was already used for a different request."
        )


class IdempotencyRequestInProgress(DomainError):
    """Raised when the first request still owns an idempotency reservation."""

    def __init__(self, retry_after_seconds: int) -> None:
        """Initialize the error.

        Args:
            retry_after_seconds: Bounded delay before a retry.
        """
        self.retry_after_seconds = retry_after_seconds
        super().__init__("A request with this idempotency key is still in progress.")


class IdempotencyRecordStateError(DomainError):
    """Raised when an idempotency record violates its state transition."""


class IdempotencyRecord(DomainModel):
    """Scoped request reservation and its replayable response."""

    id: uuid.UUID = Field(default_factory=uuid7)
    actor_account_id: uuid.UUID
    actor_principal_kind: str
    actor_principal_identity: str
    method: str
    route: str
    caller_key: str
    fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: IdempotencyState = IdempotencyState.PENDING
    response_status: int | None = None
    response_body: bytes | None = None
    response_headers: dict[str, str] | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None
    created: datetime | None = None
    updated: datetime | None = None

    @model_validator(mode="after")
    def validate_state(self) -> "IdempotencyRecord":
        """Validate pending and completed field coherence.

        Raises:
            ValueError: Fields do not match the record state.

        Returns:
            The validated record.
        """
        response_fields = (
            self.response_status,
            self.response_body,
            self.response_headers,
            self.completed_at,
            self.expires_at,
        )
        if self.state is IdempotencyState.PENDING:
            if any(value is not None for value in response_fields):
                raise ValueError("Pending idempotency records cannot store a response")
            return self
        if any(value is None for value in response_fields):
            raise ValueError("Completed idempotency records require a response")
        assert self.response_status is not None
        assert self.completed_at is not None
        assert self.expires_at is not None
        if not 100 <= self.response_status <= 599:
            raise ValueError("Response status must be between 100 and 599")
        if self.expires_at <= self.completed_at:
            raise ValueError("Idempotency expiry must follow completion")
        return self
