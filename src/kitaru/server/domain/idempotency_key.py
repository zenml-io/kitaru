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
"""Idempotency key entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, ValidationError
from kitaru.server.domain.ids import uuid7

MAX_IDEMPOTENCY_KEY_LENGTH = 255
MAX_IDEMPOTENCY_PATH_LENGTH = 2048


class IdempotencyKeyAlreadyExists(ConflictError):
    """Raised when an idempotency key is already registered for an account."""

    def __init__(self, account_id: uuid.UUID, key: str) -> None:
        """Initialize the error.

        Args:
            account_id: Id of the account the key is scoped to.
            key: Idempotency key that is already registered.
        """
        super().__init__(
            f"Idempotency key '{key}' is already registered for account {account_id}"
        )


class IdempotencyKeyMismatch(ValidationError):
    """Raised when a stored idempotency key was used with a different request."""

    def __init__(self) -> None:
        """Initialize the error."""
        super().__init__("Idempotency-Key was already used with a different request")


class IdempotencyKey(DomainModel):
    """Idempotency key."""

    id: uuid.UUID = Field(default_factory=uuid7)
    account_id: uuid.UUID
    key: str
    fingerprint: str
    method: str
    path: str
    response_status: int | None = None
    response_body: bytes | None = None
    response_content_type: str | None = None
    created: datetime | None = None
