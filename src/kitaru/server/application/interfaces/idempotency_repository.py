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
"""Repository contract for scoped idempotency."""

import uuid
from datetime import datetime
from typing import Protocol

from kitaru.server.application.models.idempotency import (
    IdempotencyClaim,
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.domain.idempotency import IdempotencyRecord


class IdempotencyRepository(Protocol):
    """Persist request reservations and replayable responses."""

    async def cleanup_expired(self, now: datetime, limit: int) -> int:
        """Delete at most ``limit`` expired completed records."""
        ...

    async def reserve(
        self,
        record: IdempotencyRecord,
        wait_timeout_seconds: float,
    ) -> IdempotencyClaim:
        """Try to own a scoped key without poisoning the transaction."""
        ...

    async def delete_expired(
        self,
        record: IdempotencyRecord,
        now: datetime,
        wait_timeout_seconds: float,
    ) -> bool:
        """Delete the record only if it is completed and expired."""
        ...

    async def complete(
        self,
        reservation: IdempotencyReservation,
        response: IdempotencyStoredResponse,
        completed_at: datetime,
        expires_at: datetime,
    ) -> IdempotencyRecord:
        """Transition an owned pending reservation to completed."""
        ...

    async def get(
        self,
        actor_account_id: uuid.UUID,
        actor_principal_kind: str,
        actor_principal_identity: str,
        request: IdempotencyRequest,
    ) -> IdempotencyRecord | None:
        """Load an authoritative record by its unique scope."""
        ...
