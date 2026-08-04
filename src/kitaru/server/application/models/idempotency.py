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
"""Application models for scoped idempotency."""

import uuid
from enum import StrEnum
from typing import Literal

from kitaru.server.base import FrozenModel
from kitaru.server.domain.idempotency import IdempotencyRecord


class IdempotencyActorScope(FrozenModel):
    """Collision-safe identity of an authenticated caller."""

    account_id: uuid.UUID
    principal_kind: Literal["account", "worker", "task"]
    principal_identity: str


class IdempotencyRequest(FrozenModel):
    """Canonical identity and fingerprint of one HTTP mutation."""

    method: str
    route: str
    caller_key: str
    fingerprint: str


class IdempotencyReservation(FrozenModel):
    """Reservation owned by the current request transaction."""

    record_id: uuid.UUID
    actor: IdempotencyActorScope
    request: IdempotencyRequest


class IdempotencyStoredResponse(FrozenModel):
    """Exact replayable response representation."""

    status_code: int
    body: bytes
    headers: dict[str, str]


class IdempotencyClaimKind(StrEnum):
    """Outcome of a repository reservation attempt."""

    OWNED = "owned"
    EXISTING = "existing"
    TIMED_OUT = "timed_out"


class IdempotencyClaim(FrozenModel):
    """Repository reservation result."""

    kind: IdempotencyClaimKind
    record: IdempotencyRecord | None = None


class IdempotencyDecisionKind(StrEnum):
    """Application decision after reserving a request."""

    EXECUTE = "execute"
    REPLAY = "replay"


class IdempotencyDecision(FrozenModel):
    """Whether the REST adapter should execute or replay."""

    kind: IdempotencyDecisionKind
    reservation: IdempotencyReservation | None = None
    response: IdempotencyStoredResponse | None = None
