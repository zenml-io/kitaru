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
"""Application service for scoped request idempotency."""

import math
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from kitaru.server.application.interfaces.idempotency_repository import (
    IdempotencyRepository,
)
from kitaru.server.application.models.auth import (
    AccountPrincipal,
    AuthContext,
    TaskPrincipal,
    WorkerPrincipal,
)
from kitaru.server.application.models.idempotency import (
    IdempotencyActorScope,
    IdempotencyClaimKind,
    IdempotencyDecision,
    IdempotencyDecisionKind,
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.domain.idempotency import (
    IdempotencyMismatch,
    IdempotencyRecord,
    IdempotencyRecordStateError,
    IdempotencyRequestInProgress,
    IdempotencyState,
)


class IdempotencyService:
    """Reserve, replay, and complete idempotent request records."""

    def __init__(
        self,
        repository: IdempotencyRepository,
        wait_timeout_seconds: float,
        retention: timedelta,
        cleanup_batch_size: int,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Idempotency persistence implementation.
            wait_timeout_seconds: Maximum PostgreSQL conflict wait.
            retention: Completed response retention window.
            cleanup_batch_size: Maximum expired rows removed per reservation.
            clock: Injectable UTC clock.
        """
        self._repository = repository
        self._wait_timeout_seconds = wait_timeout_seconds
        self._retention = retention
        self._cleanup_batch_size = cleanup_batch_size
        self._clock = clock or (lambda: datetime.now(UTC))

    async def begin(
        self, request: IdempotencyRequest, actor: AuthContext
    ) -> IdempotencyDecision:
        """Reserve a request or return its previously completed response.

        Args:
            request: Canonical request identity and fingerprint.
            actor: Authenticated caller.

        Raises:
            IdempotencyMismatch: The key names a different request.
            IdempotencyRequestInProgress: The first request still owns the key.

        Returns:
            Execute or replay decision.
        """
        scope = self._get_actor_scope(actor)
        now = self._clock()
        await self._repository.cleanup_expired(now, self._cleanup_batch_size)
        record = IdempotencyRecord(
            actor_account_id=scope.account_id,
            actor_principal_kind=scope.principal_kind,
            actor_principal_identity=scope.principal_identity,
            method=request.method,
            route=request.route,
            caller_key=request.caller_key,
            fingerprint=request.fingerprint,
        )
        while True:
            claim = await self._repository.reserve(record, self._wait_timeout_seconds)
            if claim.kind is IdempotencyClaimKind.TIMED_OUT:
                raise self._get_in_progress_error()
            if claim.record is None:
                raise IdempotencyRecordStateError(
                    "Idempotency reservation returned no record"
                )
            if claim.kind is IdempotencyClaimKind.OWNED:
                return IdempotencyDecision(
                    kind=IdempotencyDecisionKind.EXECUTE,
                    reservation=IdempotencyReservation(
                        record_id=claim.record.id,
                        actor=scope,
                        request=request,
                    ),
                )

            existing = claim.record
            now = self._clock()
            if (
                existing.state is IdempotencyState.COMPLETED
                and existing.expires_at is not None
                and existing.expires_at <= now
            ):
                if await self._repository.delete_expired(
                    existing, now, self._wait_timeout_seconds
                ):
                    continue
                raise self._get_in_progress_error()
            if existing.fingerprint != request.fingerprint:
                raise IdempotencyMismatch()
            if existing.state is IdempotencyState.PENDING:
                raise self._get_in_progress_error()
            if (
                existing.response_status is None
                or existing.response_body is None
                or existing.response_headers is None
            ):
                raise IdempotencyRecordStateError(
                    "Completed idempotency record has no response"
                )
            return IdempotencyDecision(
                kind=IdempotencyDecisionKind.REPLAY,
                response=IdempotencyStoredResponse(
                    status_code=existing.response_status,
                    body=existing.response_body,
                    headers=dict(existing.response_headers),
                ),
            )

    async def complete(
        self,
        reservation: IdempotencyReservation,
        response: IdempotencyStoredResponse,
        actor: AuthContext,
    ) -> IdempotencyRecord:
        """Complete an owned reservation without committing its transaction.

        Args:
            reservation: Reservation created by ``begin``.
            response: Exact response representation to replay.
            actor: Authenticated caller.

        Raises:
            IdempotencyRecordStateError: The caller differs from the owner.

        Returns:
            Completed record.
        """
        if self._get_actor_scope(actor) != reservation.actor:
            raise IdempotencyRecordStateError(
                "Idempotency reservation actor changed before completion"
            )
        completed_at = self._clock()
        return await self._repository.complete(
            reservation,
            response,
            completed_at,
            completed_at + self._retention,
        )

    def _get_in_progress_error(self) -> IdempotencyRequestInProgress:
        """Build a bounded retryable in-progress error.

        Returns:
            In-progress error with whole-second guidance.
        """
        retry_after = min(60, max(1, math.ceil(self._wait_timeout_seconds)))
        return IdempotencyRequestInProgress(retry_after)

    @staticmethod
    def _get_actor_scope(actor: AuthContext) -> IdempotencyActorScope:
        """Build the persisted identity for an authenticated actor.

        Args:
            actor: Authenticated caller.

        Returns:
            Collision-safe actor scope.
        """
        principal = actor.principal
        if isinstance(principal, AccountPrincipal):
            return IdempotencyActorScope(
                account_id=actor.account.id,
                principal_kind="account",
                principal_identity=str(actor.account.id),
            )
        if isinstance(principal, WorkerPrincipal):
            return IdempotencyActorScope(
                account_id=actor.account.id,
                principal_kind="worker",
                principal_identity=str(principal.worker_id),
            )
        if isinstance(principal, TaskPrincipal):
            return IdempotencyActorScope(
                account_id=actor.account.id,
                principal_kind="task",
                principal_identity=f"{principal.task_id}:{principal.attempt}",
            )
        raise IdempotencyRecordStateError("Unsupported authentication principal")
