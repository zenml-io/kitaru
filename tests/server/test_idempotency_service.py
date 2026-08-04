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
"""Unit tests for scoped idempotency decisions."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from kitaru.server.application.models.auth import (
    AuthContext,
    TaskPrincipal,
    WorkerPrincipal,
)
from kitaru.server.application.models.idempotency import (
    IdempotencyClaim,
    IdempotencyClaimKind,
    IdempotencyDecisionKind,
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.application.services.idempotency_service import (
    IdempotencyService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.idempotency import (
    IdempotencyMismatch,
    IdempotencyRecord,
    IdempotencyRequestInProgress,
    IdempotencyState,
)

NOW = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)
ACCOUNT = Account(id=uuid.uuid4(), name="owner")
REQUEST = IdempotencyRequest(
    method="POST",
    route="/v1/replays",
    caller_key="request-1",
    fingerprint="a" * 64,
)


class FakeIdempotencyRepository:
    """Minimal transaction-free repository for service decisions."""

    def __init__(self) -> None:
        self.records: dict[tuple[object, ...], IdempotencyRecord] = {}
        self.timed_out = False
        self.cleanup_calls: list[tuple[datetime, int]] = []

    @staticmethod
    def _get_scope(record: IdempotencyRecord) -> tuple[object, ...]:
        return (
            record.actor_account_id,
            record.actor_principal_kind,
            record.actor_principal_identity,
            record.method,
            record.route,
            record.caller_key,
        )

    async def cleanup_expired(self, now: datetime, limit: int) -> int:
        self.cleanup_calls.append((now, limit))
        expired = [
            key
            for key, record in self.records.items()
            if record.state is IdempotencyState.COMPLETED
            and record.expires_at is not None
            and record.expires_at <= now
        ][:limit]
        for key in expired:
            del self.records[key]
        return len(expired)

    async def reserve(
        self, record: IdempotencyRecord, wait_timeout_seconds: float
    ) -> IdempotencyClaim:
        _ = wait_timeout_seconds
        if self.timed_out:
            return IdempotencyClaim(kind=IdempotencyClaimKind.TIMED_OUT)
        scope = self._get_scope(record)
        existing = self.records.get(scope)
        if existing is not None:
            return IdempotencyClaim(kind=IdempotencyClaimKind.EXISTING, record=existing)
        self.records[scope] = record
        return IdempotencyClaim(kind=IdempotencyClaimKind.OWNED, record=record)

    async def delete_expired(
        self,
        record: IdempotencyRecord,
        now: datetime,
        wait_timeout_seconds: float,
    ) -> bool:
        _ = wait_timeout_seconds
        if record.expires_at is None or record.expires_at > now:
            return False
        return self.records.pop(self._get_scope(record), None) is not None

    async def complete(
        self,
        reservation: IdempotencyReservation,
        response: IdempotencyStoredResponse,
        completed_at: datetime,
        expires_at: datetime,
    ) -> IdempotencyRecord:
        record = next(
            record
            for record in self.records.values()
            if record.id == reservation.record_id
        )
        completed = record.model_copy(
            update={
                "state": IdempotencyState.COMPLETED,
                "response_status": response.status_code,
                "response_body": response.body,
                "response_headers": response.headers,
                "completed_at": completed_at,
                "expires_at": expires_at,
            }
        )
        completed = IdempotencyRecord.model_validate(completed.model_dump())
        self.records[self._get_scope(record)] = completed
        return completed

    async def get(
        self,
        actor_account_id: uuid.UUID,
        actor_principal_kind: str,
        actor_principal_identity: str,
        request: IdempotencyRequest,
    ) -> IdempotencyRecord | None:
        return self.records.get(
            (
                actor_account_id,
                actor_principal_kind,
                actor_principal_identity,
                request.method,
                request.route,
                request.caller_key,
            )
        )


def _service(repository: FakeIdempotencyRepository) -> IdempotencyService:
    return IdempotencyService(
        repository=repository,
        wait_timeout_seconds=5,
        retention=timedelta(hours=24),
        cleanup_batch_size=10,
        clock=lambda: NOW,
    )


async def test_execute_complete_and_replay_exact_response() -> None:
    """Complete an owned reservation and replay its exact representation."""
    repository = FakeIdempotencyRepository()
    service = _service(repository)
    actor = AuthContext(account=ACCOUNT)

    decision = await service.begin(REQUEST, actor=actor)
    assert decision.kind is IdempotencyDecisionKind.EXECUTE
    assert decision.reservation is not None
    stored = IdempotencyStoredResponse(
        status_code=201,
        body=b'{"id":"stable"}',
        headers={"content-type": "application/json"},
    )
    completed = await service.complete(decision.reservation, stored, actor=actor)
    assert completed.expires_at == NOW + timedelta(hours=24)

    replay = await service.begin(REQUEST, actor=actor)
    assert replay.kind is IdempotencyDecisionKind.REPLAY
    assert replay.response == stored
    assert repository.cleanup_calls[-1] == (NOW, 10)


async def test_changed_fingerprint_is_a_permanent_mismatch() -> None:
    """Reject a key reused for different mutation bytes."""
    repository = FakeIdempotencyRepository()
    service = _service(repository)
    actor = AuthContext(account=ACCOUNT)
    decision = await service.begin(REQUEST, actor=actor)
    assert decision.reservation is not None
    await service.complete(
        decision.reservation,
        IdempotencyStoredResponse(status_code=201, body=b"{}", headers={}),
        actor=actor,
    )

    with pytest.raises(IdempotencyMismatch):
        await service.begin(
            REQUEST.model_copy(update={"fingerprint": "b" * 64}), actor=actor
        )


async def test_timeout_and_committed_pending_are_in_progress() -> None:
    """Return retryable in-progress semantics without executing again."""
    repository = FakeIdempotencyRepository()
    repository.timed_out = True
    with pytest.raises(IdempotencyRequestInProgress) as exc_info:
        await _service(repository).begin(REQUEST, actor=AuthContext(account=ACCOUNT))
    assert exc_info.value.retry_after_seconds == 5

    repository.timed_out = False
    await _service(repository).begin(REQUEST, actor=AuthContext(account=ACCOUNT))
    with pytest.raises(IdempotencyRequestInProgress):
        await _service(repository).begin(REQUEST, actor=AuthContext(account=ACCOUNT))


@pytest.mark.parametrize(
    ("principal", "kind"),
    [
        (WorkerPrincipal(worker_id=uuid.uuid4()), "worker"),
        (
            TaskPrincipal(
                task_id=uuid.uuid4(),
                attempt=3,
                worker_id=uuid.uuid4(),
                job_id=uuid.uuid4(),
            ),
            "task",
        ),
    ],
)
async def test_principal_kind_and_attempt_are_part_of_actor_scope(
    principal: WorkerPrincipal | TaskPrincipal,
    kind: str,
) -> None:
    """Separate non-account credentials even when their account is shared."""
    repository = FakeIdempotencyRepository()
    actor = AuthContext(account=ACCOUNT, principal=principal)
    await _service(repository).begin(REQUEST, actor=actor)
    record = next(iter(repository.records.values()))
    assert record.actor_principal_kind == kind
    if isinstance(principal, WorkerPrincipal):
        assert record.actor_principal_identity == str(principal.worker_id)
    else:
        assert record.actor_principal_identity == (
            f"{principal.task_id}:{principal.attempt}"
        )


async def test_expired_record_retry_runs_cleanup_once() -> None:
    """Retry reservation without repeating per-request cleanup."""
    repository = FakeIdempotencyRepository()
    expired = IdempotencyRecord(
        actor_account_id=ACCOUNT.id,
        actor_principal_kind="account",
        actor_principal_identity=str(ACCOUNT.id),
        method=REQUEST.method,
        route=REQUEST.route,
        caller_key=REQUEST.caller_key,
        fingerprint=REQUEST.fingerprint,
        state=IdempotencyState.COMPLETED,
        response_status=201,
        response_body=b"{}",
        response_headers={},
        completed_at=NOW - timedelta(seconds=2),
        expires_at=NOW - timedelta(seconds=1),
    )
    unrelated = expired.model_copy(
        update={"id": uuid.uuid4(), "caller_key": "unrelated"}
    )
    repository.records[repository._get_scope(unrelated)] = unrelated
    repository.records[repository._get_scope(expired)] = expired
    service = IdempotencyService(
        repository=repository,
        wait_timeout_seconds=5,
        retention=timedelta(hours=24),
        cleanup_batch_size=1,
        clock=lambda: NOW,
    )

    decision = await service.begin(
        REQUEST.model_copy(update={"fingerprint": "b" * 64}),
        actor=AuthContext(account=ACCOUNT),
    )

    assert decision.kind is IdempotencyDecisionKind.EXECUTE
    assert repository.cleanup_calls == [(NOW, 1)]


async def test_different_actors_and_expired_records_can_reuse_a_key() -> None:
    """Isolate actors and reclaim a completed key after retention."""
    repository = FakeIdempotencyRepository()
    service = _service(repository)
    first_actor = AuthContext(account=ACCOUNT)
    first = await service.begin(REQUEST, actor=first_actor)
    assert first.reservation is not None
    completed = await service.complete(
        first.reservation,
        IdempotencyStoredResponse(status_code=201, body=b"{}", headers={}),
        actor=first_actor,
    )
    repository.records[repository._get_scope(completed)] = completed.model_copy(
        update={"expires_at": NOW - timedelta(seconds=1)}
    )

    reused = await service.begin(
        REQUEST.model_copy(update={"fingerprint": "b" * 64}), actor=first_actor
    )
    assert reused.kind is IdempotencyDecisionKind.EXECUTE

    other_actor = AuthContext(account=Account(id=uuid.uuid4(), name="other"))
    independent = await service.begin(REQUEST, actor=other_actor)
    assert independent.kind is IdempotencyDecisionKind.EXECUTE
