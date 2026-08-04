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
"""PostgreSQL repository for scoped idempotency."""

import uuid
from datetime import datetime

from sqlalchemy import Select, delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.orm.idempotency_record import (
    IDEMPOTENCY_RECORD_SCOPE_UNIQUE,
    IdempotencyRecordORM,
)
from kitaru.server.application.models.idempotency import (
    IdempotencyClaim,
    IdempotencyClaimKind,
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.domain.idempotency import (
    IdempotencyRecord,
    IdempotencyRecordStateError,
    IdempotencyState,
)

_LOCK_TIMEOUT_SQLSTATE = "55P03"


def _is_lock_timeout(error: DBAPIError) -> bool:
    """Whether a database error was raised by PostgreSQL lock_timeout.

    Args:
        error: SQLAlchemy database error.

    Returns:
        Whether any wrapped driver error has SQLSTATE 55P03.
    """
    cause: BaseException | None = error.orig
    while cause is not None:
        if getattr(cause, "sqlstate", None) == _LOCK_TIMEOUT_SQLSTATE:
            return True
        cause = cause.__cause__
    return False


class SQLIdempotencyRepository:
    """Store idempotency records in the request transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Request-scoped database session.
        """
        self._session = session

    async def cleanup_expired(self, now: datetime, limit: int) -> int:
        """Delete a bounded batch of expired completed records.

        Args:
            now: Expiry cutoff.
            limit: Maximum records to delete.

        Returns:
            Number of deleted records.
        """
        ids_statement = (
            select(IdempotencyRecordORM.id)
            .where(
                IdempotencyRecordORM.state == IdempotencyState.COMPLETED.value,
                IdempotencyRecordORM.expires_at <= now,
            )
            .order_by(IdempotencyRecordORM.expires_at, IdempotencyRecordORM.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        record_ids = list((await self._session.scalars(ids_statement)).all())
        if not record_ids:
            return 0
        statement = (
            delete(IdempotencyRecordORM)
            .where(IdempotencyRecordORM.id.in_(record_ids))
            .returning(IdempotencyRecordORM.id)
        )
        return len((await self._session.scalars(statement)).all())

    async def reserve(
        self,
        record: IdempotencyRecord,
        wait_timeout_seconds: float,
    ) -> IdempotencyClaim:
        """Reserve a unique scope with bounded conflict waiting.

        Args:
            record: Pending record to insert.
            wait_timeout_seconds: PostgreSQL lock wait bound.

        Returns:
            Owned, existing, or timed-out claim.
        """
        row = IdempotencyRecordORM.from_domain(record)
        values = {
            column.name: getattr(row, column.name)
            for column in IdempotencyRecordORM.__table__.columns
            if column.name not in {"created", "updated"}
        }
        timeout_ms = max(1, round(wait_timeout_seconds * 1000))
        try:
            async with self._session.begin_nested():
                await self._session.execute(
                    select(func.set_config("lock_timeout", str(timeout_ms), True))
                )
                statement = (
                    insert(IdempotencyRecordORM)
                    .values(**values)
                    .on_conflict_do_nothing(constraint=IDEMPOTENCY_RECORD_SCOPE_UNIQUE)
                    .returning(IdempotencyRecordORM)
                )
                inserted = (await self._session.scalars(statement)).one_or_none()
                await self._session.execute(
                    select(func.set_config("lock_timeout", "0", True))
                )
        except DBAPIError as error:
            if _is_lock_timeout(error):
                return IdempotencyClaim(kind=IdempotencyClaimKind.TIMED_OUT)
            raise
        if inserted is not None:
            return IdempotencyClaim(
                kind=IdempotencyClaimKind.OWNED,
                record=inserted.to_domain(),
            )
        existing = await self.get(
            record.actor_account_id,
            record.actor_principal_kind,
            record.actor_principal_identity,
            IdempotencyRequest(
                method=record.method,
                route=record.route,
                caller_key=record.caller_key,
                fingerprint=record.fingerprint,
            ),
        )
        if existing is None:
            raise IdempotencyRecordStateError(
                "Conflicting idempotency record disappeared"
            )
        return IdempotencyClaim(
            kind=IdempotencyClaimKind.EXISTING,
            record=existing,
        )

    async def get(
        self,
        actor_account_id: uuid.UUID,
        actor_principal_kind: str,
        actor_principal_identity: str,
        request: IdempotencyRequest,
    ) -> IdempotencyRecord | None:
        """Load a record using its unique scope.

        Args:
            actor_account_id: Owning account.
            actor_principal_kind: Principal kind.
            actor_principal_identity: Principal-specific identity.
            request: Canonical request scope.

        Returns:
            Authoritative record, if present.
        """
        statement = (
            select(IdempotencyRecordORM)
            .where(
                IdempotencyRecordORM.actor_account_id == actor_account_id,
                IdempotencyRecordORM.actor_principal_kind == actor_principal_kind,
                IdempotencyRecordORM.actor_principal_identity
                == actor_principal_identity,
                IdempotencyRecordORM.method == request.method,
                IdempotencyRecordORM.route == request.route,
                IdempotencyRecordORM.caller_key == request.caller_key,
            )
            .execution_options(populate_existing=True)
        )
        return await self._get_optional_domain(statement)

    async def _get_optional_domain(
        self, statement: Select[tuple[IdempotencyRecordORM]]
    ) -> IdempotencyRecord | None:
        """Execute a single-row statement and convert it.

        Args:
            statement: SQLAlchemy select statement.

        Returns:
            Stored domain record, if present.
        """
        row = (await self._session.scalars(statement)).one_or_none()
        return None if row is None else row.to_domain()

    async def delete_expired(
        self,
        record: IdempotencyRecord,
        now: datetime,
        wait_timeout_seconds: float,
    ) -> bool:
        """Delete a completed record only after its expiry.

        Args:
            record: Existing record.
            now: Expiry cutoff.
            wait_timeout_seconds: PostgreSQL lock wait bound.

        Returns:
            Whether the record was deleted before the lock wait elapsed.
        """
        statement = (
            delete(IdempotencyRecordORM)
            .where(
                IdempotencyRecordORM.id == record.id,
                IdempotencyRecordORM.state == IdempotencyState.COMPLETED.value,
                IdempotencyRecordORM.expires_at <= now,
            )
            .returning(IdempotencyRecordORM.id)
        )
        timeout_ms = max(1, round(wait_timeout_seconds * 1000))
        try:
            async with self._session.begin_nested():
                await self._session.execute(
                    select(func.set_config("lock_timeout", str(timeout_ms), True))
                )
                deleted_id = await self._session.scalar(statement)
                await self._session.execute(
                    select(func.set_config("lock_timeout", "0", True))
                )
        except DBAPIError as error:
            if _is_lock_timeout(error):
                return False
            raise
        return deleted_id is not None

    async def complete(
        self,
        reservation: IdempotencyReservation,
        response: IdempotencyStoredResponse,
        completed_at: datetime,
        expires_at: datetime,
    ) -> IdempotencyRecord:
        """Complete a pending reservation in the current transaction.

        Args:
            reservation: Owned request reservation.
            response: Exact replayable response.
            completed_at: Completion time.
            expires_at: Expiry time.

        Raises:
            IdempotencyRecordStateError: The reservation is missing or invalid.

        Returns:
            Completed record.
        """
        statement = select(IdempotencyRecordORM).where(
            IdempotencyRecordORM.id == reservation.record_id,
            IdempotencyRecordORM.actor_account_id == reservation.actor.account_id,
            IdempotencyRecordORM.actor_principal_kind
            == reservation.actor.principal_kind,
            IdempotencyRecordORM.actor_principal_identity
            == reservation.actor.principal_identity,
            IdempotencyRecordORM.method == reservation.request.method,
            IdempotencyRecordORM.route == reservation.request.route,
            IdempotencyRecordORM.caller_key == reservation.request.caller_key,
            IdempotencyRecordORM.fingerprint == reservation.request.fingerprint,
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None or row.state != IdempotencyState.PENDING.value:
            raise IdempotencyRecordStateError(
                "Idempotency reservation is missing or already completed"
            )
        row.state = IdempotencyState.COMPLETED.value
        row.response_status = response.status_code
        row.response_body = response.body
        row.response_headers = dict(response.headers)
        row.completed_at = completed_at
        row.expires_at = expires_at
        await self._session.flush()
        return row.to_domain()
