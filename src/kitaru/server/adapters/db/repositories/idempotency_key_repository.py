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
"""SQL idempotency key repository."""

import uuid
from datetime import datetime

from sqlalchemy import CursorResult, delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.orm.idempotency_key import (
    IDEMPOTENCY_KEY_ACCOUNT_ID_KEY_UNIQUE_CONSTRAINT,
    IdempotencyKeyORM,
)
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.domain.idempotency_key import (
    IdempotencyKey,
    IdempotencyKeyAlreadyExists,
)


class SQLIdempotencyKeyRepository(BaseSQLRepository[IdempotencyKeyORM]):
    """Idempotency key repository backed by the application database."""

    orm_class = IdempotencyKeyORM

    def __init__(self, session: AsyncSession, cipher: AesGcmCipher) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
            cipher: Cipher for response bodies stored encrypted at rest.
        """
        super().__init__(session)
        self._cipher = cipher

    async def create(self, idempotency_key: IdempotencyKey) -> IdempotencyKey:
        """Persist a new idempotency key.

        Args:
            idempotency_key: Idempotency key to store.

        Raises:
            IdempotencyKeyAlreadyExists: The account already has this key.

        Returns:
            Stored idempotency key with the created timestamp set.
        """
        row = IdempotencyKeyORM.from_domain(idempotency_key)
        await self._add(
            row,
            {
                IDEMPOTENCY_KEY_ACCOUNT_ID_KEY_UNIQUE_CONSTRAINT: lambda: (
                    IdempotencyKeyAlreadyExists(
                        idempotency_key.account_id, idempotency_key.key
                    )
                )
            },
        )
        return row.to_domain()

    async def get(
        self, account_id: uuid.UUID, key: str, encrypted: bool = False
    ) -> IdempotencyKey | None:
        """Load an idempotency key by account and key.

        Args:
            account_id: Id of the account the key is scoped to.
            key: Idempotency key.
            encrypted: Whether the stored response body is encrypted at rest.

        Returns:
            Stored idempotency key, or ``None`` when no row matches.
        """
        statement = select(IdempotencyKeyORM).where(
            IdempotencyKeyORM.account_id == account_id, IdempotencyKeyORM.key == key
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            return None
        idempotency_key = row.to_domain()
        if encrypted and idempotency_key.response_body is not None:
            idempotency_key.response_body = self._cipher.decrypt_bytes(
                idempotency_key.response_body
            )
        return idempotency_key

    async def store_response(
        self,
        idempotency_key_id: uuid.UUID,
        response_status: int,
        response_body: bytes,
        response_content_type: str | None,
        encrypt: bool = False,
    ) -> None:
        """Record the response a request committed under this key.

        Args:
            idempotency_key_id: Id of the idempotency key.
            response_status: HTTP status code of the committed response.
            response_body: Raw response body.
            response_content_type: Content type of the response, when set.
            encrypt: Whether to store the body encrypted at rest.
        """
        if encrypt:
            response_body = self._cipher.encrypt_bytes(response_body)
        statement = (
            update(IdempotencyKeyORM)
            .where(IdempotencyKeyORM.id == idempotency_key_id)
            .values(
                response_status=response_status,
                response_body=response_body,
                response_content_type=response_content_type,
            )
        )
        await self._session.execute(statement)

    async def delete_expired(self, cutoff: datetime, limit: int) -> int:
        """Delete idempotency keys created before a cutoff, up to a limit.

        Args:
            cutoff: Rows created before this time are eligible for deletion.
            limit: Maximum number of rows to delete in this batch.

        Returns:
            Number of deleted rows.
        """
        if limit <= 0:
            return 0
        candidates = (
            select(IdempotencyKeyORM.id)
            .where(IdempotencyKeyORM.created < cutoff)
            .limit(limit)
        )
        statement = delete(IdempotencyKeyORM).where(
            IdempotencyKeyORM.id.in_(candidates)
        )
        result = await self._session.execute(statement)
        return result.rowcount if isinstance(result, CursorResult) else 0
