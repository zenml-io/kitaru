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
"""Idempotency key repository interface."""

import uuid
from datetime import datetime
from typing import Protocol

from kitaru.server.domain.idempotency_key import IdempotencyKey


class IdempotencyKeyRepository(Protocol):
    """Idempotency key persistence operations."""

    async def create(self, idempotency_key: IdempotencyKey) -> IdempotencyKey:
        """Persist a new idempotency key.

        Args:
            idempotency_key: Idempotency key to store.

        Raises:
            IdempotencyKeyAlreadyExists: The account already has this key.

        Returns:
            Stored idempotency key with the created timestamp set.
        """
        ...

    async def get(self, account_id: uuid.UUID, key: str) -> IdempotencyKey | None:
        """Load an idempotency key by account and key.

        Args:
            account_id: Id of the account the key is scoped to.
            key: Idempotency key.

        Returns:
            Stored idempotency key, or ``None`` when no row matches.
        """
        ...

    async def store_response(
        self,
        idempotency_key_id: uuid.UUID,
        response_status: int,
        response_body: bytes,
        response_content_type: str | None,
    ) -> None:
        """Record the response a request committed under this key.

        Args:
            idempotency_key_id: Id of the idempotency key.
            response_status: HTTP status code of the committed response.
            response_body: Raw response body.
            response_content_type: Content type of the response, when set.
        """
        ...

    async def delete_expired(self, cutoff: datetime, limit: int) -> int:
        """Delete idempotency keys created before a cutoff, up to a limit.

        Args:
            cutoff: Rows created before this time are eligible for deletion.
            limit: Maximum number of rows to delete in this batch.

        Returns:
            Number of deleted rows.
        """
        ...
