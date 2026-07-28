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
"""SQL API key repository."""

import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.api_key import (
    API_KEY_NAME_UNIQUE_CONSTRAINT,
    ApiKeyORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.application.models.api_keys import ApiKeyFilter
from kitaru.server.domain.api_key import (
    ApiKey,
    ApiKeyNotFound,
    DuplicateApiKeyName,
)


class SQLApiKeyRepository:
    """API key repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, api_key: ApiKey) -> ApiKey:
        """Persist a new API key.

        Args:
            api_key: API key to store.

        Raises:
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with timestamps set.
        """
        row = ApiKeyORM.from_domain(api_key)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == API_KEY_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateApiKeyName(api_key.name) from exc
            raise
        return row.to_domain()

    async def get(self, api_key_id: uuid.UUID) -> ApiKey:
        """Load an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.

        Returns:
            Stored API key.
        """
        row = await self._session.get(ApiKeyORM, api_key_id)
        if row is None:
            raise ApiKeyNotFound(api_key_id)
        return row.to_domain()

    async def query(self, api_key_filter: ApiKeyFilter) -> tuple[list[ApiKey], int]:
        """Query API keys matching a filter.

        Args:
            api_key_filter: Filter and pagination parameters.

        Returns:
            Page of matching API keys and the total match count.
        """
        statement = select(ApiKeyORM)
        if api_key_filter.name is not None:
            statement = statement.where(ApiKeyORM.name == api_key_filter.name)
        if api_key_filter.owner_id is not None:
            statement = statement.where(
                ApiKeyORM.owner_id == api_key_filter.owner_id
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=ApiKeyORM.id,
            page=api_key_filter.page,
            page_size=api_key_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, api_key: ApiKey) -> ApiKey:
        """Persist changes to an existing API key.

        Args:
            api_key: API key with modified fields.

        Raises:
            ApiKeyNotFound: No API key has this id.
            DuplicateApiKeyName: The API key name is already registered.

        Returns:
            Stored API key with the updated timestamp renewed.
        """
        row = await self._session.get(ApiKeyORM, api_key.id)
        if row is None:
            raise ApiKeyNotFound(api_key.id)
        row.owner_id = api_key.owner_id
        row.name = api_key.name
        row.key_hash = api_key.key_hash
        row.active = api_key.active
        row.last_used = api_key.last_used
        try:
            async with self._session.begin_nested():
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == API_KEY_NAME_UNIQUE_CONSTRAINT:
                raise DuplicateApiKeyName(api_key.name) from exc
            raise
        return row.to_domain()

    async def delete(self, api_key_id: uuid.UUID) -> None:
        """Delete an API key by id.

        Args:
            api_key_id: Id of the API key.

        Raises:
            ApiKeyNotFound: No API key has this id.
        """
        row = await self._session.get(ApiKeyORM, api_key_id)
        if row is None:
            raise ApiKeyNotFound(api_key_id)
        await self._session.delete(row)
        await self._session.flush()
