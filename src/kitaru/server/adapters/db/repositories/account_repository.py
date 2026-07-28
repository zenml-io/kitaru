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
"""SQL account repository."""

import uuid

from sqlalchemy import select

from kitaru.server.adapters.db.orm.account import (
    ACCOUNT_NAME_UNIQUE_CONSTRAINT,
    AccountORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.base import NotFoundError


class SQLAccountRepository(BaseSQLRepository[AccountORM]):
    """Account repository backed by the application database."""

    orm_class = AccountORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return AccountNotFound(entity_id)

    async def create(self, account: Account) -> Account:
        """Persist a new account.

        Args:
            account: Account to store.

        Raises:
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with timestamps set.
        """
        row = AccountORM.from_domain(account)
        await self._add(
            row,
            {
                ACCOUNT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAccountName(
                    account.name
                )
            },
        )
        return row.to_domain()

    async def get(self, account_id: uuid.UUID) -> Account:
        """Load an account by id.

        Args:
            account_id: Id of the account.

        Raises:
            AccountNotFound: No account has this id.

        Returns:
            Stored account.
        """
        row = await self._get_row(account_id)
        return row.to_domain()

    async def get_by_name(self, name: str, is_service_account: bool = False) -> Account:
        """Load an account by name.

        Args:
            name: Name of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this name.

        Returns:
            Stored account.
        """
        statement = select(AccountORM).where(
            AccountORM.name == name,
            AccountORM.is_service_account == is_service_account,
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise AccountNotFound(name)
        return row.to_domain()

    async def get_by_external_id(
        self, external_id: uuid.UUID, is_service_account: bool = False
    ) -> Account:
        """Load an account by external id.

        Args:
            external_id: External id of the account.
            is_service_account: Whether to look up a service account.

        Raises:
            AccountNotFound: No account has this external id.

        Returns:
            Stored account.
        """
        statement = select(AccountORM).where(
            AccountORM.external_id == external_id,
            AccountORM.is_service_account == is_service_account,
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise AccountNotFound(external_id)
        return row.to_domain()

    async def query(self, account_filter: AccountFilter) -> tuple[list[Account], int]:
        """Query accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.

        Returns:
            Page of matching accounts and the total match count.
        """
        statement = select(AccountORM)
        if account_filter.name is not None:
            statement = statement.where(AccountORM.name == account_filter.name)
        if account_filter.active is not None:
            statement = statement.where(AccountORM.active == account_filter.active)
        rows, total = await paginate(
            self._session,
            statement,
            order_by=AccountORM.id,
            page=account_filter.page,
            page_size=account_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def update(self, account: Account) -> Account:
        """Persist changes to an existing account.

        Args:
            account: Account with modified fields.

        Raises:
            AccountNotFound: No account has this id.
            DuplicateAccountName: The account name is already registered.

        Returns:
            Stored account with the updated timestamp renewed.
        """
        row = await self._get_row(account.id)
        row.is_service_account = account.is_service_account
        row.external_id = account.external_id
        row.name = account.name
        row.email = account.email
        row.password_hash = account.password_hash
        row.active = account.active
        await self._flush(
            {ACCOUNT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAccountName(account.name)}
        )
        return row.to_domain()
