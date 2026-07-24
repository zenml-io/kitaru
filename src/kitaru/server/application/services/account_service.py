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
"""Account use cases."""

import uuid

from anyio import to_thread

from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.interfaces.password_hasher import PasswordHasher
from kitaru.server.application.models.accounts import (
    AccountFilter,
    AccountUpdate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
    InvalidAccount,
)


class AccountService:
    """Account use cases."""

    def __init__(
        self, repository: AccountRepository, password_hasher: PasswordHasher
    ) -> None:
        """Initialize the service.

        Args:
            repository: Account repository.
            password_hasher: Password hasher for login credentials.
        """
        self._repository = repository
        self._password_hasher = password_hasher

    async def create_account(
        self,
        name: str,
        email: str | None,
        password: str | None,
        actor: AuthContext,
    ) -> Account:
        """Create an account.

        Args:
            name: Account name.
            email: Contact email.
            password: Login password, hashed before storage.
            actor: Caller context.

        Raises:
            DuplicateAccountName: The account name is already registered.

        Returns:
            Created account.
        """
        _ = actor
        password_hash = None
        if password is not None:
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
        account = Account(name=name, email=email, password_hash=password_hash)
        return await self._repository.create(account)

    async def ensure_account(self, name: str, password: str | None) -> Account:
        """Create an account at startup when it does not exist.

        Args:
            name: Account name.
            password: Login password, hashed before storage.

        Returns:
            Stored account.
        """
        try:
            return await self._repository.get_by_name(name)
        except AccountNotFound:
            pass
        password_hash = None
        if password is not None:
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
        try:
            return await self._repository.create(
                Account(name=name, password_hash=password_hash)
            )
        except DuplicateAccountName:
            return await self._repository.get_by_name(name)

    async def get_account(self, account_id: uuid.UUID, actor: AuthContext) -> Account:
        """Get an account by id.

        Args:
            account_id: Id of the account.
            actor: Caller context.

        Raises:
            AccountNotFound: No account has this id.

        Returns:
            Stored account.
        """
        _ = actor
        return await self._repository.get(account_id)

    async def list_accounts(
        self, account_filter: AccountFilter, actor: AuthContext
    ) -> tuple[list[Account], int]:
        """List accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching accounts and the total match count.
        """
        _ = actor
        return await self._repository.query(account_filter)

    async def update_account(
        self,
        account_id: uuid.UUID,
        command: AccountUpdate,
        actor: AuthContext,
    ) -> Account:
        """Partially update an account.

        Fields absent from the command stay unchanged. An explicit null is
        rejected for the active state and the password.

        Args:
            account_id: Id of the account.
            command: Account update command.
            actor: Caller context.

        Raises:
            AccountNotFound: No account has this id.
            InvalidAccount: The active state or the password is null.

        Returns:
            Updated account.
        """
        _ = actor
        account = await self._repository.get(account_id)
        if "active" in command.model_fields_set:
            if command.active is None:
                raise InvalidAccount("Account active state cannot be null")
            account.update_active(command.active)
        if "password" in command.model_fields_set:
            if command.password is None:
                raise InvalidAccount("Account password cannot be null")
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, command.password
            )
            account.update_password_hash(password_hash)
        return await self._repository.update(account)
