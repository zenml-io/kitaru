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
from typing import Any

from anyio import to_thread

from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.interfaces.password_hasher import PasswordHasher
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.permissions import Action, ResourceType
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.domain.account import (
    Account,
    AccountNotFound,
    DuplicateAccountName,
)
from kitaru.server.domain.base import ForbiddenError
from kitaru.server.domain.keys import generate_secret, hash_secret, verify_secret


class AccountService:
    """Account use cases."""

    def __init__(
        self,
        repository: AccountRepository,
        password_hasher: PasswordHasher,
        permission_service: PermissionService,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Account repository.
            password_hasher: Password hasher for login credentials.
            permission_service: Permission service for authorization checks.
        """
        self._repository = repository
        self._password_hasher = password_hasher
        self._permission_service = permission_service

    async def create_account(
        self,
        name: str,
        email: str | None,
        password: str | None,
        is_admin: bool,
        actor: AuthContext,
    ) -> tuple[Account, str | None]:
        """Create an account, active with a password or pending activation.

        Args:
            name: Account name.
            email: Contact email.
            password: Login password, hashed before storage.
            is_admin: Whether the account has admin rights.
            actor: Caller context.

        Raises:
            ForbiddenError: The caller may not create accounts.
            DuplicateAccountName: The account name is already registered.

        Returns:
            Created account and its activation token when one was generated.
        """
        await self._permission_service.check(actor, ResourceType.ACCOUNT, Action.CREATE)
        if password is not None:
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
            account = Account(
                name=name, email=email, password_hash=password_hash, is_admin=is_admin
            )
            return await self._repository.create(account), None
        activation_token = generate_secret()
        account = Account(
            name=name,
            email=email,
            active=False,
            activation_token_hash=hash_secret(activation_token),
            is_admin=is_admin,
        )
        return await self._repository.create(account), activation_token

    async def ensure_account(self, name: str, password: str | None) -> Account:
        """Create an account at startup when it does not exist.

        Args:
            name: Account name.
            password: Login password, hashed before storage.

        Returns:
            Stored account.
        """
        try:
            account = await self._repository.get_by_name(name)
        except AccountNotFound:
            account = None
        if account is not None:
            if account.is_admin:
                return account
            account.update_is_admin(True)
            return await self._repository.update(account)
        password_hash = None
        if password is not None:
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
        try:
            return await self._repository.create(
                Account(name=name, password_hash=password_hash, is_admin=True)
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
    ) -> tuple[list[Account], str | None]:
        """List accounts matching a filter.

        Args:
            account_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching accounts and the next cursor.
        """
        _ = actor
        return await self._repository.query(account_filter)

    async def update_account(
        self,
        account_id: uuid.UUID,
        password: str | None,
        old_password: str | None,
        metadata: dict[str, Any] | None,
        is_admin: bool | None,
        actor: AuthContext,
    ) -> Account:
        """Partially update an account.

        Args:
            account_id: Id of the account.
            password: New login password, unchanged when ``None``.
            old_password: Current login password, required to set a new one.
            metadata: New metadata, unchanged when ``None``.
            is_admin: New admin rights state, unchanged when ``None``.
            actor: Caller context.

        Raises:
            AccountNotFound: No account has this id.
            ForbiddenError: The caller may not set admin rights, writes its
                own admin flag or another account's password or metadata,
                the current password is missing or does not match, or the
                target account is a service account.

        Returns:
            Updated account.
        """
        account = await self._repository.get(account_id)
        if account_id != actor.account.id:
            if password is not None:
                raise ForbiddenError(
                    "Accounts cannot change the password of other accounts"
                )
            if metadata is not None:
                raise ForbiddenError("Accounts can only update their own metadata")
        if metadata is not None:
            account.update_metadata(metadata)
        if is_admin is not None:
            await self._permission_service.check(
                actor, ResourceType.ACCOUNT, Action.SET_ADMIN, resource_id=account_id
            )
            if account_id == actor.account.id:
                raise ForbiddenError("Accounts cannot change their own admin flag")
            if account.is_service_account:
                raise ForbiddenError("Service accounts cannot be admins")
            account.update_is_admin(is_admin)
        if password is not None:
            if old_password is None:
                raise ForbiddenError(
                    "The current password must be supplied when changing the password"
                )
            stored_hash = account.password_hash or self._password_hasher.dummy_hash
            verified = await to_thread.run_sync(
                self._password_hasher.verify, old_password, stored_hash
            )
            if not verified:
                raise ForbiddenError("The current password is incorrect")
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
            account.update_password_hash(password_hash)
        return await self._repository.update(account)

    async def deactivate_account(
        self, account_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Account, str]:
        """Deactivate an account and mint a fresh activation token for it.

        Args:
            account_id: Id of the account.
            actor: Caller context.

        Raises:
            AccountNotFound: No account has this id.
            ForbiddenError: The caller may not deactivate this account or is
                deactivating itself.

        Returns:
            Deactivated account and its activation token.
        """
        await self._permission_service.check(
            actor, ResourceType.ACCOUNT, Action.DEACTIVATE, resource_id=account_id
        )
        if account_id == actor.account.id:
            raise ForbiddenError("Accounts cannot deactivate themselves")
        account = await self._repository.get(account_id)
        activation_token = generate_secret()
        account.update_active(False)
        account.update_activation_token_hash(hash_secret(activation_token))
        return await self._repository.update(account), activation_token

    async def activate_account(
        self, account_id: uuid.UUID, activation_token: str, password: str
    ) -> Account:
        """Activate an account with its activation token and a new password.

        Args:
            account_id: Id of the account.
            activation_token: Activation token issued for the account.
            password: Login password to set.

        Raises:
            AccountNotFound: No account has this id.
            ForbiddenError: The account has no pending token or it does not match.

        Returns:
            Activated account.
        """
        account = await self._repository.get(account_id)
        if account.activation_token_hash is None or not verify_secret(
            activation_token, account.activation_token_hash
        ):
            raise ForbiddenError("The activation token is incorrect")
        password_hash = await to_thread.run_sync(self._password_hasher.hash, password)
        account.update_password_hash(password_hash)
        account.update_activation_token_hash(None)
        account.update_active(True)
        return await self._repository.update(account)
