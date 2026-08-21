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

from kitaru.analytics.events import (
    FINISHED_ONBOARDING_SURVEY_KEY,
    AccountOrigin,
    AnalyticsEvent,
)
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.interfaces.password_hasher import PasswordHasher
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.permissions import Action, ResourceType
from kitaru.server.application.services.analytics_events import (
    build_account_created_properties,
    build_account_traits,
    build_user_enriched_properties,
)
from kitaru.server.application.services.permission_service import PermissionService
from kitaru.server.application.services.server_analytics import ServerAnalytics
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
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Account repository.
            password_hasher: Password hasher for login credentials.
            permission_service: Permission service for authorization checks.
            analytics: Analytics tracker, None skips tracking.
        """
        self._repository = repository
        self._password_hasher = password_hasher
        self._permission_service = permission_service
        self._analytics = analytics

    def _identify(self, account: Account, origin: AccountOrigin) -> None:
        """Send the traits of an account.

        Args:
            account: Account to identify.
            origin: Where the account was created.
        """
        if self._analytics is None:
            return
        self._analytics.identify(account.id, build_account_traits(account, origin))

    def _track_account_created(self, account: Account, origin: AccountOrigin) -> None:
        """Track the creation of an account.

        Args:
            account: Created account.
            origin: Where the account was created.
        """
        if self._analytics is None:
            return
        self._analytics.track(
            account.id,
            AnalyticsEvent.ACCOUNT_CREATED,
            build_account_created_properties(account, origin),
        )

    async def create_user(
        self,
        name: str,
        email: str | None,
        password: str | None,
        is_admin: bool,
        actor: AuthContext,
    ) -> tuple[Account, str | None]:
        """Create a user, active with a password or pending activation.

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
        activation_token = None
        if password is not None:
            password_hash = await to_thread.run_sync(
                self._password_hasher.hash, password
            )
            account = Account(
                name=name, email=email, password_hash=password_hash, is_admin=is_admin
            )
        else:
            activation_token = generate_secret()
            account = Account(
                name=name,
                email=email,
                active=False,
                activation_token_hash=hash_secret(activation_token),
                is_admin=is_admin,
            )
        account = await self._repository.create(account)
        self._identify(account, AccountOrigin.API)
        self._track_account_created(account, AccountOrigin.API)
        return account, activation_token

    async def create_service_account(
        self, name: str, email: str | None, actor: AuthContext
    ) -> Account:
        """Create a service account, active without credentials.

        Args:
            name: Account name.
            email: Contact email.
            actor: Caller context.

        Raises:
            ForbiddenError: The caller may not create accounts.
            DuplicateAccountName: The account name is already registered.

        Returns:
            Created account.
        """
        await self._permission_service.check(actor, ResourceType.ACCOUNT, Action.CREATE)
        account = await self._repository.create(
            Account(name=name, email=email, is_service_account=True)
        )
        self._identify(account, AccountOrigin.API)
        self._track_account_created(account, AccountOrigin.API)
        return account

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
            if not account.is_admin:
                account.update_is_admin(True)
                account = await self._repository.update(account)
        else:
            password_hash = None
            if password is not None:
                password_hash = await to_thread.run_sync(
                    self._password_hasher.hash, password
                )
            try:
                account = await self._repository.create(
                    Account(name=name, password_hash=password_hash, is_admin=True)
                )
                self._identify(account, AccountOrigin.BOOTSTRAP)
                self._track_account_created(account, AccountOrigin.BOOTSTRAP)
            except DuplicateAccountName:
                account = await self._repository.get_by_name(name)
        return account

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

    async def update_user(
        self,
        account_id: uuid.UUID,
        password: str | None,
        old_password: str | None,
        metadata: dict[str, Any] | None,
        is_admin: bool | None,
        actor: AuthContext,
    ) -> Account:
        """Partially update a user.

        Args:
            account_id: Id of the account.
            password: New login password, unchanged when ``None``.
            old_password: Current login password, required to set a new one.
            metadata: New metadata, unchanged when ``None``.
            is_admin: New admin rights state, unchanged when ``None``.
            actor: Caller context.

        Raises:
            AccountNotFound: No user has this id.
            ForbiddenError: The caller may not set admin rights, writes its
                own admin flag or another account's password or metadata,
                or the current password is missing or does not match.

        Returns:
            Updated account.
        """
        account = await self._repository.get(account_id, is_service_account=False)
        if account_id != actor.account.id:
            if password is not None:
                raise ForbiddenError(
                    "Accounts cannot change the password of other accounts"
                )
            if metadata is not None:
                raise ForbiddenError("Accounts can only update their own metadata")
        survey_finished_before = FINISHED_ONBOARDING_SURVEY_KEY in account.metadata
        if metadata is not None:
            account.update_metadata(metadata)
        if is_admin is not None:
            await self._permission_service.check(
                actor, ResourceType.ACCOUNT, Action.SET_ADMIN, resource_id=account_id
            )
            if account_id == actor.account.id:
                raise ForbiddenError("Accounts cannot change their own admin flag")
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
        account = await self._repository.update(account)
        if (
            self._analytics is not None
            and not survey_finished_before
            and FINISHED_ONBOARDING_SURVEY_KEY in account.metadata
        ):
            self._analytics.track(
                account.id,
                AnalyticsEvent.USER_ENRICHED,
                build_user_enriched_properties(account),
            )
        return account

    async def update_service_account(
        self,
        account_id: uuid.UUID,
        metadata: dict[str, Any] | None,
        active: bool | None,
        actor: AuthContext,
    ) -> Account:
        """Partially update a service account.

        Args:
            account_id: Id of the account.
            metadata: New metadata, unchanged when ``None``.
            active: New active state, unchanged when ``None``.
            actor: Caller context.

        Raises:
            AccountNotFound: No service account has this id.
            ForbiddenError: The caller may not update service accounts.

        Returns:
            Updated account.
        """
        await self._permission_service.check(
            actor, ResourceType.ACCOUNT, Action.UPDATE, resource_id=account_id
        )
        account = await self._repository.get(account_id, is_service_account=True)
        if metadata is not None:
            account.update_metadata(metadata)
        if active is not None:
            account.update_active(active)
        return await self._repository.update(account)

    async def deactivate_user(
        self, account_id: uuid.UUID, actor: AuthContext
    ) -> tuple[Account, str]:
        """Deactivate a user and mint a fresh activation token for it.

        Args:
            account_id: Id of the account.
            actor: Caller context.

        Raises:
            AccountNotFound: No user has this id.
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
        account = await self._repository.get(account_id, is_service_account=False)
        activation_token = generate_secret()
        account.update_active(False)
        account.update_activation_token_hash(hash_secret(activation_token))
        return await self._repository.update(account), activation_token

    async def activate_user(
        self, account_id: uuid.UUID, activation_token: str, password: str
    ) -> Account:
        """Activate a user with its activation token and a new password.

        Args:
            account_id: Id of the account.
            activation_token: Activation token issued for the account.
            password: Login password to set.

        Raises:
            AccountNotFound: No user has this id.
            ForbiddenError: The account has no pending token or it does not match.

        Returns:
            Activated account.
        """
        account = await self._repository.get(account_id, is_service_account=False)
        if account.activation_token_hash is None or not verify_secret(
            activation_token, account.activation_token_hash
        ):
            raise ForbiddenError("The activation token is incorrect")
        password_hash = await to_thread.run_sync(self._password_hasher.hash, password)
        account.update_password_hash(password_hash)
        account.update_activation_token_hash(None)
        account.update_active(True)
        return await self._repository.update(account)
