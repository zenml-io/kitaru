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
"""Authentication service for direct Kitaru server access."""

import secrets
import uuid
from datetime import UTC, datetime

from anyio import to_thread

from kitaru.server.adapters.auth.jwt import JWTToken, TokenError
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.interfaces.api_key_repository import (
    ApiKeyRepository,
)
from kitaru.server.application.interfaces.password_hasher import PasswordHasher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account, AccountNotFound
from kitaru.server.domain.api_key import (
    API_KEY_PREFIX,
    ApiKey,
    ApiKeyNotFound,
    InvalidApiKey,
    decode_api_key,
    hash_secret,
)

# Skip the last_used write while the stored value is this fresh, so requests
# sharing an API key do not serialize on its row lock.
LAST_USED_UPDATE_INTERVAL_SECONDS = 60


class AuthenticationError(Exception):
    """Raised when request authentication fails."""


class AuthService:
    """Resolve bearer credentials into request contexts."""

    def __init__(
        self,
        settings: APISettings,
        account_repository: AccountRepository,
        api_key_repository: ApiKeyRepository,
        password_hasher: PasswordHasher,
    ) -> None:
        """Create an authentication service.

        Args:
            settings: Runtime settings for this server.
            account_repository: Account repository.
            api_key_repository: API key repository.
            password_hasher: Password hasher for login credentials.
        """
        self._settings = settings
        self._account_repository = account_repository
        self._api_key_repository = api_key_repository
        self._password_hasher = password_hasher

    async def resolve(
        self,
        credential: str,
        csrf_token: str | None = None,
    ) -> AuthContext:
        """Authenticate a bearer credential for API route handling.

        Args:
            credential: Bearer token supplied by the caller.
            csrf_token: CSRF token supplied alongside the bearer token.

        Raises:
            AuthenticationError: The credential cannot be validated.

        Returns:
            Request context accepted by this server.
        """
        if credential.startswith(API_KEY_PREFIX):
            context = await self._authenticate_api_key(credential)
        else:
            try:
                token = JWTToken.decode(credential, self._settings)
            except TokenError as exc:
                raise AuthenticationError("Invalid bearer credential.") from exc
            context = await self._resolve_token(token)
        if context.csrf_token is not None and not secrets.compare_digest(
            csrf_token or "", context.csrf_token
        ):
            raise AuthenticationError("Missing or invalid CSRF token.")
        return context

    async def resolve_default_account(self) -> AuthContext:
        """Build the auth context of the default account.

        Raises:
            AccountNotFound: The default account does not exist.

        Returns:
            Request context acting as the default account.
        """
        account = await self._account_repository.get_by_name(
            self._settings.DEFAULT_ACCOUNT_NAME
        )
        return AuthContext(account=account)

    async def login_with_password(
        self, username: str, password: str
    ) -> tuple[str, datetime, str | None]:
        """Authenticate an account by password and issue a session token.

        Args:
            username: Account name.
            password: Plaintext password.

        Raises:
            AuthenticationError: The credentials cannot be validated.

        Returns:
            Encoded bearer token, its expiry time, and the CSRF token when
            cookie authentication is configured.
        """
        try:
            account = await self._account_repository.get_by_name(username)
        except AccountNotFound:
            account = None
        if account is None or account.password_hash is None:
            await to_thread.run_sync(
                self._password_hasher.verify,
                password,
                self._password_hasher.dummy_hash,
            )
            raise AuthenticationError("Invalid username or password.")
        valid = await to_thread.run_sync(
            self._password_hasher.verify, password, account.password_hash
        )
        if not valid or not account.active:
            raise AuthenticationError("Invalid username or password.")
        csrf_token = None
        if self._settings.AUTH_COOKIE_NAME:
            csrf_token = secrets.token_hex(16)
        token, expires_at = self.issue_token(
            AuthContext(account=account), csrf_token=csrf_token
        )
        return token, expires_at, csrf_token

    def issue_token(
        self, context: AuthContext, csrf_token: str | None = None
    ) -> tuple[str, datetime]:
        """Issue a local session for an auth context.

        Args:
            context: Resolved context to store in the session token.
            csrf_token: CSRF token associated with a browser cookie session.

        Returns:
            Encoded bearer token and its expiry time.
        """
        token = JWTToken.from_auth_context(
            context,
            csrf_token=csrf_token,
        )
        expires_at = token.expires(self._settings)
        token = token.model_copy(update={"expires_at": expires_at})
        return token.encode(self._settings), expires_at

    async def _authenticate_api_key(self, credential: str) -> AuthContext:
        try:
            key_id, secret = decode_api_key(credential)
        except InvalidApiKey as exc:
            raise AuthenticationError("Invalid API key.") from exc
        try:
            api_key = await self._api_key_repository.get(key_id)
        except ApiKeyNotFound as exc:
            raise AuthenticationError("Invalid API key.") from exc
        if not secrets.compare_digest(hash_secret(secret), api_key.key_hash):
            raise AuthenticationError("Invalid API key.")
        if not api_key.active:
            raise AuthenticationError("Invalid API key.")
        account = await self._load_active_account(api_key.owner_id, "Invalid API key.")
        await self._touch_api_key(api_key)
        return AuthContext(account=account)

    async def _touch_api_key(self, api_key: ApiKey) -> None:
        now = datetime.now(UTC)
        if (
            api_key.last_used is not None
            and (now - api_key.last_used).total_seconds()
            < LAST_USED_UPDATE_INTERVAL_SECONDS
        ):
            return
        api_key.mark_used(now)
        await self._api_key_repository.update(api_key)

    async def _resolve_token(self, token: JWTToken) -> AuthContext:
        account = await self._load_active_account(
            token.account_id, "Invalid session token."
        )
        return AuthContext(account=account, csrf_token=token.csrf_token)

    async def _load_active_account(
        self, account_id: uuid.UUID, message: str
    ) -> Account:
        try:
            account = await self._account_repository.get(account_id)
        except AccountNotFound as exc:
            raise AuthenticationError(message) from exc
        if not account.active:
            raise AuthenticationError(message)
        return account
