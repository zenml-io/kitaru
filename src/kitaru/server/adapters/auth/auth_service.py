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

import logging
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

from anyio import to_thread

from kitaru.api_models.v1.auth import CONTROL_PLANE_API_KEY_PREFIX
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneError,
)
from kitaru.server.adapters.auth.jwt import (
    AccountSubject,
    JWTToken,
    TaskSubject,
    TokenError,
    WorkerSubject,
)
from kitaru.server.api.config import APISettings
from kitaru.server.application.interfaces.account_repository import (
    AccountRepository,
)
from kitaru.server.application.interfaces.api_key_repository import (
    ApiKeyRepository,
)
from kitaru.server.application.interfaces.password_hasher import PasswordHasher
from kitaru.server.application.models.auth import (
    AccountPrincipal,
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.account import Account, AccountNotFound
from kitaru.server.domain.api_key import (
    API_KEY_PREFIX,
    ApiKey,
    ApiKeyNotFound,
    InvalidApiKey,
    decode_api_key,
)
from kitaru.server.domain.device import DeviceError, DeviceNotFound
from kitaru.server.domain.keys import verify_secret
from kitaru.server.utils import is_stale, to_tz_aware

logger = logging.getLogger(__name__)

# Skip the last_used write while the stored value is this fresh, so requests
# sharing an API key do not serialize on its row lock.
LAST_USED_UPDATE_INTERVAL_SECONDS = 60


class AuthenticationError(Exception):
    """Raised when request authentication fails."""


class IssuedToken(NamedTuple):
    """Issued bearer token."""

    token: str
    expires_at: datetime
    csrf_token: str | None = None


class AuthService:
    """Resolve bearer credentials into request contexts."""

    def __init__(
        self,
        settings: APISettings,
        account_repository: AccountRepository,
        api_key_repository: ApiKeyRepository,
        password_hasher: PasswordHasher,
        device_service: DeviceService | None = None,
        control_plane: ControlPlaneAuthenticator | None = None,
    ) -> None:
        """Create an authentication service.

        Args:
            settings: Runtime settings for this server.
            account_repository: Account repository.
            api_key_repository: API key repository.
            password_hasher: Password hasher for login credentials.
            device_service: Device service backing the device authorization
                grant.
            control_plane: Control plane authenticator, set when the server
                runs the control plane auth scheme.
        """
        self._settings = settings
        self._account_repository = account_repository
        self._api_key_repository = api_key_repository
        self._password_hasher = password_hasher
        self._device_service = device_service
        self._control_plane = control_plane

    async def resolve(
        self,
        credential: str,
        csrf_token: str | None = None,
        from_cookie: bool = False,
    ) -> AuthContext:
        """Authenticate a bearer credential for API route handling.

        Args:
            credential: Bearer token supplied by the caller.
            csrf_token: CSRF token supplied alongside the bearer token.
            from_cookie: Whether the credential arrived in the auth cookie.

        Raises:
            AuthenticationError: The credential cannot be validated.

        Returns:
            Request context accepted by this server.
        """
        if self._settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            context = await self._resolve_control_plane_credential(credential)
        elif credential.startswith(API_KEY_PREFIX):
            context = await self._authenticate_api_key(credential)
        else:
            context = await self._resolve_session_token(credential)
        # Only a cookie rides along on a cross-site request, so only a cookie
        # needs the caller to prove it can read the login response.
        if from_cookie and (
            context.csrf_token is None
            or not secrets.compare_digest(csrf_token or "", context.csrf_token)
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

    async def login_with_password(self, username: str, password: str) -> IssuedToken:
        """Authenticate an account by password and issue a session token.

        Args:
            username: Account name.
            password: Plaintext password.

        Raises:
            AuthenticationError: The credentials cannot be validated.

        Returns:
            Issued token, carrying the CSRF token when cookie authentication
            is configured.
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
        return self._issue_session(AuthContext(account=account))

    async def login_with_api_key(self, credential: str) -> IssuedToken:
        """Exchange an API key for a session token.

        Args:
            credential: API key.

        Raises:
            AuthenticationError: The credential cannot be validated.

        Returns:
            Issued token.
        """
        context = await self._authenticate_api_key(credential)
        # The key is only checked here. A session token carries no reference to
        # the key it came from, so deactivating or deleting the key leaves the
        # tokens already issued from it valid until they expire.
        return self.issue_token(context)

    async def login_with_control_plane(self, credential: str) -> IssuedToken:
        """Exchange a control plane credential for a local session token.

        Args:
            credential: Control plane session token or API key.

        Raises:
            AuthenticationError: The credential cannot be validated.

        Returns:
            Issued token, carrying the CSRF token when cookie authentication
            is configured.
        """
        return self._issue_session(await self._authenticate_control_plane(credential))

    async def login_with_device(
        self, device_id: uuid.UUID, device_code: str
    ) -> IssuedToken:
        """Exchange a verified device code for a session token.

        Args:
            device_id: Id of the device.
            device_code: Plaintext device code held by the polling client.

        Raises:
            AuthenticationError: Device authentication is not configured.
            DeviceNotFound: No device has this id.
            DeviceLocked: The device is locked.
            DeviceExpired: The device authorization has expired.
            InvalidDeviceCode: The code does not match.
            DeviceAuthorizationPending: No account has approved the device yet.

        Returns:
            Issued token.
        """
        if self._device_service is None:
            raise AuthenticationError("Device authentication is not configured.")
        device = await self._device_service.authenticate_device(device_id, device_code)
        assert device.account_id is not None
        account = await self._load_active_account(
            device.account_id, "Invalid device code."
        )
        return self.issue_token(
            AuthContext(account=account),
            device_id=device.id,
            expires_at=device.expires,
        )

    def issue_token(
        self,
        context: AuthContext,
        csrf_token: str | None = None,
        device_id: uuid.UUID | None = None,
        expires_at: datetime | None = None,
    ) -> IssuedToken:
        """Issue a local session for an auth context.

        Args:
            context: Resolved context to store in the session token.
            csrf_token: CSRF token associated with a browser cookie session.
            device_id: Device the session was issued for.
            expires_at: Upper bound on the session lifetime, applied when it
                falls before the configured lifetime.

        Returns:
            Issued token.
        """
        session_expires_at = datetime.now(UTC) + timedelta(
            seconds=self._settings.JWT_LIFETIME_SECONDS
        )
        if expires_at is not None:
            session_expires_at = min(session_expires_at, to_tz_aware(expires_at))
        token = JWTToken(
            subject=AccountSubject(
                account_id=context.account.id,
                csrf_token=csrf_token,
                device_id=device_id,
            ),
            expires_at=session_expires_at,
        )
        return IssuedToken(token.encode(self._settings), session_expires_at, csrf_token)

    def issue_worker_token(
        self, worker_id: uuid.UUID, account_id: uuid.UUID
    ) -> IssuedToken:
        """Issue a token scoped to a registered worker.

        Args:
            worker_id: Id of the registered worker.
            account_id: Id of the registering account.

        Returns:
            Issued token.
        """
        expires_at = datetime.now(UTC) + timedelta(
            seconds=self._settings.WORKER_TOKEN_LIFETIME_SECONDS
        )
        token = JWTToken(
            subject=WorkerSubject(worker_id=worker_id, account_id=account_id),
            expires_at=expires_at,
        )
        return IssuedToken(token.encode(self._settings), expires_at)

    def issue_task_token(
        self, subject: TaskSubject, timeout_seconds: int
    ) -> IssuedToken:
        """Issue a token scoped to a claimed task attempt.

        The token outlives the task's execution timeout by the configured
        expiry leeway.

        Args:
            subject: Task attempt the token is scoped to.
            timeout_seconds: Execution timeout of the claimed task.

        Returns:
            Issued token.
        """
        expires_at = datetime.now(UTC) + timedelta(
            seconds=timeout_seconds + self._settings.TASK_TOKEN_EXPIRY_LEEWAY_SECONDS
        )
        token = JWTToken(subject=subject, expires_at=expires_at)
        return IssuedToken(token.encode(self._settings), expires_at)

    def _issue_session(self, context: AuthContext) -> IssuedToken:
        csrf_token = None
        if self._settings.AUTH_COOKIE_NAME:
            csrf_token = secrets.token_hex(16)
        return self.issue_token(context, csrf_token=csrf_token)

    async def _resolve_control_plane_credential(self, credential: str) -> AuthContext:
        if credential.startswith(CONTROL_PLANE_API_KEY_PREFIX):
            return await self._authenticate_control_plane(credential)
        if credential.startswith(API_KEY_PREFIX):
            raise AuthenticationError(
                "Local API keys are rejected under control plane authentication."
            )
        context = await self._resolve_session_token(credential)
        if (
            isinstance(context.principal, AccountPrincipal)
            and context.account.external_id is None
        ):
            raise AuthenticationError(
                "Local accounts are rejected under control plane authentication."
            )
        return context

    async def try_resolve_worker_or_task(self, credential: str) -> AuthContext | None:
        """Resolve a bearer credential as a worker or task token, when it is one.

        Used under the ``none`` auth scheme, where an account-less request
        runs as the default account but a worker or task credential still
        resolves to its own principal.

        Args:
            credential: Bearer token supplied by the caller.

        Raises:
            AuthenticationError: The credential decodes as a worker or task
                token but cannot be resolved.

        Returns:
            Resolved context, or None when the credential is not a worker or
            task token.
        """
        try:
            token = JWTToken.decode(credential, self._settings)
        except TokenError:
            return None
        if isinstance(token.subject, AccountSubject):
            return None
        return await self._resolve_token(token)

    async def _authenticate_control_plane(self, credential: str) -> AuthContext:
        if self._control_plane is None:
            raise AuthenticationError("Control plane authentication is not configured.")
        try:
            return await self._control_plane.authenticate(credential)
        except ControlPlaneError as exc:
            # The caller only learns the credential was rejected, so the reason
            # is only ever visible here.
            logger.warning("Control plane authentication failed: %s", exc)
            raise AuthenticationError("Invalid control plane credential.") from exc

    async def _resolve_session_token(self, credential: str) -> AuthContext:
        try:
            token = JWTToken.decode(credential, self._settings)
        except TokenError as exc:
            raise AuthenticationError("Invalid bearer credential.") from exc
        return await self._resolve_token(token)

    async def _authenticate_api_key(self, credential: str) -> AuthContext:
        try:
            key_id, secret = decode_api_key(credential)
        except InvalidApiKey as exc:
            raise AuthenticationError("Invalid API key.") from exc
        try:
            api_key = await self._api_key_repository.get(key_id)
        except ApiKeyNotFound as exc:
            raise AuthenticationError("Invalid API key.") from exc
        if not verify_secret(secret, api_key.key_hash):
            raise AuthenticationError("Invalid API key.")
        if not api_key.active:
            raise AuthenticationError("Invalid API key.")
        account = await self._load_active_account(api_key.owner_id, "Invalid API key.")
        await self._touch_api_key(api_key)
        return AuthContext(account=account)

    async def _touch_api_key(self, api_key: ApiKey) -> None:
        now = datetime.now(UTC)
        if not is_stale(api_key.last_used, LAST_USED_UPDATE_INTERVAL_SECONDS, now):
            return
        api_key.mark_used(now)
        await self._api_key_repository.update(api_key)

    async def _resolve_token(self, token: JWTToken) -> AuthContext:
        if isinstance(token.subject, WorkerSubject):
            return await self._resolve_worker_token(token.subject)
        if isinstance(token.subject, TaskSubject):
            return await self._resolve_task_token(token.subject)
        account = await self._load_active_account(
            token.subject.account_id, "Invalid session token."
        )
        if token.subject.device_id is not None:
            await self._authorize_session_device(token.subject.device_id, account.id)
        return AuthContext(account=account, csrf_token=token.subject.csrf_token)

    async def _resolve_worker_token(self, subject: WorkerSubject) -> WorkerAuthContext:
        account = await self._load_active_account(
            subject.account_id, "Invalid worker token."
        )
        return WorkerAuthContext(
            account=account, principal=WorkerPrincipal(worker_id=subject.worker_id)
        )

    async def _resolve_task_token(self, subject: TaskSubject) -> TaskAuthContext:
        account = await self._load_active_account(
            subject.account_id, "Invalid task token."
        )
        return TaskAuthContext(
            account=account,
            principal=TaskPrincipal(
                task_id=subject.task_id,
                attempt=subject.attempt,
                worker_id=subject.worker_id,
                job_id=subject.job_id,
                grants=subject.grants,
            ),
        )

    async def _authorize_session_device(
        self, device_id: uuid.UUID, account_id: uuid.UUID
    ) -> None:
        if self._device_service is None:
            raise AuthenticationError("Device authentication is not configured.")
        try:
            await self._device_service.authorize_session(device_id, account_id)
        except (DeviceNotFound, DeviceError) as exc:
            raise AuthenticationError("Invalid session token.") from exc

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
