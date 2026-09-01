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
"""Authentication routes."""

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    Form,
    HTTPException,
    Request,
    Response,
    status,
)

from kitaru.api_models.v1.auth import (
    DeviceAuthorizationResponse,
    GrantType,
    TokenErrorCode,
    TokenErrorResponse,
    TokenResponse,
)
from kitaru.api_models.v1.base import PlainStr, ValidationErrorBody
from kitaru.api_models.v1.info import AuthScheme
from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthService,
    IssuedToken,
)
from kitaru.server.adapters.rest.dependencies import (
    get_app_settings,
    get_auth_service,
    get_bearer_credential,
    get_device_service,
)
from kitaru.server.adapters.rest.mapping.devices import (
    device_to_authorization_response,
)
from kitaru.server.adapters.rest.responses import error_responses
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.device import DeviceFingerprint
from kitaru.server.application.services.device_service import DeviceService
from kitaru.server.domain.device import (
    DeviceAuthorizationPending,
    DeviceExpired,
    DeviceLocked,
    DeviceNotFound,
    InvalidDeviceCode,
)

router = APIRouter(route_class=KitaruAPIRoute)


class TokenGrantError(Exception):
    """Raised when a token grant fails with an OAuth 2.0 error code."""

    def __init__(self, error: TokenErrorCode, description: str) -> None:
        """Initialize the error.

        Args:
            error: OAuth 2.0 error code.
            description: Human readable error message.
        """
        super().__init__(description)
        self.error = error
        self.description = description

    def to_response(self) -> TokenErrorResponse:
        """Build the error body clients receive.

        Returns:
            Token error response.
        """
        return TokenErrorResponse(error=self.error, detail=self.description)


class LoginRequestForm:
    """Login request form resolving and validating the grant type."""

    def __init__(
        self,
        settings: Annotated[APISettings, Depends(get_app_settings)],
        grant_type: Annotated[str | None, Form()] = None,
        username: Annotated[str | None, Form()] = None,
        password: Annotated[str | None, Form()] = None,
        device_id: Annotated[uuid.UUID | None, Form()] = None,
        device_code: Annotated[str | None, Form()] = None,
    ) -> None:
        """Resolve the grant type and validate the fields it requires.

        Args:
            settings: Service settings governing auth behavior.
            grant_type: Requested grant type, inferred when omitted.
            username: Account name, required by the password grant type.
            password: Login password.
            device_id: Id of the device, required by the device grant type.
            device_code: Device code, required by the device grant type.

        Raises:
            TokenGrantError: The grant type is unknown, is not accepted by this
                server's auth scheme, or is missing a required field.
        """
        self.grant_type = self._resolve_grant_type(
            settings, grant_type, username, device_code
        )
        if self.grant_type is GrantType.PASSWORD:
            if settings.AUTH_SCHEME is not AuthScheme.LOCAL:
                raise self._unsupported_grant_type(self.grant_type)
            if not username:
                raise TokenGrantError(
                    TokenErrorCode.INVALID_REQUEST,
                    "Invalid request: username is required.",
                )
        elif self.grant_type is GrantType.API_KEY:
            if settings.AUTH_SCHEME is not AuthScheme.LOCAL:
                raise self._unsupported_grant_type(self.grant_type)
        elif self.grant_type is GrantType.CONTROL_PLANE:
            if settings.AUTH_SCHEME is not AuthScheme.CONTROL_PLANE:
                raise self._unsupported_grant_type(self.grant_type)
        elif self.grant_type is GrantType.DEVICE_CODE:
            if settings.AUTH_SCHEME is AuthScheme.NONE:
                raise self._unsupported_grant_type(self.grant_type)
            if device_id is None or not device_code:
                raise TokenGrantError(
                    TokenErrorCode.INVALID_REQUEST,
                    "Invalid request: device_id and device_code are required.",
                )
        self.username = username or ""
        self.password = password or ""
        self.device_id = device_id
        self.device_code = device_code or ""

    @classmethod
    def _resolve_grant_type(
        cls,
        settings: APISettings,
        grant_type: str | None,
        username: str | None,
        device_code: str | None,
    ) -> GrantType:
        if grant_type is not None:
            if grant_type not in set(GrantType):
                raise cls._unsupported_grant_type(grant_type)
            return GrantType(grant_type)
        if username is not None:
            return GrantType.PASSWORD
        if device_code is not None:
            return GrantType.DEVICE_CODE
        if settings.AUTH_SCHEME is AuthScheme.CONTROL_PLANE:
            return GrantType.CONTROL_PLANE
        if settings.AUTH_SCHEME is AuthScheme.LOCAL:
            return GrantType.PASSWORD
        raise TokenGrantError(
            TokenErrorCode.INVALID_REQUEST,
            "Invalid request: grant type is required.",
        )

    @staticmethod
    def _unsupported_grant_type(grant_type: str) -> TokenGrantError:
        return TokenGrantError(
            TokenErrorCode.UNSUPPORTED_GRANT_TYPE,
            f"Unsupported grant type: {grant_type}",
        )


@router.post(
    "/device_authorization",
    responses={
        400: {"model": TokenErrorResponse},
        422: {"model": ValidationErrorBody, "description": "Validation Error"},
        **error_responses(503),
    },
)
async def device_authorization(
    request: Request,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    service: Annotated[DeviceService, Depends(get_device_service)],
    hostname: Annotated[PlainStr | None, Form()] = None,
    os: Annotated[PlainStr | None, Form()] = None,
    python_version: Annotated[PlainStr | None, Form()] = None,
    client_version: Annotated[PlainStr | None, Form()] = None,
) -> DeviceAuthorizationResponse:
    """Start a device authorization and receive its codes.

    The caller shows the user code to a person, who confirms it at the
    verification URI while signed in. The caller then polls ``/api/v1/login`` with
    the device grant type until the confirmation lands. Clients observe HTTP
    200 on success and 400 when this server does not authenticate requests.
    The codes are returned exactly once.

    Args:
        request: Incoming request.
        settings: Service settings governing auth behavior.
        service: Device service.
        hostname: Host the caller runs on.
        os: Operating system the caller runs on.
        python_version: Python version the caller runs.
        client_version: Kitaru version the caller runs.

    Raises:
        TokenGrantError: This server does not authenticate requests.

    Returns:
        Device authorization carrying the plaintext codes.
    """
    if settings.AUTH_SCHEME is AuthScheme.NONE:
        raise TokenGrantError(
            TokenErrorCode.UNSUPPORTED_GRANT_TYPE,
            "This server does not authenticate requests.",
        )
    fingerprint = DeviceFingerprint(
        hostname=hostname,
        os=os,
        python_version=python_version,
        client_version=client_version,
        ip_address=request.client.host if request.client else None,
    )
    device, user_code, device_code = await service.request_authorization(fingerprint)
    return device_to_authorization_response(
        device,
        user_code=user_code,
        device_code=device_code,
        dashboard_url=settings.DASHBOARD_URL or str(request.base_url),
        policy=service.policy,
    )


@router.post(
    "/login",
    responses={
        400: {"model": TokenErrorResponse},
        422: {"model": ValidationErrorBody, "description": "Validation Error"},
        **error_responses(401, 503),
    },
)
async def login(
    request: Request,
    response: Response,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    service: Annotated[AuthService, Depends(get_auth_service)],
    form: Annotated[LoginRequestForm, Depends()],
) -> TokenResponse:
    """Log in and receive a bearer token.

    The ``password`` grant type takes the form username and password and is
    accepted under the ``local`` auth scheme. The ``api-key`` grant type reads
    an API key from the authorization header and is accepted under the same
    scheme. The ``control-plane`` grant type reads a control plane credential
    from the authorization header and mirrors the control plane user into a
    local account. The device grant type takes the ``device_id`` and
    ``device_code`` of a device authorization and returns a token once an
    account has confirmed it.

    Clients observe HTTP 200 on success, 400 when the grant type is not
    accepted by this server or a device authorization is not ready, and 401
    when the credentials cannot be validated. A 400 carries an OAuth 2.0
    ``error`` code, of which ``authorization_pending`` means the caller
    should poll again.

    Args:
        request: Incoming request.
        response: Outgoing response.
        settings: Service settings governing auth behavior.
        service: Authentication service.
        form: Login request form carrying the resolved grant type.

    Raises:
        HTTPException: The credentials cannot be validated.

    Returns:
        Issued token.
    """
    try:
        if form.grant_type is GrantType.DEVICE_CODE:
            assert form.device_id is not None
            issued = await _login_with_device(service, form.device_id, form.device_code)
        elif form.grant_type is GrantType.API_KEY:
            issued = await service.login_with_api_key(
                _require_bearer_credential(request, "API key")
            )
        elif form.grant_type is GrantType.CONTROL_PLANE:
            issued = await service.login_with_control_plane(
                _require_bearer_credential(request, "control plane credential")
            )
        else:
            issued = await service.login_with_password(form.username, form.password)
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
    expires_in = int((issued.expires_at - datetime.now(UTC)).total_seconds())
    # A device or API key token belongs to a headless client, so it never rides
    # a cookie.
    if settings.AUTH_COOKIE_NAME and form.grant_type not in (
        GrantType.DEVICE_CODE,
        GrantType.API_KEY,
    ):
        response.set_cookie(
            key=settings.AUTH_COOKIE_NAME,
            value=issued.token,
            max_age=expires_in,
            httponly=True,
            samesite="lax",
            secure=_use_secure_cookie(request, settings),
            domain=settings.AUTH_COOKIE_DOMAIN or None,
        )
    return TokenResponse(
        access_token=issued.token,
        token_type="bearer",
        expires_in=expires_in,
        csrf_token=issued.csrf_token,
    )


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    response: Response,
    settings: Annotated[APISettings, Depends(get_app_settings)],
) -> None:
    """Log out and clear the auth cookie.

    Clients observe HTTP 204.

    Args:
        response: Outgoing response.
        settings: Service settings governing auth behavior.
    """
    if settings.AUTH_COOKIE_NAME:
        response.delete_cookie(
            settings.AUTH_COOKIE_NAME, domain=settings.AUTH_COOKIE_DOMAIN or None
        )


async def _login_with_device(
    service: AuthService, device_id: uuid.UUID, device_code: str
) -> IssuedToken:
    """Run the device grant and translate its failures into OAuth 2.0 errors.

    Args:
        service: Authentication service.
        device_id: Id of the device.
        device_code: Plaintext device code held by the polling client.

    Raises:
        TokenGrantError: The device authorization is not ready or not valid.

    Returns:
        Issued token.
    """
    try:
        return await service.login_with_device(device_id, device_code)
    except DeviceAuthorizationPending as exc:
        raise TokenGrantError(TokenErrorCode.AUTHORIZATION_PENDING, str(exc)) from exc
    except DeviceExpired as exc:
        raise TokenGrantError(TokenErrorCode.EXPIRED_TOKEN, str(exc)) from exc
    except DeviceLocked as exc:
        raise TokenGrantError(TokenErrorCode.ACCESS_DENIED, str(exc)) from exc
    except (DeviceNotFound, InvalidDeviceCode) as exc:
        raise TokenGrantError(
            TokenErrorCode.INVALID_GRANT, "Invalid device code"
        ) from exc


def _require_bearer_credential(request: Request, kind: str) -> str:
    """Read a bearer credential the grant type cannot run without.

    Args:
        request: Incoming request.
        kind: Name of the missing credential for the error message.

    Raises:
        AuthenticationError: The authorization header carries no credential.

    Returns:
        Credential string without the ``Bearer`` prefix.
    """
    credential = get_bearer_credential(request)
    if credential is None:
        raise AuthenticationError(f"Missing {kind}.")
    return credential


def _use_secure_cookie(request: Request, settings: APISettings) -> bool:
    """Decide whether the auth cookie is restricted to HTTPS.

    Args:
        request: Incoming request.
        settings: Service settings governing auth behavior.

    Returns:
        Whether to set the cookie's secure attribute. Without an explicit
        setting the request scheme decides, which is wrong behind a proxy that
        terminates TLS.
    """
    if settings.AUTH_COOKIE_SECURE is not None:
        return settings.AUTH_COOKIE_SECURE
    return request.url.scheme == "https"
