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
"""Auth routes."""

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

from kitaru.api_models.v1.auth import TokenResponse
from kitaru.server.adapters.auth.auth_service import (
    AuthenticationError,
    AuthService,
)
from kitaru.server.adapters.rest.dependencies import (
    get_app_settings,
    get_auth_service,
)
from kitaru.server.api.config import APISettings, AuthScheme

router = APIRouter()


@router.post("/login")
async def login(
    request: Request,
    response: Response,
    settings: Annotated[APISettings, Depends(get_app_settings)],
    service: Annotated[AuthService, Depends(get_auth_service)],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
) -> TokenResponse:
    """Log in with a username and password and receive a bearer token.

    Clients observe HTTP 200 on success and 401 when password login is not
    enabled or the credentials cannot be validated.

    Args:
        request: Incoming request.
        response: Outgoing response.
        settings: Service settings governing auth behavior.
        service: Authentication service.
        username: Account name.
        password: Login password.

    Raises:
        HTTPException: Password login is not enabled or the credentials
            cannot be validated.

    Returns:
        Issued token.
    """
    if settings.AUTH_SCHEME is not AuthScheme.LOCAL:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Password login is not enabled.",
        )
    try:
        token, expires_at, csrf_token = await service.login_with_password(
            username, password
        )
    except AuthenticationError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
        ) from exc
    expires_in = int((expires_at - datetime.now(UTC)).total_seconds())
    if settings.AUTH_COOKIE_NAME:
        response.set_cookie(
            key=settings.AUTH_COOKIE_NAME,
            value=token,
            max_age=expires_in,
            httponly=True,
            samesite="lax",
            secure=request.url.scheme == "https",
        )
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        csrf_token=csrf_token,
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
        response.delete_cookie(settings.AUTH_COOKIE_NAME)
