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
"""Idempotency key enforcement dependency."""

import hashlib
import inspect
from collections.abc import Callable, Coroutine
from typing import Annotated, Any, get_args, get_origin

from fastapi import Depends, HTTPException, Request, params, status

from kitaru.server.adapters.rest.dependencies import get_idempotency_key_repository
from kitaru.server.adapters.rest.request_state import (
    IdempotencyKeyHandle,
    attach_idempotency_key_handle,
)
from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.idempotency_key import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    MAX_IDEMPOTENCY_PATH_LENGTH,
    IdempotencyKey,
    IdempotencyKeyAlreadyExists,
    IdempotencyKeyMismatch,
)

_IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"


class IdempotencyKeyReused(Exception):
    """Raised when a reused idempotency key already has a stored response."""

    def __init__(self, status_code: int, body: bytes, content_type: str | None) -> None:
        """Initialize the exception.

        Args:
            status_code: HTTP status code of the stored response.
            body: Raw stored response body.
            content_type: Content type of the stored response, when set.
        """
        super().__init__("Idempotency key already has a stored response")
        self.status_code = status_code
        self.body = body
        self.content_type = content_type


def _fingerprint_request(method: str, path: str, query: str, body: bytes) -> str:
    """Fingerprint a request by its method, path, query and raw body.

    Args:
        method: HTTP method.
        path: Request path.
        query: Raw query string.
        body: Raw request body.

    Returns:
        Hex digest of the request.
    """
    return hashlib.sha256(f"{method}\n{path}?{query}\n".encode() + body).hexdigest()


def _find_auth_dependency(endpoint: Callable[..., Any]) -> params.Depends:
    """Return the auth context dependency declared by a route handler.

    Args:
        endpoint: Route handler to inspect.

    Raises:
        TypeError: The handler declares no auth context dependency.

    Returns:
        ``Depends`` of the handler's auth context parameter.
    """
    for parameter in inspect.signature(endpoint).parameters.values():
        if get_origin(parameter.annotation) is not Annotated:
            continue
        base, *metadata = get_args(parameter.annotation)
        if not (isinstance(base, type) and issubclass(base, AuthContext)):
            continue
        for item in metadata:
            if isinstance(item, params.Depends):
                return item
    raise TypeError(
        f"Idempotent route {endpoint!r} declares no auth context dependency"
    )


def build_idempotency_enforcer(
    endpoint: Callable[..., Any], encrypt_response: bool
) -> Callable[..., Coroutine[Any, Any, None]]:
    """Build the enforcement dependency for a route handler.

    Args:
        endpoint: Route handler whose auth dependency resolves before enforcing.
        encrypt_response: Whether the route's stored responses are encrypted.

    Raises:
        TypeError: The handler declares no auth context dependency.

    Returns:
        Dependency replaying or registering the request's idempotency key.
    """
    authorize = _find_auth_dependency(endpoint).dependency

    async def enforce_idempotency(
        request: Request,
        context: Annotated[AuthContext, Depends(authorize)],
        repository: Annotated[
            IdempotencyKeyRepository, Depends(get_idempotency_key_repository)
        ],
    ) -> None:
        await _enforce_idempotency(request, context, repository, encrypt_response)

    return enforce_idempotency


async def _enforce_idempotency(
    request: Request,
    context: AuthContext,
    repository: IdempotencyKeyRepository,
    encrypt_response: bool,
) -> None:
    """Replay or register a request's idempotency key before it runs.

    A request without the header runs normally. A first use of the key
    registers it against the request's method, path, query, and body fingerprint,
    for the route to fill in with the committed response. A reused key with
    a matching fingerprint short-circuits the route with the stored response,
    and a reused key with a different fingerprint is rejected.

    Args:
        request: Incoming request.
        context: Resolved auth context.
        repository: Idempotency key repository for the current request.
        encrypt_response: Whether the route's stored responses are encrypted.

    Raises:
        HTTPException: The header is present but empty, too long, or not
            printable, the request path is too long, or a reused key's
            original request is still in flight.
        IdempotencyKeyMismatch: A reused key was registered for a different
            method, path, query, or body.
        IdempotencyKeyReused: A reused key already has a stored response, to
            replay in place of running the route.
    """
    header = request.headers.get(_IDEMPOTENCY_KEY_HEADER)
    if header is None:
        return
    key = header.strip()
    if not key or len(key) > MAX_IDEMPOTENCY_KEY_LENGTH or not key.isprintable():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Idempotency-Key header.",
        )
    # Dependencies resolve before path parameter validation, so an overlong
    # path would otherwise reach the key insert and fail on the column width.
    if len(request.url.path) > MAX_IDEMPOTENCY_PATH_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request path too long for Idempotency-Key.",
        )

    fingerprint = _fingerprint_request(
        request.method, request.url.path, request.url.query, await request.body()
    )
    try:
        created = await repository.create(
            IdempotencyKey(
                account_id=context.account.id,
                key=key,
                fingerprint=fingerprint,
                method=request.method,
                path=request.url.path,
            )
        )
    except IdempotencyKeyAlreadyExists:
        stored = await repository.get(
            context.account.id, key, encrypted=encrypt_response
        )
        if stored is None:
            # Raced the retention sweep between the conflict and this read.
            # Nothing to replay, so let the request run as if it were new.
            return
        if stored.fingerprint != fingerprint:
            raise IdempotencyKeyMismatch() from None
        if stored.response_status is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key request is still in progress.",
            ) from None
        raise IdempotencyKeyReused(
            status_code=stored.response_status,
            body=stored.response_body or b"",
            content_type=stored.response_content_type,
        ) from None
    else:
        attach_idempotency_key_handle(
            request, IdempotencyKeyHandle(repository, created.id)
        )
