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
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from kitaru.server.adapters.rest.commit_route import (
    StoredResponse,
    attach_pending_idempotency_key,
)
from kitaru.server.adapters.rest.dependencies import (
    _resolve_auth_context,
    get_idempotency_key_repository,
)
from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.idempotency_key import (
    MAX_IDEMPOTENCY_KEY_LENGTH,
    IdempotencyKey,
    IdempotencyKeyAlreadyExists,
    IdempotencyKeyMismatch,
)

_IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"


async def _enforce_idempotency(
    request: Request,
    context: Annotated[AuthContext, Depends(_resolve_auth_context)],
    repository: Annotated[
        IdempotencyKeyRepository, Depends(get_idempotency_key_repository)
    ],
) -> None:
    """Replay or register a request's idempotency key before it runs.

    A request without the header runs normally. A first use of the key
    registers it against the request's method, path, and body fingerprint,
    for ``CommitRoute`` to fill in with the committed response. A reused key
    with a matching fingerprint short-circuits the route with the stored
    response, and a reused key with a different fingerprint is rejected.

    Args:
        request: Incoming request.
        context: Resolved auth context.
        repository: Idempotency key repository for the current request.

    Raises:
        HTTPException: The header is present but empty or too long, or a
            reused key's original request is still in flight.
        IdempotencyKeyMismatch: A reused key was registered for a different
            method, path, or body.
        StoredResponse: A reused key already has a stored response, to
            replay in place of running the route.
    """
    header = request.headers.get(_IDEMPOTENCY_KEY_HEADER)
    if header is None:
        return
    key = header.strip()
    if not key or len(key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Idempotency-Key header.",
        )

    body = await request.body()
    fingerprint = hashlib.sha256(
        f"{request.method}\n{request.url.path}\n".encode() + body
    ).hexdigest()

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
        stored = await repository.get(context.account.id, key)
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
        raise StoredResponse(
            status_code=stored.response_status,
            body=stored.response_body or b"",
            content_type=stored.response_content_type,
        ) from None
    else:
        attach_pending_idempotency_key(request, repository, created.id)
