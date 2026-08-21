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
"""Per-request state shared between dependencies and the route class."""

import uuid
from typing import NamedTuple

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)

_SESSION_STATE_ATTR = "db_session"
_IDEMPOTENCY_KEY_HANDLE_STATE_ATTR = "idempotency_key_handle"


class IdempotencyKeyHandle(NamedTuple):
    """Idempotency key handle."""

    repository: IdempotencyKeyRepository
    idempotency_key_id: uuid.UUID


def attach_request_session(request: Request, session: AsyncSession) -> None:
    """Attach the request's database session for the route to commit.

    Args:
        request: Incoming request.
        session: Session created by ``get_session``.
    """
    setattr(request.state, _SESSION_STATE_ATTR, session)


def get_request_session(request: Request) -> AsyncSession | None:
    """Return the database session attached to the request, if any.

    Args:
        request: Completed request.

    Returns:
        Session attached by ``attach_request_session``, or ``None``.
    """
    return getattr(request.state, _SESSION_STATE_ATTR, None)


def attach_idempotency_key_handle(
    request: Request, handle: IdempotencyKeyHandle
) -> None:
    """Attach the idempotency key the request registered for the route to fill in.

    Args:
        request: Incoming request.
        handle: Repository and id of the key registered for this request.
    """
    setattr(request.state, _IDEMPOTENCY_KEY_HANDLE_STATE_ATTR, handle)


def get_idempotency_key_handle(request: Request) -> IdempotencyKeyHandle | None:
    """Return the idempotency key handle attached to the request, if any.

    Args:
        request: Completed request.

    Returns:
        Handle attached by ``attach_idempotency_key_handle``, or ``None``.
    """
    return getattr(request.state, _IDEMPOTENCY_KEY_HANDLE_STATE_ATTR, None)
