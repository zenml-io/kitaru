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
"""API route that commits the request session before the response is sent."""

import uuid
from collections.abc import Callable, Coroutine
from typing import Any, NamedTuple, TypeVar

from fastapi import Depends, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)

_SESSION_STATE_ATTR = "db_session"
_PENDING_IDEMPOTENCY_KEY_STATE_ATTR = "pending_idempotency_key"
_IDEMPOTENT_ENDPOINT_ATTR = "_kitaru_idempotent"

F = TypeVar("F", bound=Callable[..., Any])


def attach_request_session(request: Request, session: AsyncSession) -> None:
    """Attach the request's database session for CommitRoute to commit.

    Args:
        request: Incoming request.
        session: Session created by ``get_session``.
    """
    setattr(request.state, _SESSION_STATE_ATTR, session)


def _get_request_session(request: Request) -> AsyncSession | None:
    """Return the database session attached to the request, if any.

    Args:
        request: Completed request.

    Returns:
        Session attached by ``attach_request_session``, or ``None``.
    """
    return getattr(request.state, _SESSION_STATE_ATTR, None)


class _PendingIdempotencyKey(NamedTuple):
    """Idempotency key awaiting its response, stashed on the request."""

    repository: IdempotencyKeyRepository
    idempotency_key_id: uuid.UUID


def attach_pending_idempotency_key(
    request: Request,
    repository: IdempotencyKeyRepository,
    idempotency_key_id: uuid.UUID,
) -> None:
    """Attach an idempotency key awaiting its response, for CommitRoute to fill in.

    Args:
        request: Incoming request.
        repository: Repository the key was created through.
        idempotency_key_id: Id of the idempotency key created for this request.
    """
    setattr(
        request.state,
        _PENDING_IDEMPOTENCY_KEY_STATE_ATTR,
        _PendingIdempotencyKey(repository, idempotency_key_id),
    )


def _get_pending_idempotency_key(request: Request) -> _PendingIdempotencyKey | None:
    """Return the request's pending idempotency key, if any.

    Args:
        request: Completed request.

    Returns:
        Key attached by ``attach_pending_idempotency_key``, or ``None``.
    """
    return getattr(request.state, _PENDING_IDEMPOTENCY_KEY_STATE_ATTR, None)


def idempotent(endpoint: F) -> F:
    """Mark a route handler as replayable through the idempotency key table.

    Args:
        endpoint: Route handler to mark.

    Returns:
        The handler, unchanged.
    """
    setattr(endpoint, _IDEMPOTENT_ENDPOINT_ATTR, True)
    return endpoint


def is_idempotent(endpoint: Callable[..., Any]) -> bool:
    """Whether a route handler carries the idempotent marker.

    Args:
        endpoint: Route handler to check.

    Returns:
        Whether ``endpoint`` was decorated with ``idempotent``.
    """
    return getattr(endpoint, _IDEMPOTENT_ENDPOINT_ATTR, False)


class StoredResponse(Exception):
    """Signal to short-circuit a route with a previously stored response."""

    def __init__(self, status_code: int, body: bytes, content_type: str | None) -> None:
        """Initialize the signal.

        Args:
            status_code: HTTP status code of the stored response.
            body: Raw stored response body.
            content_type: Content type of the stored response, when set.
        """
        super().__init__("Idempotency key already has a stored response")
        self.status_code = status_code
        self.body = body
        self.content_type = content_type


class CommitRoute(APIRoute):
    """API route that commits the request database session before responding."""

    def __init__(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
        """Build the route, wiring idempotency enforcement when marked.

        Args:
            path: Route path.
            endpoint: Route handler.
            **kwargs: Remaining ``APIRoute`` constructor arguments.
        """
        if is_idempotent(endpoint):
            # Deferred import breaks the cycle: dependencies.py imports this
            # module for attach_request_session, so idempotency.py (which
            # depends on dependencies.py) can only be imported here, once
            # this module has finished loading.
            from kitaru.server.adapters.rest.idempotency import _enforce_idempotency

            dependencies = list(kwargs.get("dependencies") or [])
            dependencies.append(Depends(_enforce_idempotency))
            kwargs["dependencies"] = dependencies
        super().__init__(path, endpoint, **kwargs)

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap the route handler to commit the session before responding.

        Returns:
            Handler that runs the original route, stores the response for a
            pending idempotency key and commits the request session on
            success, and returns the response. An exception from the handler
            propagates without committing.
        """
        original_route_handler = super().get_route_handler()

        async def commit_route_handler(request: Request) -> Response:
            try:
                response = await original_route_handler(request)
            except StoredResponse as stored:
                return Response(
                    content=stored.body,
                    status_code=stored.status_code,
                    media_type=stored.content_type,
                    headers={"Idempotent-Replayed": "true"},
                )
            pending = _get_pending_idempotency_key(request)
            if (
                pending is not None
                and 200 <= response.status_code < 300
                and not isinstance(response, StreamingResponse)
                and isinstance(response.body, bytes)
            ):
                await pending.repository.store_response(
                    pending.idempotency_key_id,
                    response_status=response.status_code,
                    response_body=response.body,
                    response_content_type=response.headers.get("content-type"),
                )
            session = _get_request_session(request)
            if session is not None:
                await session.commit()
            return response

        return commit_route_handler
