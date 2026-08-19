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
"""API route class owning the request lifecycle around the handler."""

from collections.abc import Callable, Coroutine
from typing import Any, NamedTuple, TypeVar

from fastapi import Depends, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute

from kitaru.server.adapters.rest.idempotency import (
    IdempotencyKeyReused,
    build_idempotency_enforcer,
)
from kitaru.server.adapters.rest.request_state import (
    get_idempotency_key_handle,
    get_request_session,
)

_IDEMPOTENT_ENDPOINT_ATTR = "_kitaru_idempotent"

F = TypeVar("F", bound=Callable[..., Any])


class IdempotencyOptions(NamedTuple):
    """Idempotency options."""

    encrypt_response: bool


def idempotent(
    endpoint: F | None = None, encrypt_response: bool = False
) -> F | Callable[[F], F]:
    """Mark a route handler as replayable through the idempotency key table.

    Args:
        endpoint: Route handler to mark, when used as a bare decorator.
        encrypt_response: Whether the stored response is encrypted at rest.

    Returns:
        The handler, unchanged, or the decorator when used with arguments.
    """

    def mark(handler: F) -> F:
        setattr(
            handler,
            _IDEMPOTENT_ENDPOINT_ATTR,
            IdempotencyOptions(encrypt_response=encrypt_response),
        )
        return handler

    if endpoint is None:
        return mark
    return mark(endpoint)


def get_idempotency_options(
    endpoint: Callable[..., Any],
) -> IdempotencyOptions | None:
    """Return the idempotency options of a route handler, if marked.

    Args:
        endpoint: Route handler to check.

    Returns:
        Options set by ``idempotent``, or ``None`` for an unmarked handler.
    """
    return getattr(endpoint, _IDEMPOTENT_ENDPOINT_ATTR, None)


def is_idempotent(endpoint: Callable[..., Any]) -> bool:
    """Whether a route handler carries the idempotent marker.

    Args:
        endpoint: Route handler to check.

    Returns:
        Whether ``endpoint`` was decorated with ``idempotent``.
    """
    return get_idempotency_options(endpoint) is not None


class KitaruAPIRoute(APIRoute):
    """API route that commits the request database session before responding."""

    def __init__(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
        """Build the route, wiring idempotency enforcement when marked.

        Args:
            path: Route path.
            endpoint: Route handler.
            **kwargs: Remaining ``APIRoute`` constructor arguments.
        """
        options = get_idempotency_options(endpoint)
        if options is not None:
            # Enforcement resolves the handler's own auth dependency first, so
            # authorization runs before any key is registered or replayed.
            enforcer = build_idempotency_enforcer(endpoint, options.encrypt_response)
            dependencies = list(kwargs.get("dependencies") or [])
            dependencies.append(Depends(enforcer))
            kwargs["dependencies"] = dependencies
        super().__init__(path, endpoint, **kwargs)

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap the route handler to commit the session before responding.

        Returns:
            Handler that runs the original route, stores the response for a
            registered idempotency key and commits the request session on
            success, and returns the response. An exception from the handler
            propagates without committing.
        """
        original_route_handler = super().get_route_handler()
        options = get_idempotency_options(self.endpoint)

        async def route_handler(request: Request) -> Response:
            try:
                response = await original_route_handler(request)
            except IdempotencyKeyReused as reused:
                return Response(
                    content=reused.body,
                    status_code=reused.status_code,
                    media_type=reused.content_type,
                    headers={"Idempotent-Replayed": "true"},
                )
            handle = get_idempotency_key_handle(request)
            if (
                handle is not None
                and options is not None
                and 200 <= response.status_code < 300
                and not isinstance(response, StreamingResponse)
                and isinstance(response.body, bytes)
            ):
                await handle.repository.store_response(
                    handle.idempotency_key_id,
                    response_status=response.status_code,
                    response_body=response.body,
                    response_content_type=response.headers.get("content-type"),
                    encrypt=options.encrypt_response,
                )
            session = get_request_session(request)
            if session is not None:
                await session.commit()
            return response

        return route_handler
