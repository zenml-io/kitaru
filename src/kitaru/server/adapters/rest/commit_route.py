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

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Response
from fastapi.routing import APIRoute
from sqlalchemy.ext.asyncio import AsyncSession

_SESSION_STATE_ATTR = "db_session"


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


class CommitRoute(APIRoute):
    """API route that commits the request database session before responding."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        """Wrap the route handler to commit the session before responding.

        Returns:
            Handler that runs the original route, commits the request
            session on success, and returns the response. An exception from
            the handler propagates without committing.
        """
        original_route_handler = super().get_route_handler()

        async def commit_route_handler(request: Request) -> Response:
            response = await original_route_handler(request)
            session = _get_request_session(request)
            if session is not None:
                await session.commit()
            return response

        return commit_route_handler
