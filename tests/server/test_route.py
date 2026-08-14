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
"""Tests for KitaruAPIRoute."""

from collections.abc import AsyncGenerator
from typing import Any, cast

import pytest
from fastapi import APIRouter, Depends, FastAPI, Request
from sqlalchemy.ext.asyncio import AsyncSession

from kitaru.server.adapters.rest.request_state import (
    attach_request_session,
    request_uses_read_engine,
)
from kitaru.server.adapters.rest.route import KitaruAPIRoute, read_only

_SCOPE: dict[str, Any] = {
    "type": "http",
    "asgi": {"version": "3.0"},
    "http_version": "1.1",
    "headers": [],
    "query_string": b"",
    "server": ("test", 80),
    "client": ("test", 123),
    "root_path": "",
}


class _RecordingSession:
    """Fake session recording commits into a shared event list."""

    def __init__(self, events: list[str]) -> None:
        """Record commits into the given event list.

        Args:
            events: Shared list commit and send events are appended to.
        """
        self.events = events

    async def commit(self) -> None:
        """Record a commit event."""
        self.events.append("committed")


class _DirtySession:
    """Fake session exposing pending-write state like a real SQLAlchemy session."""

    def __init__(self, events: list[str], new: bool = False) -> None:
        """Record commits into the given event list, optionally starting dirty.

        Args:
            events: Shared list commit and send events are appended to.
            new: Whether the session should report a pending new object.
        """
        self.events = events
        self.new = {object()} if new else set()
        self.dirty: set[object] = set()
        self.deleted: set[object] = set()

    async def commit(self) -> None:
        """Record a commit event."""
        self.events.append("committed")


def _drive(scope: dict[str, Any], events: list[str]) -> Any:
    """Build receive/send callables that record the response send event.

    Args:
        scope: ASGI scope for the request.
        events: Shared list commit and send events are appended to.

    Returns:
        Receive and send ASGI callables.
    """
    request_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body" and not message.get(
            "more_body", False
        ):
            events.append("response_sent")

    return receive, send


async def test_route_commits_before_response_is_sent() -> None:
    """Commit the request session before the response body is sent."""
    events: list[str] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    async def session_dependency(request: Request) -> AsyncGenerator[None, None]:
        attach_request_session(request, cast(AsyncSession, _RecordingSession(events)))
        yield

    @router.post("/items", dependencies=[Depends(session_dependency)])
    async def create_item() -> dict[str, str]:
        return {"status": "created"}

    app = FastAPI()
    app.include_router(router)

    scope = {**_SCOPE, "method": "POST", "path": "/items", "raw_path": b"/items"}
    receive, send = _drive(scope, events)
    await app(scope, receive, send)

    assert events == ["committed", "response_sent"]


async def test_route_skips_commit_on_exception() -> None:
    """Skip the commit when the route handler raises."""
    events: list[str] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    async def session_dependency(request: Request) -> AsyncGenerator[None, None]:
        attach_request_session(request, cast(AsyncSession, _RecordingSession(events)))
        yield

    @router.get("/boom", dependencies=[Depends(session_dependency)])
    async def boom() -> None:
        raise RuntimeError("boom")

    app = FastAPI()
    app.include_router(router)

    scope = {**_SCOPE, "method": "GET", "path": "/boom", "raw_path": b"/boom"}
    receive, send = _drive(scope, events)
    with pytest.raises(RuntimeError):
        await app(scope, receive, send)

    assert "committed" not in events


async def test_route_without_session_returns_response() -> None:
    """Return the response untouched when no dependency created a session."""
    events: list[str] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    @router.get("/no-session")
    async def no_session() -> dict[str, str]:
        return {"status": "ok"}

    app = FastAPI()
    app.include_router(router)

    scope = {
        **_SCOPE,
        "method": "GET",
        "path": "/no-session",
        "raw_path": b"/no-session",
    }
    receive, send = _drive(scope, events)
    await app(scope, receive, send)

    assert events == ["response_sent"]


async def test_route_marks_request_state_for_a_read_only_endpoint() -> None:
    """KitaruAPIRoute exposes the read-only flag before the handler runs."""
    seen: list[bool] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    async def probe_dependency(request: Request) -> None:
        seen.append(request_uses_read_engine(request))

    @router.get("/read-only", dependencies=[Depends(probe_dependency)])
    @read_only
    async def read_only_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/normal", dependencies=[Depends(probe_dependency)])
    async def normal_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    app = FastAPI()
    app.include_router(router)

    events: list[str] = []
    for path in ("/read-only", "/normal"):
        scope = {**_SCOPE, "method": "GET", "path": path, "raw_path": path.encode()}
        receive, send = _drive(scope, events)
        await app(scope, receive, send)

    assert seen == [True, False]


async def test_route_raises_when_a_read_only_route_has_pending_writes() -> None:
    """A read-only route that dirties its session fails instead of committing."""
    events: list[str] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    async def session_dependency(request: Request) -> AsyncGenerator[None, None]:
        attach_request_session(
            request, cast(AsyncSession, _DirtySession(events, new=True))
        )
        yield

    @router.get("/read-only", dependencies=[Depends(session_dependency)])
    @read_only
    async def read_only_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    app = FastAPI()
    app.include_router(router)

    scope = {**_SCOPE, "method": "GET", "path": "/read-only", "raw_path": b"/read-only"}
    receive, send = _drive(scope, events)
    with pytest.raises(RuntimeError, match="pending database writes"):
        await app(scope, receive, send)

    assert "committed" not in events


async def test_route_commits_pending_writes_on_an_unmarked_route() -> None:
    """The pending-writes guard only applies to routes marked read-only."""
    events: list[str] = []
    router = APIRouter(route_class=KitaruAPIRoute)

    async def session_dependency(request: Request) -> AsyncGenerator[None, None]:
        attach_request_session(
            request, cast(AsyncSession, _DirtySession(events, new=True))
        )
        yield

    @router.get("/normal", dependencies=[Depends(session_dependency)])
    async def normal_endpoint() -> dict[str, str]:
        return {"status": "ok"}

    app = FastAPI()
    app.include_router(router)

    scope = {**_SCOPE, "method": "GET", "path": "/normal", "raw_path": b"/normal"}
    receive, send = _drive(scope, events)
    await app(scope, receive, send)

    assert events == ["committed", "response_sent"]
