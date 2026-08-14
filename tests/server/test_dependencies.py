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
"""Tests for read-replica session routing in FastAPI dependencies."""

from typing import Any, cast

from fastapi import FastAPI, Request
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import local_settings
from kitaru.server.adapters.rest.dependencies import get_auth_session, get_session
from kitaru.server.adapters.rest.request_state import mark_request_read_only
from kitaru.server.database.service import DatabaseService

_SCOPE: dict[str, Any] = {
    "type": "http",
    "asgi": {"version": "3.0"},
    "http_version": "1.1",
    "headers": [],
    "query_string": b"",
    "server": ("test", 80),
    "client": ("test", 123),
    "root_path": "",
    "method": "GET",
    "path": "/",
    "raw_path": b"/",
}


def _request(database: DatabaseService, read_only: bool = False) -> Request:
    """Build a request scoped to the given database service.

    Args:
        database: Database service the request's app exposes.
        read_only: Whether to mark the request the way KitaruAPIRoute marks
            a read-only route before calling its handler.

    Returns:
        Request usable by get_session and get_auth_session.
    """
    app = FastAPI()
    app.state.database = database
    request = Request({**_SCOPE, "app": app})
    if read_only:
        mark_request_read_only(request)
    return request


def _spy_commit(session: AsyncSession, calls: list[int]) -> None:
    """Wrap a session's commit method to append to calls on every commit.

    Args:
        session: Session whose commit calls to record.
        calls: List appended to on each commit call.
    """
    original_commit = session.commit

    async def recording_commit() -> None:
        calls.append(1)
        await original_commit()

    cast(Any, session).commit = recording_commit


async def test_get_session_binds_to_the_read_engine_for_a_read_only_request() -> None:
    """get_session binds to the read engine when the request is read-only."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        request = _request(database, read_only=True)
        async for session in get_session(request):
            assert session.get_bind() is database.read_engine.sync_engine
    finally:
        await database.cleanup()


async def test_get_session_binds_to_the_primary_engine_by_default() -> None:
    """get_session binds the session to the primary engine for a normal request."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        request = _request(database)
        async for session in get_session(request):
            assert session.get_bind() is database.engine.sync_engine
    finally:
        await database.cleanup()


async def test_get_auth_session_reuses_the_request_session_when_not_read_only() -> None:
    """get_auth_session yields the shared request session outside read-only routes."""
    database = DatabaseService(local_settings())
    try:
        request = _request(database)
        async for session in database.get_async_session():
            async for auth_session in get_auth_session(request, session):
                assert auth_session is session
    finally:
        await database.cleanup()


async def test_get_auth_session_commits_a_writer_session_when_read_only() -> None:
    """get_auth_session writes through a separate writer session on read-only routes."""
    database = DatabaseService(local_settings(DB_READ_HOST="replica-host"))
    try:
        request = _request(database, read_only=True)
        commit_calls: list[int] = []
        async for read_session in database.get_async_session(read_only=True):
            async for auth_session in get_auth_session(request, read_session):
                assert auth_session is not read_session
                assert auth_session.get_bind() is database.engine.sync_engine
                _spy_commit(auth_session, commit_calls)
        assert commit_calls == [1]
    finally:
        await database.cleanup()
