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
"""Connection read and management tool contracts."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.connection import ConnectionListParams, ConnectionResponse
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData
from kitaru.mcp.models.connections import (
    ConnectionCreate,
    ConnectionGetRequest,
    ConnectionListRequest,
    ConnectionSetDefault,
    ConnectionUpdate,
)
from kitaru.mcp.references import ReferenceResolutionError
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.connections import (
    handle_connection_read,
    handle_connections_manage,
)


def _get_state(client: object) -> MCPServerState:
    return MCPServerState(MCPSettings(), cast(Any, client))


def _get_connection(name: str, *, connection_id: uuid.UUID | None = None) -> Any:
    now = datetime.now(UTC)
    return ConnectionResponse(
        id=connection_id or uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name=name,
        provider="langfuse",
        env={"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"},
        secret_id=uuid.uuid4(),
        secret_keys=["LANGFUSE_SECRET_KEY"],
        default=True,
        created=now,
        updated=now,
    )


async def test_connection_list_filters_by_provider_and_pages() -> None:
    """Listing sends one bounded page request narrowed to the provider."""
    calls: list[ConnectionListParams] = []
    connection = _get_connection("langfuse-prod")

    async def list_connections(params: ConnectionListParams) -> Any:
        calls.append(params)
        return Page(items=[connection], next_cursor="next")

    state = _get_state(
        SimpleNamespace(connections=SimpleNamespace(list=list_connections))
    )
    result = cast(
        PageData[ConnectionResponse],
        await handle_connection_read(
            state,
            ConnectionListRequest(
                operation="list", provider="langfuse", size=7, cursor="before"
            ),
        ),
    )

    [params] = calls
    assert params.size == 7
    assert params.cursor == "before"
    assert isinstance(params.filter, FilterCondition)
    assert params.filter.field == "provider"
    assert params.filter.op is FilterOp.EQ
    assert params.filter.value == "langfuse"
    assert result.items == [connection]
    assert result.page.size == 7
    assert result.page.has_more is True


async def test_connection_list_without_provider_sends_no_filter() -> None:
    """An unfiltered list request reaches the SDK without a filter."""
    calls: list[ConnectionListParams] = []

    async def list_connections(params: ConnectionListParams) -> Any:
        calls.append(params)
        return Page(items=[], next_cursor=None)

    await handle_connection_read(
        _get_state(SimpleNamespace(connections=SimpleNamespace(list=list_connections))),
        ConnectionListRequest(operation="list"),
    )

    [params] = calls
    assert params.filter is None


async def test_connection_get_accepts_a_uuid_and_an_exact_name() -> None:
    """A reference resolves through one direct get or one bounded list."""
    connection = _get_connection("langfuse-prod")
    gets: list[uuid.UUID] = []

    async def get(connection_id: uuid.UUID) -> Any:
        gets.append(connection_id)
        return connection

    async def list_connections(params: ConnectionListParams) -> Any:
        assert params.size == 2
        return Page(items=[connection], next_cursor=None)

    state = _get_state(
        SimpleNamespace(
            connections=SimpleNamespace(get=get, list=list_connections),
        )
    )
    by_id = await handle_connection_read(
        state, ConnectionGetRequest(operation="get", reference=str(connection.id))
    )
    by_name = await handle_connection_read(
        state, ConnectionGetRequest(operation="get", reference="langfuse-prod")
    )

    assert gets == [connection.id]
    assert by_id is connection
    assert by_name is connection


async def test_connection_get_reports_an_unknown_name() -> None:
    """A name with no exact match is a bounded not-found failure."""

    async def list_connections(_params: ConnectionListParams) -> Any:
        return Page(items=[], next_cursor=None)

    with pytest.raises(ReferenceResolutionError, match="was not found"):
        await handle_connection_read(
            _get_state(
                SimpleNamespace(connections=SimpleNamespace(list=list_connections))
            ),
            ConnectionGetRequest(operation="get", reference="missing"),
        )


async def test_connection_read_never_returns_secret_values() -> None:
    """The read result carries key names only."""
    connection = _get_connection("langfuse-prod")

    async def get(_connection_id: uuid.UUID) -> Any:
        return connection

    result = cast(
        ConnectionResponse,
        await handle_connection_read(
            _get_state(SimpleNamespace(connections=SimpleNamespace(get=get))),
            ConnectionGetRequest(operation="get", reference=str(connection.id)),
        ),
    )

    dumped = result.model_dump(mode="json")
    assert dumped["secret_keys"] == ["LANGFUSE_SECRET_KEY"]
    assert "secrets" not in dumped


async def test_connection_management_uses_only_typed_sdk_mutations() -> None:
    """Every mutation reaches the SDK as its exact typed request."""
    calls: list[tuple[str, object]] = []
    idempotency_keys: list[str | None] = []
    connection_id = uuid.uuid4()

    async def create(request: object, idempotency_key: str | None = None) -> object:
        calls.append(("create", request))
        idempotency_keys.append(idempotency_key)
        return SimpleNamespace()

    async def update(_connection_id: uuid.UUID, request: object) -> object:
        calls.append(("update", request))
        return SimpleNamespace()

    state = _get_state(
        SimpleNamespace(connections=SimpleNamespace(create=create, update=update))
    )
    await handle_connections_manage(
        state,
        ConnectionCreate(
            operation="create",
            name="langfuse-prod",
            provider="langfuse",
            env={"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"},
            secrets={"LANGFUSE_SECRET_KEY": "sk-live"},
            default=True,
            idempotency_key="retry-connection-1",
        ),
    )
    await handle_connections_manage(
        state,
        ConnectionUpdate(
            operation="update",
            connection_id=connection_id,
            secrets={"LANGFUSE_SECRET_KEY": "sk-rotated"},
        ),
    )
    await handle_connections_manage(
        state,
        ConnectionSetDefault(operation="set_default", connection_id=connection_id),
    )

    assert [name for name, _ in calls] == ["create", "update", "update"]
    created = cast(Any, calls[0][1])
    assert created.name == "langfuse-prod"
    assert created.provider == "langfuse"
    assert created.env == {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}
    assert created.secrets["LANGFUSE_SECRET_KEY"].get_secret_value() == "sk-live"
    assert created.default is True
    updated = cast(Any, calls[1][1])
    assert set(updated.model_dump(exclude_unset=True)) == {"secrets"}
    assert updated.secrets["LANGFUSE_SECRET_KEY"].get_secret_value() == "sk-rotated"
    assert cast(Any, calls[2][1]).model_dump(exclude_unset=True) == {"default": True}
    assert idempotency_keys == ["retry-connection-1"]


async def test_connection_update_requires_at_least_one_field() -> None:
    """An update naming no field is rejected before any SDK call."""
    with pytest.raises(ValueError, match="at least one field"):
        ConnectionUpdate(operation="update", connection_id=uuid.uuid4())


async def test_connection_update_rejects_explicit_nulls() -> None:
    """An update cannot clear env, secrets, or the default flag with null."""
    with pytest.raises(ValueError, match="env cannot be null"):
        ConnectionUpdate(
            operation="update", connection_id=uuid.uuid4(), env=cast(Any, None)
        )
