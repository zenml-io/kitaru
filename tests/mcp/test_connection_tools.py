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
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult
from mcp_fakes import build_server_context

from kitaru.api_models.v1.base import Page
from kitaru.api_models.v1.connection import (
    ConnectionListParams,
    ConnectionResponse,
    ConnectionUpdateRequest,
)
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
from kitaru.mcp.settings import CapabilityMode, MCPSettings
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


async def test_credential_env_keys_are_refused_without_echoing_values() -> None:
    """A key that names a credential belongs in `secrets`, never in `env`."""
    server, context = build_server_context(
        SimpleNamespace(), mode=CapabilityMode.STANDARD
    )
    env = {
        "OPENAI_API_KEY": "sk-planted-value",
        "awsSecretAccessKey": "planted-camel-value",
        "HFToken": "planted-acronym-value",
        "MAX_TOKENS": "4096",
    }
    request = {"operation": "create", "name": "openai", "provider": "openai"}

    with pytest.raises(ToolError) as refused:
        await server.call_tool(
            "kitaru_connections_manage", {"request": {**request, "env": env}}, context
        )

    message = str(refused.value)
    for name in ("OPENAI_API_KEY", "awsSecretAccessKey", "HFToken"):
        assert name in message
    assert "MAX_TOKENS" not in message
    assert "secrets" in message
    assert "planted" not in message


def _get_legacy_client(
    sent: list[ConnectionUpdateRequest],
) -> tuple[ConnectionResponse, SimpleNamespace]:
    """A connection made before `env` refused credentials, holding one there."""
    env = {
        "OPENAI_API_KEY": "sk-legacy",
        "OTEL_HEADERS": "Authorization=Bearer abc123def",
        "AWS_REGION": "eu",
    }
    stored = _get_connection("legacy").model_copy(update={"env": env})

    async def get(_connection_id: uuid.UUID) -> Any:
        return stored

    async def update(_connection_id: uuid.UUID, request: Any) -> Any:
        sent.append(request)
        return stored

    return stored, SimpleNamespace(connections=SimpleNamespace(get=get, update=update))


def _structured(result: object) -> dict[str, Any]:
    return cast(dict[str, Any], cast(CallToolResult, result).structured_content)


async def test_update_sending_back_the_read_env_keeps_the_masked_values() -> None:
    """An agent edits the `env` it read; every value the read masked stays stored."""
    sent: list[ConnectionUpdateRequest] = []
    stored, client = _get_legacy_client(sent)
    server, context = build_server_context(client, mode=CapabilityMode.STANDARD)
    read = await server.call_tool(
        "kitaru_connection_read",
        {"request": {"operation": "get", "reference": str(stored.id)}},
        context,
    )
    env = {**_structured(read)["data"]["env"], "AWS_REGION": "us"}
    request = {"operation": "update", "connection_id": str(stored.id)}

    updated = await server.call_tool(
        "kitaru_connections_manage", {"request": {**request, "env": env}}, context
    )
    # A masked form that is not what the read showed for this key is a new value.
    await server.call_tool(
        "kitaru_connections_manage",
        {"request": {**request, "env": {**env, "OTEL_HEADERS": "KITKEY_***"}}},
        context,
    )

    kept, changed = sent
    assert env["OPENAI_API_KEY"] == "***"
    assert env["OTEL_HEADERS"] == "Authorization=Bearer ***"
    assert _structured(updated)["ok"] is True
    assert kept.env == {**stored.env, "AWS_REGION": "us"}
    assert "secrets" not in kept.model_fields_set
    assert changed.env is not None
    assert changed.env["OTEL_HEADERS"] == "KITKEY_***"


async def test_update_refuses_any_credential_value_but_the_masked_one() -> None:
    """A plaintext credential never enters `env`, even the stored one."""
    sent: list[ConnectionUpdateRequest] = []
    stored, client = _get_legacy_client(sent)
    server, context = build_server_context(client, mode=CapabilityMode.STANDARD)
    request = {"operation": "update", "connection_id": str(stored.id)}

    with pytest.raises(ToolError) as plaintext:
        await server.call_tool(
            "kitaru_connections_manage",
            {"request": {**request, "env": {"OPENAI_API_KEY": "sk-legacy"}}},
            context,
        )
    unknown = await server.call_tool(
        "kitaru_connections_manage",
        {"request": {**request, "env": {"HF_TOKEN": "***"}}},
        context,
    )

    assert sent == []
    assert "OPENAI_API_KEY" in str(plaintext.value)
    assert "sk-legacy" not in str(plaintext.value)
    assert "replaces every stored secret" in str(plaintext.value)
    refusal = _structured(unknown)["error"]
    assert refusal["code"] == "invalid_arguments"
    assert "HF_TOKEN" in refusal["message"]


async def test_update_refuses_a_masked_secret_value() -> None:
    """`***` cannot keep a stored secret: `secrets` replaces all of them."""
    sent: list[ConnectionUpdateRequest] = []
    stored, client = _get_legacy_client(sent)
    server, context = build_server_context(client, mode=CapabilityMode.STANDARD)
    secrets = {"LANGFUSE_SECRET_KEY": "***", "HF_TOKEN": "hf-planted"}
    request = {"operation": "update", "connection_id": str(stored.id)}

    with pytest.raises(ToolError) as refused:
        await server.call_tool(
            "kitaru_connections_manage",
            {"request": {**request, "secrets": secrets}},
            context,
        )

    assert sent == []
    assert "LANGFUSE_SECRET_KEY" in str(refused.value)
    assert "send every value" in str(refused.value)
    assert "hf-planted" not in str(refused.value)


# One rule decides both what `env` refuses on write and what reads mask.
CREDENTIAL_NAMES = [
    ("OPENAI_API_KEY", True),
    ("HF_TOKEN", True),
    ("HFToken", True),
    ("DB_PASSWORD", True),
    ("PGPASSWORD", True),
    ("PASSPHRASE", True),
    ("AUTHORIZATION", True),
    ("SIGNING_KEY", True),
    ("ENCRYPTION_KEY", True),
    ("SECRETS", True),
    ("API_KEY1", True),
    ("TOKEN1", True),
    ("AWS_SECRET_ACCESS_KEY", True),
    ("awsSecretAccessKey", True),
    ("GOOGLE_APPLICATION_CREDENTIALS", True),
    ("GITHUB_TOKENS", True),
    ("oauth_tokens", True),
    ("SIGNING_KEYS", True),
    ("ENCRYPTION_KEYS", True),
    ("AUTH_KEYS", True),
    ("PASSPHRASES", True),
    ("MAX_TOKENS", False),
    ("HTTPMaxTokens", False),
    ("max_new_tokens", False),
    ("prompt_tokens", False),
    ("secret_keys", False),
    ("TOKENIZER_NAME", False),
    ("AWS_WEB_IDENTITY_TOKEN_FILE", False),
    ("LANGFUSE_PUBLIC_KEY", False),
    ("LANGFUSE_BASE_URL", False),
]


@pytest.mark.parametrize(("name", "credential"), CREDENTIAL_NAMES)
async def test_env_refuses_on_write_exactly_what_reads_mask(
    name: str, credential: bool
) -> None:
    stored = _get_connection("c").model_copy(update={"env": {name: "4096"}})
    created: list[object] = []

    async def create(request: object, idempotency_key: str | None = None) -> Any:
        created.append(request)
        return stored

    async def get(_connection_id: uuid.UUID) -> Any:
        return stored

    client = SimpleNamespace(connections=SimpleNamespace(create=create, get=get))
    server, context = build_server_context(client, mode=CapabilityMode.STANDARD)
    write = {"operation": "create", "name": "c", "provider": "p", "env": {name: "4096"}}
    read = {"operation": "get", "reference": str(stored.id)}

    try:
        await server.call_tool("kitaru_connections_manage", {"request": write}, context)
    except ToolError as refused:
        assert name in str(refused)
    result = await server.call_tool(
        "kitaru_connection_read", {"request": read}, context
    )

    assert bool(created) is not credential
    assert _structured(result)["data"]["env"][name] == ("***" if credential else "4096")
