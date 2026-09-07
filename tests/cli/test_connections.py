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
"""Provider connection CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.connection import (
    ConnectionCreateRequest,
    ConnectionListParams,
    ConnectionUpdateRequest,
)
from kitaru.cli import app as app_module
from kitaru.cli import connections
from kitaru.cli.output import CLIError, CommandResult

_SCHEMA = {
    "type": "object",
    "properties": {
        "LANGFUSE_PUBLIC_KEY": {"type": "string", "writeOnly": True},
        "LANGFUSE_SECRET_KEY": {"type": "string", "writeOnly": True},
        "LANGFUSE_BASE_URL": {
            "type": "string",
            "description": "Langfuse host.",
            "default": "https://cloud.langfuse.com",
        },
        "LANGFUSE_PROJECT": {"type": "string"},
    },
    "required": ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"],
}


@dataclass
class StubModel:
    """Small response exposing the Pydantic serialization surface."""

    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.values[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


class StubConnectionClient:
    """Protocol-shaped client recording connection SDK calls."""

    def __init__(self, *, connection_schema: dict[str, Any] | None = None) -> None:
        self.importer = StubModel(
            uuid.uuid4(),
            {
                "name": "zenml/langfuse",
                "provider": "langfuse",
                "connection_schema": connection_schema,
            },
        )
        self.connection = StubModel(
            uuid.uuid4(),
            {
                "name": "langfuse-prod",
                "provider": "langfuse",
                "env": {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"},
                "secret_keys": ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"],
                "default": True,
            },
        )
        self.created: list[ConnectionCreateRequest] = []
        self.create_idempotency_keys: list[str | None] = []
        self.list_calls: list[ConnectionListParams] = []
        self.update_calls: list[tuple[uuid.UUID, ConnectionUpdateRequest]] = []
        self.deleted: list[uuid.UUID] = []
        self.importers = self._Importers(self)
        self.connections = self._Connections(self)

    class _Importers:
        def __init__(self, owner: "StubConnectionClient") -> None:
            self.owner = owner

        async def get(self, importer_id: uuid.UUID) -> StubModel:
            assert importer_id == self.owner.importer.id
            return self.owner.importer

        async def list(self, params: Any) -> Any:
            assert params.size == 2
            return SimpleNamespace(items=[self.owner.importer], next_cursor=None)

    class _Connections:
        def __init__(self, owner: "StubConnectionClient") -> None:
            self.owner = owner

        async def create(
            self, request: ConnectionCreateRequest, idempotency_key: str | None = None
        ) -> StubModel:
            self.owner.created.append(request)
            self.owner.create_idempotency_keys.append(idempotency_key)
            return self.owner.connection

        async def get(self, connection_id: uuid.UUID) -> StubModel:
            assert connection_id == self.owner.connection.id
            return self.owner.connection

        async def list(self, params: ConnectionListParams) -> Any:
            self.owner.list_calls.append(params)
            return SimpleNamespace(items=[self.owner.connection], next_cursor="next")

        async def update(
            self, connection_id: uuid.UUID, request: ConnectionUpdateRequest
        ) -> StubModel:
            self.owner.update_calls.append((connection_id, request))
            return self.owner.connection

        async def delete(self, connection_id: uuid.UUID) -> None:
            self.owner.deleted.append(connection_id)


def _answer(answers: dict[str, str]) -> Any:
    """Return a prompt callable answering by the key inside the label."""

    def prompt(label: str) -> str:
        key = label.split(" ")[0].rstrip(":")
        assert key in answers, label
        return answers[key]

    return prompt


async def test_create_prompts_every_schema_property_in_order() -> None:
    """Schema prompts fill secrets and env from writeOnly and defaults."""
    client = StubConnectionClient(connection_schema=_SCHEMA)
    prompted: list[str] = []
    secret_prompted: list[str] = []

    def value_prompt(label: str) -> str:
        prompted.append(label)
        return ""

    def secret_prompt(label: str) -> str:
        secret_prompted.append(label)
        return "pk-live" if label.startswith("LANGFUSE_PUBLIC_KEY") else "sk-live"

    result = await connections.create_connection(
        client,
        "langfuse-prod",
        importer="zenml/langfuse",
        provider=None,
        values=None,
        secret_values=None,
        default=True,
        non_interactive=False,
        value_prompt=value_prompt,
        secret_prompt=secret_prompt,
    )

    assert secret_prompted == ["LANGFUSE_PUBLIC_KEY: ", "LANGFUSE_SECRET_KEY: "]
    assert prompted == [
        "LANGFUSE_BASE_URL (Langfuse host.) [https://cloud.langfuse.com]: ",
        "LANGFUSE_PROJECT: ",
    ]
    [request] = client.created
    assert request.provider == "langfuse"
    assert request.env == {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}
    assert request.secrets["LANGFUSE_PUBLIC_KEY"].get_secret_value() == "pk-live"
    assert request.secrets["LANGFUSE_SECRET_KEY"].get_secret_value() == "sk-live"
    assert request.default is True
    assert result.item["name"] == "langfuse-prod"


async def test_create_skips_prompts_for_supplied_keys() -> None:
    """A key given on the command line is never prompted for."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    await connections.create_connection(
        client,
        "langfuse-prod",
        importer="zenml/langfuse",
        provider=None,
        values=["LANGFUSE_BASE_URL=https://self.hosted"],
        secret_values=["LANGFUSE_PUBLIC_KEY=pk-live"],
        default=False,
        non_interactive=False,
        value_prompt=_answer({"LANGFUSE_PROJECT": ""}),
        secret_prompt=_answer({"LANGFUSE_SECRET_KEY": "sk-live"}),
    )

    [request] = client.created
    assert request.env == {"LANGFUSE_BASE_URL": "https://self.hosted"}
    assert sorted(request.secrets) == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]


async def test_create_rejects_an_empty_required_property() -> None:
    """A required property cannot be left blank at the prompt."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="LANGFUSE_PUBLIC_KEY is required"):
        await connections.create_connection(
            client,
            "langfuse-prod",
            importer="zenml/langfuse",
            provider=None,
            values=None,
            secret_values=None,
            default=False,
            non_interactive=False,
            value_prompt=_answer({}),
            secret_prompt=_answer({"LANGFUSE_PUBLIC_KEY": ""}),
        )
    assert client.created == []


async def test_non_interactive_create_reports_missing_required_properties() -> None:
    """Without prompts every required property must be supplied directly."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="LANGFUSE_SECRET_KEY"):
        await connections.create_connection(
            client,
            "langfuse-prod",
            importer="zenml/langfuse",
            provider=None,
            values=None,
            secret_values=["LANGFUSE_PUBLIC_KEY=pk-live"],
            default=False,
            non_interactive=True,
        )
    assert client.created == []


async def test_non_interactive_create_accepts_every_required_property() -> None:
    """Supplying the required properties creates without any prompt."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    await connections.create_connection(
        client,
        "langfuse-prod",
        importer="zenml/langfuse",
        provider=None,
        values=None,
        secret_values=["LANGFUSE_PUBLIC_KEY=pk-live", "LANGFUSE_SECRET_KEY=sk-live"],
        default=False,
        non_interactive=True,
    )

    [request] = client.created
    assert sorted(request.secrets) == ["LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    assert request.env == {}


async def test_create_without_a_schema_requires_direct_values() -> None:
    """An importer carrying no schema cannot drive a create form."""
    client = StubConnectionClient(connection_schema=None)

    with pytest.raises(CLIError, match="has no connection schema"):
        await connections.create_connection(
            client,
            "langfuse-prod",
            importer="zenml/langfuse",
            provider=None,
            values=None,
            secret_values=None,
            default=False,
            non_interactive=False,
        )


async def test_create_from_a_provider_sends_only_direct_values() -> None:
    """A provider create skips every importer lookup."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    await connections.create_connection(
        client,
        "langfuse-prod",
        importer=None,
        provider="langfuse",
        values=["LANGFUSE_BASE_URL=https://self.hosted"],
        secret_values=["LANGFUSE_SECRET_KEY=sk-live"],
        default=True,
        non_interactive=True,
        idempotency_key="retry-connection-1",
    )

    [request] = client.created
    assert request.provider == "langfuse"
    assert request.env == {"LANGFUSE_BASE_URL": "https://self.hosted"}
    assert request.secrets["LANGFUSE_SECRET_KEY"].get_secret_value() == "sk-live"
    assert client.create_idempotency_keys == ["retry-connection-1"]


@pytest.mark.parametrize(
    ("importer", "provider"),
    [(None, None), ("zenml/langfuse", "langfuse")],
)
async def test_create_requires_exactly_one_source(
    importer: str | None, provider: str | None
) -> None:
    """The importer and provider options are mutually exclusive."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="exactly one of --importer or --provider"):
        await connections.create_connection(
            client,
            "langfuse-prod",
            importer=importer,
            provider=provider,
            values=None,
            secret_values=None,
            default=False,
            non_interactive=True,
        )


async def test_create_rejects_malformed_values_before_any_lookup() -> None:
    """A malformed KEY=VALUE token fails before the importer is resolved."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="use KEY=VALUE"):
        await connections.create_connection(
            client,
            "langfuse-prod",
            importer="zenml/langfuse",
            provider=None,
            values=["not-an-assignment"],
            secret_values=None,
            default=False,
            non_interactive=True,
        )
    assert client.created == []


async def test_update_sends_only_the_selected_fields() -> None:
    """Only explicitly selected fields reach the merge request."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    await connections.update_connection(
        client,
        "langfuse-prod",
        values=None,
        secret_values=["LANGFUSE_SECRET_KEY=sk-rotated"],
        default=False,
    )

    [(connection_id, request)] = client.update_calls
    assert connection_id == client.connection.id
    assert set(request.model_dump(exclude_unset=True)) == {"secrets", "default"}
    assert request.secrets is not None
    assert request.secrets["LANGFUSE_SECRET_KEY"].get_secret_value() == "sk-rotated"
    assert request.default is False


async def test_update_requires_at_least_one_field() -> None:
    """An update naming no field never reaches the server."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="at least one connection update"):
        await connections.update_connection(
            client,
            "langfuse-prod",
            values=None,
            secret_values=None,
            default=None,
        )
    assert client.update_calls == []


async def test_set_default_patches_only_the_default_flag() -> None:
    """Selecting a default sends exactly one field."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    await connections.set_default_connection(client, "langfuse-prod")

    [(_, request)] = client.update_calls
    assert request.model_dump(exclude_unset=True) == {"default": True}


async def test_delete_requires_force() -> None:
    """Deleting a connection and its secret is guarded."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    with pytest.raises(CLIError, match="requires --force"):
        await connections.delete_connection(client, "langfuse-prod", force=False)
    assert client.deleted == []


@pytest.fixture
def argv_client(monkeypatch: pytest.MonkeyPatch) -> StubConnectionClient:
    """Route public CLI invocations through one recording client."""
    client = StubConnectionClient(connection_schema=_SCHEMA)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    return client


def test_public_connection_argv_covers_every_command(
    argv_client: StubConnectionClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """The registered leaves emit standard JSON and text results."""
    client = argv_client

    assert (
        app_module.main(
            [
                "connection",
                "create",
                "langfuse-prod",
                "--provider",
                "langfuse",
                "--set",
                "LANGFUSE_BASE_URL=https://self.hosted",
                "--set-secret",
                "LANGFUSE_SECRET_KEY=sk-live",
                "--default",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "connection.create"
    assert payload["item"]["id"] == str(client.connection.id)

    assert app_module.main(["connection", "list", "--size", "2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "connection.list"
    assert payload["count"] == 1

    assert (
        app_module.main(["connection", "get", "langfuse-prod", "--output", "text"]) == 0
    )
    assert "langfuse-prod" in capsys.readouterr().out

    assert (
        app_module.main(
            [
                "connection",
                "update",
                "langfuse-prod",
                "--set-secret",
                "LANGFUSE_SECRET_KEY=sk-rotated",
                "--no-default",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "connection.update"

    assert app_module.main(["connection", "set-default", "langfuse-prod"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "connection.set-default"

    assert app_module.main(["connection", "delete", "langfuse-prod", "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "connection.delete"
    assert payload["item"]["deleted"] is True
    assert client.deleted == [client.connection.id]


def test_public_connection_output_never_shows_secret_values(
    argv_client: StubConnectionClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """A created connection renders key names without their values."""
    assert (
        app_module.main(
            [
                "connection",
                "create",
                "langfuse-prod",
                "--provider",
                "langfuse",
                "--set-secret",
                "LANGFUSE_SECRET_KEY=sk-live",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert "sk-live" not in captured.out
    assert "sk-live" not in captured.err
    payload = json.loads(captured.out)
    assert payload["item"]["secret_keys"] == [
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
    ]


def test_non_interactive_create_exits_with_interaction_required(
    argv_client: StubConnectionClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """JSON output implies non-interactive, so a missing property is an error."""
    assert (
        app_module.main(
            ["connection", "create", "langfuse-prod", "--importer", "zenml/langfuse"]
        )
        == 5
    )
    error = json.loads(capsys.readouterr().err)
    assert error["error"]["kind"] == "interaction_required"


def test_importer_register_sends_a_connection_schema_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The schema file is parsed and carried on the importer create request."""
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(_SCHEMA), encoding="utf-8")
    requests: list[Any] = []

    async def register_plugin(_client: Any, **kwargs: Any) -> Any:
        requests.append(kwargs["parent_request"])
        return CommandResult(item={"ok": True})

    @asynccontextmanager
    async def fake_open_client():
        yield SimpleNamespace()

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)
    monkeypatch.setattr(app_module.registration, "register_plugin", register_plugin)
    monkeypatch.setattr(
        app_module.registration,
        "prepare_plugin_source",
        lambda **_kwargs: SimpleNamespace(),
    )

    assert (
        app_module.main(
            [
                "importer",
                "register",
                "zenml/langfuse",
                "--package",
                "example==1.2.3",
                "--entrypoint",
                "example:parse",
                "--provider",
                "langfuse",
                "--connection-schema",
                str(schema_file),
            ]
        )
        == 0
    )
    capsys.readouterr()
    [request] = requests
    assert request.connection_schema == _SCHEMA


def test_importer_register_rejects_a_malformed_connection_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A schema file that is not a JSON object is rejected."""
    schema_file = tmp_path / "schema.json"
    schema_file.write_text("[]", encoding="utf-8")

    @asynccontextmanager
    async def fake_open_client():
        yield SimpleNamespace()

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "importer",
                "register",
                "zenml/langfuse",
                "--package",
                "example==1.2.3",
                "--entrypoint",
                "example:parse",
                "--connection-schema",
                str(schema_file),
            ]
        )
        == 2
    )
    error = json.loads(capsys.readouterr().err)
    assert "must contain a JSON object" in error["error"]["message"]
