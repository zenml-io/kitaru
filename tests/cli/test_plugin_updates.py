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
"""Plugin update commands and script description handling."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.cli.output_rendering import render_text

from kitaru.api_models.v1.analyzer import AnalyzerUpdateRequest
from kitaru.api_models.v1.evaluator import EvaluatorUpdateRequest
from kitaru.api_models.v1.importer import ImporterUpdateRequest
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context
from kitaru.cli.output import CommandResult
from kitaru.cli.registration import ScriptSource, prepare_plugin_source


@dataclass
class StubModel:
    """Small response model supporting dump and partial updates."""

    name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    latest_version: int = 1
    version: int = 1
    description: str | None = None
    logo_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    connection_schema: dict[str, Any] | None = None

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        """Return a JSON-like response projection."""
        assert mode == "json"
        return {
            "id": str(self.id),
            "name": self.name,
            "latest_version": self.latest_version,
            "version": self.version,
            "description": self.description,
            "logo_url": self.logo_url,
            "metadata": self.metadata,
            "connection_schema": self.connection_schema,
        }


class StubPluginResource:
    """Async SDK fake for importer, evaluator, and analyzer resources."""

    def __init__(self, items: list[StubModel] | None = None) -> None:
        """Initialize recorded calls and stored parents."""
        self.items = items or []
        self.created_requests: list[Any] = []
        self.updated_requests: list[tuple[uuid.UUID, Any]] = []
        self.version_requests: list[tuple[uuid.UUID, Any]] = []
        self.parent = StubModel("created")
        self.version = StubModel("version", version=2)
        self.update_error: Exception | None = None
        self.version_error: Exception | None = None

    async def list(self, params: Any = None) -> Any:
        """Return one bounded page of configured parents."""
        assert params is not None
        return SimpleNamespace(items=self.items[: params.size], next_cursor=None)

    async def get(self, item_id: uuid.UUID) -> StubModel:
        """Return one exact parent by UUID."""
        for item in self.items:
            if item.id == item_id:
                return item
        raise AssertionError("unexpected UUID")

    async def create(
        self, request: Any, idempotency_key: str | None = None
    ) -> StubModel:
        """Record parent creation."""
        del idempotency_key
        self.created_requests.append(request)
        created = replace(
            self.parent,
            name=request.name,
            description=request.description,
            metadata=request.metadata,
            connection_schema=request.connection_schema,
        )
        self.items = [created]
        return created

    async def update(self, item_id: uuid.UUID, request: Any) -> StubModel:
        """Apply explicit update fields and return the updated parent."""
        self.updated_requests.append((item_id, request))
        if self.update_error is not None:
            raise self.update_error
        parent = await self.get(item_id)
        updates: dict[str, Any] = {}
        for field_name in request.model_fields_set:
            updates[field_name] = getattr(request, field_name)
        updated = replace(parent, **updates)
        self.items = [updated if item.id == item_id else item for item in self.items]
        return updated

    async def create_version(
        self,
        parent_id: uuid.UUID,
        request: Any,
        idempotency_key: str | None = None,
    ) -> StubModel:
        """Record version creation and bump latest_version."""
        del idempotency_key
        self.version_requests.append((parent_id, request))
        if self.version_error is not None:
            raise self.version_error
        parent = await self.get(parent_id)
        updated = replace(
            parent, latest_version=max(parent.latest_version, self.version.version)
        )
        self.items = [updated if item.id == parent_id else item for item in self.items]
        return self.version


class StubBlobs:
    """Blob upload fake recording script bytes."""

    def __init__(self) -> None:
        """Initialize upload list."""
        self.uploads: list[tuple[bytes, str, str | None]] = []
        self.blob = StubModel("blob")

    async def upload(
        self, content: bytes, media_type: str, filename: str | None
    ) -> StubModel:
        """Record one blob upload."""
        self.uploads.append((content, media_type, filename))
        return self.blob


class StubClient:
    """Asset SDK client fake for all three plugin resources."""

    def __init__(self) -> None:
        """Initialize plugin resources and blob storage."""
        self.importers = StubPluginResource()
        self.evaluators = StubPluginResource()
        self.analyzers = StubPluginResource()
        self.blobs = StubBlobs()


def _install_client(monkeypatch: pytest.MonkeyPatch, client: StubClient) -> None:
    """Patch CLI asset client factory with the given fake."""

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)


@pytest.mark.parametrize(
    ("kind", "request_type"),
    [
        ("evaluator", EvaluatorUpdateRequest),
        ("importer", ImporterUpdateRequest),
        ("analyzer", AnalyzerUpdateRequest),
    ],
)
def test_cli_plugin_update_sends_only_selected_fields(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    kind: str,
    request_type: type[Any],
) -> None:
    """Plugin update sends only explicitly provided fields and clears nullable ones."""
    client = StubClient()
    resource = getattr(client, f"{kind}s")
    parent = StubModel(
        "refund-quality",
        description="Old description",
        logo_url="https://example.com/old.png",
        metadata={"team": "qa"},
        connection_schema={"type": "object"},
    )
    resource.items = [parent]
    _install_client(monkeypatch, client)

    schema = tmp_path / "connection.json"
    schema.write_text('{"type": "object", "properties": {"BASE_URL": {}}}', "utf-8")

    assert (
        app_module.main(
            [
                kind,
                "update",
                "refund-quality",
                "--description",
                "Checks refund accuracy.",
                "--logo-url",
                "https://example.com/new.png",
                "--metadata",
                '{"owner":"ops"}',
                "--connection-schema",
                str(schema),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == f"{kind}.update"
    assert payload["item"]["description"] == "Checks refund accuracy."
    assert payload["item"]["logo_url"] == "https://example.com/new.png"
    assert payload["item"]["metadata"] == {"owner": "ops"}
    assert payload["item"]["connection_schema"] == {
        "type": "object",
        "properties": {"BASE_URL": {}},
    }
    target_id, request = resource.updated_requests[-1]
    assert target_id == parent.id
    assert isinstance(request, request_type)
    assert request.connection_schema == {
        "type": "object",
        "properties": {"BASE_URL": {}},
    }
    assert request.model_fields_set == {
        "description",
        "logo_url",
        "metadata",
        "connection_schema",
    }

    assert (
        app_module.main(
            [
                kind,
                "update",
                str(parent.id),
                "--clear-description",
                "--clear-logo-url",
                "--clear-connection-schema",
                "--metadata",
                "{}",
            ]
        )
        == 0
    )
    cleared = json.loads(capsys.readouterr().out)
    assert cleared["item"]["description"] is None
    assert cleared["item"]["logo_url"] is None
    assert cleared["item"]["connection_schema"] is None
    assert cleared["item"]["metadata"] == {}
    _, clear_request = resource.updated_requests[-1]
    assert clear_request.model_fields_set == {
        "description",
        "logo_url",
        "metadata",
        "connection_schema",
    }
    assert clear_request.description is None
    assert clear_request.logo_url is None
    assert clear_request.connection_schema is None
    assert clear_request.metadata == {}


@pytest.mark.parametrize("kind", ["evaluator", "importer", "analyzer"])
def test_cli_plugin_update_single_field_omits_untouched_fields(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    kind: str,
) -> None:
    """Updating only --description leaves all other request fields unset."""
    client = StubClient()
    resource = getattr(client, f"{kind}s")
    parent = StubModel("demo", logo_url="https://example.com/keep.png")
    resource.items = [parent]
    _install_client(monkeypatch, client)

    assert app_module.main([kind, "update", "demo", "--description", "Updated"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["item"]["description"] == "Updated"
    assert payload["item"]["logo_url"] == "https://example.com/keep.png"
    _, request = resource.updated_requests[0]
    assert request.model_fields_set == {"description"}


@pytest.mark.parametrize(
    ("args", "fragment"),
    [
        ([], "Select at least one evaluator update"),
        (
            ["--description", "A", "--clear-description"],
            "--description and --clear-description",
        ),
        (
            ["--logo-url", "https://example.com/a.png", "--clear-logo-url"],
            "--logo-url and --clear-logo-url",
        ),
        (
            ["--connection-schema", "schema.json", "--clear-connection-schema"],
            "--connection-schema and --clear-connection-schema",
        ),
    ],
)
def test_cli_plugin_update_rejects_invalid_combinations_before_network(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    fragment: str,
) -> None:
    """Invalid or empty update options fail with exit 2 before any API call."""
    client = StubClient()
    client.evaluators.items = [StubModel("demo")]
    _install_client(monkeypatch, client)

    exit_code = app_module.main(["evaluator", "update", "demo", *args])
    assert exit_code == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert fragment in error["message"]
    assert client.evaluators.updated_requests == []


@pytest.mark.parametrize("kind", ["evaluator", "importer", "analyzer"])
def test_cli_plugin_update_rejects_builtin_plugins(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    kind: str,
) -> None:
    """Updating a built-in kitaru/ plugin exits 3 without calling update."""
    client = StubClient()
    resource = getattr(client, f"{kind}s")
    resource.items = [StubModel("kitaru/default")]
    _install_client(monkeypatch, client)

    exit_code = app_module.main(
        [kind, "update", "kitaru/default", "--description", "Nope", "--output", "json"]
    )
    assert exit_code == 3
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "forbidden"
    assert "kitaru/default" in error["message"]
    assert "read-only" in error["message"]
    assert resource.updated_requests == []


@pytest.mark.parametrize(
    ("source_text", "expected_description"),
    [
        (
            '"""Module docstring."""\n\n'
            'DESCRIPTION = "Constant wins."\n\n'
            "def evaluate(session, **params):\n    return []\n",
            "Constant wins.",
        ),
        (
            'DESCRIPTION: str = """\n    Multi-line\n    annotated description.\n"""\n'
            "def evaluate(session, **params):\n    return []\n",
            "Multi-line\nannotated description.",
        ),
        (
            '"""\n    Module docstring fallback.\n\n    Second line.\n"""\n'
            "def evaluate(session, **params):\n    return []\n",
            "Module docstring fallback.\n\nSecond line.",
        ),
        (
            '"""Docstring fallback when DESCRIPTION is dynamic."""\n'
            "DESCRIPTION = 'a'.upper()\n"
            "def evaluate(session, **params):\n    return []\n",
            "Docstring fallback when DESCRIPTION is dynamic.",
        ),
        (
            '"""   \n\t  """\n'
            'DESCRIPTION = "   "\n'
            "def evaluate(session, **params):\n    return []\n",
            None,
        ),
        (
            "DESCRIPTION = 123\ndef evaluate(session, **params):\n    return []\n",
            None,
        ),
    ],
)
def test_prepare_plugin_source_extracts_static_script_description(
    tmp_path: Path, source_text: str, expected_description: str | None
) -> None:
    """Static script parsing prefers DESCRIPTION, then module docstring."""
    script = tmp_path / "evaluator.py"
    script.write_text(source_text, encoding="utf-8")

    source = prepare_plugin_source(script=script, package=None, entrypoint="evaluate")
    assert isinstance(source, ScriptSource)
    assert source.description == expected_description


@pytest.mark.parametrize("kind", ["evaluator", "importer", "analyzer"])
def test_cli_plugin_register_uses_script_description_unless_flag_overrides(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    kind: str,
) -> None:
    """Register uses the script description when --description is omitted."""
    client = StubClient()
    resource = getattr(client, f"{kind}s")
    _install_client(monkeypatch, client)

    script = tmp_path / f"{kind}.py"
    script.write_text(
        '"""Checks outputs against policy."""\n'
        "def run(*args, **kwargs):\n    return []\n",
        encoding="utf-8",
    )

    assert (
        app_module.main(
            [
                kind,
                "register",
                "from-script",
                "--script",
                str(script),
                "--entrypoint",
                "run",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert resource.created_requests[-1].description == "Checks outputs against policy."

    assert (
        app_module.main(
            [
                kind,
                "register",
                "explicit-flag",
                "--script",
                str(script),
                "--entrypoint",
                "run",
                "--description",
                "CLI flag wins.",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert resource.created_requests[-1].description == "CLI flag wins."

    assert (
        app_module.main(
            [
                kind,
                "register",
                "from-package",
                "--package",
                "demo-plugin==1.0.0",
                "--entrypoint",
                "demo_plugin:run",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert resource.created_requests[-1].description is None


@pytest.mark.parametrize("kind", ["evaluator", "importer", "analyzer"])
def test_cli_plugin_version_register_updates_description_only_when_explicit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    kind: str,
) -> None:
    """Version register never auto-overwrites parent description from script."""
    client = StubClient()
    resource = getattr(client, f"{kind}s")
    parent = StubModel("demo", description="Manually curated description")
    resource.items = [parent]
    _install_client(monkeypatch, client)

    script = tmp_path / f"{kind}.py"
    script.write_text(
        'DESCRIPTION = "Script text that must not silently overwrite."\n'
        "def run(*args, **kwargs):\n    return []\n",
        encoding="utf-8",
    )

    assert (
        app_module.main(
            [
                kind,
                "version",
                "register",
                "demo",
                "--script",
                str(script),
                "--entrypoint",
                "run",
            ]
        )
        == 0
    )
    without_flag = json.loads(capsys.readouterr().out)
    assert resource.updated_requests == []
    assert without_flag["item"][kind]["description"] == "Manually curated description"

    assert (
        app_module.main(
            [
                kind,
                "version",
                "register",
                "demo",
                "--script",
                str(script),
                "--entrypoint",
                "run",
                "--description",
                "Updated explicitly on v2.",
            ]
        )
        == 0
    )
    with_flag = json.loads(capsys.readouterr().out)
    assert len(resource.updated_requests) == 1
    target_id, update_request = resource.updated_requests[0]
    assert target_id == parent.id
    assert update_request.model_fields_set == {"description"}
    assert update_request.description == "Updated explicitly on v2."
    assert with_flag["item"][kind]["description"] == "Updated explicitly on v2."


def test_cli_version_register_reports_partial_failure_on_description_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """A failed description PATCH after version creation reports the created version."""
    client = StubClient()
    parent = StubModel("demo", description="Old")
    client.evaluators.items = [parent]
    client.evaluators.update_error = RuntimeError("update rejected")
    _install_client(monkeypatch, client)

    script = tmp_path / "eval.py"
    script.write_text("def evaluate(session, **params):\n    return []\n", "utf-8")

    exit_code = app_module.main(
        [
            "evaluator",
            "version",
            "register",
            "demo",
            "--script",
            str(script),
            "--entrypoint",
            "evaluate",
            "--description",
            "New description",
            "--output",
            "json",
        ]
    )
    assert exit_code != 0
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "partial_failure"
    assert payload["error"]["details"]["version"] == {
        "completed": True,
        "id": str(client.evaluators.version.id),
        "version": 2,
    }
    assert payload["error"]["details"]["parent_update"] == {"completed": False}
    assert payload["error"]["details"]["blob"]["id"] == str(client.blobs.blob.id)
    assert f"kitaru evaluator update {parent.id}" in payload["error"]["hint"]


def test_cli_plugin_version_register_skips_description_update_when_version_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """A failed version creation does not mutate the parent description."""
    client = StubClient()
    client.evaluators.items = [StubModel("demo", description="Old")]
    client.evaluators.version_error = RuntimeError("version rejected")
    _install_client(monkeypatch, client)

    script = tmp_path / "eval.py"
    script.write_text("def evaluate(session, **params):\n    return []\n", "utf-8")

    exit_code = app_module.main(
        [
            "evaluator",
            "version",
            "register",
            "demo",
            "--script",
            str(script),
            "--entrypoint",
            "evaluate",
            "--description",
            "Should not apply",
        ]
    )
    assert exit_code != 0
    capsys.readouterr()
    assert client.evaluators.updated_requests == []


@pytest.mark.parametrize("kind", ["importer", "evaluator", "analyzer"])
def test_human_plugin_update_renders_description(kind: str) -> None:
    """Human output for plugin updates renders the updated description."""
    stdout, _ = render_text(
        f"{kind}.update",
        CommandResult(
            item={
                "id": str(uuid.uuid4()),
                "name": "demo",
                "description": "Updated human description.",
                "latest_version": 2,
                "created": "2026-08-03T00:00:00Z",
                "updated": "2026-08-03T01:00:00Z",
                "metadata": {},
            }
        ),
        rich=False,
    )

    assert "demo" in stdout
    assert "Updated human description." in stdout
