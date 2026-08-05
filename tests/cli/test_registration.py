#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Asset registration, source validation, and exact lookup behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.importer import ImporterCreateRequest
from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.registration import (
    PackageSource,
    ScriptSource,
    get_agent_version,
    get_plugin_version,
    normalize_agent_source,
    page_result,
    prepare_plugin_source,
    register_agent,
    register_plugin,
    register_plugin_version,
    resolve_asset,
    validate_package_source,
)


@dataclass
class StubModel:
    """Small response object exposing the SDK model surface used by helpers."""

    name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    latest_version: int = 1
    version: int = 1
    values: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        """Return a JSON-like response projection."""
        assert mode == "json"
        return {
            "id": str(self.id),
            "name": self.name,
            "latest_version": self.latest_version,
            "version": self.version,
            **self.values,
        }


class StubResource:
    """Configurable async SDK resource fake."""

    def __init__(self, items: list[StubModel] | None = None) -> None:
        """Initialize resource state."""
        self.items = items or []
        self.versions: list[StubModel] = []
        self.created_requests: list[Any] = []
        self.version_requests: list[tuple[uuid.UUID, Any]] = []
        self.create_error: Exception | None = None
        self.version_error: Exception | None = None
        self.parent = StubModel("created")
        self.version = StubModel("version", version=2)

    async def iter(self):
        """Yield configured parents."""
        for item in self.items:
            yield item

    async def list(self, params: Any = None) -> Any:
        """Return one configured bounded parent page."""
        assert params is not None
        assert params.size == 2
        return SimpleNamespace(items=self.items[: params.size], next_cursor=None)

    async def get(self, item_id: uuid.UUID) -> StubModel:
        """Return one exact configured UUID."""
        for item in self.items:
            if item.id == item_id:
                return item
        raise AssertionError("unexpected UUID")

    async def iter_versions(self, parent_id: uuid.UUID):
        """Yield configured versions for one parent."""
        del parent_id
        for item in self.versions:
            yield item

    async def get_version(self, parent_id: uuid.UUID, version: int) -> StubModel:
        """Return one configured numeric version."""
        del parent_id
        for item in self.versions:
            if item.version == version:
                return item
        raise AssertionError("unexpected version")

    async def create(self, request: Any) -> StubModel:
        """Record parent creation."""
        self.created_requests.append(request)
        if self.create_error:
            raise self.create_error
        return self.parent

    async def create_version(self, parent_id: uuid.UUID, request: Any) -> StubModel:
        """Record version creation."""
        self.version_requests.append((parent_id, request))
        if self.version_error:
            raise self.version_error
        return self.version


class StubBlobs:
    """Blob upload fake recording exact bytes."""

    def __init__(self) -> None:
        """Initialize uploads."""
        self.uploads: list[tuple[bytes, str, str | None]] = []
        self.blob = StubModel("blob")

    async def upload(
        self, content: bytes, media_type: str, filename: str | None
    ) -> StubModel:
        """Record one upload."""
        self.uploads.append((content, media_type, filename))
        return self.blob


class StubClient:
    """Asset SDK client fake."""

    def __init__(self) -> None:
        """Initialize all asset resources."""
        self.agents = StubResource()
        self.importers = StubResource()
        self.evaluators = StubResource()
        self.blobs = StubBlobs()


def test_agent_source_normalization_accepts_only_explicit_commands() -> None:
    """Agent registration stores commands and rejects removed entrypoint syntax."""
    assert normalize_agent_source(command="run-agent --fast", entrypoint=None) == (
        "run-agent --fast"
    )
    with pytest.raises(CLIError, match="not supported"):
        normalize_agent_source(command=None, entrypoint="pkg.agent:run")
    with pytest.raises(CLIError, match="not supported"):
        normalize_agent_source(command="run", entrypoint="pkg.agent:run")
    with pytest.raises(CLIError, match="requires --command"):
        normalize_agent_source(command=None, entrypoint=None)
    with pytest.raises(CLIError, match="cannot be blank"):
        normalize_agent_source(command="  ", entrypoint=None)


def test_plugin_source_validation_reads_script_once_and_requires_a_pin(
    tmp_path: Path,
) -> None:
    """Registration preflight validates syntax, top-level attributes, and pins."""
    script = tmp_path / "parser.py"
    content = b"def parse(payload, params):\n    return iter(())\n"
    script.write_bytes(content)

    source = prepare_plugin_source(script=script, package=None, entrypoint="parse")
    assert source == ScriptSource(script, content, "parse")

    package = validate_package_source("example[fast]==1.2.3", "example:parse")
    assert package == PackageSource("example[fast]==1.2.3", "example:parse")
    with pytest.raises(CLIError, match="must have one exact"):
        validate_package_source("example>=1", "example:parse")
    with pytest.raises(CLIError, match="must have one exact"):
        validate_package_source("example===1.2.3", "example:parse")
    with pytest.raises(CLIError, match="exceeds 255"):
        validate_package_source(f"example==1.{('0' * 250)}", "example:parse")
    with pytest.raises(CLIError, match="no top-level attribute"):
        prepare_plugin_source(script=script, package=None, entrypoint="missing")


async def test_exact_resolution_never_uses_fuzzy_or_ambiguous_names() -> None:
    """Bare UUIDs use get while names require one exact case-sensitive match."""
    exact = StubModel("Example")
    resource = StubResource([exact, StubModel("example")])
    assert await resolve_asset(resource, str(exact.id), "Agent") is exact
    assert await resolve_asset(resource, "Example", "Agent") is exact
    with pytest.raises(CLIError, match="was not found"):
        await resolve_asset(resource, "EXAMPLE", "Agent")

    duplicate = StubResource([StubModel("same"), StubModel("same")])
    with pytest.raises(CLIError) as error:
        await resolve_asset(duplicate, "same", "Agent")
    assert error.value.kind == "conflict"


async def test_version_reads_resolve_latest_to_exact_server_numbers() -> None:
    """Receipts from @latest still contain the exact resolved version number."""
    parent = StubModel("asset", latest_version=3)
    agent_version = StubModel("agent-version", version=3)
    plugin_version = StubModel("plugin-version", version=3)
    client = StubClient()
    client.agents.items = [parent]
    client.agents.versions = [agent_version]
    client.importers.items = [parent]
    client.importers.versions = [plugin_version]

    resolved_parent, resolved_agent = await get_agent_version(client, "asset@latest")
    assert resolved_parent is parent
    assert resolved_agent.version == 3

    resolved_parent, resolved_plugin = await get_plugin_version(
        client.importers, "asset@latest", "Importer"
    )
    assert resolved_parent is parent
    assert resolved_plugin.version == 3


async def test_agent_registration_reports_surviving_parent_on_version_failure() -> None:
    """A failed second request produces an actionable receipt without rollback."""
    client = StubClient()
    client.agents.version_error = RuntimeError("version rejected")

    with pytest.raises(CLIError) as error:
        await register_agent(
            client,
            AgentCreateRequest(name="agent"),
            AgentVersionCreateRequest(run_spec=RunSpec(command="run")),
        )

    assert error.value.kind == "partial_failure"
    assert error.value.details["parent"] == {
        "completed": True,
        "id": str(client.agents.parent.id),
    }
    assert error.value.details["version"] == {"completed": False}
    assert len(client.agents.created_requests) == 1


async def test_script_plugin_registration_uploads_validated_bytes_before_version() -> (
    None
):
    """Script registration passes the returned blob ID into the version request."""
    client = StubClient()
    source = ScriptSource(Path("parser.py"), b"script bytes", "parse")

    result = await register_plugin(
        client,
        kind="importer",
        parent_request=ImporterCreateRequest(name="provider", provider="demo"),
        source=source,
        display_version="v1",
    )

    assert client.blobs.uploads == [(b"script bytes", "text/x-python", "parser.py")]
    _, request = client.importers.version_requests[0]
    assert request.source.blob_id == client.blobs.blob.id
    assert request.source.entrypoint == "parse"
    assert request.display_version == "v1"
    assert result.item["phases"]["blob"]["id"] == str(client.blobs.blob.id)


async def test_uploaded_blob_is_reported_when_version_registration_fails() -> None:
    """A later version failure identifies the reusable uploaded blob."""
    parent = StubModel("existing")
    client = StubClient()
    client.importers.items = [parent]
    client.importers.version_error = RuntimeError("version rejected")

    with pytest.raises(CLIError) as error:
        await register_plugin_version(
            client,
            kind="importer",
            reference="existing",
            source=ScriptSource(Path("parser.py"), b"bytes", "parse"),
            display_version=None,
        )

    assert error.value.kind == "partial_failure"
    assert error.value.details["blob"]["id"] == str(client.blobs.blob.id)
    assert error.value.details["parent"]["id"] == str(parent.id)


def test_cli_agent_register_uses_shared_runner_and_output_contract(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The Cyclopts leaf passes normalized requests through the existing envelope."""
    client = StubClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "agent",
                "register",
                "demo",
                "--command",
                "python -m example.agent",
                "--env",
                "MODE=test",
                "--tool",
                "search",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["command"] == "agent.register"
    request = client.agents.version_requests[0][1]
    assert request.run_spec.command == "python -m example.agent"
    assert request.run_spec.env == {"MODE": "test"}
    assert request.capabilities.tools == ["search"]


def test_cli_agent_entrypoint_is_rejected_before_api_mutation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Removed agent entrypoint syntax cannot create a parent or version."""
    client = StubClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            ["agent", "register", "demo", "--entrypoint", "example.agent:run"]
        )
        == 2
    )
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert client.agents.created_requests == []
    assert client.agents.version_requests == []


def test_page_result_preserves_server_order_and_cursor() -> None:
    """List envelopes use the SDK page cursor instead of inferred pagination."""
    from kitaru.api_models.v1.base import Page

    first = StubModel("first")
    second = StubModel("second")
    result = page_result(Page[Any](items=[first, second], next_cursor="next"), size=2)
    assert [item["name"] for item in result.items or []] == ["first", "second"]
    assert result.page == {"limit": 2, "next_cursor": "next", "truncated": True}
