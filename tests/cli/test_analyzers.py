#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Analyzer registration and inspection CLI behavior."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.registration import plugin_parent_request, resolve_analyzer_configs


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
        self.parent = StubModel("created")
        self.version = StubModel("version", version=2)

    async def list(self, params: Any = None) -> Any:
        """Return one configured bounded parent page."""
        assert params is not None
        return SimpleNamespace(items=self.items[: params.size], next_cursor=None)

    async def get(self, item_id: uuid.UUID) -> StubModel:
        """Return one exact configured UUID."""
        for item in self.items:
            if item.id == item_id:
                return item
        raise AssertionError("unexpected UUID")

    async def get_version(self, parent_id: uuid.UUID, version: int) -> StubModel:
        """Return one configured numeric version."""
        del parent_id
        for item in self.versions:
            if item.version == version:
                return item
        raise AssertionError("unexpected version")

    async def list_versions(self, parent_id: uuid.UUID, params: Any = None) -> Any:
        """Return one configured bounded version page."""
        del parent_id
        assert params is not None
        return SimpleNamespace(items=self.versions[: params.size], next_cursor=None)

    async def create(
        self, request: Any, idempotency_key: str | None = None
    ) -> StubModel:
        """Record parent creation."""
        del idempotency_key
        self.created_requests.append(request)
        return self.parent

    async def create_version(
        self,
        parent_id: uuid.UUID,
        request: Any,
        idempotency_key: str | None = None,
    ) -> StubModel:
        """Record version creation."""
        del idempotency_key
        self.version_requests.append((parent_id, request))
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
    """Asset SDK client fake exposing the analyzer resource."""

    def __init__(self) -> None:
        """Initialize the analyzer resource and its blob dependency."""
        self.analyzers = StubResource()
        self.blobs = StubBlobs()


def test_analyzer_parent_request_rejects_agent_id_and_provider() -> None:
    """Analyzer parents accept neither agent scoping nor a source provider."""
    with pytest.raises(CLIError, match="only valid for evaluators"):
        plugin_parent_request(
            "analyzer",
            "demo",
            description=None,
            provider=None,
            metadata=None,
            agent_id=uuid.uuid4(),
        )
    with pytest.raises(CLIError, match="only valid for importers"):
        plugin_parent_request(
            "analyzer",
            "demo",
            description=None,
            provider="demo-provider",
            metadata=None,
            agent_id=None,
        )


async def test_resolve_analyzer_configs_reads_selected_versions_and_params() -> None:
    """Selected analyzer tokens resolve to exact configs and identities."""
    analyzer = StubModel("quality")
    version = StubModel("quality-version", version=3)
    client = StubClient()
    client.analyzers.items = [analyzer]
    client.analyzers.versions = [version]

    configs, identities, version_ids = await resolve_analyzer_configs(
        client, ["quality@3"], ['quality@3={"threshold": 0.8}']
    )

    assert len(configs) == 1
    assert configs[0].analyzer == "quality"
    assert configs[0].version == 3
    assert configs[0].params == {"threshold": 0.8}
    assert identities == [
        {
            "id": str(analyzer.id),
            "name": "quality",
            "version_id": str(version.id),
            "version": 3,
        }
    ]
    assert version_ids == [version.id]


async def test_resolve_analyzer_configs_rejects_unselected_params_token() -> None:
    """Parameters for a token outside the selected analyzers fail before upload."""
    client = StubClient()

    with pytest.raises(CLIError, match="is not a selected analyzer"):
        await resolve_analyzer_configs(
            client, ["quality@3"], ['other@1={"threshold": 0.8}']
        )


def test_cli_analyzer_register_uses_shared_runner_and_output_contract(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path
) -> None:
    """The Cyclopts leaf registers a parent and version through one envelope."""
    client = StubClient()
    script = tmp_path / "analyzer.py"
    script.write_text("def analyze(sessions, **params):\n    return []\n")

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "analyzer",
                "register",
                "demo",
                "--script",
                str(script),
                "--entrypoint",
                "analyze",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["command"] == "analyzer.register"
    assert len(client.analyzers.created_requests) == 1
    assert client.analyzers.created_requests[0].name == "demo"


def test_cli_analyzer_agent_id_option_is_rejected(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path
) -> None:
    """Analyzer registration offers no --agent-id, unlike evaluator registration."""
    client = StubClient()
    script = tmp_path / "analyzer.py"
    script.write_text("def analyze(sessions, **params):\n    return []\n")

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "analyzer",
                "register",
                "demo",
                "--script",
                str(script),
                "--entrypoint",
                "analyze",
                "--agent-id",
                str(uuid.uuid4()),
            ]
        )
        == 2
    )
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert client.analyzers.created_requests == []


def test_cli_analyzer_list_and_get_return_standard_envelopes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Analyzer list and get resolve through the shared plugin helpers."""
    client = StubClient()
    analyzer = StubModel("quality")
    client.analyzers.items = [analyzer]

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert app_module.main(["analyzer", "list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["items"][0]["name"] == "quality"

    assert app_module.main(["analyzer", "get", str(analyzer.id)]) == 0
    fetched = json.loads(capsys.readouterr().out)
    assert fetched["item"]["name"] == "quality"


def test_cli_analyzer_version_register_list_and_get(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path
) -> None:
    """Analyzer version commands resolve the parent and forward the source."""
    client = StubClient()
    analyzer = StubModel("quality")
    version = StubModel("quality-version", version=2)
    client.analyzers.items = [analyzer]
    client.analyzers.versions = [version]
    script = tmp_path / "analyzer.py"
    script.write_text("def analyze(sessions, **params):\n    return []\n")

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "analyzer",
                "version",
                "register",
                str(analyzer.id),
                "--script",
                str(script),
                "--entrypoint",
                "analyze",
            ]
        )
        == 0
    )
    parent_id, request = client.analyzers.version_requests[0]
    assert parent_id == analyzer.id
    assert request.source.entrypoint == "analyze"
    capsys.readouterr()

    assert app_module.main(["analyzer", "version", "list", str(analyzer.id)]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed["items"][0]["version"] == 2

    assert app_module.main(["analyzer", "version", "get", f"{analyzer.id}@2"]) == 0
    fetched = json.loads(capsys.readouterr().out)
    assert fetched["item"]["version"] == 2
