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
from kitaru.api_models.v1.evaluator import EvaluatorCreateRequest
from kitaru.api_models.v1.importer import ImporterCreateRequest
from kitaru.cli import app as app_module
from kitaru.cli.output import CLIError
from kitaru.cli.registration import (
    PackageSource,
    ScriptSource,
    get_agent_version,
    get_plugin_version,
    list_params,
    load_agent_register_spec,
    normalize_agent_source,
    page_result,
    parse_version_reference,
    plugin_parent_request,
    prepare_plugin_source,
    register_agent,
    register_plugin,
    register_plugin_version,
    resolve_analyzer_configs,
    resolve_asset,
    resolve_evaluator_configs,
    validate_package_source,
    version_list_params,
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
        self.create_idempotency_keys: list[str | None] = []
        self.version_idempotency_keys: list[str | None] = []
        self.create_error: Exception | None = None
        self.version_error: Exception | None = None
        self.parent = StubModel("created")
        self.version = StubModel("version", version=2)
        self.deleted: list[uuid.UUID] = []

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

    async def delete(self, item_id: uuid.UUID) -> None:
        """Record one deletion."""
        self.deleted.append(item_id)

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

    async def create(
        self, request: Any, idempotency_key: str | None = None
    ) -> StubModel:
        """Record parent creation."""
        self.created_requests.append(request)
        self.create_idempotency_keys.append(idempotency_key)
        if self.create_error:
            raise self.create_error
        return self.parent

    async def create_version(
        self,
        parent_id: uuid.UUID,
        request: Any,
        idempotency_key: str | None = None,
    ) -> StubModel:
        """Record version creation."""
        self.version_requests.append((parent_id, request))
        self.version_idempotency_keys.append(idempotency_key)
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


def test_evaluator_parent_request_accepts_connection_schema(tmp_path: Path) -> None:
    """Evaluator parents carry provider connection metadata like analyzers."""
    schema = tmp_path / "connection.json"
    schema.write_text('{"type":"object","properties":{"API_KEY":{}}}')

    request = plugin_parent_request(
        "evaluator",
        "demo",
        description=None,
        provider="demo-provider",
        metadata=None,
        agent_id=None,
        connection_schema=schema,
    )

    assert isinstance(request, EvaluatorCreateRequest)
    assert request.provider == "demo-provider"
    assert request.connection_schema == {
        "type": "object",
        "properties": {"API_KEY": {}},
    }


def test_cli_evaluator_register_forwards_connection_schema(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """The Cyclopts leaf forwards --connection-schema into the create request."""
    client = StubClient()
    script = tmp_path / "evaluator.py"
    script.write_text("def evaluate(sessions, **params):\n    return []\n")
    schema = tmp_path / "connection.json"
    schema.write_text('{"type":"object","properties":{"API_KEY":{}}}')

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert (
        app_module.main(
            [
                "evaluator",
                "register",
                "demo",
                "--script",
                str(script),
                "--entrypoint",
                "evaluate",
                "--provider",
                "model-provider",
                "--connection-schema",
                str(schema),
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["command"] == "evaluator.register"
    assert len(client.evaluators.created_requests) == 1
    assert client.evaluators.created_requests[0].name == "demo"
    assert client.evaluators.created_requests[0].provider == "model-provider"
    assert client.evaluators.created_requests[0].connection_schema == {
        "type": "object",
        "properties": {"API_KEY": {}},
    }


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


def test_cli_agent_delete_requires_force_before_network_access(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Agent deletion is force-gated before exact resolution."""
    client = StubClient()
    parent = StubModel("demo")
    client.agents.items = [parent]

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    assert app_module.main(["agent", "delete", "demo"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["command"] == "agent.delete"
    assert error["error"]["kind"] == "invalid_arguments"
    assert client.agents.deleted == []

    assert app_module.main(["agent", "delete", str(parent.id), "--force"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "agent.delete"
    assert payload["item"] == {"id": str(parent.id), "deleted": True}
    assert client.agents.deleted == [parent.id]


def test_page_result_preserves_server_order_and_cursor() -> None:
    """List envelopes use the SDK page cursor instead of inferred pagination."""
    from kitaru.api_models.v1.base import Page

    first = StubModel("first")
    second = StubModel("second")
    result = page_result(Page[Any](items=[first, second], next_cursor="next"), size=2)
    assert [item["name"] for item in result.items or []] == ["first", "second"]
    assert result.page == {"limit": 2, "next_cursor": "next", "truncated": True}


@pytest.mark.parametrize(
    ("reference", "fragment"),
    [
        ("agent", "must be PARENT@VERSION"),
        ("agent@", "must be PARENT@VERSION"),
        ("agent@two", "positive integer or 'latest'"),
        ("agent@0", "canonical positive integer"),
        ("agent@01", "canonical positive integer"),
    ],
)
def test_version_references_reject_malformed_versions(
    reference: str, fragment: str
) -> None:
    """Only PARENT@N with a canonical positive N or @latest selects a version."""
    with pytest.raises(CLIError) as raised:
        parse_version_reference(reference, "Agent")

    assert raised.value.kind == "invalid_arguments"
    assert fragment in raised.value.message


async def test_agent_version_lookup_reports_missing_and_duplicate_versions() -> None:
    """A version number that matches zero or several records is never guessed."""
    parent = StubModel("asset", latest_version=2)
    client = StubClient()
    client.agents.items = [parent]
    client.agents.versions = [StubModel("v1", version=1)]

    with pytest.raises(CLIError) as missing:
        await get_agent_version(client, "asset@2")
    assert missing.value.kind == "not_found"
    assert "has no version 2" in missing.value.message

    duplicates = [StubModel("a", version=1), StubModel("b", version=1)]
    client.agents.versions = duplicates
    with pytest.raises(CLIError) as conflict:
        await get_agent_version(client, "asset@1")
    assert conflict.value.kind == "conflict"
    assert conflict.value.details == {"ids": [str(item.id) for item in duplicates]}


@pytest.mark.parametrize(
    ("args", "fragment"),
    [
        (["--env", "MODE=a", "--env", "MODE=b"], "'MODE' was repeated"),
        (["--env", "1BAD=x"], "use KEY=VALUE"),
        (["--env", "NOVALUE"], "use KEY=VALUE"),
    ],
)
def test_cli_agent_register_rejects_invalid_env_before_api_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    fragment: str,
) -> None:
    """Bad --env values exit 2 with a structured error and create nothing."""
    client = StubClient()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(app_module, "_open_asset_client", fake_open_client)

    exit_code = app_module.main(
        ["agent", "register", "demo", "--command", "run", *args]
    )

    assert exit_code == 2
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["kind"] == "invalid_arguments"
    assert fragment in error["message"]
    assert client.agents.created_requests == []


@pytest.mark.parametrize(
    ("script_body", "package", "entrypoint", "fragment"),
    [
        (b"def parse(): pass\n", "example==1.0", "parse", "Exactly one of"),
        (None, None, "parse", "Exactly one of"),
        (b"def parse(): pass\n", None, " ", "--entrypoint is required"),
        (b"def parse(): pass\n", None, "pkg.parse", "one top-level attribute"),
        (b"def parse(:\n", None, "parse", "Invalid script"),
        (b"\xff\xfe", None, "parse", "Invalid script"),
        (None, "example==1.0", "example", "expected MODULE:ATTRIBUTE"),
        (None, "example==1.0", "1bad:parse", "expected MODULE:ATTRIBUTE"),
        (None, "not a requirement!", "example:parse", "Invalid package requirement"),
        (None, "example==1.0; python_version>'3'", "example:parse", "exact =="),
    ],
)
def test_plugin_source_preflight_rejects_unusable_sources(
    tmp_path: Path,
    script_body: bytes | None,
    package: str | None,
    entrypoint: str,
    fragment: str,
) -> None:
    """Plugin sources are validated locally before any upload or API call."""
    script = None
    if script_body is not None:
        script = tmp_path / "plugin.py"
        script.write_bytes(script_body)

    with pytest.raises(CLIError) as raised:
        prepare_plugin_source(script=script, package=package, entrypoint=entrypoint)

    assert raised.value.kind == "invalid_arguments"
    assert fragment in raised.value.message


def test_script_source_must_be_a_regular_file(tmp_path: Path) -> None:
    """A missing or directory script path is rejected by name."""
    with pytest.raises(CLIError, match="is not a regular file"):
        prepare_plugin_source(script=tmp_path, package=None, entrypoint="parse")


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"metadata": "{not json"}, "--metadata is not valid JSON"),
        ({"metadata": "[1, 2]"}, "--metadata must contain a JSON object"),
        ({"agent_id": uuid.uuid4()}, "--agent-id is only valid for evaluators"),
    ],
)
def test_importer_parent_request_rejects_invalid_options(
    kwargs: dict[str, Any], fragment: str
) -> None:
    """Invalid metadata JSON or evaluator-only options fail as invalid arguments."""
    options: dict[str, Any] = {
        "description": None,
        "provider": None,
        "metadata": None,
        "agent_id": None,
        **kwargs,
    }

    with pytest.raises(CLIError) as raised:
        plugin_parent_request("importer", "demo", **options)

    assert raised.value.kind == "invalid_arguments"
    assert fragment in raised.value.message


@pytest.mark.parametrize(
    ("content", "fragment"),
    [
        (None, "is not a regular file"),
        ("version: [unclosed\n", "Could not read Spec"),
        ("- just\n- a list\n", "must contain one mapping document"),
        ("version:\n  run_spec:\n    command: '  '\n", "nonblank version.run_spec"),
    ],
)
def test_agent_spec_loading_rejects_unusable_documents(
    tmp_path: Path, content: str | None, fragment: str
) -> None:
    """Spec files must be readable mappings whose version has a run command."""
    path = tmp_path / "agent.yaml"
    if content is not None:
        path.write_text(content, encoding="utf-8")

    with pytest.raises(CLIError) as raised:
        load_agent_register_spec("demo", path)

    assert raised.value.kind == "invalid_arguments"
    assert fragment in raised.value.message


@pytest.mark.parametrize(
    ("kwargs", "fragment"),
    [
        ({"sort": "name:asc"}, "--sort must be created:asc or created:desc."),
        ({"size": 0}, "--size must be between 1 and 1000."),
        ({"filter": "not json"}, "--filter must be a valid JSON filter expression."),
    ],
)
def test_list_params_name_the_offending_option(
    kwargs: dict[str, Any], fragment: str
) -> None:
    """Invalid list options produce one concise error naming the CLI flag."""
    options: dict[str, Any] = {
        "size": 20,
        "cursor": None,
        "sort": "created:desc",
        "filter": None,
        **kwargs,
    }

    with pytest.raises(CLIError) as raised:
        list_params("agent", **options)

    assert raised.value.kind == "invalid_arguments"
    assert raised.value.message == fragment


def test_version_list_params_reject_out_of_range_size() -> None:
    """Version listing enforces the same page-size bound as parent listing."""
    with pytest.raises(CLIError, match="--size must be between 1 and 1000"):
        version_list_params(size=5000, cursor=None, sort="created:asc")


@pytest.mark.parametrize(
    ("tokens", "params", "connections", "fragment"),
    [
        ([], [], [], "Provide at least one --{kind}."),
        (["x@1", "x@1"], [], [], "Each --{kind} token must be unique."),
        (["x@1"], ["x@1"], [], "--{kind}-params must be"),
        (["x@1"], ["y@1={}"], [], "token 'y@1' is not a selected {kind}"),
        (["x@1"], ["x@1={}", "x@1={}"], [], "provided more than once"),
        (["x@1"], [], ["x@1="], "--{kind}-connection must be"),
        (["x@1"], [], ["y@1=conn"], "token 'y@1' is not a selected {kind}"),
        (["x@1"], [], ["x@1=a", "x@1=b"], "provided more than once"),
    ],
)
@pytest.mark.parametrize("kind", ["evaluator", "analyzer"])
async def test_plugin_config_resolution_rejects_malformed_selections(
    kind: str,
    tokens: list[str],
    params: list[str],
    connections: list[str],
    fragment: str,
) -> None:
    """Evaluator and analyzer selections fail before any server lookup."""
    resolve = (
        resolve_evaluator_configs if kind == "evaluator" else resolve_analyzer_configs
    )
    client = SimpleNamespace(
        evaluators=None, analyzers=None, connections=StubResource([StubModel("a")])
    )

    with pytest.raises(CLIError) as raised:
        await resolve(client, tokens, params, connections)

    assert raised.value.kind == "invalid_arguments"
    assert fragment.format(kind=kind) in raised.value.message


@pytest.mark.parametrize("kind", ["evaluator", "analyzer"])
async def test_plugin_config_resolution_rejects_tokens_for_the_same_version(
    kind: str,
) -> None:
    """NAME@1 and NAME@latest cannot both select the same stored version."""
    resolve = (
        resolve_evaluator_configs if kind == "evaluator" else resolve_analyzer_configs
    )
    resource = StubResource([StubModel("judge", latest_version=1)])
    resource.versions = [StubModel("judge-v1", version=1)]
    client = SimpleNamespace(evaluators=resource, analyzers=resource)

    with pytest.raises(CLIError) as raised:
        await resolve(client, ["judge@1", "judge@latest"], [])

    assert raised.value.kind == "invalid_arguments"
    assert f"resolved to the same {kind} version" in raised.value.message
