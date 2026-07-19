"""Focused tests for Agent registration identity and Pipeline binding."""

from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, Literal, cast
from unittest.mock import MagicMock, Mock

import pytest
from pydantic import ValidationError
from pydantic_ai import Agent
from pydantic_ai import mcp as pydantic_ai_mcp
from pydantic_ai.models.test import TestModel
from pydantic_ai.output import ToolOutput
from pydantic_ai.providers.anthropic import AnthropicProvider
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.tools import Tool

from kitaru._agent_registration import (
    RegisteredAgentVersionBinding,
    RegistrationIdentity,
    _registered_imported_replay_compatibility,
    build_agent_version_pipeline_name,
    canonicalize_registration_value,
    find_exact_project_pipeline,
    hash_registration_value,
    resolve_registered_agent_version,
    resolve_registration_identity,
    verify_submitted_run_binding,
)
from kitaru._config import _projects as project_ops
from kitaru._config._agents import (
    RegisteredToolEffect,
    _AgentVersionManifest,
    _PydanticAIReplayManifest,
    _RegisteredPydanticAITool,
    _RegisteredPydanticAIToolSource,
)
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.adapters.pydantic_ai import _agent as agent_module
from kitaru.errors import (
    KitaruMetadataConflictError,
    KitaruMetadataReconciliationError,
    KitaruStateError,
    KitaruUsageError,
)
from kitaru.flow import flow
from kitaru.replay import ExperimentReplayContext
from kitaru.scoring import ProtectionDeclaration

REGISTERABLE_AGENT: KitaruAgent[Any, str] | None = None


async def _importable_tool(value: str) -> str:
    return value


async def _changed_importable_tool(value: str) -> str:
    return f"changed:{value}"


async def _integer_importable_tool(value: int) -> str:
    return str(value)


def _registration_protection(_: object) -> bool:
    return True


def _closure_tool(config: dict[str, str]) -> Any:
    async def configured_tool(value: str) -> str:
        return f"{config['mode']}:{value}"

    return configured_tool


def _agent_with_tool(tool: Any) -> KitaruAgent[Any, str]:
    return KitaruAgent(
        Agent(TestModel(), name="closure-agent", tools=[tool], output_type=str)
    )


class _ProviderTestModel(TestModel):
    def __init__(self, provider: Any) -> None:
        super().__init__()
        self._provider = provider

    @property
    def provider(self) -> Any:
        return self._provider


def _agent_with_provider(provider: Any) -> KitaruAgent[Any, str]:
    return KitaruAgent(
        Agent(_ProviderTestModel(provider), name="provider-agent", output_type=str)
    )


_BedrockProviderStub = type(
    "BedrockProvider",
    (),
    {"__module__": "pydantic_ai.providers.bedrock"},
)


def _bedrock_provider(*, region_name: str, endpoint_url: str) -> Any:
    provider: Any = _BedrockProviderStub()
    provider.name = "bedrock"
    provider.base_url = endpoint_url
    provider.client = SimpleNamespace(
        meta=SimpleNamespace(
            endpoint_url=endpoint_url,
            region_name=region_name,
        )
    )
    return provider


class _FakeRegistrationClient:
    def __init__(self, project_id: str = "project-id") -> None:
        self.project_id = project_id
        self.metadata: dict[str, Any] = {}
        self.pipeline: Any | None = None
        self.update_calls: list[dict[str, Any]] = []
        self.list_calls: list[dict[str, Any]] = []
        self.discard_updates = False
        self.zen_store = SimpleNamespace(
            url="http://127.0.0.1",
            get_store_info=lambda: SimpleNamespace(is_pro_server=lambda: False),
        )

    def _project(self) -> SimpleNamespace:
        return SimpleNamespace(
            id=self.project_id,
            name="default",
            display_name=None,
            description=None,
            project_metadata=deepcopy(self.metadata),
        )

    @property
    def active_project(self) -> SimpleNamespace:
        return self._project()

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> SimpleNamespace:
        assert selector == self.project_id
        assert allow_name_prefix_match is False
        assert hydrate is True
        return self._project()

    def update_project(
        self,
        selector: str,
        *,
        project_metadata: dict[str, Any],
    ) -> SimpleNamespace:
        assert selector == self.project_id
        if not self.discard_updates:
            self.metadata = deepcopy(project_metadata)
        self.update_calls.append(deepcopy(project_metadata))
        return self._project()

    def list_pipelines(self, **kwargs: Any) -> SimpleNamespace:
        self.list_calls.append(kwargs)
        items = [] if self.pipeline is None else [self.pipeline]
        return SimpleNamespace(items=items)


class _FakeProRegistrationClient(_FakeRegistrationClient):
    def __init__(self, *, project_name: str) -> None:
        super().__init__()
        self.project_name = project_name
        self.zen_store = SimpleNamespace(
            url="https://cloud.zenml.io",
            get_store_info=lambda: SimpleNamespace(is_pro_server=lambda: True),
        )

    def _project(self) -> SimpleNamespace:
        return SimpleNamespace(
            id=self.project_id,
            name=self.project_name,
            display_name=None,
            description=None,
            project_metadata=deepcopy(self.metadata),
        )

    def get_project(
        self,
        selector: str,
        *,
        allow_name_prefix_match: bool,
        hydrate: bool,
    ) -> SimpleNamespace:
        assert selector in {self.project_id, self.project_name}
        assert allow_name_prefix_match is False
        assert hydrate is True
        return self._project()


class _FakePipeline:
    def __init__(self, client: _FakeRegistrationClient, name: str) -> None:
        self._client = client
        self.name = name
        self.register_calls = 0

    def register(self) -> Any:
        self.register_calls += 1
        pipeline = SimpleNamespace(
            id="pipeline-id",
            name=self.name,
            body=SimpleNamespace(project_id=self._client.project_id),
        )
        self._client.pipeline = pipeline
        return pipeline


class _FakeFlow:
    def __init__(self, client: _FakeRegistrationClient, name: str) -> None:
        self._pipeline = _FakePipeline(client, name)
        self.binding: RegisteredAgentVersionBinding | None = None

    def _bind_registered_version(
        self,
        binding: RegisteredAgentVersionBinding,
    ) -> None:
        if self.binding is not None and self.binding != binding:
            raise AssertionError("flow rebound")
        self.binding = binding


def _manifest(pipeline_id: str = "pipeline-id") -> _AgentVersionManifest:
    return _AgentVersionManifest(
        schema_version=1,
        agent_version_id=pipeline_id,
        pipeline_id=pipeline_id,
        pipeline_name="support_agent__av_12345678_abcdef123456",
        fingerprint="sha256:fingerprint",
        git_sha="1234567890abcdef",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash="sha256:configuration",
        worldview_hash="sha256:worldview",
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
        registered_at="2026-07-17T10:00:00Z",
        source="registration",
    )


def _resolver_project(
    *manifests: _AgentVersionManifest,
    aliases: dict[str, str] | None = None,
) -> SimpleNamespace:
    project_id = "00000000-0000-0000-0000-000000000001"
    return SimpleNamespace(
        id=project_id,
        name="actual-project",
        project_metadata={
            "kitaru": {
                "schema_version": 1,
                "agent": {
                    "agent_id": project_id,
                    "name": "support-agent",
                },
                "agent_versions": {
                    manifest.agent_version_id: manifest.model_dump(mode="json")
                    for manifest in manifests
                },
                "agent_version_aliases": aliases or {},
            }
        },
    )


def _resolver_client(
    project: SimpleNamespace,
    *,
    pipeline_id: str = "pipeline-id",
) -> MagicMock:
    client = MagicMock()
    client.get_project.return_value = project
    client.list_pipelines.return_value = SimpleNamespace(
        items=[
            SimpleNamespace(
                id=pipeline_id,
                name=_manifest().pipeline_name,
                body=SimpleNamespace(project_id=project.id),
            )
        ]
    )
    return client


def test_resolves_registered_agent_version_by_exact_alias_or_uuid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest()
    project = _resolver_project(manifest, aliases={"prod": manifest.pipeline_id})
    client = _resolver_client(project)
    monkeypatch.setattr(
        "kitaru._agent_registration._resolve_agent_project",
        lambda received_client, selector: (
            project
            if received_client is client and selector == "support-agent"
            else None
        ),
    )

    by_alias = resolve_registered_agent_version(
        client,
        agent="support-agent",
        version="prod",
    )
    by_id = resolve_registered_agent_version(
        client,
        agent="support-agent",
        version=manifest.pipeline_id,
    )

    assert by_alias.manifest == by_id.manifest == manifest
    assert by_alias.requested_alias == "prod"
    assert by_alias.aliases == ("prod",)
    assert by_id.requested_alias is None
    assert by_id.agent_name == "support-agent"
    assert by_id.project_name == "actual-project"


def test_rejects_agent_version_id_alias_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _manifest("pipeline-id")
    second = _manifest("other-pipeline-id").model_copy(
        update={
            "pipeline_name": "support_agent__av_other_abcdef123456",
            "fingerprint": "sha256:other-fingerprint",
        }
    )
    project = _resolver_project(
        first,
        second,
        aliases={first.pipeline_id: second.pipeline_id},
    )
    client = _resolver_client(project)
    monkeypatch.setattr(
        "kitaru._agent_registration._resolve_agent_project",
        lambda _client, _selector: project,
    )

    with pytest.raises(KitaruStateError, match="ambiguous"):
        resolve_registered_agent_version(
            client,
            agent="support-agent",
            version=first.pipeline_id,
        )

    client.list_pipelines.assert_not_called()


def test_rejects_missing_or_recreated_source_agent_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _manifest()
    project = _resolver_project(manifest)
    client = _resolver_client(project, pipeline_id="recreated-pipeline-id")
    monkeypatch.setattr(
        "kitaru._agent_registration._resolve_agent_project",
        lambda _client, _selector: project,
    )

    with pytest.raises(KitaruStateError, match="was not found"):
        resolve_registered_agent_version(
            client,
            agent="support-agent",
            version="missing",
        )
    with pytest.raises(KitaruStateError, match="different UUID"):
        resolve_registered_agent_version(
            client,
            agent="support-agent",
            version=manifest.pipeline_id,
        )


def test_canonical_identity_is_deterministic_and_rejects_unsupported_values(
    tmp_path: Path,
) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    import subprocess

    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repository), "config", "user.name", "Test"],
        check=True,
    )
    (repository / "agent.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "-C", str(repository), "add", "agent.py"], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-qm", "initial"],
        check=True,
    )

    first = resolve_registration_identity(
        repo_root=repository,
        entrypoint="agent:registered",
        configuration={"unordered": {"b", "a"}},
        worldview={"model": "test"},
    )
    second = resolve_registration_identity(
        repo_root=repository,
        entrypoint="agent:registered",
        configuration={"unordered": {"a", "b"}},
        worldview={"model": "test"},
    )

    assert first == second
    assert first.git_dirty is False
    configuration_changed = resolve_registration_identity(
        repo_root=repository,
        entrypoint="agent:registered",
        configuration={"unordered": {"a", "b"}, "checkpoint_strategy": "turn"},
        worldview={"model": "test"},
    )
    worldview_changed = resolve_registration_identity(
        repo_root=repository,
        entrypoint="agent:registered",
        configuration={"unordered": {"a", "b"}},
        worldview={"model": "different"},
    )
    assert configuration_changed.configuration_hash != first.configuration_hash
    assert configuration_changed.fingerprint != first.fingerprint
    assert worldview_changed.worldview_hash != first.worldview_hash
    assert worldview_changed.fingerprint != first.fingerprint

    (repository / "agent.py").write_text("VALUE = 2\n")
    dirty = resolve_registration_identity(
        repo_root=repository,
        entrypoint="agent:registered",
        configuration={"unordered": {"a", "b"}},
        worldview={"model": "test"},
    )
    assert dirty.git_dirty is True
    assert dirty.working_tree_hash is not None
    assert dirty.fingerprint != first.fingerprint

    with pytest.raises(KitaruUsageError, match="unsupported"):
        canonicalize_registration_value(object())


@pytest.mark.parametrize(
    ("first_output", "second_output"),
    [
        (list[str], list[int]),
        (str | int, str | float),
        (Annotated[list[str], "first"], Annotated[list[str], "second"]),
        (Literal["first"], Literal["second"]),
        (ToolOutput(list[str]), ToolOutput(list[int])),
    ],
)
def test_parameterized_output_types_have_distinct_registration_fingerprints(
    first_output: Any,
    second_output: Any,
) -> None:
    first = KitaruAgent(
        Agent(TestModel(), name="typed-agent", output_type=first_output)
    )._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )
    second = KitaruAgent(
        Agent(TestModel(), name="typed-agent", output_type=second_output)
    )._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )

    assert first.worldview_hash != second.worldview_hash
    assert first.fingerprint != second.fingerprint


def test_provider_credentials_do_not_change_registration_fingerprint() -> None:
    first = _agent_with_provider(
        OpenAIProvider(
            base_url="https://models.example/v1",
            api_key="first-secret",
        )
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )
    rotated = _agent_with_provider(
        OpenAIProvider(
            base_url="https://models.example/v1",
            api_key="second-secret",
        )
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )

    assert first.fingerprint == rotated.fingerprint
    assert "first-secret" not in first.canonical_json
    assert "second-secret" not in rotated.canonical_json


def test_openai_provider_uri_values_are_normalized_to_sanitized_strings() -> None:
    provider = OpenAIProvider(
        base_url="https://user:secret@models.example/v1?api_key=hidden&mode=fast",
        api_key="provider-secret",
    )

    worldview = _agent_with_provider(provider)._registration_worldview()

    provider_identity = worldview["model"]["provider"]
    assert provider_identity["kind"] == ("pydantic_ai.providers.openai:OpenAIProvider")
    assert provider_identity["name"] == "openai"
    base_url = provider_identity["behavior"]["base_url"]
    assert isinstance(base_url, str)
    assert base_url.startswith("https://models.example/v1?mode=fast")
    assert "user" not in base_url
    assert "secret" not in base_url
    assert "api_key" not in base_url


def test_provider_base_url_changes_registration_fingerprint() -> None:
    first = _agent_with_provider(
        OpenAIProvider(base_url="https://one.example/v1", api_key="test-key")
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )
    second = _agent_with_provider(
        OpenAIProvider(base_url="https://two.example/v1", api_key="test-key")
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )

    assert first.worldview_hash != second.worldview_hash
    assert first.fingerprint != second.fingerprint


def test_provider_implementation_changes_registration_fingerprint() -> None:
    openai = _agent_with_provider(
        OpenAIProvider(base_url="https://models.example/v1", api_key="test-key")
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )
    anthropic = _agent_with_provider(
        AnthropicProvider(base_url="https://models.example/v1", api_key="test-key")
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )

    assert openai.worldview_hash != anthropic.worldview_hash
    assert openai.fingerprint != anthropic.fingerprint


@pytest.mark.parametrize("field_name", ["region_name", "endpoint_url"])
def test_bedrock_routing_changes_registration_fingerprint(field_name: str) -> None:
    first_values = {
        "region_name": "us-east-1",
        "endpoint_url": "https://bedrock.us-east-1.example",
    }
    second_values = {
        **first_values,
        field_name: (
            "eu-west-1"
            if field_name == "region_name"
            else "https://bedrock-alt.example"
        ),
    }
    first = _agent_with_provider(
        _bedrock_provider(**first_values)
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )
    second = _agent_with_provider(
        _bedrock_provider(**second_values)
    )._resolve_registration_identity(
        repo_root=Path.cwd(), entrypoint=f"{__name__}:REGISTERABLE_AGENT"
    )

    assert first.worldview_hash != second.worldview_hash
    assert first.fingerprint != second.fingerprint


def test_ambiguous_provider_routing_configuration_fails_closed() -> None:
    provider_type = type(
        "OpenAIProvider",
        (),
        {"__module__": "pydantic_ai.providers.openai"},
    )
    provider: Any = provider_type()
    provider.name = "openai"
    provider.base_url = "https://provider.example/v1"
    provider.client = SimpleNamespace(base_url="https://client.example/v1")

    with pytest.raises(KitaruUsageError, match="ambiguous provider field 'base_url'"):
        _agent_with_provider(provider)._registration_worldview()


def test_unsupported_provider_configuration_fails_closed() -> None:
    provider = SimpleNamespace(
        name="custom",
        base_url="https://models.example/v1",
        client=SimpleNamespace(base_url="https://models.example/v1"),
    )

    with pytest.raises(
        KitaruUsageError,
        match="routing configuration cannot be projected safely",
    ):
        _agent_with_provider(provider)._registration_worldview()


def test_importable_function_tool_uses_stable_import_path() -> None:
    worldview = _agent_with_tool(_importable_tool)._registration_worldview()

    implementation = worldview["tools_and_mcp"][0]["tools"][0]["implementation"]

    assert implementation == f"{__name__}:_importable_tool"


def test_closure_tool_configuration_changes_registration_fingerprint() -> None:
    first = _agent_with_tool(
        _closure_tool({"mode": "strict", "openai_api_key": "first-secret"})
    )._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )
    rotated_secret = _agent_with_tool(
        _closure_tool({"mode": "strict", "openai_api_key": "second-secret"})
    )._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )
    changed_mode = _agent_with_tool(
        _closure_tool({"mode": "lenient", "openai_api_key": "second-secret"})
    )._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )

    assert first.fingerprint == rotated_secret.fingerprint
    assert first.worldview_hash == rotated_secret.worldview_hash
    assert first.fingerprint != changed_mode.fingerprint
    assert first.worldview_hash != changed_mode.worldview_hash
    assert "first-secret" not in first.canonical_json
    assert "second-secret" not in rotated_secret.canonical_json


def test_closure_tool_mutation_is_checked_by_registration_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {"mode": "strict", "openai_api_key": "first-secret"}
    durable_agent = _agent_with_tool(_closure_tool(config))
    identity = durable_agent._resolve_registration_identity(
        repo_root=Path.cwd(),
        entrypoint=f"{__name__}:REGISTERABLE_AGENT",
    )
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=_manifest().model_copy(update={"fingerprint": identity.fingerprint}),
    )
    durable_agent._registered_state = agent_module._RegisteredAgentState(
        repo_root=Path.cwd(),
        identity=identity,
        binding=binding,
    )
    monkeypatch.setattr(
        agent_module,
        "resolve_agent_entrypoint",
        lambda **kwargs: kwargs["entrypoint"],
    )

    config["openai_api_key"] = "second-secret"
    durable_agent._preflight_registered_identity()

    config["mode"] = "lenient"
    with pytest.raises(KitaruStateError, match="worldview"):
        durable_agent._preflight_registered_identity()


def _replay_tool_metadata(
    *,
    logical_id: str = "lookup",
    aliases: list[str] | None = None,
    effect: str = "read_only",
) -> dict[str, Any]:
    return {
        "kitaru_replay": {
            "logical_id": logical_id,
            "aliases": aliases or ["legacy_lookup"],
            "effect": effect,
        }
    }


def _agent_with_replay_tool(
    function: Any = _importable_tool,
    *,
    metadata: dict[str, Any] | None = None,
) -> KitaruAgent[Any, str]:
    return KitaruAgent(
        Agent(
            TestModel(),
            name="replay-contract-agent",
            tools=[
                Tool(
                    function,
                    name="lookup",
                    metadata=metadata or _replay_tool_metadata(),
                )
            ],
            output_type=str,
        )
    )


def _replay_manifest(
    *,
    tools: tuple[_RegisteredPydanticAITool, ...] = (),
    unresolved_sources: tuple[_RegisteredPydanticAIToolSource, ...] = (),
) -> _PydanticAIReplayManifest:
    return _PydanticAIReplayManifest(
        driver_revision="pydantic_ai_imported_replay_v1",
        preparation_revision="kitaru_imported_replay_v1",
        driver_entrypoint="kitaru.adapters.pydantic_ai._agent:KitaruAgent",
        resume_kinds=("root_input", "model_message", "tool_result"),
        argument_normalizer_revision="pydantic_ai_strict_json_v1",
        tools=tools,
        unresolved_tool_sources=unresolved_sources,
    )


def test_registration_projects_versioned_pydantic_ai_replay_contract() -> None:
    agent = _agent_with_replay_tool()

    replay = agent._pydantic_ai_replay_manifest()

    assert replay.driver_revision == "pydantic_ai_imported_replay_v1"
    assert replay.preparation_revision == "kitaru_imported_replay_v1"
    assert replay.resume_kinds == ("root_input", "model_message", "tool_result")
    assert replay.argument_normalizer_revision == "pydantic_ai_strict_json_v1"
    assert replay.unresolved_tool_sources == ()
    assert len(replay.tools) == 1
    registered_tool = replay.tools[0]
    assert registered_tool.logical_id == "lookup"
    assert registered_tool.aliases == ("legacy_lookup", "lookup")
    assert isinstance(registered_tool.implementation_identity, str)
    assert registered_tool.implementation_identity.endswith(":_importable_tool")
    assert registered_tool.effect is RegisteredToolEffect.READ_ONLY
    assert registered_tool.argument_normalizer_revision == (
        "pydantic_ai_strict_json_v1"
    )
    wrapped_tool = cast(Any, agent.toolsets[0]).wrapped.tools["lookup"]
    assert registered_tool.input_schema_hash == hash_registration_value(
        wrapped_tool.function_schema.json_schema
    )
    assert registered_tool.output_schema_hash == hash_registration_value(
        wrapped_tool.function_schema.return_schema
    )
    assert agent._registration_worldview()["pydantic_ai_replay"] == (
        replay.model_dump(mode="json")
    )


def test_replay_contract_changes_registration_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _agent_with_replay_tool()
    changed_schema = _agent_with_replay_tool(_integer_importable_tool)
    changed_implementation = _agent_with_replay_tool(_changed_importable_tool)
    changed_alias = _agent_with_replay_tool(
        metadata=_replay_tool_metadata(aliases=["renamed_lookup"])
    )
    changed_effect = _agent_with_replay_tool(
        metadata=_replay_tool_metadata(effect="write")
    )
    base_hash = hash_registration_value(base._registration_worldview())

    assert (
        hash_registration_value(changed_schema._registration_worldview()) != base_hash
    )
    assert (
        hash_registration_value(changed_implementation._registration_worldview())
        != base_hash
    )
    assert hash_registration_value(changed_alias._registration_worldview()) != base_hash
    assert (
        hash_registration_value(changed_effect._registration_worldview()) != base_hash
    )

    monkeypatch.setattr(
        agent_module,
        "_PYDANTIC_AI_REPLAY_DRIVER_REVISION",
        "pydantic_ai_imported_replay_v2",
    )
    assert hash_registration_value(base._registration_worldview()) != base_hash
    monkeypatch.setattr(
        agent_module,
        "_PYDANTIC_AI_REPLAY_DRIVER_REVISION",
        "pydantic_ai_imported_replay_v1",
    )
    monkeypatch.setattr(
        agent_module,
        "_PYDANTIC_AI_ARGUMENT_NORMALIZER_REVISION",
        "pydantic_ai_strict_json_v2",
    )
    assert hash_registration_value(base._registration_worldview()) != base_hash


def test_replay_contract_rejects_ambiguous_aliases() -> None:
    first = Tool(
        _importable_tool,
        name="first",
        metadata=_replay_tool_metadata(logical_id="first", aliases=["shared"]),
    )
    second = Tool(
        _changed_importable_tool,
        name="second",
        metadata=_replay_tool_metadata(logical_id="second", aliases=["shared"]),
    )
    agent = KitaruAgent(
        Agent(
            TestModel(),
            name="ambiguous-tools",
            tools=[first, second],
            output_type=str,
        )
    )

    with pytest.raises(KitaruUsageError, match="ambiguous or malformed"):
        agent._pydantic_ai_replay_manifest()


def test_legacy_registration_uses_root_input_with_tools_blocked() -> None:
    compatibility = _registered_imported_replay_compatibility(_manifest())

    assert compatibility.root_input_supported is True
    assert compatibility.message_history_supported is False
    assert compatibility.tool_result_boundary_supported is False
    assert compatibility.recorded_responses_supported is False
    assert compatibility.all_tools_blocked is True
    assert compatibility.reason == "registered_replay_manifest_missing"


def test_registered_replay_compatibility_fails_closed_per_tool_and_source() -> None:
    unknown_tool = _RegisteredPydanticAITool(
        logical_id="unknown_tool",
        aliases=("unknown_tool",),
        input_schema_hash="sha256:input",
        output_schema_hash="sha256:output",
        implementation_identity="tests.test_agent_registration:_importable_tool",
        effect=RegisteredToolEffect.UNKNOWN,
        argument_normalizer_revision="pydantic_ai_strict_json_v1",
    )
    unresolved_source = _RegisteredPydanticAIToolSource(
        source_id="support-mcp",
        kind="pydantic_ai.mcp:MCPServerStdio",
        configuration_hash="sha256:source",
    )

    tool_compatibility = _registered_imported_replay_compatibility(
        _manifest().model_copy(
            update={"pydantic_ai_replay": _replay_manifest(tools=(unknown_tool,))}
        )
    )
    source_compatibility = _registered_imported_replay_compatibility(
        _manifest().model_copy(
            update={
                "pydantic_ai_replay": _replay_manifest(
                    unresolved_sources=(unresolved_source,)
                )
            }
        )
    )
    supported = _registered_imported_replay_compatibility(
        _manifest().model_copy(update={"pydantic_ai_replay": _replay_manifest()})
    )

    assert tool_compatibility.root_input_supported is True
    assert tool_compatibility.message_history_supported is True
    assert tool_compatibility.tool_result_boundary_supported is True
    assert tool_compatibility.recorded_responses_supported is False
    assert tool_compatibility.all_tools_blocked is True
    assert tool_compatibility.blocked_tool_ids == ("unknown_tool",)
    assert source_compatibility.recorded_responses_supported is False
    assert source_compatibility.all_tools_blocked is True
    assert source_compatibility.blocked_tool_source_ids == ("support-mcp",)
    assert supported.recorded_responses_supported is True


def test_closure_tool_rejects_unsupported_non_secret_configuration() -> None:
    config: dict[str, Any] = {"mode": object()}

    with pytest.raises(KitaruUsageError, match="unsupported"):
        _agent_with_tool(_closure_tool(config))._registration_worldview()


def test_exact_pipeline_lookup_is_project_scoped_and_uses_equals() -> None:
    client = _FakeRegistrationClient()
    client.pipeline = SimpleNamespace(
        id="pipeline-id",
        name="manifest-name",
        body=SimpleNamespace(project_id="project-id"),
    )

    pipeline = find_exact_project_pipeline(
        client,
        project_id="project-id",
        pipeline_name="manifest-name",
    )

    assert pipeline is client.pipeline
    assert client.list_calls == [
        {
            "name": "equals:manifest-name",
            "project": "project-id",
            "hydrate": True,
            "size": 2,
        }
    ]


def test_post_submit_verification_detects_delete_recreate_race() -> None:
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=_manifest().model_copy(update={"pipeline_name": "manifest-name"}),
    )
    snapshot = SimpleNamespace(
        body=SimpleNamespace(
            project_id="project-id",
            pipeline_id="recreated-pipeline-id",
        )
    )
    hydrated_run = SimpleNamespace(
        id="run-id",
        body=SimpleNamespace(project_id="project-id"),
        snapshot=snapshot,
    )
    client = SimpleNamespace(
        get_pipeline_run=Mock(return_value=hydrated_run),
    )

    with pytest.raises(KitaruStateError, match="deleted and recreated"):
        verify_submitted_run_binding(
            client,
            run=SimpleNamespace(id="run-id"),
            binding=binding,
        )


def test_version_specific_pipeline_names_change_with_identity(tmp_path: Path) -> None:
    base = RegistrationIdentity(
        entrypoint="module:agent",
        git_sha="1234567890",
        git_dirty=False,
        working_tree_hash=None,
        configuration_hash="sha256:configuration",
        worldview_hash="sha256:worldview",
        fingerprint="sha256:aaaaaaaaaaaaaaaa",
        canonical_json="{}",
    )
    changed = RegistrationIdentity(
        entrypoint=base.entrypoint,
        git_sha=base.git_sha,
        git_dirty=base.git_dirty,
        working_tree_hash=base.working_tree_hash,
        configuration_hash=base.configuration_hash,
        worldview_hash=base.worldview_hash,
        fingerprint="sha256:bbbbbbbbbbbbbbbb",
        canonical_json=base.canonical_json,
    )

    first = build_agent_version_pipeline_name(
        agent_name="support-agent",
        identity=base,
    )
    second = build_agent_version_pipeline_name(
        agent_name="support-agent",
        identity=changed,
    )

    assert first != second
    assert first.startswith("support_agent__av_")


def test_mcp_stdio_environment_tracks_non_secret_values_only() -> None:
    def projection(env: dict[str, str]) -> dict[str, Any]:
        mcp_server_stdio = vars(pydantic_ai_mcp)["MCPServerStdio"]
        return agent_module._toolset_worldview(
            mcp_server_stdio("mcp-server", args=[], env=env)
        )

    first = projection(
        {
            "MODE": "strict",
            "REGION": "us-east-1",
            "OPENAI_API_KEY": "first-secret",
            "AWS_SECRET_ACCESS_KEY": "first-aws-secret",
            "AWS_ACCESS_KEY_ID": "first-access-id",
            "SSH_PRIVATE_KEY_PATH": "/credentials/first-ssh-key",
        }
    )
    rotated_secret = projection(
        {
            "MODE": "strict",
            "REGION": "us-east-1",
            "OPENAI_API_KEY": "second-secret",
            "AWS_SECRET_ACCESS_KEY": "second-aws-secret",
            "AWS_ACCESS_KEY_ID": "second-access-id",
            "SSH_PRIVATE_KEY_PATH": "/credentials/second-ssh-key",
        }
    )
    changed_mode = projection(
        {
            "MODE": "lenient",
            "REGION": "us-east-1",
            "OPENAI_API_KEY": "second-secret",
            "AWS_SECRET_ACCESS_KEY": "second-aws-secret",
            "AWS_ACCESS_KEY_ID": "second-access-id",
            "SSH_PRIVATE_KEY_PATH": "/credentials/second-ssh-key",
        }
    )
    changed_region = projection(
        {
            "MODE": "strict",
            "REGION": "eu-west-1",
            "OPENAI_API_KEY": "second-secret",
            "AWS_SECRET_ACCESS_KEY": "second-aws-secret",
            "AWS_ACCESS_KEY_ID": "second-access-id",
            "SSH_PRIVATE_KEY_PATH": "/credentials/second-ssh-key",
        }
    )

    assert first == rotated_secret
    assert first["env_keys"] == [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "MODE",
        "OPENAI_API_KEY",
        "REGION",
        "SSH_PRIVATE_KEY_PATH",
    ]
    assert first["env_hash"] != changed_mode["env_hash"]
    assert first["env_hash"] != changed_region["env_hash"]


def test_toolset_projection_hashes_arguments_without_retaining_secrets() -> None:
    first = hash_registration_value(
        agent_module._safe_command_args(
            [
                "--header",
                "Authorization: Bearer first-secret",
                "-HX-API-Key: first-key",
                "--api-key-file",
                "/credentials/first.json",
                "--endpoint",
                "https://first:password@mcp.example/tools"
                "?access_token=first-token&region=us",
                "--mode",
                "strict",
            ]
        )
    )
    rotated_credentials = hash_registration_value(
        agent_module._safe_command_args(
            [
                "--header",
                "Authorization: Bearer second-secret",
                "-HX-API-Key: second-key",
                "--api-key-file",
                "/credentials/second.json",
                "--endpoint",
                "https://second:password@mcp.example/tools"
                "?access_token=second-token&region=us",
                "--mode",
                "strict",
            ]
        )
    )
    changed_non_secret = hash_registration_value(
        agent_module._safe_command_args(
            [
                "--header",
                "Authorization: Bearer second-secret",
                "-HX-API-Key: second-key",
                "--api-key-file",
                "/credentials/second.json",
                "--endpoint",
                "https://second:password@mcp.example/tools"
                "?access_token=second-token&region=eu",
                "--mode",
                "strict",
            ]
        )
    )

    assert first == rotated_credentials
    assert first != changed_non_secret


def test_structured_headers_exclude_credentials_but_preserve_other_fields() -> None:
    first = hash_registration_value(
        agent_module._safe_headers(
            {
                "Authorization": "Bearer first-secret",
                "X-API-Key": "first-key",
                "User-Agent": "kitaru/1",
            }
        )
    )
    rotated_credentials = hash_registration_value(
        agent_module._safe_headers(
            {
                "Authorization": "Bearer second-secret",
                "X-API-Key": "second-key",
                "User-Agent": "kitaru/1",
            }
        )
    )
    changed_user_agent = hash_registration_value(
        agent_module._safe_headers(
            {
                "Authorization": "Bearer second-secret",
                "X-API-Key": "second-key",
                "User-Agent": "kitaru/2",
            }
        )
    )

    assert first == rotated_credentials
    assert first != changed_user_agent


def test_uri_projection_excludes_credentials_but_preserves_other_query_fields() -> None:
    first = hash_registration_value(
        agent_module._safe_uri(
            "https://first:password@mcp.example/tools?api_key=first&region=us"
        )
    )
    rotated_credentials = hash_registration_value(
        agent_module._safe_uri(
            "https://second:password@mcp.example/tools?api_key=second&region=us"
        )
    )
    changed_region = hash_registration_value(
        agent_module._safe_uri(
            "https://second:password@mcp.example/tools?api_key=second&region=eu"
        )
    )

    assert first == rotated_credentials
    assert first != changed_region


def test_uri_projection_sanitizes_vendor_credentials_and_fragments() -> None:
    first = agent_module._safe_uri(
        "https://mcp.example/tools?openai_api_key=first&tokenizer=wordpiece"
        "#session_token=first-session&region=us"
    )
    rotated_credentials = agent_module._safe_uri(
        "https://mcp.example/tools?openai_api_key=second&tokenizer=wordpiece"
        "#session_token=second-session&region=us"
    )
    changed_region = agent_module._safe_uri(
        "https://mcp.example/tools?openai_api_key=second&tokenizer=wordpiece"
        "#session_token=second-session&region=eu"
    )

    assert first == rotated_credentials
    assert first == "https://mcp.example/tools?tokenizer=wordpiece#region=us"
    assert first != changed_region


def test_uri_projection_sanitizes_scheme_relative_credentials() -> None:
    assert (
        agent_module._safe_uri(
            "//user:password@mcp.example/tools?api_key=secret&region=us"
        )
        == "//mcp.example/tools?region=us"
    )


@pytest.mark.parametrize(
    "uri",
    [
        "/tools?api_key=secret&region=us",
        "/tools#refresh_token=secret&region=us",
    ],
)
def test_uri_projection_rejects_ambiguous_secret_values(uri: str) -> None:
    with pytest.raises(KitaruUsageError, match="ambiguous URI"):
        agent_module._safe_uri(uri)


@pytest.mark.parametrize(
    "args",
    [
        ["--header"],
        ["--header", "Authorization"],
        ["--api-key-file"],
        ["--token", "--verbose"],
    ],
)
def test_command_projection_rejects_ambiguous_credential_forms(
    args: list[str],
) -> None:
    with pytest.raises(KitaruUsageError, match="safely"):
        agent_module._safe_command_args(args)


def test_command_projection_sanitizes_segment_aware_credential_flags() -> None:
    first = agent_module._safe_command_args(
        [
            "--openai-api-key",
            "first-key",
            "--refresh-token=first-refresh",
            "--session-token-file",
            "/credentials/first-session",
            "--aws-secret-access-key",
            "first-aws-secret",
            "--aws-access-key-id=first-access-id",
            "--ssh-private-key-path",
            "/credentials/first-ssh-key",
            "--tokenizer",
            "wordpiece",
        ]
    )
    rotated_credentials = agent_module._safe_command_args(
        [
            "--openai-api-key",
            "second-key",
            "--refresh-token=second-refresh",
            "--session-token-file",
            "/credentials/second-session",
            "--aws-secret-access-key",
            "second-aws-secret",
            "--aws-access-key-id=second-access-id",
            "--ssh-private-key-path",
            "/credentials/second-ssh-key",
            "--tokenizer",
            "wordpiece",
        ]
    )

    assert first == rotated_credentials
    assert first == [
        "--openai-api-key",
        "--refresh-token",
        "--session-token-file",
        "--aws-secret-access-key",
        "--aws-access-key-id",
        "--ssh-private-key-path",
        "--tokenizer",
        "wordpiece",
    ]


@pytest.mark.parametrize(
    "name",
    [
        "openai_api_key",
        "vendor_openai_api_key_file",
        "refresh_token",
        "session_token_file",
        "aws_secret_access_key",
        "aws_access_key",
        "aws_access_key_id",
        "ssh_private_key_path",
        "ssh_private_key_file",
        "ssh_private_key_file_path",
        "--openai-api-key",
    ],
)
def test_credential_recognition_matches_segment_suffixes(name: str) -> None:
    assert agent_module._is_sensitive_field(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "tokenizer",
        "tokenizer_file",
        "tokenizer_path",
        "checkpoint_path",
        "public_key_file",
    ],
)
def test_credential_recognition_avoids_tokenizer_false_positives(name: str) -> None:
    assert agent_module._is_sensitive_field(name) is False


def test_toolset_projection_does_not_treat_tokenizer_as_a_secret() -> None:
    first = hash_registration_value(
        agent_module._safe_command_args(["--tokenizer", "wordpiece"])
    )
    second = hash_registration_value(
        agent_module._safe_command_args(["--tokenizer", "sentencepiece"])
    )

    assert first != second


def test_structured_secret_policy_preserves_legitimate_field_names() -> None:
    assert agent_module._safe_mapping(
        {
            "tokenizer": "wordpiece",
            "api_key": "do-not-persist",
            "openai_api_key": "do-not-persist",
            "refresh_token": "do-not-persist",
            "session_token_file": "/credentials/session",
            "aws_secret_access_key": "do-not-persist",
            "aws_access_key_id": "do-not-persist",
            "ssh_private_key_file_path": "/credentials/private-key",
            "checkpoint_path": "/models/checkpoint",
        }
    ) == {
        "tokenizer": "wordpiece",
        "checkpoint_path": "/models/checkpoint",
    }


def test_structured_credential_rotation_does_not_change_fingerprint() -> None:
    def fingerprint(
        *,
        secret_access_key: str,
        access_key_id: str,
        private_key_path: str,
        region: str,
    ) -> str:
        return hash_registration_value(
            agent_module._safe_mapping(
                {
                    "aws_secret_access_key": secret_access_key,
                    "aws_access_key_id": access_key_id,
                    "ssh_private_key_path": private_key_path,
                    "region": region,
                }
            )
        )

    first = fingerprint(
        secret_access_key="first-secret",
        access_key_id="first-id",
        private_key_path="/credentials/first.pem",
        region="us-east-1",
    )
    rotated = fingerprint(
        secret_access_key="second-secret",
        access_key_id="second-id",
        private_key_path="/credentials/second.pem",
        region="us-east-1",
    )
    changed_region = fingerprint(
        secret_access_key="second-secret",
        access_key_id="second-id",
        private_key_path="/credentials/second.pem",
        region="eu-west-1",
    )

    assert first == rotated
    assert first != changed_region


def test_registration_project_lookup_failure_does_not_create_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable_agent = KitaruAgent(
        Agent(TestModel(), name="support-agent", output_type=str)
    )
    lookup_error = RuntimeError("permission denied")
    client = SimpleNamespace(
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(is_pro_server=lambda: True),
        ),
        get_project=Mock(side_effect=lookup_error),
    )
    create_project = Mock()
    monkeypatch.setattr(project_ops, "create_project", create_project)

    with pytest.raises(RuntimeError, match="permission denied") as exc_info:
        durable_agent._resolve_registration_project(client)

    assert exc_info.value is lookup_error
    create_project.assert_not_called()


def test_openai_shorthand_agent_registers_without_uri_type_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    client = _FakeRegistrationClient()
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(client, pipeline_name))

    durable_agent = KitaruAgent(
        Agent("openai:gpt-4o-mini", name="openai-agent", output_type=str)
    )
    provider_identity = durable_agent._registration_worldview()["model"]["provider"]
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        result = durable_agent.register(
            entrypoint=f"{__name__}:REGISTERABLE_AGENT",
        )
    finally:
        REGISTERABLE_AGENT = None

    assert provider_identity == {
        "kind": "pydantic_ai.providers.openai:OpenAIProvider",
        "name": "openai",
        "behavior": {"base_url": "https://api.openai.com/v1/"},
    }
    assert result.created is True
    assert result.agent_version.pipeline_id == "pipeline-id"
    assert next(iter(flows.values()))._pipeline.register_calls == 1


def test_registration_is_idempotent_and_does_not_invoke_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    client = _FakeRegistrationClient()
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(client, pipeline_name))

    model = TestModel()
    durable_agent = KitaruAgent(Agent(model, name="support-agent", output_type=str))
    durable_agent.protection(
        "safe-output",
        capability="pure",
        configuration={"rule_version": 1},
    )(_registration_protection)
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        first = durable_agent.register(
            label="stable",
            entrypoint=f"{__name__}:REGISTERABLE_AGENT",
        )
        second = durable_agent.register(
            label="stable",
            entrypoint=f"{__name__}:REGISTERABLE_AGENT",
        )
        durable_agent._checkpoint_strategy = "turn"
        with pytest.raises(KitaruStateError, match="configuration"):
            durable_agent._preflight_registered_identity()
        durable_agent._checkpoint_strategy = "calls"
    finally:
        REGISTERABLE_AGENT = None

    flow = next(iter(flows.values()))
    assert first.agent_version.pipeline_id == "pipeline-id"
    assert first.created is True
    assert second.agent_version.pipeline_id == "pipeline-id"
    assert second.created is False
    assert list(first.agent_version.protections) == ["safe-output"]
    persisted = client.metadata["kitaru"]["agent_versions"]["pipeline-id"][
        "protections"
    ]["safe-output"]
    assert persisted["pass_rule"] == "score == 1.0"
    assert "callable" not in str(persisted).lower()
    assert flow._pipeline.register_calls == 1
    assert client.update_calls
    assert len(client.update_calls) == 1
    assert model.last_model_request_parameters is None
    assert flow.binding is not None
    assert all(
        call["name"].startswith("equals:") and call["project"] == "project-id"
        for call in client.list_calls
    )


def test_protection_identity_changes_registration_configuration() -> None:
    first = KitaruAgent(Agent(TestModel(), name="support-agent", output_type=str))
    second = KitaruAgent(Agent(TestModel(), name="support-agent", output_type=str))
    second.protection("safe-output", capability="pure")(_registration_protection)

    assert "protections" not in first._registration_configuration()
    assert hash_registration_value(first._registration_configuration()) != (
        hash_registration_value(second._registration_configuration())
    )


def test_agent_version_rejects_mismatched_protection_map_keys() -> None:
    declaration = ProtectionDeclaration.from_callable(
        _registration_protection,
        protection_id="safe-output",
        capability="pure",
    )
    payload = _manifest().model_dump(mode="json")
    payload["future_namespace"] = {"preserve": True}
    round_tripped = _AgentVersionManifest.model_validate(payload)
    assert round_tripped.model_dump(mode="json")["future_namespace"] == {
        "preserve": True
    }

    payload["protections"] = {"wrong-key": declaration.snapshot.model_dump(mode="json")}
    with pytest.raises(ValidationError, match="map keys"):
        _AgentVersionManifest.model_validate(payload)


def test_registered_protection_callable_must_match_manifest() -> None:
    agent = KitaruAgent(Agent(TestModel(), name="support-agent", output_type=str))
    declaration = agent.protection("safe-output", capability="pure")(
        _registration_protection
    )
    manifest = _manifest().model_copy(
        update={"protections": {"safe-output": declaration.snapshot}},
        deep=True,
    )
    agent._registered_state = cast(
        Any,
        SimpleNamespace(
            binding=RegisteredAgentVersionBinding(
                project_id="project-id",
                manifest=manifest,
            )
        ),
    )
    agent._protections.clear()

    with pytest.raises(KitaruMetadataConflictError, match="do not match"):
        agent._registered_protection_declarations(project_id="project-id")


def test_local_registration_rejects_different_logical_agent_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    client = _FakeRegistrationClient()
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(client, pipeline_name))

    alpha = KitaruAgent(Agent(TestModel(), name="alpha", output_type=str))
    beta = KitaruAgent(Agent(TestModel(), name="beta", output_type=str))
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        REGISTERABLE_AGENT = alpha
        first = alpha.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
        metadata_after_alpha = deepcopy(client.metadata)
        pipeline_after_alpha = client.pipeline
        update_count = len(client.update_calls)
        list_count = len(client.list_calls)
        flow_count = len(flows)

        REGISTERABLE_AGENT = beta
        with pytest.raises(
            KitaruMetadataConflictError,
            match="already registered to Agent 'alpha', not 'beta'",
        ):
            beta.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
    finally:
        REGISTERABLE_AGENT = None

    assert first.agent.agent_id == "project-id"
    assert first.agent.name == "alpha"
    assert first.agent.display_name == "alpha"
    assert client.metadata == metadata_after_alpha
    assert client.metadata["kitaru"]["agent"]["name"] == "alpha"
    assert client.pipeline is pipeline_after_alpha
    assert len(client.update_calls) == update_count
    assert len(client.list_calls) == list_count
    assert len(flows) == flow_count


def test_pro_registration_rejects_mismatched_logical_agent_before_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    client = _FakeProRegistrationClient(project_name="beta")
    client.metadata = {
        "kitaru": {
            "schema_version": 1,
            "agent": {
                "agent_id": "project-id",
                "name": "alpha",
                "default_agent_version_id": None,
                "default_executable": None,
            },
            "agent_version_order": None,
            "agent_version_aliases": {},
            "agent_versions": {},
        }
    }
    beta = KitaruAgent(Agent(TestModel(), name="beta", output_type=str))
    create_project = Mock()
    auto_flow = Mock()
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", auto_flow)
    monkeypatch.setattr(project_ops, "create_project", create_project)

    try:
        REGISTERABLE_AGENT = beta
        with pytest.raises(
            KitaruMetadataConflictError,
            match="already registered to Agent 'alpha', not 'beta'",
        ):
            beta.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
    finally:
        REGISTERABLE_AGENT = None

    create_project.assert_not_called()
    auto_flow.assert_not_called()
    assert client.pipeline is None
    assert client.list_calls == []
    assert client.update_calls == []


def test_registration_rejects_different_project_before_durable_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    first_client = _FakeRegistrationClient("project-one")
    second_client = _FakeRegistrationClient("project-two")
    current_client = first_client
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(current_client, pipeline_name))

    durable_agent = KitaruAgent(
        Agent(TestModel(), name="support-agent", output_type=str)
    )
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: current_client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
        current_client = second_client
        with pytest.raises(KitaruStateError, match="registered Project"):
            durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
    finally:
        REGISTERABLE_AGENT = None

    assert second_client.pipeline is None
    assert second_client.update_calls == []
    assert second_client.list_calls == []


def test_reregistration_on_connection_without_bound_project_has_no_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    first_client = _FakeRegistrationClient("project-one")
    second_client = Mock()
    second_client.zen_store = SimpleNamespace(
        get_store_info=lambda: SimpleNamespace(is_pro_server=lambda: True),
    )
    second_client.get_project.side_effect = KeyError("missing")
    current_client: Any = first_client
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(first_client, pipeline_name))

    create_project = Mock()
    durable_agent = KitaruAgent(
        Agent(TestModel(), name="support-agent", output_type=str)
    )
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: current_client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(project_ops, "create_project", create_project)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
        flow = next(iter(flows.values()))
        current_client = second_client
        with pytest.raises(KitaruStateError, match="registered Project is unavailable"):
            durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
    finally:
        REGISTERABLE_AGENT = None

    second_client.get_project.assert_called_once_with(
        "project-one",
        allow_name_prefix_match=False,
        hydrate=True,
    )
    create_project.assert_not_called()
    second_client.list_pipelines.assert_not_called()
    second_client.update_project.assert_not_called()
    assert flow._pipeline.register_calls == 1


def test_reregistration_does_not_replace_missing_bound_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    client = _FakeRegistrationClient()
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(client, pipeline_name))

    durable_agent = KitaruAgent(
        Agent(TestModel(), name="support-agent", output_type=str)
    )
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
        flow = next(iter(flows.values()))
        client.pipeline = None
        update_count = len(client.update_calls)
        with pytest.raises(KitaruStateError, match="no longer exists"):
            durable_agent.register(entrypoint=f"{__name__}:REGISTERABLE_AGENT")
    finally:
        REGISTERABLE_AGENT = None

    assert flow._pipeline.register_calls == 1
    assert len(client.update_calls) == update_count


def test_metadata_failure_keeps_wrapper_unregistered_and_retry_reuses_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global REGISTERABLE_AGENT

    client = _FakeRegistrationClient()
    client.discard_updates = True
    flows: dict[str, _FakeFlow] = {}

    def fake_auto_flow(
        _agent_name: str,
        *,
        pipeline_name: str | None = None,
    ) -> _FakeFlow:
        assert pipeline_name is not None
        return flows.setdefault(pipeline_name, _FakeFlow(client, pipeline_name))

    durable_agent = KitaruAgent(Agent(TestModel(), name="retry-agent", output_type=str))
    REGISTERABLE_AGENT = durable_agent
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(agent_module, "find_repository_root", Path.cwd)
    monkeypatch.setattr(agent_module, "_auto_flow_for_agent", fake_auto_flow)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )

    try:
        with pytest.raises(KitaruMetadataReconciliationError):
            durable_agent.register(
                entrypoint=f"{__name__}:REGISTERABLE_AGENT",
            )
        assert durable_agent._registered_state is None
        flow = next(iter(flows.values()))
        assert flow._pipeline.register_calls == 1

        client.discard_updates = False
        result = durable_agent.register(
            entrypoint=f"{__name__}:REGISTERABLE_AGENT",
        )
    finally:
        REGISTERABLE_AGENT = None

    assert result.agent_version.pipeline_id == "pipeline-id"
    assert flow._pipeline.register_calls == 1
    assert durable_agent._registered_state is not None


def test_auto_flow_cache_is_version_specific(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(agent_module, "_AUTO_FLOW_DEFINITIONS", {})

    first = agent_module._auto_flow_for_agent(
        "support-agent",
        pipeline_name="support_agent__av_one",
    )
    second = agent_module._auto_flow_for_agent(
        "support-agent",
        pipeline_name="support_agent__av_two",
    )

    assert first is not second
    assert first._pipeline.name == "support_agent__av_one"
    assert second._pipeline.name == "support_agent__av_two"


def test_replay_static_drift_fails_before_loading_any_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib

    flow_module = importlib.import_module("kitaru.flow")
    wrapped = flow(lambda value: value)
    pipeline_name = str(wrapped._pipeline.name)
    manifest = _manifest().model_copy(update={"pipeline_name": pipeline_name})
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=manifest,
    )
    wrapped._bind_registered_version(binding)
    child_client = Mock()
    monkeypatch.setattr(
        flow_module,
        "resolve_connection_config",
        lambda **_kwargs: SimpleNamespace(project=None),
    )
    monkeypatch.setattr(
        flow_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )
    monkeypatch.setattr(flow_module, "Client", child_client)

    def reject_drift() -> None:
        raise KitaruStateError("configuration changed")

    with (
        wrapped._registered_preflight_scope(reject_drift),
        pytest.raises(KitaruStateError, match="configuration changed"),
    ):
        wrapped.replay(
            ["run-one", "run-two"],
            at="checkpoint",
            wait=False,
        )

    child_client.assert_not_called()


def test_registered_agent_replay_builds_one_durable_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable_agent = KitaruAgent(
        Agent(TestModel(), name="registered-agent", output_type=str)
    )
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=_manifest(),
    )
    durable_agent._registered_state = cast(Any, SimpleNamespace(binding=binding))
    flow_definition = MagicMock()
    flow_definition._registered_preflight_scope.side_effect = lambda _callback: (
        nullcontext()
    )
    monkeypatch.setattr(durable_agent, "_registered_flow", lambda: flow_definition)
    preflight = Mock()
    monkeypatch.setattr(durable_agent, "_preflight_registered_identity", preflight)
    client = object()
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(
        agent_module,
        "_temporary_active_project",
        lambda _project: nullcontext(),
    )
    draft = object()
    plan = SimpleNamespace(
        spec=SimpleNamespace(
            experiment_id="exp-1",
            regression_limits=None,
            replay_inputs=SimpleNamespace(
                flow_overrides=None,
                checkpoint_overrides=None,
                invocation_overrides=None,
                skip=None,
            ),
            at="checkpoint",
            wait=False,
        )
    )
    preplan = Mock(return_value=draft)
    freeze = Mock(return_value=plan)
    monkeypatch.setattr(agent_module, "preplan_replay_attempt", preplan)
    monkeypatch.setattr(agent_module, "freeze_replay_attempt", freeze)
    result = object()

    def execute(
        received_plan: Any,
        *,
        submit_trial: Any,
        tag: str | None,
        observe_trial: Any,
        finalize_operational_limit: Any,
        client_factory: Any,
    ) -> Any:
        assert observe_trial is None
        assert finalize_operational_limit is None
        assert received_plan is plan
        assert tag == "review"
        assert client_factory() is client
        submitted = submit_trial(
            trial=SimpleNamespace(
                target_execution_id="target-1",
                repeat_index=1,
                parent_execution_id="target-1",
                root_execution_id="root-1",
            ),
            replay_plan=SimpleNamespace(document=object()),
            submission_id="rs-exp-1",
        )
        assert submitted == "child-projection"
        return result

    monkeypatch.setattr(agent_module, "execute_replay_attempt", execute)
    flow_definition.replay.return_value = "child-projection"

    actual = durable_agent.replay(
        ["target-1", "target-2"],
        at="checkpoint",
        on_error="collect",
        uncovered_policy="top",
        idempotency_key="request-1",
        name="candidate",
        repeats=2,
        tag="review",
    )

    assert actual is result
    preflight.assert_called_once_with()
    preplan_call = preplan.call_args.kwargs
    assert preplan_call["binding"] == binding
    assert preplan_call["on_error"] == "collect"
    assert preplan_call["uncovered_policy"] == "top"
    assert preplan_call["repeats"] == 2
    assert preplan_call["wait"] is False
    freeze.assert_called_once_with(draft, client=client)
    replay_call = flow_definition.replay.call_args
    assert replay_call.args == ("target-1",)
    assert replay_call.kwargs["on_error"] == "collect"
    assert replay_call.kwargs["experiment_context"] == ExperimentReplayContext(
        experiment_id="exp-1",
        target_execution_id="target-1",
        repeat_index=1,
        parent_execution_id="target-1",
        root_execution_id="root-1",
    )
    assert replay_call.kwargs["replay_submission_id"] == "rs-exp-1"


def test_registered_agent_rerun_uses_current_candidate_and_frozen_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    durable_agent = KitaruAgent(
        Agent(TestModel(), name="registered-agent", output_type=str)
    )
    binding = RegisteredAgentVersionBinding(
        project_id="project-id",
        manifest=_manifest("current-pipeline"),
    )
    durable_agent._registered_state = cast(Any, SimpleNamespace(binding=binding))
    flow_definition = MagicMock()
    flow_definition._registered_preflight_scope.side_effect = lambda _callback: (
        nullcontext()
    )
    monkeypatch.setattr(durable_agent, "_registered_flow", lambda: flow_definition)
    monkeypatch.setattr(
        durable_agent,
        "_preflight_registered_identity",
        Mock(),
    )
    client = object()
    monkeypatch.setattr(agent_module, "Client", lambda: client)
    monkeypatch.setattr(
        agent_module,
        "_get_project_by_exact_selector",
        lambda received_client, project_id: (
            received_client is client and project_id == "project-id" and object()
        ),
    )
    monkeypatch.setattr(
        agent_module, "_active_project_id", lambda _client: "project-id"
    )
    source = SimpleNamespace(
        spec=SimpleNamespace(
            experiment_id="exp-source",
            candidate_pipeline_id="old-pipeline",
        )
    )
    source_before = deepcopy(source)
    agent_info = SimpleNamespace(
        resolve_suite_rerun_request=Mock(return_value=(None, source)),
    )
    monkeypatch.setattr(
        agent_module,
        "_agent_info_from_project_model",
        lambda _project, active_project_id: (
            agent_info if active_project_id == "project-id" else None
        ),
    )
    frozen_inputs = SimpleNamespace(
        flow_overrides={"model": "frozen"},
        checkpoint_overrides={"checkpoint": {"output": "frozen"}},
        invocation_overrides={},
        skip=["later"],
    )
    plan = SimpleNamespace(
        spec=SimpleNamespace(
            experiment_id="exp-rerun",
            kind="replay",
            source_experiment_id="exp-source",
            suite_key="regression-suite",
            request_hash="sha256:" + "1" * 64,
            at="checkpoint",
            wait=True,
            replay_inputs=frozen_inputs,
            grounded_policy=None,
            regression_limits=None,
        )
    )
    planner = Mock(return_value=plan)
    monkeypatch.setattr(agent_module, "plan_suite_rerun", planner)
    result = SimpleNamespace(
        record=SimpleNamespace(score_aggregate=None),
    )

    execute_calls = 0

    def execute(
        received_plan: Any,
        *,
        submit_trial: Any,
        tag: str | None,
        observe_trial: Any,
        finalize_operational_limit: Any,
        client_factory: Any,
    ) -> Any:
        assert observe_trial is None
        assert finalize_operational_limit is None
        nonlocal execute_calls
        execute_calls += 1
        assert client_factory() is client
        assert tag == "rerun"
        if execute_calls == 2:
            assert received_plan.spec is plan.spec
            return result
        assert received_plan is plan
        submitted = submit_trial(
            trial=SimpleNamespace(
                target_execution_id="old-child",
                repeat_index=0,
                parent_execution_id="old-child",
                root_execution_id="old-root",
            ),
            replay_plan=object(),
            submission_id="rs-exp-rerun",
        )
        assert submitted == "current-child"
        return result

    monkeypatch.setattr(agent_module, "execute_replay_attempt", execute)
    flow_definition.replay.return_value = "current-child"

    with pytest.raises(KitaruUsageError, match="executable callables"):
        durable_agent.replay(
            experiment="regression-suite",
            idempotency_key="snapshot-only",
            scorers=[SimpleNamespace(snapshot=object())],
        )
    planner.assert_not_called()
    agent_info.resolve_suite_rerun_request.assert_not_called()

    actual = durable_agent.replay(
        experiment="regression-suite",
        idempotency_key="rerun-request",
        repeats=2,
        tag="rerun",
    )

    assert actual is result
    planner.assert_called_once()
    planner_call = planner.call_args
    assert planner_call.args == (source,)
    assert planner_call.kwargs["binding"] == binding
    assert planner_call.kwargs["repeats"] == 2
    assert planner_call.kwargs["protections"] == []
    assert agent_info.resolve_suite_rerun_request.call_args.args == (
        "regression-suite",
        "rerun-request",
    )
    replay_call = flow_definition.replay.call_args
    assert replay_call.args == ("old-child",)
    assert replay_call.kwargs["at"] == "checkpoint"
    assert replay_call.kwargs["wait"] is True
    assert replay_call.kwargs["flow_overrides"] == {"model": "frozen"}
    assert replay_call.kwargs["checkpoint_overrides"] == {
        "checkpoint": {"output": "frozen"}
    }
    assert source == source_before
    assert source.spec.candidate_pipeline_id == "old-pipeline"
    assert binding.pipeline_id == "current-pipeline"

    existing = SimpleNamespace(spec=plan.spec)
    agent_info.resolve_suite_rerun_request.return_value = (existing, source)
    validator = Mock(return_value=plan)
    monkeypatch.setattr(agent_module, "validate_existing_suite_rerun", validator)
    flow_definition.replay.reset_mock()

    retried = durable_agent.replay(
        experiment="regression-suite",
        idempotency_key="rerun-request",
        repeats=2,
        tag="rerun",
    )

    assert retried is result
    assert execute_calls == 2
    flow_definition.replay.assert_not_called()
    planner.assert_called_once()
    validator.assert_called_once()


def test_unregistered_direct_native_and_replay_paths_fail_before_body() -> None:
    durable_agent = KitaruAgent(
        Agent(TestModel(), name="unregistered-agent", output_type=str)
    )
    body = Mock()

    with pytest.raises(KitaruStateError, match="not registered"):
        durable_agent._invoke_in_auto_flow(body)
    with pytest.raises(KitaruStateError, match="not registered"):
        durable_agent.replay(
            "run-id",
            at="checkpoint",
            on_error="fail",
            uncovered_policy="fail",
            idempotency_key="request-1",
        )

    body.assert_not_called()
