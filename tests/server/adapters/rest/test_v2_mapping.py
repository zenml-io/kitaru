"""REST boundary conversion tests for v2 resources."""

import uuid

from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.plugin import (
    PackagePluginSource,
    ScriptPluginSource,
)
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.api_models.v1.worker import WorkerRuntime
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_create_values,
    agent_version_update_to_command,
)
from kitaru.server.adapters.rest.mapping.plugins import plugin_source_to_domain
from kitaru.server.adapters.rest.mapping.tags import tag_resource_type_to_domain
from kitaru.server.adapters.rest.mapping.workers import worker_runtime_to_domain
from kitaru.server.domain.agent_version import (
    AgentCapabilities as DomainAgentCapabilities,
)
from kitaru.server.domain.agent_version import RunSpec as DomainRunSpec
from kitaru.server.domain.plugin import (
    PackagePluginSource as DomainPackagePluginSource,
)
from kitaru.server.domain.plugin import (
    ScriptPluginSource as DomainScriptPluginSource,
)
from kitaru.server.domain.tag import TagResourceType as DomainTagResourceType
from kitaru.server.domain.worker import WorkerRuntime as DomainWorkerRuntime


def test_agent_version_create_converts_nested_wire_models() -> None:
    """Nested request values become domain value objects."""
    body = AgentVersionCreateRequest(
        display_version="stable",
        description="Production agent",
        run_spec=RunSpec(command="python agent.py"),
        capabilities=AgentCapabilities(tools=["search"]),
    )

    display_version, description, run_spec, capabilities = agent_version_create_values(
        body
    )

    assert display_version == "stable"
    assert description == "Production agent"
    assert isinstance(run_spec, DomainRunSpec)
    assert run_spec.command == "python agent.py"
    assert isinstance(capabilities, DomainAgentCapabilities)
    assert capabilities.tools == ["search"]


def test_agent_version_update_preserves_omitted_and_explicit_null() -> None:
    """PATCH conversion retains Pydantic's supplied-field distinction."""
    omitted = agent_version_update_to_command(AgentVersionUpdateRequest())
    explicit_null = agent_version_update_to_command(
        AgentVersionUpdateRequest(run_spec=None)
    )

    assert "run_spec" not in omitted.model_fields_set
    assert "run_spec" in explicit_null.model_fields_set
    assert explicit_null.run_spec is None


def test_plugin_sources_convert_to_domain_variants() -> None:
    """Both discriminated plugin source variants cross the REST boundary."""
    blob_id = uuid.uuid4()

    script = plugin_source_to_domain(
        ScriptPluginSource(
            blob_id=blob_id,
            entrypoint="plugin:Importer",
        )
    )
    package = plugin_source_to_domain(
        PackagePluginSource(
            requirement="example-plugin==1.2.3",
            entrypoint="example_plugin:Importer",
        )
    )

    assert isinstance(script, DomainScriptPluginSource)
    assert script.blob_id == blob_id
    assert isinstance(package, DomainPackagePluginSource)
    assert package.requirement == "example-plugin==1.2.3"


def test_enum_and_runtime_values_convert_to_domain_types() -> None:
    """Sibling wire and domain models are converted instead of passed through."""
    resource_type = tag_resource_type_to_domain(TagResourceType.SESSION)
    runtime = worker_runtime_to_domain(
        WorkerRuntime(platform="python", python_version="3.14")
    )

    assert resource_type is DomainTagResourceType.SESSION
    assert isinstance(runtime, DomainWorkerRuntime)
    assert runtime.platform == "python"
    assert runtime.python_version == "3.14"
