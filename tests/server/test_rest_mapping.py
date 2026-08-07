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
"""Isolated unit tests for the REST mapping layer."""

import uuid

from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionUpdateRequest,
    CommandRunSpec,
)
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_update_to_command,
    capabilities_to_domain,
    run_spec_to_domain,
)
from kitaru.server.adapters.rest.mapping.plugins import plugin_source_to_domain
from kitaru.server.domain.agent_version import (
    AgentCapabilities as DomainAgentCapabilities,
)
from kitaru.server.domain.agent_version import CommandRunSpec as DomainCommandRunSpec
from kitaru.server.domain.plugin import (
    PackagePluginSource as DomainPackagePluginSource,
)
from kitaru.server.domain.plugin import ScriptPluginSource as DomainScriptPluginSource


def test_agent_version_create_converts_nested_wire_models() -> None:
    """Convert a create request's run spec and capabilities to domain value objects."""
    run_spec = run_spec_to_domain(CommandRunSpec(command="python agent.py"))
    capabilities = capabilities_to_domain(AgentCapabilities(tools=["search"]))

    assert isinstance(run_spec, DomainCommandRunSpec)
    assert run_spec.command == "python agent.py"
    assert isinstance(capabilities, DomainAgentCapabilities)
    assert capabilities.tools == ["search"]


def test_agent_version_update_preserves_omitted_and_explicit_null() -> None:
    """Retain the update request's supplied-field distinction across conversion."""
    omitted = agent_version_update_to_command(AgentVersionUpdateRequest())
    explicit_null = agent_version_update_to_command(
        AgentVersionUpdateRequest(run_spec=None)
    )

    assert "run_spec" not in omitted.model_fields_set
    assert "run_spec" in explicit_null.model_fields_set
    assert explicit_null.run_spec is None


def test_plugin_sources_convert_to_domain_variants() -> None:
    """Convert both discriminated plugin source variants to domain value objects."""
    blob_id = uuid.uuid4()

    script = plugin_source_to_domain(
        ScriptPluginSource(blob_id=blob_id, entrypoint="score")
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
