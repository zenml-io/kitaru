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
from datetime import UTC, datetime

from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionUpdateRequest,
    ReplayCapabilities,
    RunSpec,
)
from kitaru.api_models.v1.hook import (
    CopyWorkdirHook,
    SetupCommandHook,
    TeardownCommandHook,
)
from kitaru.api_models.v1.plugin import PackagePluginSource, ScriptPluginSource
from kitaru.api_models.v1.task import TaskKind
from kitaru.server.adapters.rest.mapping.agent_versions import (
    agent_version_to_response,
    agent_version_update_to_command,
    capabilities_to_domain,
    run_spec_to_domain,
)
from kitaru.server.adapters.rest.mapping.plugins import plugin_source_to_domain
from kitaru.server.adapters.rest.mapping.tasks import spec_to_response
from kitaru.server.domain.agent_version import (
    AgentCapabilities as DomainAgentCapabilities,
)
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.agent_version import (
    ReplayCapabilities as DomainReplayCapabilities,
)
from kitaru.server.domain.agent_version import RunSpec as DomainRunSpec
from kitaru.server.domain.hook import (
    CopyWorkdirHook as DomainCopyWorkdirHook,
)
from kitaru.server.domain.hook import (
    SetupCommandHook as DomainSetupCommandHook,
)
from kitaru.server.domain.hook import (
    TeardownCommandHook as DomainTeardownCommandHook,
)
from kitaru.server.domain.plugin import (
    PackagePluginSource as DomainPackagePluginSource,
)
from kitaru.server.domain.plugin import ScriptPluginSource as DomainScriptPluginSource
from kitaru.server.domain.task import (
    AgentTaskDetails as DomainAgentTaskDetails,
)
from kitaru.server.domain.task import TaskSpec as DomainTaskSpec


def test_agent_version_create_converts_nested_wire_models() -> None:
    """Convert a create request's run spec and capabilities to domain value objects."""
    run_spec = run_spec_to_domain(RunSpec(command="python agent.py"))
    capabilities = capabilities_to_domain(AgentCapabilities(tools=["search"]))

    assert isinstance(run_spec, DomainRunSpec)
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


def test_run_spec_hooks_convert_to_domain_variants() -> None:
    """Convert every discriminated hook variant to its domain value object."""
    run_spec = run_spec_to_domain(
        RunSpec(
            command="run.sh",
            hooks=[
                CopyWorkdirHook(),
                SetupCommandHook(command="setup.sh"),
                TeardownCommandHook(command="teardown.sh", on="always"),
            ],
        )
    )

    assert run_spec.hooks == [
        DomainCopyWorkdirHook(),
        DomainSetupCommandHook(command="setup.sh"),
        DomainTeardownCommandHook(command="teardown.sh", on="always"),
    ]


def test_agent_version_response_carries_run_spec_hooks() -> None:
    """Convert a stored run spec's hooks back to their wire variants."""
    now = datetime.now(UTC)
    version = AgentVersion(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        run_spec=DomainRunSpec(
            command="run.sh",
            hooks=[
                DomainCopyWorkdirHook(),
                DomainSetupCommandHook(command="setup.sh"),
                DomainTeardownCommandHook(command="teardown.sh", on="always"),
            ],
        ),
        created=now,
        updated=now,
    )

    response = agent_version_to_response(version)

    assert response.run_spec is not None
    assert response.run_spec.hooks == [
        CopyWorkdirHook(),
        SetupCommandHook(command="setup.sh"),
        TeardownCommandHook(command="teardown.sh", on="always"),
    ]


def test_run_spec_replay_capabilities_convert_to_domain() -> None:
    """Convert declared and omitted replay capabilities to domain value objects."""
    declared = run_spec_to_domain(
        RunSpec(
            command="run.sh",
            replay_capabilities=ReplayCapabilities(
                overrides=False, tool_policies=False
            ),
        )
    )
    omitted = run_spec_to_domain(RunSpec(command="run.sh"))

    assert declared.replay_capabilities == DomainReplayCapabilities(
        overrides=False, tool_policies=False
    )
    assert omitted.replay_capabilities == DomainReplayCapabilities()


def test_agent_version_response_carries_run_spec_replay_capabilities() -> None:
    """Convert a stored run spec's replay capabilities back to their wire values."""
    now = datetime.now(UTC)
    version = AgentVersion(
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        run_spec=DomainRunSpec(
            command="run.sh",
            replay_capabilities=DomainReplayCapabilities(
                overrides=False, tool_policies=True
            ),
        ),
        created=now,
        updated=now,
    )

    response = agent_version_to_response(version)

    assert response.run_spec is not None
    assert response.run_spec.replay_capabilities == ReplayCapabilities(
        overrides=False, tool_policies=True
    )


def test_task_spec_response_carries_hooks() -> None:
    """Convert a task spec's hooks to their wire variants."""
    spec = DomainTaskSpec(
        task_id=uuid.uuid4(),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        hooks=[
            DomainCopyWorkdirHook(),
            DomainSetupCommandHook(command="setup.sh"),
            DomainTeardownCommandHook(command="teardown.sh", on="always"),
        ],
        details=DomainAgentTaskDetails(),
    )

    response = spec_to_response(spec)

    assert response.hooks == [
        CopyWorkdirHook(),
        SetupCommandHook(command="setup.sh"),
        TeardownCommandHook(command="teardown.sh", on="always"),
    ]


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
