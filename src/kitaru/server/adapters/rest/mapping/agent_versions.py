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
"""Agent version DTO conversions."""

from kitaru.api_models.v1.agent_versions import (
    AgentCapabilities as AgentCapabilitiesModel,
)
from kitaru.api_models.v1.agent_versions import (
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.api_models.v1.agent_versions import RunSpec as RunSpecModel
from kitaru.server.adapters.rest.mapping.partial import set_fields
from kitaru.server.application.models.agent_versions import AgentVersionUpdate
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)


def run_spec_to_domain(run_spec: RunSpecModel | None) -> RunSpec | None:
    """Convert an optional run spec DTO to its domain value object.

    Args:
        run_spec: Run spec DTO.

    Returns:
        Domain run spec, ``None`` for ``None``.
    """
    if run_spec is None:
        return None
    return RunSpec(
        command=run_spec.command,
        working_dir=run_spec.working_dir,
        env=run_spec.env,
        secret_ids=run_spec.secret_ids,
        timeout_seconds=run_spec.timeout_seconds,
    )


def run_spec_to_response(run_spec: RunSpec | None) -> RunSpecModel | None:
    """Convert an optional domain run spec to its DTO.

    Args:
        run_spec: Domain run spec.

    Returns:
        Run spec DTO, ``None`` for ``None``.
    """
    if run_spec is None:
        return None
    return RunSpecModel(
        command=run_spec.command,
        working_dir=run_spec.working_dir,
        env=run_spec.env,
        secret_ids=run_spec.secret_ids,
        timeout_seconds=run_spec.timeout_seconds,
    )


def capabilities_to_domain(
    capabilities: AgentCapabilitiesModel | None,
) -> AgentCapabilities | None:
    """Convert an optional capabilities DTO to its domain value object.

    Args:
        capabilities: Capabilities DTO.

    Returns:
        Domain capabilities, ``None`` for ``None``.
    """
    if capabilities is None:
        return None
    return AgentCapabilities(
        tools=capabilities.tools,
        mcp_servers=capabilities.mcp_servers,
        skills=capabilities.skills,
    )


def capabilities_to_response(
    capabilities: AgentCapabilities,
) -> AgentCapabilitiesModel:
    """Convert domain capabilities to their DTO.

    Args:
        capabilities: Domain capabilities.

    Returns:
        Capabilities DTO.
    """
    return AgentCapabilitiesModel(
        tools=capabilities.tools,
        mcp_servers=capabilities.mcp_servers,
        skills=capabilities.skills,
    )


def agent_version_update_to_command(
    body: AgentVersionUpdateRequest,
) -> AgentVersionUpdate:
    """Convert an agent version update request to its command.

    Only fields set on the request are set on the command, so an absent
    field stays distinguishable from an explicit null.

    Args:
        body: Agent version update request.

    Returns:
        Agent version update command.
    """
    fields = set_fields(body)
    if "run_spec" in fields:
        fields["run_spec"] = run_spec_to_domain(body.run_spec)
    if "capabilities" in fields:
        fields["capabilities"] = capabilities_to_domain(body.capabilities)
    return AgentVersionUpdate(**fields)


def agent_version_to_response(version: AgentVersion) -> AgentVersionResponse:
    """Convert an agent version entity to its response DTO.

    Args:
        version: Stored agent version.

    Returns:
        Agent version response.
    """
    assert version.created is not None
    assert version.updated is not None
    return AgentVersionResponse(
        id=version.id,
        owner_id=version.owner_id,
        agent_id=version.agent_id,
        version=version.version,
        description=version.description,
        run_spec=run_spec_to_response(version.run_spec),
        capabilities=capabilities_to_response(version.capabilities),
        created=version.created,
        updated=version.updated,
    )
