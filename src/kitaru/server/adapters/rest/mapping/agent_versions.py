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

import uuid
from typing import Any

from kitaru.api_models.v1.agent_version import (
    AgentCapabilities as WireAgentCapabilities,
)
from kitaru.api_models.v1.agent_version import (
    AgentVersionListParams,
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.api_models.v1.agent_version import RunSpec as WireRunSpec
from kitaru.api_models.v1.hook import (
    CopyWorkdirHook as WireCopyWorkdirHook,
)
from kitaru.api_models.v1.hook import (
    GitCloneHook as WireGitCloneHook,
)
from kitaru.api_models.v1.hook import (
    GitPushHook as WireGitPushHook,
)
from kitaru.api_models.v1.hook import (
    TaskHook as WireTaskHook,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.agent_version import (
    AgentVersionFilter,
    AgentVersionUpdate,
)
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)
from kitaru.server.domain.hook import (
    CopyWorkdirHook,
    GitCloneHook,
    GitPushHook,
    TaskHook,
)


def _hook_to_domain(hook: WireTaskHook) -> TaskHook:
    """Convert a wire task hook to its domain value object.

    Args:
        hook: Wire task hook.

    Returns:
        Domain task hook.
    """
    if isinstance(hook, WireCopyWorkdirHook):
        return CopyWorkdirHook()
    if isinstance(hook, WireGitCloneHook):
        return GitCloneHook(url=hook.url, ref=hook.ref)
    return GitPushHook(branch=hook.branch)


def _hook_to_response(hook: TaskHook) -> WireTaskHook:
    """Convert a domain task hook to its wire value object.

    Args:
        hook: Domain task hook.

    Returns:
        Wire task hook.
    """
    if isinstance(hook, CopyWorkdirHook):
        return WireCopyWorkdirHook()
    if isinstance(hook, GitCloneHook):
        return WireGitCloneHook(url=hook.url, ref=hook.ref)
    return WireGitPushHook(branch=hook.branch)


def run_spec_to_domain(run_spec: WireRunSpec) -> RunSpec:
    """Convert a wire run spec to its domain value object.

    Args:
        run_spec: Wire run spec.

    Returns:
        Domain run spec.
    """
    return RunSpec(
        command=run_spec.command,
        working_dir=run_spec.working_dir,
        env=run_spec.env,
        secret_ids=run_spec.secret_ids,
        hooks=[_hook_to_domain(hook) for hook in run_spec.hooks],
        timeout_seconds=run_spec.timeout_seconds,
    )


def _run_spec_to_response(run_spec: RunSpec) -> WireRunSpec:
    """Convert a domain run spec to its wire value object.

    Args:
        run_spec: Domain run spec.

    Returns:
        Wire run spec.
    """
    return WireRunSpec(
        command=run_spec.command,
        working_dir=run_spec.working_dir,
        env=run_spec.env,
        secret_ids=run_spec.secret_ids,
        hooks=[_hook_to_response(hook) for hook in run_spec.hooks],
        timeout_seconds=run_spec.timeout_seconds,
    )


def capabilities_to_domain(capabilities: WireAgentCapabilities) -> AgentCapabilities:
    """Convert wire agent capabilities to their domain value object.

    Args:
        capabilities: Wire agent capabilities.

    Returns:
        Domain agent capabilities.
    """
    return AgentCapabilities(
        tools=capabilities.tools,
        mcp_servers=capabilities.mcp_servers,
        skills=capabilities.skills,
    )


def _capabilities_to_response(capabilities: AgentCapabilities) -> WireAgentCapabilities:
    """Convert domain agent capabilities to their wire value object.

    Args:
        capabilities: Domain agent capabilities.

    Returns:
        Wire agent capabilities.
    """
    return WireAgentCapabilities(
        tools=capabilities.tools,
        mcp_servers=capabilities.mcp_servers,
        skills=capabilities.skills,
    )


def agent_version_to_response(agent_version: AgentVersion) -> AgentVersionResponse:
    """Convert an agent version entity to its response DTO.

    Args:
        agent_version: Stored agent version.

    Returns:
        Agent version response.
    """
    assert agent_version.created is not None
    assert agent_version.updated is not None
    run_spec = agent_version.run_spec
    return AgentVersionResponse(
        id=agent_version.id,
        owner_id=agent_version.owner_id,
        agent_id=agent_version.agent_id,
        version=agent_version.version,
        display_version=agent_version.display_version,
        description=agent_version.description,
        run_spec=_run_spec_to_response(run_spec) if run_spec is not None else None,
        capabilities=_capabilities_to_response(agent_version.capabilities),
        created=agent_version.created,
        updated=agent_version.updated,
    )


def agent_version_list_params_to_filter(
    agent_id: uuid.UUID, params: AgentVersionListParams
) -> AgentVersionFilter:
    """Convert list params to the application filter scoped to one agent.

    Args:
        agent_id: Id of the agent whose versions to list.
        params: Agent version list params.

    Returns:
        Agent version filter.
    """
    return AgentVersionFilter(
        agent_id=agent_id,
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def agent_version_update_to_command(
    body: AgentVersionUpdateRequest,
) -> AgentVersionUpdate:
    """Convert an agent version update request to its application command.

    Args:
        body: Agent version update request.

    Returns:
        Update command carrying only the fields the request set.
    """
    fields = body.model_fields_set
    values: dict[str, Any] = {}
    if "display_version" in fields:
        values["display_version"] = body.display_version
    if "description" in fields:
        values["description"] = body.description
    if "run_spec" in fields:
        values["run_spec"] = (
            run_spec_to_domain(body.run_spec) if body.run_spec is not None else None
        )
    if "capabilities" in fields:
        values["capabilities"] = (
            capabilities_to_domain(body.capabilities)
            if body.capabilities is not None
            else None
        )
    return AgentVersionUpdate.model_validate(values)
