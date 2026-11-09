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
"""Agent and agent version commands."""

import uuid
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.agent_version import AgentVersionListParams
from kitaru.cli import invocation_context, registration
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    LIST_PARAMETERS,
    VERSION_LIST_PARAMETERS,
    agent_app,
    agent_version_app,
    build_spec,
    register_command,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.schema import ParameterSpec

_AGENT_SOURCE_PARAMETERS = (
    ParameterSpec("--spec", "path", "option", False, "YAML or JSON spec document."),
    ParameterSpec(
        "--command", "string", "option", False, "Shell command stored as supplied."
    ),
    ParameterSpec(
        "--display-version", "string", "option", False, "Human-readable version."
    ),
    ParameterSpec(
        "--working-dir", "path", "option", False, "Worker process directory."
    ),
    ParameterSpec(
        "--env", "KEY=VALUE[]", "option", False, "Non-secret process environment."
    ),
    ParameterSpec("--secret-id", "UUID[]", "option", False, "Server secret IDs."),
    ParameterSpec(
        "--timeout-seconds", "positive integer", "option", False, "Process timeout."
    ),
    ParameterSpec("--tool", "string[]", "option", False, "Declared tools."),
    ParameterSpec("--mcp-server", "string[]", "option", False, "Declared MCP servers."),
    ParameterSpec("--skill", "string[]", "option", False, "Declared skills."),
)


def _agent_direct_values(
    *values: Any,
    env: list[str] | None,
    secret_id: list[uuid.UUID] | None,
    tool: list[str] | None,
    mcp_server: list[str] | None,
    skill: list[str] | None,
) -> bool:
    """Return whether any direct agent option was supplied alongside a spec."""
    return any(value is not None for value in values) or any(
        value is not None for value in (env, secret_id, tool, mcp_server, skill)
    )


@register_command(
    agent_app,
    build_spec(
        ("agent", "register"),
        "Create an agent and its first mutable version.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New agent name."),
            ParameterSpec(
                "--description", "string", "option", False, "Parent description."
            ),
            ParameterSpec(
                "--version-description",
                "string",
                "option",
                False,
                "Version description.",
            ),
            *_AGENT_SOURCE_PARAMETERS,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_parent_then_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def agent_register(
    name: str,
    /,
    *,
    spec: Path | None = None,
    command: str | None = None,
    description: str | None = None,
    version_description: str | None = None,
    display_version: str | None = None,
    working_dir: str | None = None,
    env: list[str] | None = None,
    secret_id: list[uuid.UUID] | None = None,
    timeout_seconds: int | None = None,
    tool: list[str] | None = None,
    mcp_server: list[str] | None = None,
    skill: list[str] | None = None,
) -> CommandResult:
    """Create a new agent parent and initial version."""
    if spec is not None:
        if _agent_direct_values(
            command,
            description,
            version_description,
            display_version,
            working_dir,
            timeout_seconds,
            env=env,
            secret_id=secret_id,
            tool=tool,
            mcp_server=mcp_server,
            skill=skill,
        ):
            raise CLIError(
                "invalid_arguments", "--spec conflicts with direct agent options."
            )
        parent_request, version_request = registration.load_agent_register_spec(
            name, spec
        )
    else:
        parent_request, version_request = registration.build_agent_requests(
            name,
            command=command,
            entrypoint=None,
            description=description,
            version_description=version_description,
            display_version=display_version,
            working_dir=working_dir,
            env=env,
            secret_ids=secret_id,
            timeout_seconds=timeout_seconds,
            tools=tool,
            mcp_servers=mcp_server,
            skills=skill,
        )
    async with invocation_context.open_asset_client() as client:
        return await registration.register_agent(
            client, parent_request, version_request
        )


@register_command(
    agent_app,
    build_spec(
        ("agent", "list"),
        "List agents.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def agent_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of agents."""
    params = registration.build_list_params(
        AgentListParams, size=size, cursor=cursor, sort=sort, filter=filter
    )
    async with invocation_context.open_asset_client() as client:
        return registration.page_result(await client.agents.list(params), size=size)


@register_command(
    agent_app,
    build_spec(
        ("agent", "get"),
        "Get an agent by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def agent_get(agent: str, /) -> CommandResult:
    """Get one exact agent."""
    async with invocation_context.open_asset_client() as client:
        item = await registration.resolve_asset(client.agents, agent, "Agent")
        return CommandResult(item=item.model_dump(mode="json"))


@register_command(
    agent_app,
    build_spec(
        ("agent", "delete"),
        "Soft delete an agent.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
            ParameterSpec(
                "--force", "boolean", "option", False, "Confirm remote deletion."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def agent_delete(agent: str, /, *, force: bool = False) -> CommandResult:
    """Soft delete one exact agent."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting an agent requires --force.")
    async with invocation_context.open_asset_client() as client:
        item = await registration.resolve_asset(client.agents, agent, "Agent")
        await client.agents.delete(item.id)
        return CommandResult(item={"id": str(item.id), "deleted": True})


@register_command(
    agent_version_app,
    build_spec(
        ("agent", "version", "register"),
        "Create the next mutable version of an existing agent.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Version description."
            ),
            *_AGENT_SOURCE_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_server_assigned_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def agent_version_register(
    agent: str,
    /,
    *,
    spec: Path | None = None,
    command: str | None = None,
    description: str | None = None,
    display_version: str | None = None,
    working_dir: str | None = None,
    env: list[str] | None = None,
    secret_id: list[uuid.UUID] | None = None,
    timeout_seconds: int | None = None,
    tool: list[str] | None = None,
    mcp_server: list[str] | None = None,
    skill: list[str] | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create the next server-assigned version of an exact agent."""
    if spec is not None:
        if _agent_direct_values(
            command,
            description,
            display_version,
            working_dir,
            timeout_seconds,
            env=env,
            secret_id=secret_id,
            tool=tool,
            mcp_server=mcp_server,
            skill=skill,
        ):
            raise CLIError(
                "invalid_arguments", "--spec conflicts with direct agent options."
            )
        request = registration.load_agent_version_spec(spec)
    else:
        request = registration.build_agent_version_request(
            command=command,
            entrypoint=None,
            description=description,
            display_version=display_version,
            working_dir=working_dir,
            env=env,
            secret_ids=secret_id,
            timeout_seconds=timeout_seconds,
            tools=tool,
            mcp_servers=mcp_server,
            skills=skill,
        )
    async with invocation_context.open_asset_client() as client:
        return await registration.register_agent_version(
            client, agent, request, idempotency_key=idempotency_key
        )


@register_command(
    agent_version_app,
    build_spec(
        ("agent", "version", "list"),
        "List versions of an exact agent.",
        parameters=(
            ParameterSpec(
                "AGENT", "reference", "argument", True, "Agent UUID or name."
            ),
            *VERSION_LIST_PARAMETERS,
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def agent_version_list(
    agent: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of agent versions."""
    params = registration.build_list_params(
        AgentVersionListParams, size=size, cursor=cursor, sort=sort, filter=None
    )
    async with invocation_context.open_asset_client() as client:
        parent = await registration.resolve_asset(client.agents, agent, "Agent")
        page = await client.agents.list_versions(parent.id, params)
        return registration.page_result(page, size=size)


@register_command(
    agent_version_app,
    build_spec(
        ("agent", "version", "get"),
        "Get an agent version by exact PARENT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "AGENT@VERSION",
                "reference",
                "argument",
                True,
                "Exact version reference.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def agent_version_get(agent_version: str, /) -> CommandResult:
    """Get one exact agent version, accepting @latest for reads."""
    async with invocation_context.open_asset_client() as client:
        _, version = await registration.get_agent_version(client, agent_version)
        return CommandResult(item=version.model_dump(mode="json"))
