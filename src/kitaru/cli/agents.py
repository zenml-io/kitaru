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
"""Agent registration, update, and soft-delete CLI commands."""

import json
import uuid
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from httpx import TransportError
from pydantic import ValidationError

from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import (
    AgentCapabilities,
    AgentVersionCreateRequest,
    AgentVersionUpdateRequest,
    RunSpec,
)
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    get_agent_version,
    load_spec_document,
    parse_env,
    resolve_asset,
)
from kitaru.cli.registration_spec import AgentParentSpec as AgentParentSpec
from kitaru.cli.registration_spec import AgentRegistrationSpec as AgentRegistrationSpec
from kitaru.client.exceptions import APIError, InvalidServerResponseError

_RETENTION_COUNT_LIMIT = 1000
_RETENTION_QUERY_ERRORS = (
    APIError,
    JSONDecodeError,
    InvalidServerResponseError,
    ValidationError,
    TransportError,
    TimeoutError,
)


def normalize_agent_source(
    *, command: str | None, entrypoint: str | None
) -> str | None:
    """Validate an optional agent source and return the stored shell command."""
    if entrypoint is not None:
        raise CLIError(
            "invalid_arguments",
            "Agent --entrypoint is not supported; use --command.",
        )
    if command is None:
        return None
    if not command.strip():
        raise CLIError("invalid_arguments", "--command cannot be blank.")
    return command


def _get_run_spec_flags(
    *,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
) -> list[str]:
    """Return the CLI names of supplied options that belong to a run spec."""
    return [
        flag
        for flag, present in (
            ("--working-dir", working_dir is not None),
            ("--env", env is not None),
            ("--secret-id", secret_ids is not None),
            ("--timeout-seconds", timeout_seconds is not None),
        )
        if present
    ]


def _format_run_spec_flag_error(flags: list[str], *, suffix: str = "") -> str:
    """Format an actionable error naming the run-spec-only options."""
    joined = ", ".join(flags)
    verb = "require" if len(flags) > 1 else "requires"
    return f"{joined} {verb} --command{suffix}."


def _validate_agent_version(
    request: AgentVersionCreateRequest | AgentVersionUpdateRequest,
    *,
    command_path: str,
) -> None:
    """Reject a blank command whenever a run spec is supplied."""
    if request.run_spec is not None and not request.run_spec.command.strip():
        raise CLIError(
            "invalid_arguments",
            f"Agent specs must include a nonblank {command_path}.",
        )


def load_agent_register_spec(
    name: str, path: Path
) -> tuple[AgentCreateRequest, AgentVersionCreateRequest]:
    """Load a strict parent-plus-version YAML or JSON document."""
    data = load_spec_document(path)
    spec = AgentRegistrationSpec.model_validate(data)
    _validate_agent_version(spec.version, command_path="version.run_spec.command")
    return (
        AgentCreateRequest(name=name, description=spec.get_agent_description()),
        spec.version,
    )


def load_agent_version_spec(path: Path) -> AgentVersionCreateRequest:
    """Load a strict agent version YAML or JSON document."""
    request = AgentVersionCreateRequest.model_validate(load_spec_document(path))
    _validate_agent_version(request, command_path="version.run_spec.command")
    return request


def load_agent_version_update_spec(path: Path) -> AgentVersionUpdateRequest:
    """Load a strict agent version update YAML or JSON document."""
    request = AgentVersionUpdateRequest.model_validate(load_spec_document(path))
    if not request.model_fields_set:
        raise CLIError(
            "invalid_arguments",
            "Agent version update spec must set at least one field.",
        )
    if "run_spec" in request.model_fields_set:
        _validate_agent_version(request, command_path="run_spec.command")
    return request


def build_agent_requests(
    name: str,
    *,
    command: str | None,
    entrypoint: str | None,
    description: str | None,
    version_description: str | None,
    display_version: str | None,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
    tools: list[str] | None,
    mcp_servers: list[str] | None,
    skills: list[str] | None,
) -> tuple[AgentCreateRequest, AgentVersionCreateRequest]:
    """Build direct agent parent and version requests."""
    normalized_command = normalize_agent_source(command=command, entrypoint=entrypoint)
    run_spec_flags = _get_run_spec_flags(
        working_dir=working_dir,
        env=env,
        secret_ids=secret_ids,
        timeout_seconds=timeout_seconds,
    )
    if normalized_command is None:
        if run_spec_flags:
            raise CLIError(
                "invalid_arguments", _format_run_spec_flag_error(run_spec_flags)
            )
        run_spec: RunSpec | None = None
    else:
        run_options: dict[str, Any] = {
            "command": normalized_command,
            "working_dir": working_dir,
            "env": parse_env(env or []),
            "secret_ids": secret_ids or [],
        }
        if timeout_seconds is not None:
            run_options["timeout_seconds"] = timeout_seconds
        run_spec = RunSpec(**run_options)
    capabilities = AgentCapabilities(
        tools=tools or [], mcp_servers=mcp_servers or [], skills=skills or []
    )
    return (
        AgentCreateRequest(name=name, description=description),
        AgentVersionCreateRequest(
            display_version=display_version,
            description=version_description,
            run_spec=run_spec,
            capabilities=capabilities,
        ),
    )


def build_agent_version_request(
    *,
    command: str | None,
    entrypoint: str | None,
    description: str | None,
    display_version: str | None,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
    tools: list[str] | None,
    mcp_servers: list[str] | None,
    skills: list[str] | None,
) -> AgentVersionCreateRequest:
    """Build one direct agent version request."""
    _, request = build_agent_requests(
        "unused",
        command=command,
        entrypoint=entrypoint,
        description=None,
        version_description=description,
        display_version=display_version,
        working_dir=working_dir,
        env=env,
        secret_ids=secret_ids,
        timeout_seconds=timeout_seconds,
        tools=tools,
        mcp_servers=mcp_servers,
        skills=skills,
    )
    return request


async def update_agent_version(
    client: Any,
    reference: str,
    *,
    spec: Path | None,
    command: str | None,
    description: str | None,
    display_version: str | None,
    working_dir: str | None,
    env: list[str] | None,
    secret_ids: list[uuid.UUID] | None,
    timeout_seconds: int | None,
    tools: list[str] | None,
    mcp_servers: list[str] | None,
    skills: list[str] | None,
) -> CommandResult:
    """Update an existing agent version in place, returning before/after run specs."""
    has_direct = any(
        value is not None
        for value in (
            command,
            description,
            display_version,
            working_dir,
            env,
            secret_ids,
            timeout_seconds,
            tools,
            mcp_servers,
            skills,
        )
    )
    if spec is not None:
        if has_direct:
            raise CLIError(
                "invalid_arguments",
                "--spec conflicts with direct agent options.",
                hint=(
                    "Put the version description in description in the version-only "
                    "spec, or omit --spec and use direct options."
                ),
            )
        spec_request = load_agent_version_update_spec(spec)
        _, existing = await get_agent_version(client, reference)
        updated = await client.agent_versions.update(existing.id, spec_request)
        return _build_version_update_result(existing, updated)

    if not has_direct:
        raise CLIError(
            "invalid_arguments",
            "Select at least one agent version update option or --spec.",
        )

    normalized_command = normalize_agent_source(command=command, entrypoint=None)
    parsed_env = parse_env(env) if env is not None else None
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise CLIError("invalid_arguments", "--timeout-seconds must be positive.")

    _, existing = await get_agent_version(client, reference)
    fields: dict[str, Any] = {}
    if description is not None:
        fields["description"] = description
    if display_version is not None:
        fields["display_version"] = display_version

    run_spec_flags = _get_run_spec_flags(
        working_dir=working_dir,
        env=env,
        secret_ids=secret_ids,
        timeout_seconds=timeout_seconds,
    )
    if normalized_command is not None or run_spec_flags:
        if existing.run_spec is None:
            if normalized_command is None:
                raise CLIError(
                    "invalid_arguments",
                    _format_run_spec_flag_error(
                        run_spec_flags,
                        suffix=" because this agent version has no existing run spec",
                    ),
                )
            run_options: dict[str, Any] = {
                "command": normalized_command,
                "working_dir": working_dir,
                "env": parsed_env or {},
                "secret_ids": secret_ids or [],
            }
            if timeout_seconds is not None:
                run_options["timeout_seconds"] = timeout_seconds
            fields["run_spec"] = RunSpec(**run_options)
        else:
            run_updates: dict[str, Any] = {}
            if normalized_command is not None:
                run_updates["command"] = normalized_command
            if working_dir is not None:
                run_updates["working_dir"] = working_dir
            if parsed_env is not None:
                run_updates["env"] = parsed_env
            if secret_ids is not None:
                run_updates["secret_ids"] = secret_ids
            if timeout_seconds is not None:
                run_updates["timeout_seconds"] = timeout_seconds
            fields["run_spec"] = existing.run_spec.model_copy(update=run_updates)

    if tools is not None or mcp_servers is not None or skills is not None:
        cap_updates: dict[str, Any] = {}
        if tools is not None:
            cap_updates["tools"] = tools
        if mcp_servers is not None:
            cap_updates["mcp_servers"] = mcp_servers
        if skills is not None:
            cap_updates["skills"] = skills
        fields["capabilities"] = existing.capabilities.model_copy(update=cap_updates)

    updated = await client.agent_versions.update(
        existing.id, AgentVersionUpdateRequest(**fields)
    )
    return _build_version_update_result(existing, updated)


def _build_version_update_result(existing: Any, updated: Any) -> CommandResult:
    """Build the update receipt carrying previous and updated run specs."""
    item = updated.model_dump(mode="json")
    item["previous_run_spec"] = (
        existing.run_spec.model_dump(mode="json")
        if existing.run_spec is not None
        else None
    )
    return CommandResult(item=item)


async def _count_retained_resources(
    resource: Any,
    params: Any,
    *,
    agent_id: uuid.UUID,
    label: str,
) -> tuple[int | None, bool | None, str | None]:
    """Count retained resources on a best-effort basis after soft deletion."""
    try:
        page = await resource.list(params)
    except _RETENTION_QUERY_ERRORS:
        return (
            None,
            None,
            f"Agent {agent_id} was deleted, but retained {label} could not be counted.",
        )
    count = len(page.items)
    truncated = page.next_cursor is not None
    warning = (
        f"More than {_RETENTION_COUNT_LIMIT} {label} remain under deleted agent "
        f"{agent_id}."
        if truncated
        else None
    )
    return count, truncated, warning


async def delete_agent(client: Any, reference: str, *, force: bool) -> CommandResult:
    """Soft delete an agent and report bounded counts of retained resources."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting an agent requires --force.")
    item = await resolve_asset(client.agents, reference, "Agent")
    await client.agents.delete(item.id)

    agent_filter = FilterCondition(field="agent_id", op=FilterOp.EQ, value=str(item.id))
    (
        retained_sessions,
        retained_sessions_truncated,
        sessions_warning,
    ) = await _count_retained_resources(
        client.sessions,
        SessionListParams(size=_RETENTION_COUNT_LIMIT, filter=agent_filter),
        agent_id=item.id,
        label="sessions",
    )
    (
        retained_cohorts,
        retained_cohorts_truncated,
        cohorts_warning,
    ) = await _count_retained_resources(
        client.cohorts,
        CohortListParams(size=_RETENTION_COUNT_LIMIT, filter=agent_filter),
        agent_id=item.id,
        label="cohorts",
    )
    warnings = [w for w in (sessions_warning, cohorts_warning) if w is not None]

    filter_json = json.dumps(
        {"field": "agent_id", "op": "eq", "value": str(item.id)},
        separators=(",", ":"),
    )
    next_actions: list[str] = []
    if retained_sessions is None or retained_sessions > 0:
        next_actions.append(f"kitaru session list --filter '{filter_json}'")
        next_actions.append(f"kitaru session delete --filter '{filter_json}' --force")
    if retained_cohorts is None or retained_cohorts > 0:
        next_actions.append(f"kitaru cohort list --filter '{filter_json}'")

    return CommandResult(
        item={
            "id": str(item.id),
            "name": item.name,
            "deleted": True,
            "soft_deleted": True,
            "retained_sessions": retained_sessions,
            "retained_sessions_truncated": retained_sessions_truncated,
            "retained_cohorts": retained_cohorts,
            "retained_cohorts_truncated": retained_cohorts_truncated,
        },
        warnings=warnings,
        next_actions=next_actions,
    )
