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
"""Cohort and immutable cohort-version CLI commands."""

import uuid
from typing import Any

from kitaru.api_models.v1.cohort import (
    CohortCreateRequest,
    CohortResponse,
    CohortUpdateRequest,
)
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionResponse,
    CohortVersionUpdateRequest,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    list_params,
    page_result,
    parse_json_object,
    parse_version_reference,
    resolve_asset,
    version_list_params,
)


async def create_cohort(
    client: Any,
    name: str,
    *,
    agent: str,
    description: str | None,
    metadata: str | None,
) -> CommandResult:
    """Create a cohort bound to one exact agent."""
    parsed_metadata = parse_json_object(metadata, option="--metadata")
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    cohort = await client.cohorts.create(
        CohortCreateRequest(
            name=name,
            description=description,
            agent_id=resolved_agent.id,
            metadata=parsed_metadata,
        )
    )
    return CommandResult(item=cohort.model_dump(mode="json"))


async def list_cohorts(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of cohorts."""
    params = list_params("cohort", size=size, cursor=cursor, sort=sort, filter=filter)
    return page_result(await client.cohorts.list(params), size=size)


async def get_cohort(client: Any, reference: str) -> CommandResult:
    """Get one cohort by exact UUID or case-sensitive name."""
    cohort = await resolve_asset(client.cohorts, reference, "Cohort")
    return CommandResult(item=cohort.model_dump(mode="json"))


async def update_cohort(
    client: Any,
    reference: str,
    *,
    name: str | None,
    description: str | None,
    clear_description: bool,
    metadata: str | None,
) -> CommandResult:
    """Update only explicitly selected cohort fields."""
    if description is not None and clear_description:
        raise CLIError(
            "invalid_arguments",
            "--description and --clear-description cannot be used together.",
        )
    fields: dict[str, Any] = {}
    if name is not None:
        fields["name"] = name
    if description is not None:
        fields["description"] = description
    elif clear_description:
        fields["description"] = None
    if metadata is not None:
        fields["metadata"] = parse_json_object(metadata, option="--metadata")
    if not fields:
        raise CLIError("invalid_arguments", "Select at least one cohort update.")

    cohort = await resolve_asset(client.cohorts, reference, "Cohort")
    updated = await client.cohorts.update(cohort.id, CohortUpdateRequest(**fields))
    return CommandResult(item=updated.model_dump(mode="json"))


async def delete_cohort(client: Any, reference: str, *, force: bool) -> CommandResult:
    """Delete one cohort and all of its versions."""
    if not force:
        raise CLIError(
            "invalid_arguments",
            "Deleting a cohort and all of its versions requires --force.",
        )
    cohort = await resolve_asset(client.cohorts, reference, "Cohort")
    await client.cohorts.delete(cohort.id)
    return CommandResult(item={"id": str(cohort.id), "deleted": True})


async def create_cohort_version(
    client: Any,
    cohort_reference: str,
    *,
    add_session_ids: list[uuid.UUID] | None,
    remove_session_ids: list[uuid.UUID] | None,
    display_version: str | None,
) -> CommandResult:
    """Create an immutable version from an ordered membership delta."""
    additions = list(add_session_ids or [])
    removals = list(remove_session_ids or [])
    if len(set(additions)) != len(additions):
        raise CLIError("invalid_arguments", "Each --add-session value must be unique.")
    if len(set(removals)) != len(removals):
        raise CLIError(
            "invalid_arguments", "Each --remove-session value must be unique."
        )
    if set(additions) & set(removals):
        raise CLIError(
            "invalid_arguments",
            "A session cannot be selected by both --add-session and --remove-session.",
        )

    cohort = await resolve_asset(client.cohorts, cohort_reference, "Cohort")
    version = await client.cohorts.create_version(
        cohort.id,
        CohortVersionCreateRequest(
            add_session_ids=additions,
            remove_session_ids=removals,
            display_version=display_version,
        ),
    )
    warnings = []
    if not additions and not removals:
        warnings.append(
            "No sessions were added or removed; cohort membership is unchanged."
        )
    return CommandResult(item=version.model_dump(mode="json"), warnings=warnings)


async def list_cohort_versions(
    client: Any,
    cohort_reference: str,
    *,
    size: int,
    cursor: str | None,
    sort: str,
) -> CommandResult:
    """List one server page of versions for an exact cohort."""
    cohort = await resolve_asset(client.cohorts, cohort_reference, "Cohort")
    params = version_list_params(size=size, cursor=cursor, sort=sort)
    return page_result(await client.cohorts.list_versions(cohort.id, params), size=size)


async def get_cohort_version(
    client: Any, reference: str
) -> tuple[CohortResponse, CohortVersionResponse]:
    """Resolve a cohort version by UUID or exact server-assigned version."""
    normalized = reference.strip()
    if not normalized:
        raise CLIError("invalid_arguments", "Cohort version reference cannot be blank.")
    try:
        version_id = uuid.UUID(normalized)
    except ValueError:
        version_id = None
    if version_id is not None:
        version = await client.cohort_versions.get(version_id)
        cohort = await client.cohorts.get(version.cohort_id)
        return cohort, version

    parent_reference, requested = parse_version_reference(normalized, "Cohort")
    cohort = await resolve_asset(client.cohorts, parent_reference, "Cohort")
    version_number = cohort.latest_version if requested == "latest" else requested
    matches = [
        item
        async for item in client.cohorts.iter_versions(cohort.id)
        if item.version == version_number
    ]
    if not matches:
        raise CLIError(
            "not_found",
            f"Cohort {cohort.name!r} has no version {version_number}.",
        )
    if len(matches) > 1:
        raise CLIError(
            "conflict",
            f"Cohort {cohort.name!r} has multiple records for version "
            f"{version_number}.",
            details={"ids": [str(item.id) for item in matches]},
        )
    return cohort, matches[0]


async def get_cohort_version_result(client: Any, reference: str) -> CommandResult:
    """Get one exact cohort version."""
    _, version = await get_cohort_version(client, reference)
    return CommandResult(item=version.model_dump(mode="json"))


async def update_cohort_version(
    client: Any,
    reference: str,
    *,
    display_version: str | None,
    clear_display_version: bool,
) -> CommandResult:
    """Update the display version of one immutable membership snapshot."""
    if display_version is not None and clear_display_version:
        raise CLIError(
            "invalid_arguments",
            "--display-version and --clear-display-version cannot be used together.",
        )
    if display_version is None and not clear_display_version:
        raise CLIError(
            "invalid_arguments",
            "Select exactly one of --display-version or --clear-display-version.",
        )

    _, version = await get_cohort_version(client, reference)
    request = CohortVersionUpdateRequest(
        display_version=None if clear_display_version else display_version
    )
    updated = await client.cohort_versions.update(version.id, request)
    return CommandResult(item=updated.model_dump(mode="json"))


async def delete_cohort_version(
    client: Any, reference: str, *, force: bool
) -> CommandResult:
    """Delete one exact cohort version."""
    if not force:
        raise CLIError(
            "invalid_arguments", "Deleting a cohort version requires --force."
        )
    _, version = await get_cohort_version(client, reference)
    await client.cohort_versions.delete(version.id)
    return CommandResult(item={"id": str(version.id), "deleted": True})
