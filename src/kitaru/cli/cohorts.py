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

import shlex
import uuid
from pathlib import Path
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
from kitaru.cli.links import resolve_dashboard_links
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    list_params,
    page_result,
    parse_json_object,
    resolve_asset,
    version_list_params,
)
from kitaru.cli.session_selection import (
    get_cohort_version as resolve_cohort_version,
)
from kitaru.cli.session_selection import (
    parse_session_uuid_file,
    select_session_ids,
)
from kitaru.client.dashboard_urls import get_cohort_url

_INLINE_RETRY_SESSION_LIMIT = 10


async def create_cohort(
    client: Any,
    name: str,
    *,
    agent: str,
    description: str | None,
    metadata: str | None,
    sessions: list[str] | None = None,
    sessions_file: Path | None = None,
    tag: str | None = None,
    cohort: str | None = None,
    filter: str | None = None,
    all_sessions: bool = False,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create a cohort and optionally its first selected membership version."""
    parsed_metadata = parse_json_object(metadata, option="--metadata")
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    has_selection = any(
        (
            bool(sessions),
            sessions_file is not None,
            tag is not None,
            cohort is not None,
            filter is not None,
            all_sessions,
        )
    )
    if display_version is not None and not has_selection:
        raise CLIError(
            "invalid_arguments",
            "--display-version requires a session selector.",
        )
    selected_session_ids = None
    if has_selection:
        selected_session_ids = await select_session_ids(
            client,
            sessions,
            sessions_file,
            tag=tag,
            cohort=cohort,
            filter=filter,
            all_sessions=all_sessions,
        )
    created_cohort = await client.cohorts.create(
        CohortCreateRequest(
            name=name,
            description=description,
            agent_id=resolved_agent.id,
            metadata=parsed_metadata,
        ),
        idempotency_key=idempotency_key,
    )
    if selected_session_ids is None:
        links, warnings = await resolve_dashboard_links(
            client,
            lambda base: {
                "dashboard": get_cohort_url(
                    base, agent_id=created_cohort.agent_id, cohort_id=created_cohort.id
                )
            },
            warning=(
                "Could not resolve a dashboard link because the server info "
                "request failed. The cohort was created."
            ),
        )
        return CommandResult(
            item=created_cohort.model_dump(mode="json"),
            links=links,
            warnings=warnings,
        )
    try:
        version = await client.cohorts.create_version(
            created_cohort.id,
            CohortVersionCreateRequest(
                add_session_ids=selected_session_ids,
                display_version=display_version,
            ),
        )
    except Exception as error:
        hint = _build_initial_version_retry_hint(
            created_cohort.id,
            selected_session_ids,
            sessions_file=sessions_file if not sessions else None,
            display_version=display_version,
        )
        raise CLIError(
            "partial_failure",
            "Created the cohort but could not create its first version.",
            details={
                "cohort": {
                    "completed": True,
                    "id": str(created_cohort.id),
                    "name": created_cohort.name,
                },
                "version": {
                    "completed": False,
                    "step": "create_initial_version",
                },
                "selected_session_ids": [
                    str(session_id) for session_id in selected_session_ids
                ],
                "cause": str(error),
            },
            hint=hint,
        ) from error
    links, warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=created_cohort.agent_id, cohort_id=created_cohort.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort was created."
        ),
    )
    return CommandResult(
        item={
            "cohort": created_cohort.model_dump(mode="json"),
            "version": version.model_dump(mode="json"),
            "reference": f"{created_cohort.name}@{version.version}",
            "session_count": len(selected_session_ids),
        },
        links=links,
        warnings=warnings,
    )


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
    links, warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=cohort.agent_id, cohort_id=cohort.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort read succeeded."
        ),
    )
    return CommandResult(
        item=cohort.model_dump(mode="json"),
        links=links,
        warnings=warnings,
    )


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
    links, warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=updated.agent_id, cohort_id=updated.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort was updated."
        ),
    )
    return CommandResult(
        item=updated.model_dump(mode="json"),
        links=links,
        warnings=warnings,
    )


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
    baseline_id: uuid.UUID | None = None,
    add_sessions_file: Path | None = None,
    remove_sessions_file: Path | None = None,
    allow_unchanged: bool = False,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create an immutable version from an ordered membership delta."""
    additions = list(add_session_ids or [])
    if add_sessions_file is not None:
        additions.extend(
            parse_session_uuid_file(add_sessions_file, option="--add-sessions-file")
        )
    removals = list(remove_session_ids or [])
    if remove_sessions_file is not None:
        removals.extend(
            parse_session_uuid_file(
                remove_sessions_file, option="--remove-sessions-file"
            )
        )
    if not additions and not removals and not allow_unchanged:
        raise CLIError(
            "invalid_arguments",
            "Select at least one --add-session or --remove-session, or use "
            "--allow-unchanged to copy existing membership.",
        )
    if len(set(additions)) != len(additions):
        option_label = (
            "--add-sessions-file"
            if add_sessions_file is not None and not add_session_ids
            else "--add-session"
        )
        raise CLIError(
            "invalid_arguments", f"Each {option_label} value must be unique."
        )
    if len(set(removals)) != len(removals):
        option_label = (
            "--remove-sessions-file"
            if remove_sessions_file is not None and not remove_session_ids
            else "--remove-session"
        )
        raise CLIError(
            "invalid_arguments", f"Each {option_label} value must be unique."
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
            baseline_id=baseline_id,
            add_session_ids=additions,
            remove_session_ids=removals,
            display_version=display_version,
        ),
        idempotency_key=idempotency_key,
    )
    warnings = []
    if not additions and not removals:
        if baseline_id is None:
            warnings.append(
                "No sessions were added or removed; cohort membership is unchanged."
            )
        else:
            warnings.append(
                "No sessions were added or removed; the new version copies "
                f"the membership of baseline {baseline_id}."
            )
    links, link_warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=cohort.agent_id, cohort_id=cohort.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort version was created."
        ),
    )
    return CommandResult(
        item=version.model_dump(mode="json"),
        links=links,
        warnings=[*warnings, *link_warnings],
    )


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
    return await resolve_cohort_version(client, reference)


async def get_cohort_version_result(client: Any, reference: str) -> CommandResult:
    """Get one exact cohort version."""
    cohort, version = await get_cohort_version(client, reference)
    links, warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=cohort.agent_id, cohort_id=cohort.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort version read succeeded."
        ),
    )
    return CommandResult(
        item=version.model_dump(mode="json"),
        links=links,
        warnings=warnings,
    )


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

    cohort, version = await get_cohort_version(client, reference)
    request = CohortVersionUpdateRequest(
        display_version=None if clear_display_version else display_version
    )
    updated = await client.cohort_versions.update(version.id, request)
    links, warnings = await resolve_dashboard_links(
        client,
        lambda base: {
            "dashboard": get_cohort_url(
                base, agent_id=cohort.agent_id, cohort_id=cohort.id
            )
        },
        warning=(
            "Could not resolve a dashboard link because the server info "
            "request failed. The cohort version was updated."
        ),
    )
    return CommandResult(
        item=updated.model_dump(mode="json"),
        links=links,
        warnings=warnings,
    )


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


def _build_initial_version_retry_hint(
    cohort_id: uuid.UUID,
    selected_session_ids: list[uuid.UUID],
    *,
    sessions_file: Path | None,
    display_version: str | None,
) -> str:
    """Build a bounded retry command for a failed initial cohort version."""
    retry_command = [
        "kitaru",
        "cohort",
        "version",
        "create",
        str(cohort_id),
    ]
    if sessions_file is not None:
        retry_command.extend(("--add-sessions-file", str(sessions_file)))
        if display_version is not None:
            retry_command.extend(("--display-version", display_version))
        return f"The cohort exists. Retry with `{shlex.join(retry_command)}`."
    if len(selected_session_ids) <= _INLINE_RETRY_SESSION_LIMIT:
        for session_id in selected_session_ids:
            retry_command.extend(("--add-session", str(session_id)))
        if display_version is not None:
            retry_command.extend(("--display-version", display_version))
        return f"The cohort exists. Retry with `{shlex.join(retry_command)}`."
    retry_command.extend(("--add-sessions-file", "<sessions-file>"))
    if display_version is not None:
        retry_command.extend(("--display-version", display_version))
    return (
        "The cohort exists. Save details.selected_session_ids (one UUID per line) "
        f"and retry with `{shlex.join(retry_command)}`."
    )
