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
"""Cohort and cohort version commands."""

import uuid
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from kitaru.cli import cohorts, invocation_context
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    LIST_PARAMETERS,
    VERSION_LIST_PARAMETERS,
    build_spec,
    cohort_app,
    cohort_version_app,
    register_command,
)
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec


@register_command(
    cohort_app,
    build_spec(
        ("cohort", "create"),
        "Create a cohort and optionally snapshot selected sessions.",
        parameters=(
            ParameterSpec("NAME", "string", "argument", True, "New cohort name."),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Exact agent UUID or case-sensitive name.",
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Cohort description."
            ),
            ParameterSpec(
                "--metadata", "JSON object", "option", False, "Cohort metadata."
            ),
            ParameterSpec(
                "--session", "UUID[]", "option", False, "Explicit session IDs."
            ),
            ParameterSpec(
                "--sessions-file",
                "path",
                "option",
                False,
                "UTF-8 file with one session UUID per nonblank line.",
            ),
            ParameterSpec("--tag", "text", "option", False, "Sessions with this tag."),
            ParameterSpec(
                "--cohort",
                "reference",
                "option",
                False,
                "Sessions in this cohort version.",
            ),
            ParameterSpec(
                "--filter", "JSON filter", "option", False, "Session filter expression."
            ),
            ParameterSpec("--all", "boolean", "option", False, "All sessions."),
            ParameterSpec(
                "--display-version",
                "string",
                "option",
                False,
                "Display version for the membership snapshot.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def cohort_create(
    name: str,
    /,
    *,
    agent: str,
    description: str | None = None,
    metadata: str | None = None,
    session: list[str] | None = None,
    sessions_file: Path | None = None,
    tag: str | None = None,
    cohort: str | None = None,
    filter: str | None = None,
    all_sessions: Annotated[
        bool, Parameter(name="--all", help="Select all sessions.")
    ] = False,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create a cohort and optionally snapshot selected sessions."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.create_cohort(
            client,
            name,
            agent=agent,
            description=description,
            metadata=metadata,
            sessions=session,
            sessions_file=sessions_file,
            tag=tag,
            cohort=cohort,
            filter=filter,
            all_sessions=all_sessions,
            display_version=display_version,
            idempotency_key=idempotency_key,
        )


@register_command(
    cohort_app,
    build_spec(
        ("cohort", "list"),
        "List cohorts.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def cohort_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of cohorts."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.list_cohorts(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    cohort_app,
    build_spec(
        ("cohort", "get"),
        "Get a cohort by exact UUID or case-sensitive name.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def cohort_get(cohort: str, /) -> CommandResult:
    """Get one exact cohort."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.get_cohort(client, cohort)


@register_command(
    cohort_app,
    build_spec(
        ("cohort", "update"),
        "Update selected fields on an exact cohort.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec("--name", "string", "option", False, "New cohort name."),
            ParameterSpec(
                "--description", "string", "option", False, "New description."
            ),
            ParameterSpec(
                "--clear-description",
                "boolean",
                "option",
                False,
                "Clear the description.",
            ),
            ParameterSpec(
                "--metadata",
                "JSON object",
                "option",
                False,
                "Replacement metadata; {} clears it.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def cohort_update(
    cohort: str,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
    metadata: str | None = None,
) -> CommandResult:
    """Update selected fields on one exact cohort."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.update_cohort(
            client,
            cohort,
            name=name,
            description=description,
            clear_description=clear_description,
            metadata=metadata,
        )


@register_command(
    cohort_app,
    build_spec(
        ("cohort", "delete"),
        "Delete a cohort and all of its versions.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec(
                "--force",
                "boolean",
                "option",
                False,
                "Confirm cascading remote deletion.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state", "deletes_remote_state"),
        idempotency="not_found after first removal",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def cohort_delete(cohort: str, /, *, force: bool = False) -> CommandResult:
    """Delete one cohort and all of its versions."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.delete_cohort(client, cohort, force=force)


@register_command(
    cohort_version_app,
    build_spec(
        ("cohort", "version", "create"),
        "Create the next immutable version from a membership delta.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            ParameterSpec(
                "--add-session",
                "UUID[]",
                "option",
                False,
                "Ordered session IDs to add.",
            ),
            ParameterSpec(
                "--remove-session",
                "UUID[]",
                "option",
                False,
                "Ordered session IDs to remove.",
            ),
            ParameterSpec(
                "--baseline",
                "UUID",
                "option",
                False,
                "Exact cohort version the membership delta applies to.",
            ),
            ParameterSpec(
                "--display-version",
                "string",
                "option",
                False,
                "Human-readable version.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_server_assigned_version",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def cohort_version_create(
    cohort: str,
    /,
    *,
    add_session: list[uuid.UUID] | None = None,
    remove_session: list[uuid.UUID] | None = None,
    baseline: uuid.UUID | None = None,
    display_version: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create an immutable version from an ordered membership delta."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.create_cohort_version(
            client,
            cohort,
            add_session_ids=add_session,
            remove_session_ids=remove_session,
            baseline_id=baseline,
            display_version=display_version,
            idempotency_key=idempotency_key,
        )


@register_command(
    cohort_version_app,
    build_spec(
        ("cohort", "version", "list"),
        "List immutable versions of an exact cohort.",
        parameters=(
            ParameterSpec(
                "COHORT", "reference", "argument", True, "Cohort UUID or name."
            ),
            *VERSION_LIST_PARAMETERS,
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def cohort_version_list(
    cohort: str,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
) -> CommandResult:
    """List one server page of immutable cohort versions."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.list_cohort_versions(
            client, cohort, size=size, cursor=cursor, sort=sort
        )


@register_command(
    cohort_version_app,
    build_spec(
        ("cohort", "version", "get"),
        "Get a version by UUID or exact COHORT@VERSION reference.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def cohort_version_get(version: str, /) -> CommandResult:
    """Get one exact immutable cohort version."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.get_cohort_version_result(client, version)


@register_command(
    cohort_version_app,
    build_spec(
        ("cohort", "version", "update"),
        "Update a cohort version's display version.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
            ),
            ParameterSpec(
                "--display-version",
                "string",
                "option",
                False,
                "New human-readable version.",
            ),
            ParameterSpec(
                "--clear-display-version",
                "boolean",
                "option",
                False,
                "Clear the human-readable version.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def cohort_version_update(
    version: str,
    /,
    *,
    display_version: str | None = None,
    clear_display_version: bool = False,
) -> CommandResult:
    """Update one cohort version's display version."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.update_cohort_version(
            client,
            version,
            display_version=display_version,
            clear_display_version=clear_display_version,
        )


@register_command(
    cohort_version_app,
    build_spec(
        ("cohort", "version", "delete"),
        "Delete one immutable cohort version without reusing its number.",
        parameters=(
            ParameterSpec(
                "VERSION",
                "UUID|COHORT@VERSION",
                "argument",
                True,
                "Cohort-version UUID or exact server-assigned version.",
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
async def cohort_version_delete(
    version: str, /, *, force: bool = False
) -> CommandResult:
    """Delete one exact immutable cohort version."""
    async with invocation_context.open_asset_client() as client:
        return await cohorts.delete_cohort_version(client, version, force=force)
