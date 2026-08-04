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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Shared CLI selection of immutable session sets."""

import uuid
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.cohort import CohortResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli.output import CLIError
from kitaru.cli.registration import (
    list_params,
    parse_version_reference,
    resolve_asset,
)


def read_session_file(path: Path) -> list[tuple[int, str]]:
    """Read UTF-8 session UUID lines without accepting stdin or comments."""
    if str(path) == "-":
        raise CLIError(
            "invalid_arguments", "--sessions-file does not accept stdin ('-')."
        )
    if not path.is_file():
        raise CLIError(
            "invalid_arguments",
            "--sessions-file must be an existing regular UTF-8 text file.",
        )
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raise CLIError(
            "invalid_arguments", "--sessions-file must contain valid UTF-8 text."
        ) from None
    except OSError as error:
        reason = error.strerror or type(error).__name__
        raise CLIError(
            "invalid_arguments", f"--sessions-file could not be read: {reason}."
        ) from None
    return [
        (line_number, line.strip())
        for line_number, line in enumerate(content.splitlines(), start=1)
        if line.strip()
    ]


def parse_session_ids(values: list[str], sessions_file: Path | None) -> list[uuid.UUID]:
    """Parse and deduplicate positional and file-based session UUIDs."""
    sources = [(value, "SESSION") for value in values]
    if sessions_file is not None:
        sources.extend(
            (value, f"--sessions-file line {line_number}")
            for line_number, value in read_session_file(sessions_file)
        )
    if not sources:
        raise CLIError(
            "invalid_arguments",
            "Provide at least one SESSION or a nonempty --sessions-file.",
        )

    session_ids: list[uuid.UUID] = []
    seen: set[uuid.UUID] = set()
    for value, source in sources:
        try:
            session_id = uuid.UUID(value)
        except ValueError as error:
            raise CLIError(
                "invalid_arguments", f"{source} must contain an exact UUID."
            ) from error
        if session_id in seen:
            raise CLIError(
                "invalid_arguments",
                f"Session {session_id} was selected more than once.",
            )
        seen.add(session_id)
        session_ids.append(session_id)
    return session_ids


async def get_cohort_version(
    client: Any, reference: str
) -> tuple[CohortResponse, CohortVersionResponse]:
    """Resolve a cohort version by UUID or exact parent-version reference."""
    normalized = reference.strip()
    if not normalized:
        raise CLIError("invalid_arguments", "--cohort must not be empty.")
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


async def select_session_ids(
    client: Any,
    values: list[str] | None,
    sessions_file: Path | None,
    *,
    tag: str | None = None,
    agent: str | None = None,
    cohort: str | None = None,
    filter: str | None = None,
    all_sessions: bool = False,
) -> list[uuid.UUID]:
    """Resolve one explicit, tag, agent, cohort, filter, or all selection."""
    explicit = bool(values) or sessions_file is not None
    modes = sum(
        (
            int(explicit),
            int(tag is not None),
            int(agent is not None),
            int(cohort is not None),
            int(filter is not None),
            int(all_sessions),
        )
    )
    if modes != 1:
        raise CLIError(
            "invalid_arguments",
            "Select sessions using IDs/--sessions-file, --tag, --agent, "
            "--cohort, --filter, or --all.",
        )
    if explicit:
        return parse_session_ids(values or [], sessions_file)

    expression = None
    selection = "--all"
    if tag is not None:
        normalized = tag.strip()
        if not normalized:
            raise CLIError("invalid_arguments", "--tag must not be empty.")
        expression = FilterCondition(field="tag", op=FilterOp.EQ, value=normalized)
        selection = f"tag {normalized!r}"
    elif agent is not None:
        resolved = await resolve_asset(client.agents, agent, "Agent")
        expression = FilterCondition(
            field="agent_id", op=FilterOp.EQ, value=str(resolved.id)
        )
        selection = f"agent {resolved.name!r}"
    elif cohort is not None:
        _, version = await get_cohort_version(client, cohort)
        expression = FilterCondition(
            field="cohort_version_id", op=FilterOp.EQ, value=str(version.id)
        )
        selection = f"cohort {cohort!r}"
    elif filter is not None:
        params = list_params(
            "session", size=1000, cursor=None, sort="created:desc", filter=filter
        )
        assert isinstance(params, SessionListParams)
        expression = params.filter
        selection = "--filter"

    params = SessionListParams(filter=expression)
    session_ids = [session.id async for session in client.sessions.iter(params)]
    if not session_ids:
        raise CLIError(
            "invalid_arguments", f"No sessions matched the {selection} selection."
        )
    return session_ids
