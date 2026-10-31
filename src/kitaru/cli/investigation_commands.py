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
"""Insight, investigation, and annotation commands."""

import uuid
from pathlib import Path
from typing import Literal

from kitaru.api_models.v1.investigation import (
    InvestigationSessionVerdict,
    InvestigationStatus,
)
from kitaru.cli import annotations, insights, investigations, invocation_context
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    LIST_PARAMETERS,
    UUID_READ_ERRORS,
    annotation_app,
    build_spec,
    insight_app,
    investigation_app,
    investigation_session_app,
    register_command,
)
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec

_CURSOR_PARAMETERS = LIST_PARAMETERS[:2]
_INVESTIGATION_SESSION_VERDICT_PARAMETERS = (
    ParameterSpec("INVESTIGATION", "UUID", "argument", True, "Investigation ID."),
    ParameterSpec(
        "SESSION",
        "UUID",
        "argument",
        True,
        "Session ID linked to the investigation.",
    ),
    ParameterSpec(
        "VERDICT",
        "acceptable|problematic|uncertain",
        "argument",
        True,
        "Verdict for the investigation session.",
    ),
)


@register_command(
    insight_app,
    build_spec(
        ("insight", "create"),
        "Create a batch of insights for one agent.",
        parameters=(
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Agent UUID or case-sensitive name.",
            ),
            ParameterSpec(
                "--insight",
                "JSON[]",
                "option",
                False,
                "Insight JSON object; repeat for each created insight.",
            ),
            ParameterSpec(
                "--insight-file",
                "path",
                "option",
                False,
                "UTF-8 JSON file with one insight object or an array of them; "
                "replaces --insight.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def insight_create(
    *,
    agent: str,
    insight: list[str] | None = None,
    insight_file: Path | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create a batch of insights for one agent."""
    async with invocation_context.open_asset_client() as client:
        return await insights.create_insights(
            client,
            agent=agent,
            insight=insight,
            insight_file=insight_file,
            idempotency_key=idempotency_key,
        )


@register_command(
    insight_app,
    build_spec(
        ("insight", "list"),
        "List insights.",
        parameters=(
            *LIST_PARAMETERS,
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                False,
                "Only insights for this exact agent UUID or name.",
            ),
            ParameterSpec(
                "--name",
                "string",
                "option",
                False,
                "Only insights with this exact name.",
            ),
            ParameterSpec(
                "--type",
                "text|categorical|binned",
                "option",
                False,
                "Only insights with this data type.",
            ),
        ),
        errors=ASSET_READ_ERRORS,
    ),
)
async def insight_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
    agent: str | None = None,
    name: str | None = None,
    type: Literal["text", "categorical", "binned"] | None = None,
) -> CommandResult:
    """List one server page of insights."""
    async with invocation_context.open_asset_client() as client:
        return await insights.list_insights(
            client,
            size=size,
            cursor=cursor,
            sort=sort,
            filter=filter,
            agent=agent,
            name=name,
            type=type,
        )


@register_command(
    insight_app,
    build_spec(
        ("insight", "get"),
        "Get an insight by exact UUID.",
        parameters=(ParameterSpec("INSIGHT", "UUID", "argument", True, "Insight ID."),),
        errors=UUID_READ_ERRORS,
    ),
)
async def insight_get(insight: uuid.UUID, /) -> CommandResult:
    """Get one insight by UUID."""
    async with invocation_context.open_asset_client() as client:
        return await insights.get_insight(client, insight)


@register_command(
    insight_app,
    build_spec(
        ("insight", "update"),
        "Update selected insight fields.",
        parameters=(
            ParameterSpec("INSIGHT", "UUID", "argument", True, "Insight ID."),
            ParameterSpec("--title", "string", "option", False, "New title."),
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
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def insight_update(
    insight: uuid.UUID,
    /,
    *,
    title: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
) -> CommandResult:
    """Update selected fields on one insight."""
    async with invocation_context.open_asset_client() as client:
        return await insights.update_insight(
            client,
            insight,
            title=title,
            description=description,
            clear_description=clear_description,
        )


@register_command(
    insight_app,
    build_spec(
        ("insight", "delete"),
        "Delete an insight.",
        parameters=(
            ParameterSpec("INSIGHT", "UUID", "argument", True, "Insight ID."),
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
async def insight_delete(
    insight: uuid.UUID, /, *, force: bool = False
) -> CommandResult:
    """Delete one insight by UUID."""
    async with invocation_context.open_asset_client() as client:
        return await insights.delete_insight(client, insight, force=force)


@register_command(
    investigation_app,
    build_spec(
        ("investigation", "create"),
        "Create an investigation with linked sessions.",
        parameters=(
            ParameterSpec(
                "NAME", "string", "argument", True, "New investigation name."
            ),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Agent UUID or case-sensitive name.",
            ),
            ParameterSpec(
                "--description", "string", "option", False, "Curator rationale."
            ),
            ParameterSpec(
                "--session",
                "UUID[]",
                "option",
                False,
                "Ordered session UUID; repeat for each linked session.",
            ),
            ParameterSpec(
                "--session-question",
                "SESSION:KEY=QUESTION[]",
                "option",
                False,
                "Keyed question for a session selected with --session.",
            ),
            ParameterSpec(
                "--session-highlights",
                "SESSION:KEY=JSON_ARRAY[]",
                "option",
                False,
                "Curated highlights for a question added with --session-question.",
            ),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_remote_create",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def investigation_create(
    name: str,
    /,
    *,
    agent: str,
    description: str | None = None,
    session: list[uuid.UUID] | None = None,
    session_question: list[str] | None = None,
    session_highlights: list[str] | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create an investigation with its linked sessions."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.create_investigation(
            client,
            name,
            agent=agent,
            description=description,
            session_ids=session or [],
            session_questions=session_question or [],
            session_highlights=session_highlights or [],
            idempotency_key=idempotency_key,
        )


@register_command(
    investigation_app,
    build_spec(
        ("investigation", "list"),
        "List investigations.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def investigation_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of investigations."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.list_investigations(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    investigation_app,
    build_spec(
        ("investigation", "get"),
        "Get an investigation by exact UUID.",
        parameters=(
            ParameterSpec(
                "INVESTIGATION", "UUID", "argument", True, "Investigation ID."
            ),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def investigation_get(investigation: uuid.UUID, /) -> CommandResult:
    """Get one investigation by UUID."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.get_investigation(client, investigation)


@register_command(
    investigation_app,
    build_spec(
        ("investigation", "update"),
        "Update selected investigation fields.",
        parameters=(
            ParameterSpec(
                "INVESTIGATION", "UUID", "argument", True, "Investigation ID."
            ),
            ParameterSpec("--name", "string", "option", False, "New name."),
            ParameterSpec(
                "--description", "string", "option", False, "New curator rationale."
            ),
            ParameterSpec(
                "--clear-description",
                "boolean",
                "option",
                False,
                "Clear the curator rationale.",
            ),
            ParameterSpec(
                "--status",
                "in_progress|completed",
                "option",
                False,
                "New investigation status.",
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def investigation_update(
    investigation: uuid.UUID,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    clear_description: bool = False,
    status: InvestigationStatus | None = None,
) -> CommandResult:
    """Update selected fields on one investigation."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.update_investigation(
            client,
            investigation,
            name=name,
            description=description,
            clear_description=clear_description,
            status=status,
        )


@register_command(
    investigation_app,
    build_spec(
        ("investigation", "delete"),
        "Delete an investigation, its linked sessions, and its answers.",
        parameters=(
            ParameterSpec(
                "INVESTIGATION", "UUID", "argument", True, "Investigation ID."
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
async def investigation_delete(
    investigation: uuid.UUID, /, *, force: bool = False
) -> CommandResult:
    """Delete one investigation and its linked review state."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.delete_investigation(
            client, investigation, force=force
        )


@register_command(
    investigation_session_app,
    build_spec(
        ("investigation", "session", "list"),
        "List sessions linked to an investigation in presentation order.",
        parameters=(
            ParameterSpec(
                "INVESTIGATION", "UUID", "argument", True, "Investigation ID."
            ),
            *_CURSOR_PARAMETERS,
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def investigation_session_list(
    investigation: uuid.UUID,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
) -> CommandResult:
    """List one ordered page of an investigation's linked sessions."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.list_investigation_sessions(
            client, investigation, size=size, cursor=cursor
        )


@register_command(
    investigation_session_app,
    build_spec(
        ("investigation", "session", "verdict"),
        "Set an investigation session verdict.",
        parameters=_INVESTIGATION_SESSION_VERDICT_PARAMETERS,
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def investigation_session_verdict(
    investigation: uuid.UUID,
    session: uuid.UUID,
    verdict: InvestigationSessionVerdict,
    /,
) -> CommandResult:
    """Set one investigation session's verdict."""
    async with invocation_context.open_asset_client() as client:
        return await investigations.update_investigation_session_verdict(
            client,
            investigation,
            session,
            verdict=verdict,
        )


@register_command(
    annotation_app,
    build_spec(
        ("annotation", "create"),
        "Create a manual annotation or answer an investigation question.",
        parameters=(
            ParameterSpec(
                "--session",
                "UUID",
                "option",
                False,
                "Session ID for a manual annotation.",
            ),
            ParameterSpec(
                "--investigation-session",
                "UUID",
                "option",
                False,
                "Investigation-session ID for an answer.",
            ),
            ParameterSpec(
                "--question-key",
                "string",
                "option",
                False,
                "Key of the question being answered.",
            ),
            ParameterSpec(
                "--selector",
                "JSON object",
                "option",
                False,
                "Optional node, JSON Pointer, or span selector.",
            ),
            ParameterSpec("--value", "JSON value", "option", True, "Annotation value."),
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="manual_non_idempotent_answer_replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def annotation_create(
    *,
    value: str,
    session: uuid.UUID | None = None,
    investigation_session: uuid.UUID | None = None,
    question_key: str | None = None,
    selector: str | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one manual annotation or investigation answer."""
    async with invocation_context.open_asset_client() as client:
        return await annotations.create_annotation(
            client,
            value=value,
            session_id=session,
            investigation_session_id=investigation_session,
            question_key=question_key,
            selector=selector,
            idempotency_key=idempotency_key,
        )


@register_command(
    annotation_app,
    build_spec(
        ("annotation", "list"),
        "List annotations.",
        parameters=LIST_PARAMETERS,
        errors=ASSET_READ_ERRORS,
    ),
)
async def annotation_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of annotations."""
    async with invocation_context.open_asset_client() as client:
        return await annotations.list_annotations(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    annotation_app,
    build_spec(
        ("annotation", "get"),
        "Get an annotation by exact UUID.",
        parameters=(
            ParameterSpec("ANNOTATION", "UUID", "argument", True, "Annotation ID."),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def annotation_get(annotation: uuid.UUID, /) -> CommandResult:
    """Get one annotation by UUID."""
    async with invocation_context.open_asset_client() as client:
        return await annotations.get_annotation(client, annotation)


@register_command(
    annotation_app,
    build_spec(
        ("annotation", "update"),
        "Replace an annotation's JSON value.",
        parameters=(
            ParameterSpec("ANNOTATION", "UUID", "argument", True, "Annotation ID."),
            ParameterSpec(
                "--value", "JSON value", "option", True, "Replacement value."
            ),
        ),
        read_only=False,
        side_effects=("mutates_remote_state",),
        idempotency="idempotent replacement",
        errors=ASSET_WRITE_ERRORS,
    ),
)
async def annotation_update(annotation: uuid.UUID, /, *, value: str) -> CommandResult:
    """Replace one annotation's value."""
    async with invocation_context.open_asset_client() as client:
        return await annotations.update_annotation(client, annotation, value=value)


@register_command(
    annotation_app,
    build_spec(
        ("annotation", "delete"),
        "Delete an annotation.",
        parameters=(
            ParameterSpec("ANNOTATION", "UUID", "argument", True, "Annotation ID."),
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
async def annotation_delete(
    annotation: uuid.UUID, /, *, force: bool = False
) -> CommandResult:
    """Delete one annotation by UUID."""
    async with invocation_context.open_asset_client() as client:
        return await annotations.delete_annotation(client, annotation, force=force)
