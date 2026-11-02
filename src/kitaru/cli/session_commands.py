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
"""Session, import, and evaluation commands."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.cli import evaluations, imports, invocation_context, registration, sessions
from kitaru.cli.command_registry import (
    ASSET_READ_ERRORS,
    ASSET_WRITE_ERRORS,
    COLLECTION_READ_ERRORS,
    IDEMPOTENCY_KEY_PARAMETER,
    JOB_WAIT_ERRORS,
    LIST_PARAMETERS,
    UUID_READ_ERRORS,
    WAIT_PARAMETERS,
    build_spec,
    evaluation_app,
    import_app,
    register_command,
    session_app,
)
from kitaru.cli.output import CommandResult
from kitaru.cli.schema import ParameterSpec
from kitaru.session_transcript import DEFAULT_MAX_CHARS


@register_command(
    import_app,
    build_spec(
        ("import", "list"),
        "List imports.",
        parameters=LIST_PARAMETERS,
        errors=COLLECTION_READ_ERRORS,
    ),
)
async def import_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of imports."""
    async with invocation_context.open_asset_client() as client:
        return await imports.list_imports(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    import_app,
    build_spec(
        ("import", "get"),
        "Get one import by exact UUID.",
        parameters=(
            ParameterSpec("IMPORT_ID", "UUID", "argument", True, "Import ID."),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def import_get(import_id: uuid.UUID, /) -> CommandResult:
    """Get one import without remapping its status."""
    async with invocation_context.open_asset_client() as client:
        return await imports.get_import(client, import_id)


@register_command(
    import_app,
    build_spec(
        ("import", "analyze"),
        "Run analyzers over the sessions of an existing import as one job.",
        parameters=(
            ParameterSpec("IMPORT_ID", "UUID", "argument", True, "Import ID."),
            ParameterSpec(
                "--analyzer",
                "ANALYZER@VERSION[]",
                "option",
                True,
                "Exact analyzer version run once over the import's sessions.",
            ),
            ParameterSpec(
                "--analyzer-params",
                "ANALYZER@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected analyzer token.",
            ),
            ParameterSpec(
                "--analyzer-connection",
                "ANALYZER@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected analyzer token.",
            ),
            *WAIT_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("creates_remote_state",),
        idempotency="non_idempotent_job_created_per_request",
        errors=(
            *ASSET_READ_ERRORS,
            *JOB_WAIT_ERRORS,
        ),
        streams=True,
    ),
)
async def import_analyze(
    import_id: uuid.UUID,
    /,
    *,
    analyzer: list[str],
    analyzer_params: list[str] | None = None,
    analyzer_connection: list[str] | None = None,
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create one analysis job over the sessions of an existing import."""
    async with invocation_context.open_asset_client() as client:
        return await imports.analyze_import(
            client,
            import_id,
            analyzers=analyzer,
            analyzer_params=analyzer_params,
            analyzer_connections=analyzer_connection,
            wait=wait,
            interval=interval,
            timeout=timeout,
            idempotency_key=idempotency_key,
        )


@register_command(
    session_app,
    build_spec(
        ("session", "import"),
        "Upload a local payload or fetch from a provider API, and create "
        "an import job.",
        parameters=(
            ParameterSpec(
                "FILE",
                "path",
                "argument",
                False,
                "Local payload file. Omit for an API import.",
            ),
            ParameterSpec(
                "--importer",
                "reference",
                "option",
                True,
                "Exact IMPORTER@VERSION reference.",
            ),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                True,
                "Exact AGENT@VERSION reference.",
            ),
            ParameterSpec(
                "--params", "JSON object", "option", False, "Importer parameters."
            ),
            ParameterSpec(
                "--join-on",
                "JSON Pointer",
                "option",
                False,
                "Group source traces by the value at this RFC 6901 JSON Pointer.",
            ),
            ParameterSpec(
                "--since",
                "timestamp or duration",
                "option",
                False,
                "For an API import, fetch traces at or after this ISO 8601 "
                "timestamp or relative duration such as 7d, 12h, or 30m.",
            ),
            ParameterSpec(
                "--until",
                "timestamp or duration",
                "option",
                False,
                "For an API import, fetch traces before this ISO 8601 "
                "timestamp or relative duration such as 7d, 12h, or 30m.",
            ),
            ParameterSpec(
                "--trace-id",
                "text[]",
                "option",
                False,
                "For an API import, fetch exactly these provider trace ids.",
            ),
            ParameterSpec(
                "--query",
                "JSON object",
                "option",
                False,
                "Additional importer-defined API selection fields, merged "
                "with --since, --until, and --trace-id.",
            ),
            ParameterSpec(
                "--connection",
                "reference",
                "option",
                False,
                "For an API import, the connection UUID or name supplying "
                "provider credentials.",
            ),
            ParameterSpec(
                "--tag",
                "text[]",
                "option",
                False,
                "Tag every session created by this import; requires --wait.",
            ),
            ParameterSpec(
                "--evaluator",
                "EVALUATOR@VERSION[]",
                "option",
                False,
                "Exact evaluator version scored against every imported session.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            ParameterSpec(
                "--evaluator-connection",
                "EVALUATOR@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected evaluator token.",
            ),
            ParameterSpec(
                "--analyzer",
                "ANALYZER@VERSION[]",
                "option",
                False,
                "Exact analyzer version run once over every imported session.",
            ),
            ParameterSpec(
                "--analyzer-params",
                "ANALYZER@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected analyzer token.",
            ),
            ParameterSpec(
                "--analyzer-connection",
                "ANALYZER@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected analyzer token.",
            ),
            ParameterSpec(
                "--media-type",
                "string",
                "option",
                False,
                "Payload media type.",
            ),
            ParameterSpec(
                "--max-upload-mib",
                "positive integer",
                "option",
                False,
                "Client-side upload size limit in MiB.",
            ),
            ParameterSpec(
                "--max-sessions",
                "integer",
                "option",
                False,
                "Stop the import after creating this many sessions.",
            ),
            *WAIT_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("uploads_data", "creates_remote_state"),
        idempotency="blob_content_deduped_import_job_created_per_request",
        errors=(
            *ASSET_WRITE_ERRORS,
            *JOB_WAIT_ERRORS,
        ),
        streams=True,
    ),
)
async def session_import(
    file: Path | None = None,
    /,
    *,
    importer: str,
    agent: str,
    params: str | None = None,
    join_on: str | None = None,
    since: str | None = None,
    until: str | None = None,
    trace_id: list[str] | None = None,
    query: str | None = None,
    connection: str | None = None,
    tag: list[str] | None = None,
    evaluator: list[str] | None = None,
    evaluator_params: list[str] | None = None,
    evaluator_connection: list[str] | None = None,
    analyzer: list[str] | None = None,
    analyzer_params: list[str] | None = None,
    analyzer_connection: list[str] | None = None,
    media_type: str | None = None,
    max_upload_mib: int | None = None,
    max_sessions: int | None = None,
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Upload a local payload or an API selection, then create one import job."""
    async with invocation_context.open_asset_client() as client:
        return await sessions.import_sessions(
            client,
            file,
            importer=importer,
            agent=agent,
            params=params,
            join_on=join_on,
            since=since,
            until=until,
            trace_ids=trace_id,
            query=query,
            connection=connection,
            tags=tag,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            evaluator_connections=evaluator_connection,
            analyzers=analyzer,
            analyzer_params=analyzer_params,
            analyzer_connections=analyzer_connection,
            media_type=media_type,
            max_upload_mib=max_upload_mib,
            max_sessions=max_sessions,
            wait=wait,
            interval=interval,
            timeout=timeout,
            idempotency_key=idempotency_key,
        )


@register_command(
    session_app,
    build_spec(
        ("session", "list"),
        "List sessions.",
        parameters=(
            *LIST_PARAMETERS,
            ParameterSpec(
                "--status",
                "in_progress|completed|failed",
                "option",
                False,
                "Only sessions with this status.",
            ),
            ParameterSpec(
                "--agent",
                "reference",
                "option",
                False,
                "Only sessions for this exact agent UUID or name.",
            ),
            ParameterSpec(
                "--origin",
                "imported|recorded|replay",
                "option",
                False,
                "Only sessions with this origin.",
            ),
            ParameterSpec(
                "--imported-from",
                "string",
                "option",
                False,
                "Only sessions imported from this exact source system.",
            ),
            ParameterSpec(
                "--tag", "text", "option", False, "Only sessions with this tag."
            ),
            ParameterSpec(
                "--cohort",
                "reference",
                "option",
                False,
                "Only sessions in this cohort version.",
            ),
            ParameterSpec(
                "--started-after",
                "timestamp",
                "option",
                False,
                "Only sessions started at or after this ISO 8601 timestamp.",
            ),
            ParameterSpec(
                "--started-before",
                "timestamp",
                "option",
                False,
                "Only sessions started before this ISO 8601 timestamp.",
            ),
            ParameterSpec(
                "--include-payloads",
                "boolean",
                "option",
                False,
                "Include session inputs and outputs.",
            ),
        ),
        errors=COLLECTION_READ_ERRORS,
    ),
)
async def session_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
    status: SessionStatus | None = None,
    agent: str | None = None,
    origin: SessionOrigin | None = None,
    imported_from: str | None = None,
    tag: str | None = None,
    cohort: str | None = None,
    started_after: datetime | None = None,
    started_before: datetime | None = None,
    include_payloads: bool = False,
) -> CommandResult:
    """List one server page of sessions."""
    registration.list_params(
        "session", size=size, cursor=cursor, sort=sort, filter=filter
    )
    async with invocation_context.open_asset_client() as client:
        return await sessions.list_sessions(
            client,
            size=size,
            cursor=cursor,
            sort=sort,
            filter=filter,
            status=status,
            agent=agent,
            origin=origin,
            imported_from=imported_from,
            tag=tag,
            cohort=cohort,
            started_after=started_after,
            started_before=started_before,
            include_payloads=include_payloads,
        )


@register_command(
    session_app,
    build_spec(
        ("session", "get"),
        "Get a session by exact UUID.",
        parameters=(ParameterSpec("SESSION", "UUID", "argument", True, "Session ID."),),
        errors=UUID_READ_ERRORS,
    ),
)
async def session_get(session: uuid.UUID, /) -> CommandResult:
    """Get one session by exact UUID."""
    async with invocation_context.open_asset_client() as client:
        return await sessions.get_session(client, session)


@register_command(
    session_app,
    build_spec(
        ("session", "nodes"),
        "List a session's nodes in index order.",
        parameters=(
            ParameterSpec("SESSION", "UUID", "argument", True, "Session ID."),
            ParameterSpec("--size", "integer", "option", False, "Items per page."),
            ParameterSpec("--cursor", "string", "option", False, "Page cursor."),
            ParameterSpec(
                "--filter",
                "JSON",
                "option",
                False,
                "Filter expression as a JSON object.",
            ),
            ParameterSpec(
                "--include-payloads",
                "boolean",
                "option",
                False,
                "Include node inputs, outputs, and attributes.",
            ),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def session_nodes(
    session: uuid.UUID,
    /,
    *,
    size: int = 20,
    cursor: str | None = None,
    include_payloads: bool = False,
    filter: str | None = None,
) -> CommandResult:
    """List one server page of a session's nodes."""
    async with invocation_context.open_asset_client() as client:
        return await sessions.list_session_nodes(
            client,
            session,
            size=size,
            cursor=cursor,
            include_payloads=include_payloads,
            filter=filter,
        )


@register_command(
    session_app,
    build_spec(
        ("session", "read"),
        "Read sessions as compact transcripts, one node per line.",
        parameters=(
            ParameterSpec(
                "SESSION", "UUID[]", "argument", False, "Explicit session IDs."
            ),
            ParameterSpec(
                "--sessions-file",
                "path",
                "option",
                False,
                "UTF-8 file with one session UUID per nonblank line.",
            ),
            ParameterSpec(
                "--max-chars",
                "integer",
                "option",
                False,
                "Longest text kept per field before it is cut.",
            ),
            ParameterSpec(
                "--full", "boolean", "option", False, "Keep every field uncut."
            ),
            ParameterSpec(
                "--reasoning",
                "boolean",
                "option",
                False,
                "Include the visible reasoning of model calls.",
            ),
            ParameterSpec(
                "--node",
                "integer",
                "option",
                False,
                "Show only the node at this zero-based position.",
            ),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def session_read(
    session: list[str] | None = None,
    /,
    *,
    sessions_file: Path | None = None,
    max_chars: int = DEFAULT_MAX_CHARS,
    full: bool = False,
    reasoning: bool = False,
    node: int | None = None,
) -> CommandResult:
    """Read sessions as compact transcripts, one node per line."""
    async with invocation_context.open_asset_client() as client:
        return await sessions.read_sessions(
            client,
            session or [],
            sessions_file=sessions_file,
            max_chars=None if full else max_chars,
            include_reasoning=reasoning,
            node=node,
        )


@register_command(
    session_app,
    build_spec(
        ("session", "evaluate"),
        "Evaluate sessions selected by ID, tag, or all sessions.",
        parameters=(
            ParameterSpec(
                "SESSION", "UUID[]", "argument", False, "Explicit session IDs."
            ),
            ParameterSpec(
                "--sessions-file",
                "path",
                "option",
                False,
                "UTF-8 file with one session UUID per nonblank line.",
            ),
            ParameterSpec(
                "--tag", "text", "option", False, "Evaluate sessions with this tag."
            ),
            ParameterSpec(
                "--agent", "reference", "option", False, "Evaluate an agent's sessions."
            ),
            ParameterSpec(
                "--cohort",
                "reference",
                "option",
                False,
                "Evaluate sessions in this cohort version.",
            ),
            ParameterSpec(
                "--filter",
                "JSON filter",
                "option",
                False,
                "Evaluate matching sessions.",
            ),
            ParameterSpec(
                "--all", "boolean", "option", False, "Evaluate all sessions."
            ),
            ParameterSpec(
                "--evaluator",
                "reference[]",
                "option",
                True,
                "Exact EVALUATOR@VERSION references.",
            ),
            ParameterSpec(
                "--evaluator-params",
                "EVALUATOR@VERSION=JSON_OBJECT[]",
                "option",
                False,
                "Parameters for a selected evaluator token.",
            ),
            ParameterSpec(
                "--evaluator-connection",
                "EVALUATOR@VERSION=CONNECTION[]",
                "option",
                False,
                "Connection for a selected evaluator token.",
            ),
            *WAIT_PARAMETERS,
            IDEMPOTENCY_KEY_PARAMETER,
        ),
        read_only=False,
        side_effects=("reads_local_file", "creates_remote_state"),
        idempotency="non_idempotent_job_created_per_request",
        errors=(
            *ASSET_READ_ERRORS,
            *JOB_WAIT_ERRORS,
        ),
        streams=True,
    ),
)
async def session_evaluate(
    session: list[str] | None = None,
    /,
    *,
    sessions_file: Path | None = None,
    tag: str | None = None,
    agent: str | None = None,
    cohort: str | None = None,
    filter: str | None = None,
    all_sessions: Annotated[
        bool, Parameter(name="--all", help="Evaluate all sessions.")
    ] = False,
    evaluator: list[str],
    evaluator_params: list[str] | None = None,
    evaluator_connection: list[str] | None = None,
    wait: bool = False,
    interval: float | None = None,
    timeout: float | None = None,
    idempotency_key: str | None = None,
) -> CommandResult:
    """Evaluate sessions selected by ID, tag, or all sessions."""
    async with invocation_context.open_asset_client() as client:
        return await evaluations.evaluate_sessions(
            client,
            session,
            sessions_file=sessions_file,
            tag=tag,
            agent=agent,
            cohort=cohort,
            filter=filter,
            all_sessions=all_sessions,
            evaluators=evaluator,
            evaluator_params=evaluator_params,
            evaluator_connections=evaluator_connection,
            wait=wait,
            interval=interval,
            timeout=timeout,
            idempotency_key=idempotency_key,
        )


@register_command(
    evaluation_app,
    build_spec(
        ("evaluation", "list"),
        "List stored evaluations.",
        parameters=LIST_PARAMETERS,
        errors=COLLECTION_READ_ERRORS,
    ),
)
async def evaluation_list(
    *,
    size: int = 20,
    cursor: str | None = None,
    sort: str = "created:desc",
    filter: str | None = None,
) -> CommandResult:
    """List one server page of stored evaluations."""
    async with invocation_context.open_asset_client() as client:
        return await evaluations.list_evaluations(
            client, size=size, cursor=cursor, sort=sort, filter=filter
        )


@register_command(
    evaluation_app,
    build_spec(
        ("evaluation", "get"),
        "Get a stored evaluation by exact UUID.",
        parameters=(
            ParameterSpec("EVALUATION", "UUID", "argument", True, "Evaluation ID."),
        ),
        errors=UUID_READ_ERRORS,
    ),
)
async def evaluation_get(evaluation: uuid.UUID, /) -> CommandResult:
    """Get one stored evaluation by exact UUID."""
    async with invocation_context.open_asset_client() as client:
        return await evaluations.get_evaluation(client, evaluation)
