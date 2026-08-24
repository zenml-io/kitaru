#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Investigation and linked-session CLI commands."""

import json
import uuid
from typing import Any

import httpx

from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationSessionHighlight,
    InvestigationSessionInput,
    InvestigationSessionQuestion,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationSessionVerdict,
    InvestigationStatus,
    InvestigationUpdateRequest,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import list_params, page_result, resolve_asset
from kitaru.client.dashboard_urls import get_investigation_review_url
from kitaru.client.exceptions import APIError


def _parse_session_key_token(token: str, *, option: str) -> tuple[uuid.UUID, str]:
    """Parse a ``SESSION:KEY`` token into a session UUID and question key."""
    session_token, separator, key = token.partition(":")
    if not separator or not session_token or not key:
        raise CLIError("invalid_arguments", f"{option} must start with SESSION:KEY.")
    try:
        session_id = uuid.UUID(session_token)
    except ValueError as error:
        raise CLIError(
            "invalid_arguments", f"{option} must start with a valid session UUID."
        ) from error
    return session_id, key


def _parse_session_questions(
    entries: list[str], selected_session_ids: list[uuid.UUID]
) -> dict[uuid.UUID, dict[str, str]]:
    """Parse ``SESSION:KEY=QUESTION`` questions for selected sessions."""
    selected = set(selected_session_ids)
    questions: dict[uuid.UUID, dict[str, str]] = {}
    for entry in entries:
        token, separator, question = entry.partition("=")
        if not separator:
            raise CLIError(
                "invalid_arguments",
                "--session-question must be SESSION:KEY=QUESTION.",
            )
        session_id, key = _parse_session_key_token(token, option="--session-question")
        if session_id not in selected:
            raise CLIError(
                "invalid_arguments",
                f"Session {session_id} from --session-question must also be "
                "selected with --session.",
            )
        session_questions = questions.setdefault(session_id, {})
        if key in session_questions:
            raise CLIError(
                "invalid_arguments",
                f"Each --session-question key must be unique per session; "
                f"repeated {key} for session {session_id}.",
            )
        session_questions[key] = question
    return questions


def _parse_highlights(
    value: str, *, option: str
) -> list[InvestigationSessionHighlight]:
    """Parse a JSON array of highlight objects."""
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise CLIError(
            "invalid_arguments", f"{option} is not valid JSON: {error}"
        ) from error
    if not isinstance(parsed, list):
        raise CLIError("invalid_arguments", f"{option} must contain a JSON array.")
    return [InvestigationSessionHighlight.model_validate(item) for item in parsed]


def _parse_session_highlights(
    entries: list[str], selected_session_ids: list[uuid.UUID]
) -> dict[uuid.UUID, dict[str, list[InvestigationSessionHighlight]]]:
    """Parse ``SESSION:KEY=JSON_ARRAY`` highlights for selected sessions."""
    selected = set(selected_session_ids)
    highlights: dict[uuid.UUID, dict[str, list[InvestigationSessionHighlight]]] = {}
    for entry in entries:
        token, separator, payload = entry.partition("=")
        if not separator:
            raise CLIError(
                "invalid_arguments",
                "--session-highlights must be SESSION:KEY=JSON_ARRAY.",
            )
        session_id, key = _parse_session_key_token(token, option="--session-highlights")
        if session_id not in selected:
            raise CLIError(
                "invalid_arguments",
                f"Session {session_id} from --session-highlights must also be "
                "selected with --session.",
            )
        session_highlights = highlights.setdefault(session_id, {})
        if key in session_highlights:
            raise CLIError(
                "invalid_arguments",
                f"Each --session-highlights key must be unique per session; "
                f"repeated {key} for session {session_id}.",
            )
        session_highlights[key] = _parse_highlights(
            payload, option="--session-highlights"
        )
    return highlights


async def create_investigation(
    client: Any,
    name: str,
    *,
    agent: str,
    description: str | None,
    session_ids: list[uuid.UUID],
    session_questions: list[str],
    session_highlights: list[str],
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create an investigation with linked sessions."""
    if len(set(session_ids)) != len(session_ids):
        raise CLIError("invalid_arguments", "Each --session value must be unique.")
    parsed_questions = _parse_session_questions(session_questions, session_ids)
    parsed_highlights = _parse_session_highlights(session_highlights, session_ids)
    for session_id, keyed_highlights in parsed_highlights.items():
        unmatched = set(keyed_highlights) - set(parsed_questions.get(session_id, {}))
        if unmatched:
            raise CLIError(
                "invalid_arguments",
                f"--session-highlights key {sorted(unmatched)[0]!r} for session "
                f"{session_id} has no matching --session-question.",
            )
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    sessions = []
    for session_id in session_ids:
        keyed_questions = parsed_questions.get(session_id, {})
        keyed_highlights = parsed_highlights.get(session_id, {})
        questions = []
        for key, question in keyed_questions.items():
            question_fields: dict[str, Any] = {"key": key, "question": question}
            if key in keyed_highlights:
                question_fields["highlights"] = keyed_highlights[key]
            questions.append(InvestigationSessionQuestion(**question_fields))
        sessions.append(
            InvestigationSessionInput(session_id=session_id, questions=questions)
        )
    investigation = await client.investigations.create(
        InvestigationCreateRequest(
            agent_id=resolved_agent.id,
            name=name,
            description=description,
            sessions=sessions,
        ),
        idempotency_key=idempotency_key,
    )
    review_url: str | None = None
    warnings: list[str] = []
    try:
        info = await client.info.get()
    # ValueError covers malformed info payloads: JSON decoding and Pydantic
    # validation errors both derive from it, and a version-skewed server must
    # not fail a create that already succeeded.
    except (APIError, httpx.HTTPError, ValueError):
        warnings.append(
            "Could not resolve a dashboard review link because the server "
            "info request failed. The investigation was created."
        )
    else:
        review_url = get_investigation_review_url(
            info,
            client.base_url,
            agent_id=investigation.agent_id,
            investigation_id=investigation.id,
        )
    return CommandResult(
        item=investigation.model_dump(mode="json"),
        links={"review": review_url} if review_url else {},
        warnings=warnings,
        next_actions=[
            f"kitaru investigation session list {investigation.id}",
            f"kitaru investigation get {investigation.id}",
        ],
    )


async def list_investigations(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
) -> CommandResult:
    """List one server page of investigations."""
    params = list_params(
        "investigation", size=size, cursor=cursor, sort=sort, filter=filter
    )
    return page_result(await client.investigations.list(params), size=size)


async def get_investigation(client: Any, investigation_id: uuid.UUID) -> CommandResult:
    """Get one investigation by UUID."""
    investigation = await client.investigations.get(investigation_id)
    return CommandResult(item=investigation.model_dump(mode="json"))


async def update_investigation(
    client: Any,
    investigation_id: uuid.UUID,
    *,
    name: str | None,
    description: str | None,
    clear_description: bool,
    status: InvestigationStatus | None,
) -> CommandResult:
    """Update only explicitly selected investigation fields."""
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
    if status is not None:
        fields["status"] = status
    if not fields:
        raise CLIError("invalid_arguments", "Select at least one investigation update.")
    investigation = await client.investigations.update(
        investigation_id, InvestigationUpdateRequest(**fields)
    )
    return CommandResult(item=investigation.model_dump(mode="json"))


async def delete_investigation(
    client: Any, investigation_id: uuid.UUID, *, force: bool
) -> CommandResult:
    """Delete an investigation and its linked sessions and answers."""
    if not force:
        raise CLIError(
            "invalid_arguments",
            "Deleting an investigation, its linked sessions, and answers requires "
            "--force.",
        )
    await client.investigations.delete(investigation_id)
    return CommandResult(item={"id": str(investigation_id), "deleted": True})


async def list_investigation_sessions(
    client: Any,
    investigation_id: uuid.UUID,
    *,
    size: int,
    cursor: str | None,
) -> CommandResult:
    """List one ordered page of sessions linked to an investigation."""
    params = InvestigationSessionsListParams(size=size, cursor=cursor)
    page = await client.investigations.list_sessions(investigation_id, params)
    return page_result(page, size=size)


async def update_investigation_session_verdict(
    client: Any,
    investigation_id: uuid.UUID,
    session_id: uuid.UUID,
    *,
    verdict: InvestigationSessionVerdict,
) -> CommandResult:
    """Set one investigation session's verdict."""
    session = await client.investigations.update_session(
        investigation_id,
        session_id,
        InvestigationSessionUpdateRequest(verdict=verdict),
    )
    return CommandResult(
        item=session.model_dump(mode="json"),
        next_actions=[f"kitaru investigation get {investigation_id}"],
    )
