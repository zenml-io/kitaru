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
from datetime import datetime
from typing import Any

import httpx

from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
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

_INVESTIGATION_LOOKUP_LIMIT = 10


def _describe_investigation_match(item: Any) -> tuple[dict[str, str], str]:
    """Build structured details and a concise label for one investigation match."""
    item_id = str(item.id)
    summary: dict[str, str] = {"id": item_id}
    parts = [item_id]
    agent_id = getattr(item, "agent_id", None)
    if agent_id is not None:
        summary["agent_id"] = str(agent_id)
        parts.append(f"agent={agent_id}")
    created = getattr(item, "created", None)
    if created is not None:
        created_text = (
            created.isoformat() if isinstance(created, datetime) else str(created)
        )
        summary["created"] = created_text
        parts.append(f"created={created_text}")
    return summary, " ".join(parts)


def _parse_investigation_reference(
    reference: str | uuid.UUID,
) -> tuple[str, uuid.UUID | None]:
    """Normalize an investigation reference into text and an optional UUID."""
    if isinstance(reference, uuid.UUID):
        return str(reference), reference
    normalized = reference.strip()
    if not normalized:
        raise CLIError("invalid_arguments", "Investigation reference cannot be blank.")
    try:
        return normalized, uuid.UUID(normalized)
    except ValueError:
        return normalized, None


async def _resolve_investigation(
    client: Any,
    reference: str | uuid.UUID,
    *,
    agent: str | None = None,
) -> Any:
    """Resolve one investigation by exact UUID or unambiguous case-sensitive name."""
    normalized, item_id = _parse_investigation_reference(reference)
    resolved_agent = (
        await resolve_asset(client.agents, agent, "Agent")
        if agent is not None
        else None
    )
    if item_id is not None:
        investigation = await client.investigations.get(item_id)
        if resolved_agent is not None and str(investigation.agent_id) != str(
            resolved_agent.id
        ):
            raise CLIError(
                "not_found",
                f"Investigation {item_id} was not found for agent "
                f"{resolved_agent.name!r}.",
            )
        return investigation

    name_condition = FilterCondition(field="name", op=FilterOp.EQ, value=normalized)
    if resolved_agent is not None:
        filter_expression: Any = AndFilter.model_validate(
            {
                "and": [
                    name_condition,
                    FilterCondition(
                        field="agent_id",
                        op=FilterOp.EQ,
                        value=str(resolved_agent.id),
                    ),
                ]
            }
        )
    else:
        filter_expression = name_condition
    page = await client.investigations.list(
        InvestigationListParams(
            size=_INVESTIGATION_LOOKUP_LIMIT,
            filter=filter_expression,
        )
    )
    matches = [item for item in page.items if item.name == normalized]
    if resolved_agent is not None:
        matches = [
            item for item in matches if str(item.agent_id) == str(resolved_agent.id)
        ]
    scope = f" for agent {resolved_agent.name!r}" if resolved_agent is not None else ""
    if not matches:
        raise CLIError(
            "not_found", f"Investigation {normalized!r} was not found{scope}."
        )
    if len(matches) > 1:
        described = [_describe_investigation_match(item) for item in matches]
        summaries = [summary for summary, _ in described]
        labels = [label for _, label in described]
        truncated = page.next_cursor is not None
        suffix = "; more matches exist" if truncated else ""
        hint = (
            "Pass the exact investigation UUID or narrow the lookup with --agent."
            if resolved_agent is None
            else "Pass the exact investigation UUID."
        )
        raise CLIError(
            "conflict",
            f"More than one investigation has the exact name {normalized!r}{scope}: "
            f"{', '.join(labels)}{suffix}.",
            details={
                "ids": [str(item.id) for item in matches],
                "matches": summaries,
                "truncated": truncated,
            },
            hint=hint,
        )
    return matches[0]


async def _resolve_investigation_id(
    client: Any,
    reference: str | uuid.UUID,
    *,
    agent: str | None = None,
) -> uuid.UUID:
    """Resolve an investigation reference to its UUID, avoiding extra gets for UUIDs."""
    _, item_id = _parse_investigation_reference(reference)
    if item_id is not None and agent is None:
        return item_id
    investigation = await _resolve_investigation(client, reference, agent=agent)
    return uuid.UUID(str(investigation.id))


async def _find_existing_named_investigations(
    client: Any,
    *,
    agent_id: uuid.UUID,
    name: str,
) -> list[Any]:
    """Check whether the target agent already has an investigation with this name."""
    filter_expression = AndFilter.model_validate(
        {
            "and": [
                FilterCondition(field="agent_id", op=FilterOp.EQ, value=str(agent_id)),
                FilterCondition(field="name", op=FilterOp.EQ, value=name),
            ]
        }
    )
    try:
        page = await client.investigations.list(
            InvestigationListParams(size=1, filter=filter_expression)
        )
    # A version-skewed server or transient list error must not block creation.
    except (APIError, httpx.HTTPError, ValueError):
        return []
    return [
        item
        for item in page.items
        if item.name == name and str(item.agent_id) == str(agent_id)
    ]


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
    missing_question_sessions = [
        session_id for session_id in session_ids if not parsed_questions.get(session_id)
    ]
    if missing_question_sessions:
        example_id = missing_question_sessions[0]
        if len(missing_question_sessions) == 1:
            subject = f"--session {example_id} has no --session-question."
        else:
            missing_list = ", ".join(
                str(session_id) for session_id in missing_question_sessions
            )
            subject = f"Sessions missing --session-question: {missing_list}."
        raise CLIError(
            "invalid_arguments",
            f"{subject} Each linked session needs at least one, for example "
            f'--session-question {example_id}:verdict="..."',
        )
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    existing_matches = await _find_existing_named_investigations(
        client, agent_id=resolved_agent.id, name=name
    )
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
    existing_id = next(
        (
            str(item.id)
            for item in existing_matches
            if str(item.id) != str(investigation.id)
        ),
        None,
    )
    if existing_id is not None:
        warnings.append(
            f"An investigation named {name!r} already exists for agent "
            f"{resolved_agent.name!r} (for example {existing_id}). "
            "Pass --idempotency-key to avoid creating duplicates on retries."
        )
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


async def get_investigation(
    client: Any,
    investigation: str | uuid.UUID,
    *,
    agent: str | None = None,
) -> CommandResult:
    """Get one investigation by exact UUID or unambiguous case-sensitive name."""
    resolved = await _resolve_investigation(client, investigation, agent=agent)
    return CommandResult(item=resolved.model_dump(mode="json"))


async def update_investigation(
    client: Any,
    investigation: str | uuid.UUID,
    *,
    name: str | None,
    description: str | None,
    clear_description: bool,
    status: InvestigationStatus | None,
    agent: str | None = None,
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
    investigation_id = await _resolve_investigation_id(
        client, investigation, agent=agent
    )
    updated = await client.investigations.update(
        investigation_id, InvestigationUpdateRequest(**fields)
    )
    return CommandResult(item=updated.model_dump(mode="json"))


async def delete_investigation(
    client: Any,
    investigation: str | uuid.UUID,
    *,
    force: bool,
    agent: str | None = None,
) -> CommandResult:
    """Delete an investigation and its linked sessions and answers."""
    if not force:
        raise CLIError(
            "invalid_arguments",
            "Deleting an investigation, its linked sessions, and answers requires "
            "--force.",
        )
    investigation_id = await _resolve_investigation_id(
        client, investigation, agent=agent
    )
    await client.investigations.delete(investigation_id)
    return CommandResult(item={"id": str(investigation_id), "deleted": True})


async def list_investigation_sessions(
    client: Any,
    investigation: str | uuid.UUID,
    *,
    size: int,
    cursor: str | None,
    agent: str | None = None,
) -> CommandResult:
    """List one ordered page of sessions linked to an investigation."""
    investigation_id = await _resolve_investigation_id(
        client, investigation, agent=agent
    )
    params = InvestigationSessionsListParams(size=size, cursor=cursor)
    page = await client.investigations.list_sessions(investigation_id, params)
    return page_result(page, size=size)


async def update_investigation_session_verdict(
    client: Any,
    investigation: str | uuid.UUID,
    session_id: uuid.UUID,
    *,
    verdict: InvestigationSessionVerdict,
    agent: str | None = None,
) -> CommandResult:
    """Set one investigation session's verdict."""
    investigation_id = await _resolve_investigation_id(
        client, investigation, agent=agent
    )
    session = await client.investigations.update_session(
        investigation_id,
        session_id,
        InvestigationSessionUpdateRequest(verdict=verdict),
    )
    return CommandResult(
        item=session.model_dump(mode="json"),
        next_actions=[f"kitaru investigation get {investigation_id}"],
    )
