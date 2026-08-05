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

import uuid
from typing import Any

from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationSessionInput,
    InvestigationSessionsListParams,
    InvestigationSessionStatus,
    InvestigationSessionUpdateRequest,
    InvestigationSessionView,
    InvestigationUpdateRequest,
    QuestionItem,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import (
    list_params,
    page_result,
    parse_json_object,
    resolve_asset,
)


def _parse_questions(entries: list[str]) -> list[QuestionItem]:
    """Parse repeatable ``KEY=QUESTION`` inputs in their supplied order."""
    questions: list[QuestionItem] = []
    keys: set[str] = set()
    for entry in entries:
        key, separator, question = entry.partition("=")
        if not separator or not key:
            raise CLIError(
                "invalid_arguments",
                "--question must be KEY=QUESTION.",
            )
        if key in keys:
            raise CLIError(
                "invalid_arguments",
                f"Each --question key must be unique; repeated {key!r}.",
            )
        keys.add(key)
        questions.append(QuestionItem(key=key, question=question))
    return questions


def _parse_session_views(
    entries: list[str], selected_session_ids: list[uuid.UUID]
) -> dict[uuid.UUID, InvestigationSessionView]:
    """Parse ``SESSION=JSON_OBJECT`` views for selected sessions."""
    selected = set(selected_session_ids)
    views: dict[uuid.UUID, InvestigationSessionView] = {}
    for entry in entries:
        session_token, separator, payload = entry.partition("=")
        if not separator or not session_token:
            raise CLIError(
                "invalid_arguments",
                "--session-view must be SESSION=JSON_OBJECT.",
            )
        try:
            session_id = uuid.UUID(session_token)
        except ValueError as error:
            raise CLIError(
                "invalid_arguments",
                "--session-view must start with a valid session UUID.",
            ) from error
        if session_id not in selected:
            raise CLIError(
                "invalid_arguments",
                f"Session {session_id} from --session-view must also be selected "
                "with --session.",
            )
        if session_id in views:
            raise CLIError(
                "invalid_arguments",
                f"Each --session-view session must be unique; repeated {session_id}.",
            )
        views[session_id] = InvestigationSessionView.model_validate(
            parse_json_object(payload, option="--session-view")
        )
    return views


async def create_investigation(
    client: Any,
    name: str,
    *,
    agent: str,
    description: str | None,
    questions: list[str],
    session_ids: list[uuid.UUID],
    session_views: list[str],
) -> CommandResult:
    """Create an investigation with ordered questions and linked sessions."""
    if len(set(session_ids)) != len(session_ids):
        raise CLIError("invalid_arguments", "Each --session value must be unique.")
    parsed_questions = _parse_questions(questions)
    parsed_views = _parse_session_views(session_views, session_ids)
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    sessions = [
        InvestigationSessionInput(session_id=session_id, view=parsed_views[session_id])
        if session_id in parsed_views
        else InvestigationSessionInput(session_id=session_id)
        for session_id in session_ids
    ]
    investigation = await client.investigations.create(
        InvestigationCreateRequest(
            agent_id=resolved_agent.id,
            name=name,
            description=description,
            questions=parsed_questions,
            sessions=sessions,
        )
    )
    return CommandResult(
        item=investigation.model_dump(mode="json"),
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


async def update_investigation_session_status(
    client: Any,
    investigation_id: uuid.UUID,
    session_id: uuid.UUID,
    *,
    status: InvestigationSessionStatus,
) -> CommandResult:
    """Set one pending investigation session to a terminal status."""
    session = await client.investigations.update_session(
        investigation_id,
        session_id,
        InvestigationSessionUpdateRequest(status=status),
    )
    return CommandResult(
        item=session.model_dump(mode="json"),
        next_actions=[f"kitaru investigation get {investigation_id}"],
    )
