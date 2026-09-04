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
"""Insight CLI commands."""

import json
import uuid
from typing import Any

from kitaru.api_models.v1.filter import AndFilter, FilterCondition, FilterOp
from kitaru.api_models.v1.insight import (
    InsightInput,
    InsightListParams,
    InsightUpdateRequest,
)
from kitaru.cli.output import CLIError, CommandResult
from kitaru.cli.registration import list_params, page_result, resolve_asset


def _parse_insight_input(value: str) -> InsightInput:
    """Parse one ``InsightInput``-shaped JSON object."""
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise CLIError(
            "invalid_arguments", f"--insight is not valid JSON: {error}"
        ) from error
    if not isinstance(parsed, dict):
        raise CLIError("invalid_arguments", "--insight must contain a JSON object.")
    return InsightInput.model_validate(parsed)


async def create_insights(
    client: Any,
    *,
    agent: str,
    insight: list[str],
    idempotency_key: str | None = None,
) -> CommandResult:
    """Create a batch of insights for one agent in one shot."""
    if not insight:
        raise CLIError("invalid_arguments", "Provide at least one --insight.")
    parsed = [_parse_insight_input(value) for value in insight]
    resolved_agent = await resolve_asset(client.agents, agent, "Agent")
    created = await client.insights.create(
        resolved_agent.id, parsed, idempotency_key=idempotency_key
    )
    return CommandResult(
        items=[item.model_dump(mode="json") for item in created],
        next_actions=[f"kitaru insight list --agent {resolved_agent.id}"],
    )


async def list_insights(
    client: Any,
    *,
    size: int,
    cursor: str | None,
    sort: str,
    filter: str | None,
    agent: str | None,
    name: str | None,
    type: str | None,
) -> CommandResult:
    """List one server page of insights."""
    params = list_params("insight", size=size, cursor=cursor, sort=sort, filter=filter)
    assert isinstance(params, InsightListParams)
    conditions = []
    if params.filter is not None:
        conditions.append(params.filter)
    if agent is not None:
        resolved_agent = await resolve_asset(client.agents, agent, "Agent")
        conditions.append(
            FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=str(resolved_agent.id)
            )
        )
    if name is not None:
        conditions.append(FilterCondition(field="name", op=FilterOp.EQ, value=name))
    if type is not None:
        conditions.append(FilterCondition(field="type", op=FilterOp.EQ, value=type))
    if conditions:
        expression = (
            conditions[0]
            if len(conditions) == 1
            else AndFilter.model_validate({"and": conditions})
        )
        params = params.model_copy(update={"filter": expression})
    return page_result(await client.insights.list(params), size=size)


async def get_insight(client: Any, insight_id: uuid.UUID) -> CommandResult:
    """Get one insight by UUID."""
    insight = await client.insights.get(insight_id)
    return CommandResult(item=insight.model_dump(mode="json"))


async def update_insight(
    client: Any,
    insight_id: uuid.UUID,
    *,
    title: str | None,
    description: str | None,
    clear_description: bool,
) -> CommandResult:
    """Update only explicitly selected insight fields."""
    if description is not None and clear_description:
        raise CLIError(
            "invalid_arguments",
            "--description and --clear-description cannot be used together.",
        )
    fields: dict[str, Any] = {}
    if title is not None:
        fields["title"] = title
    if description is not None:
        fields["description"] = description
    elif clear_description:
        fields["description"] = None
    if not fields:
        raise CLIError("invalid_arguments", "Select at least one insight update.")
    insight = await client.insights.update(insight_id, InsightUpdateRequest(**fields))
    return CommandResult(item=insight.model_dump(mode="json"))


async def delete_insight(
    client: Any, insight_id: uuid.UUID, *, force: bool
) -> CommandResult:
    """Delete an insight."""
    if not force:
        raise CLIError("invalid_arguments", "Deleting an insight requires --force.")
    await client.insights.delete(insight_id)
    return CommandResult(item={"id": str(insight_id), "deleted": True})
