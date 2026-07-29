"""Agent DTO conversions."""

from kitaru.api_models.v1.agent import (
    AgentListParams,
    AgentResponse,
    AgentUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.agent import AgentFilter, AgentUpdate
from kitaru.server.domain.agent import Agent


def agent_to_response(agent: Agent) -> AgentResponse:
    """Convert an agent entity to its response."""
    assert agent.created is not None
    assert agent.updated is not None
    return AgentResponse(
        id=agent.id,
        owner_id=agent.owner_id,
        name=agent.name,
        description=agent.description,
        latest_version=agent.latest_version,
        created=agent.created,
        updated=agent.updated,
    )


def agent_list_params_to_filter(params: AgentListParams) -> AgentFilter:
    """Convert agent list query parameters."""
    return AgentFilter(
        name=params.name,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def agent_update_to_command(body: AgentUpdateRequest) -> AgentUpdate:
    """Convert an agent PATCH body while preserving unset fields."""
    return to_partial(AgentUpdate, body)
