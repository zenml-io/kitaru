"""Session DTO conversions."""

from kitaru.api_models.v1.session import (
    SessionCreateRequest,
    SessionListParams,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.session import (
    TokenUsage as TokenUsageDTO,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.session import (
    SessionCreate,
    SessionFilter,
    SessionUpdate,
)
from kitaru.server.domain.session import Session


def session_to_response(session: Session) -> SessionResponse:
    """Convert a session entity to its response."""
    assert session.created is not None
    assert session.updated is not None
    tokens = (
        TokenUsageDTO.model_validate(session.tokens.model_dump())
        if session.tokens is not None
        else None
    )
    return SessionResponse(
        id=session.id,
        owner_id=session.owner_id,
        agent_id=session.agent_id,
        agent_version_id=session.agent_version_id,
        task_id=session.task_id,
        origin=session.origin,
        status=session.status,
        name=session.name,
        inputs=session.inputs,
        outputs=session.outputs,
        expected=session.expected,
        error=session.error,
        started_at=session.started_at,
        ended_at=session.ended_at,
        external_id=session.external_id,
        metadata=session.metadata,
        provider=session.provider,
        framework=session.framework,
        adapter_version=session.adapter_version,
        cost=session.cost,
        tokens=tokens,
        llm_call_count=session.llm_call_count,
        tool_call_count=session.tool_call_count,
        created=session.created,
        updated=session.updated,
    )


def session_create_to_command(body: SessionCreateRequest) -> SessionCreate:
    """Convert a session create body."""
    return SessionCreate.model_validate(
        body.model_dump(mode="python", exclude_none=True)
    )


def session_update_to_command(body: SessionUpdateRequest) -> SessionUpdate:
    """Convert a session PATCH body while preserving unset fields."""
    return to_partial(SessionUpdate, body)


def session_list_params_to_filter(params: SessionListParams) -> SessionFilter:
    """Convert session list query parameters."""
    return SessionFilter(**params.model_dump(mode="python"))
