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
"""Session DTO conversions."""

import kitaru.api_models.v1.sessions as session_models
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionResponse,
    SessionUpdateRequest,
)
from kitaru.server.application.models.sessions import (
    SessionCreate,
    SessionUpdate,
)
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)


def origin_to_domain(origin: session_models.SessionOrigin) -> SessionOrigin:
    """Convert an origin DTO to its domain enum.

    Args:
        origin: Origin from the API.

    Returns:
        Domain origin.
    """
    return SessionOrigin(origin.value)


def status_to_domain(
    status: session_models.SessionStatus | None,
) -> SessionStatus | None:
    """Convert an optional status DTO to its domain enum.

    Args:
        status: Status from the API.

    Returns:
        Domain status, ``None`` for ``None``.
    """
    if status is None:
        return None
    return SessionStatus(status.value)


def provider_to_domain(provider: str | None) -> str | None:
    """Pass an optional provider id to the domain.

    Args:
        provider: Provider from the API.

    Returns:
        Provider id, ``None`` for ``None``.
    """
    if provider is None:
        return None
    return provider


def token_usage_to_domain(
    tokens: session_models.TokenUsage | None,
) -> TokenUsage | None:
    """Convert an optional token usage DTO to its domain value object.

    Args:
        tokens: Token usage DTO.

    Returns:
        Domain token usage, ``None`` for ``None``.
    """
    if tokens is None:
        return None
    return TokenUsage(
        input_tokens=tokens.input_tokens,
        output_tokens=tokens.output_tokens,
        cached_input_tokens=tokens.cached_input_tokens,
        reasoning_tokens=tokens.reasoning_tokens,
    )


def token_usage_to_response(
    tokens: TokenUsage | None,
) -> session_models.TokenUsage | None:
    """Convert an optional domain token usage to its DTO.

    Args:
        tokens: Domain token usage.

    Returns:
        Token usage DTO, ``None`` for ``None``.
    """
    if tokens is None:
        return None
    return session_models.TokenUsage(
        input_tokens=tokens.input_tokens,
        output_tokens=tokens.output_tokens,
        cached_input_tokens=tokens.cached_input_tokens,
        reasoning_tokens=tokens.reasoning_tokens,
    )


def session_create_to_command(body: SessionCreateRequest) -> SessionCreate:
    """Convert a session create request to its command.

    Args:
        body: Session create request.

    Returns:
        Session create command.
    """
    return SessionCreate(
        agent_id=body.agent_id,
        agent_version_id=body.agent_version_id,
        origin=origin_to_domain(body.origin),
        status=status_to_domain(body.status),
        name=body.name,
        inputs=body.inputs,
        outputs=body.outputs,
        expected=body.expected,
        error=body.error,
        started_at=body.started_at,
        ended_at=body.ended_at,
        external_id=body.external_id,
        metadata=body.metadata,
        provider=provider_to_domain(body.provider),
        framework=body.framework,
        adapter_version=body.adapter_version,
        log_uri=body.log_uri,
        replay_id=body.replay_id,
    )


def session_update_to_command(body: SessionUpdateRequest) -> SessionUpdate:
    """Convert a session update request to its command.

    Args:
        body: Session update request.

    Returns:
        Session update command.
    """
    return SessionUpdate(
        status=status_to_domain(body.status),
        outputs=body.outputs,
        error=body.error,
        ended_at=body.ended_at,
        log_uri=body.log_uri,
        name=body.name,
        expected=body.expected,
        metadata=body.metadata,
    )


def session_to_response(session: Session) -> SessionResponse:
    """Convert a session entity to its response DTO.

    Args:
        session: Stored session.

    Returns:
        Session response.
    """
    assert session.created is not None
    assert session.updated is not None
    return SessionResponse(
        id=session.id,
        owner_id=session.owner_id,
        agent_id=session.agent_id,
        agent_version_id=session.agent_version_id,
        origin=session_models.SessionOrigin(session.origin.value),
        status=session_models.SessionStatus(session.status.value),
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
        source_instance=session.source_instance,
        source_revision=session.source_revision,
        source_digest=session.source_digest,
        source_metadata=session.source_metadata,
        replay_readiness=session.replay_readiness,
        normalization_warnings=session.normalization_warnings,
        import_job_id=session.import_job_id,
        supersedes_session_id=session.supersedes_session_id,
        framework=session.framework,
        adapter_version=session.adapter_version,
        log_uri=session.log_uri,
        scores=session.scores,
        cost=session.cost,
        tokens=token_usage_to_response(session.tokens),
        llm_call_count=session.llm_call_count,
        tool_call_count=session.tool_call_count,
        created=session.created,
        updated=session.updated,
    )
