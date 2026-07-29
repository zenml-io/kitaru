"""Agent version DTO conversions."""

import uuid

from kitaru.api_models.v1.agent_version import (
    AgentCapabilities as AgentCapabilitiesDTO,
)
from kitaru.api_models.v1.agent_version import (
    AgentVersionCreateRequest,
    AgentVersionResponse,
    AgentVersionUpdateRequest,
)
from kitaru.api_models.v1.agent_version import (
    RunSpec as RunSpecDTO,
)
from kitaru.api_models.v1.base import ListParams
from kitaru.server.application.models.agent_version import (
    AgentVersionFilter,
    AgentVersionUpdate,
)
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    RunSpec,
)


def agent_version_to_response(version: AgentVersion) -> AgentVersionResponse:
    """Convert an agent version entity to its response."""
    assert version.created is not None
    assert version.updated is not None
    run_spec = (
        RunSpecDTO.model_validate(version.run_spec.model_dump())
        if version.run_spec is not None
        else None
    )
    capabilities = AgentCapabilitiesDTO.model_validate(
        version.capabilities.model_dump()
    )
    return AgentVersionResponse(
        id=version.id,
        owner_id=version.owner_id,
        agent_id=version.agent_id,
        version=version.version,
        display_version=version.display_version,
        description=version.description,
        run_spec=run_spec,
        capabilities=capabilities,
        created=version.created,
        updated=version.updated,
    )


def agent_version_list_filter(
    agent_id: uuid.UUID, params: ListParams
) -> AgentVersionFilter:
    """Convert parent id and generic pagination fields."""
    return AgentVersionFilter(
        agent_id=agent_id,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def agent_version_update_to_command(
    body: AgentVersionUpdateRequest,
) -> AgentVersionUpdate:
    """Convert an agent version PATCH body while preserving unset fields."""
    values = {name: getattr(body, name) for name in body.model_fields_set}
    if body.run_spec is not None and "run_spec" in values:
        values["run_spec"] = RunSpec.model_validate(body.run_spec.model_dump())
    if body.capabilities is not None and "capabilities" in values:
        values["capabilities"] = AgentCapabilities.model_validate(
            body.capabilities.model_dump()
        )
    return AgentVersionUpdate(**values)


def agent_version_create_values(
    body: AgentVersionCreateRequest,
) -> tuple[
    str | None,
    str | None,
    RunSpec | None,
    AgentCapabilities | None,
]:
    """Convert nested agent version create values to domain types."""
    run_spec = (
        RunSpec.model_validate(body.run_spec.model_dump())
        if body.run_spec is not None
        else None
    )
    capabilities = (
        AgentCapabilities.model_validate(body.capabilities.model_dump())
        if body.capabilities is not None
        else None
    )
    return body.display_version, body.description, run_spec, capabilities
