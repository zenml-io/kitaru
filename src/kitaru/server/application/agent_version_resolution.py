"""Shared agent-version resolution."""

import uuid

from kitaru.server.application.interfaces.agent_repository import (
    AgentVersionRepository,
)
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.base import ValidationError


def get_agent_run_spec(version: AgentVersion) -> RunSpec:
    """Get an agent version's required executable run specification."""
    if version.run_spec is None:
        raise ValidationError(f"Agent version {version.id} has no run specification")
    return version.run_spec


async def resolve_agent_version(
    version_id: uuid.UUID,
    repository: AgentVersionRepository,
    *,
    require_run: bool = True,
) -> AgentVersion:
    """Resolve an agent version and optionally require an executable run spec."""
    version = await repository.get(version_id)
    if require_run:
        get_agent_run_spec(version)
    return version
