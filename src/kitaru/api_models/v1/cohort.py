"""Cohort API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import ListParams, OwnedResponseModel, RequestModel


class CohortCreateRequest(RequestModel):
    """Cohort create request."""

    name: str = Field(description="Cohort name.")
    description: str | None = Field(default=None, description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    session_ids: list[uuid.UUID] = Field(description="Ordered session ids.")


class CohortUpdateRequest(RequestModel):
    """Cohort update request."""

    name: str | None = Field(default=None, description="New cohort name.")
    description: str | None = Field(default=None, description="New description.")


class CohortListParams(ListParams):
    """Cohort list params."""

    name: str | None = Field(default=None, description="Filter on cohort name.")
    tag: str | None = Field(default=None, description="Filter on tag name.")


class CohortResponse(OwnedResponseModel):
    """Cohort response."""

    id: uuid.UUID = Field(description="Cohort id.")
    name: str = Field(description="Cohort name.")
    description: str | None = Field(description="Cohort description.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    session_count: int = Field(description="Number of sessions.")
