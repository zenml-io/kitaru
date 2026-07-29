"""Tag API models."""

import uuid
from enum import StrEnum

from pydantic import Field

from kitaru.api_models.v1.base import (
    ListParams,
    OwnedResponseModel,
    RequestModel,
    TimestampedResponseModel,
)


class TagResourceType(StrEnum):
    """Taggable resource type."""

    SESSION = "session"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    EXPERIMENT_RUN = "experiment_run"


class TagCreateRequest(RequestModel):
    """Tag create request."""

    name: str = Field(description="Tag name.")


class TagUpdateRequest(RequestModel):
    """Tag update request."""

    name: str = Field(description="New tag name.")


class TagListParams(ListParams):
    """Tag list params."""

    name: str | None = Field(default=None, description="Filter on tag name.")


class TagResponse(OwnedResponseModel):
    """Tag response."""

    id: uuid.UUID = Field(description="Tag id.")
    name: str = Field(description="Tag name.")


class TagLinkCreateRequest(RequestModel):
    """Tag link create request."""

    resource_type: TagResourceType = Field(description="Linked resource type.")
    resource_id: uuid.UUID = Field(description="Linked resource id.")


class TagLinkResponse(TimestampedResponseModel):
    """Tag link response."""

    id: uuid.UUID = Field(description="Tag link id.")
    tag_id: uuid.UUID = Field(description="Tag id.")
    resource_type: TagResourceType = Field(description="Linked resource type.")
    resource_id: uuid.UUID = Field(description="Linked resource id.")
