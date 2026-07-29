"""Importer API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    ListParams,
    OwnedResponseModel,
    RequestModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.plugin import PluginSource


class ImporterCreateRequest(RequestModel):
    """Importer create request."""

    name: str = Field(description="Importer name.")
    description: str | None = Field(default=None, description="Importer description.")
    provider: str | None = Field(default=None, description="Source provider.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Importer metadata."
    )


class ImporterUpdateRequest(RequestModel):
    """Importer update request."""

    description: str | None = Field(default=None, description="New description.")
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Replacement metadata."
    )


class ImporterListParams(ListParams):
    """Importer list params."""

    name: str | None = Field(default=None, description="Filter on importer name.")
    provider: str | None = Field(default=None, description="Filter on provider.")


class ImporterResponse(OwnedResponseModel):
    """Importer response."""

    id: uuid.UUID = Field(description="Importer id.")
    name: str = Field(description="Importer name.")
    description: str | None = Field(description="Importer description.")
    provider: str | None = Field(description="Source provider.")
    metadata: dict[str, JsonValue] = Field(description="Importer metadata.")
    latest_version: int = Field(description="Latest version number.")


class ImporterVersionCreateRequest(RequestModel):
    """Importer version create request."""

    source: PluginSource = Field(description="Plugin source.")
    display_version: str | None = Field(
        default=None, description="Human-readable version."
    )


class ImporterVersionUpdateRequest(RequestModel):
    """Importer version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable version."
    )


class ImporterVersionResponse(TimestampedResponseModel):
    """Importer version response."""

    id: uuid.UUID = Field(description="Importer version id.")
    importer_id: uuid.UUID = Field(description="Importer id.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable version.")
    source: PluginSource = Field(description="Plugin source.")
