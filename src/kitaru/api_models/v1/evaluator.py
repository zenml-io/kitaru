"""Evaluator API models."""

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


class EvaluatorCreateRequest(RequestModel):
    """Evaluator create request."""

    name: str = Field(description="Evaluator name.")
    description: str | None = Field(default=None, description="Evaluator description.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Evaluator metadata."
    )


class EvaluatorUpdateRequest(RequestModel):
    """Evaluator update request."""

    description: str | None = Field(default=None, description="New description.")
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Replacement metadata."
    )


class EvaluatorListParams(ListParams):
    """Evaluator list params."""

    name: str | None = Field(default=None, description="Filter on evaluator name.")


class EvaluatorResponse(OwnedResponseModel):
    """Evaluator response."""

    id: uuid.UUID = Field(description="Evaluator id.")
    name: str = Field(description="Evaluator name.")
    description: str | None = Field(description="Evaluator description.")
    metadata: dict[str, JsonValue] = Field(description="Evaluator metadata.")
    latest_version: int = Field(description="Latest version number.")


class EvaluatorVersionCreateRequest(RequestModel):
    """Evaluator version create request."""

    source: PluginSource = Field(description="Plugin source.")
    display_version: str | None = Field(
        default=None, description="Human-readable version."
    )


class EvaluatorVersionUpdateRequest(RequestModel):
    """Evaluator version update request."""

    display_version: str | None = Field(
        default=None, description="New human-readable version."
    )


class EvaluatorVersionResponse(TimestampedResponseModel):
    """Evaluator version response."""

    id: uuid.UUID = Field(description="Evaluator version id.")
    evaluator_id: uuid.UUID = Field(description="Evaluator id.")
    version: int = Field(description="Server-assigned version number.")
    display_version: str | None = Field(description="Human-readable version.")
    source: PluginSource = Field(description="Plugin source.")
