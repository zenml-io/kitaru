#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict evaluator parent and version management inputs."""

import uuid
from typing import Annotated, Literal

from packaging.requirements import InvalidRequirement, Requirement
from pydantic import Field, field_validator, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.plugin import (
    PluginSource,
    ScriptPluginSource,
)
from kitaru.mcp.models.common import IDEMPOTENCY_KEY_DESCRIPTION, MCPModel


class EvaluatorCreate(MCPModel):
    """Create an evaluator parent."""

    operation: Literal["create"]
    name: str = Field(min_length=1)
    description: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )


class EvaluatorUpdate(MCPModel):
    """Sparsely update an evaluator parent."""

    operation: Literal["update"]
    evaluator_id: uuid.UUID
    description: str | None = None
    clear_description: bool = False
    metadata: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def _validate_update(self) -> "EvaluatorUpdate":
        if (
            "description" in self.model_fields_set
            and self.description is None
            and not self.clear_description
        ):
            raise ValueError("description cannot be null without clear_description")
        if self.description is not None and self.clear_description:
            raise ValueError("description and clear_description conflict")
        if "metadata" in self.model_fields_set and self.metadata is None:
            raise ValueError("metadata cannot be null")
        if not ({"description", "metadata"} & self.model_fields_set) and not (
            self.clear_description
        ):
            raise ValueError("evaluator update must change at least one field")
        return self


class EvaluatorVersionCreate(MCPModel):
    """Create a version from an existing blob or exact package pin."""

    operation: Literal["create_version"]
    evaluator_id: uuid.UUID
    source: PluginSource
    display_version: str | None = None
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )

    @field_validator("source")
    @classmethod
    def _validate_source(cls, value: PluginSource) -> PluginSource:
        if isinstance(value, ScriptPluginSource):
            return value
        if len(value.requirement) > 255:
            raise ValueError("package requirement must be at most 255 characters")
        try:
            parsed = Requirement(value.requirement)
        except InvalidRequirement as error:
            raise ValueError("package requirement must be valid PEP 508") from error
        specifiers = list(parsed.specifier)
        pinned = (
            parsed.url is None
            and parsed.marker is None
            and len(specifiers) == 1
            and specifiers[0].operator == "=="
            and "*" not in specifiers[0].version
        )
        if not pinned:
            raise ValueError(
                "package requirement must be exactly pinned with one == version"
            )
        return value


class EvaluatorVersionUpdate(MCPModel):
    """Set or clear one evaluator version display label."""

    operation: Literal["update_version"]
    evaluator_id: uuid.UUID
    version: int = Field(ge=1)
    display_version: str | None = None
    clear_display_version: bool = False

    @model_validator(mode="after")
    def _validate_update(self) -> "EvaluatorVersionUpdate":
        if (self.display_version is None) == (not self.clear_display_version):
            raise ValueError(
                "exactly one of display_version or clear_display_version is required"
            )
        return self


EvaluatorsManageRequest = Annotated[
    EvaluatorCreate | EvaluatorUpdate | EvaluatorVersionCreate | EvaluatorVersionUpdate,
    Field(discriminator="operation"),
]
