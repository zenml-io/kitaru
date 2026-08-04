#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict cohort and experiment management inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.replay_config import ReplayOverride, ToolPolicy
from kitaru.mcp.models.common import MCPModel


class EvaluatorSelection(MCPModel):
    """Exact evaluator parent/version selection for a mutation."""

    evaluator_id: uuid.UUID
    version: int = Field(ge=1)
    params: dict[str, JsonValue] = Field(default_factory=dict)


class CohortCreate(MCPModel):
    """Create a cohort."""

    operation: Literal["create"]
    agent_id: uuid.UUID
    name: str = Field(min_length=1)
    description: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class CohortUpdate(MCPModel):
    """Sparsely update cohort metadata."""

    operation: Literal["update"]
    cohort_id: uuid.UUID
    name: str | None = None
    description: str | None = None
    clear_description: bool = False
    metadata: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def _validate_update(self) -> "CohortUpdate":
        if (
            "description" in self.model_fields_set
            and self.description is None
            and not self.clear_description
        ):
            raise ValueError("description cannot be null without clear_description")
        if self.description is not None and self.clear_description:
            raise ValueError("description and clear_description conflict")
        for field in ("name", "metadata"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        if (
            not ({"name", "description", "metadata"} & self.model_fields_set)
            and not self.clear_description
        ):
            raise ValueError("cohort update must change at least one field")
        return self


class CohortVersionCreate(MCPModel):
    """Create a cohort version from ordered membership changes."""

    operation: Literal["create_version"]
    cohort_id: uuid.UUID
    add_session_ids: list[uuid.UUID] = Field(default_factory=list)
    remove_session_ids: list[uuid.UUID] = Field(default_factory=list)
    display_version: str | None = None

    @model_validator(mode="after")
    def _validate_membership(self) -> "CohortVersionCreate":
        if len(set(self.add_session_ids)) != len(self.add_session_ids):
            raise ValueError("add_session_ids must be unique")
        if len(set(self.remove_session_ids)) != len(self.remove_session_ids):
            raise ValueError("remove_session_ids must be unique")
        if set(self.add_session_ids) & set(self.remove_session_ids):
            raise ValueError("a session cannot be both added and removed")
        return self


class CohortVersionUpdate(MCPModel):
    """Set or clear a cohort version display label."""

    operation: Literal["update_version"]
    version_id: uuid.UUID
    display_version: str | None = None
    clear_display_version: bool = False

    @model_validator(mode="after")
    def _validate_update(self) -> "CohortVersionUpdate":
        if (self.display_version is None) == (not self.clear_display_version):
            raise ValueError(
                "exactly one of display_version or clear_display_version is required"
            )
        return self


CohortsManageRequest = Annotated[
    CohortCreate | CohortUpdate | CohortVersionCreate | CohortVersionUpdate,
    Field(discriminator="operation"),
]


class ExperimentCreate(MCPModel):
    """Create an experiment with exact evaluator selections."""

    operation: Literal["create"]
    name: str = Field(min_length=1)
    description: str | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorSelection] = Field(min_length=1, max_length=10)


class ExperimentUpdate(MCPModel):
    """Sparsely update an experiment."""

    operation: Literal["update"]
    experiment_id: uuid.UUID
    name: str | None = None
    description: str | None = None
    clear_description: bool = False
    override: ReplayOverride | None = None
    clear_override: bool = False
    tool_policy: ToolPolicy | None = None
    clear_tool_policy: bool = False
    evaluators: list[EvaluatorSelection] | None = Field(
        default=None, min_length=1, max_length=10
    )

    @model_validator(mode="after")
    def _validate_update(self) -> "ExperimentUpdate":
        pairs = (
            ("description", self.description, self.clear_description),
            ("override", self.override, self.clear_override),
            ("tool_policy", self.tool_policy, self.clear_tool_policy),
        )
        if any(value is not None and clear for _, value, clear in pairs):
            raise ValueError("a field cannot be set and cleared together")
        for field, value, clear in pairs:
            if field in self.model_fields_set and value is None and not clear:
                raise ValueError(f"{field} cannot be null without clear_{field}")
        for field in ("name", "evaluators"):
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        fields = {"name", "description", "override", "tool_policy", "evaluators"}
        if not (fields & self.model_fields_set) and not (
            self.clear_description or self.clear_override or self.clear_tool_policy
        ):
            raise ValueError("experiment update must change at least one field")
        return self


ExperimentsManageRequest = Annotated[
    ExperimentCreate | ExperimentUpdate,
    Field(discriminator="operation"),
]
