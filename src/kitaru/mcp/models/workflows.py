#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict workflow, cancellation, and deletion inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.mcp.models.common import IDEMPOTENCY_KEY_DESCRIPTION, DeleteKind, MCPModel
from kitaru.mcp.models.management import EvaluatorSelection


class SessionImportRequest(MCPModel):
    """Import sessions from an existing payload blob."""

    payload_blob_id: uuid.UUID
    importer_id: uuid.UUID
    importer_version: int = Field(ge=1)
    agent_version_id: uuid.UUID
    params: dict[str, JsonValue] = Field(default_factory=dict)
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )


class EvaluationStart(MCPModel):
    """Start one bounded evaluation batch."""

    operation: Literal["evaluation"]
    session_ids: list[uuid.UUID] = Field(min_length=1, max_length=100)
    evaluators: list[EvaluatorSelection] = Field(min_length=1, max_length=100)
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )

    @model_validator(mode="after")
    def _validate_batch(self) -> "EvaluationStart":
        if len(set(self.session_ids)) != len(self.session_ids):
            raise ValueError("session_ids must be unique")
        if len(self.session_ids) * len(self.evaluators) > 100:
            raise ValueError(
                "evaluation batches support at most 100 session/evaluator pairs"
            )
        return self


class ExperimentRunStart(MCPModel):
    """Start one exact experiment run."""

    operation: Literal["experiment_run"]
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluate_baselines: bool = False
    idempotency_key: str | None = Field(
        default=None,
        description=IDEMPOTENCY_KEY_DESCRIPTION,
    )


WorkflowStartRequest = Annotated[
    EvaluationStart | ExperimentRunStart,
    Field(discriminator="operation"),
]


class JobCancel(MCPModel):
    """Cancel one job."""

    operation: Literal["job"]
    id: uuid.UUID


class ExperimentRunCancel(MCPModel):
    """Cancel one experiment run."""

    operation: Literal["experiment_run"]
    id: uuid.UUID


WorkflowCancelRequest = Annotated[
    JobCancel | ExperimentRunCancel,
    Field(discriminator="operation"),
]


class ResourceDelete(MCPModel):
    """Delete one allowlisted exact resource."""

    kind: DeleteKind
    id: uuid.UUID


class TagLinkDelete(MCPModel):
    """Delete one exact tag-to-resource link."""

    kind: Literal["tag_link"]
    tag_id: uuid.UUID
    resource_type: TagResourceType
    resource_id: uuid.UUID


DeleteRequest = Annotated[
    ResourceDelete | TagLinkDelete,
    Field(discriminator="kind"),
]
