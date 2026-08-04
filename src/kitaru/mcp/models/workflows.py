#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict workflow, cancellation, and deletion inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import AfterValidator, Field, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.replay_config import ReplayOverride, ToolPolicy
from kitaru.mcp.models.common import MCPModel
from kitaru.mcp.models.management import EvaluatorSelection
from kitaru.transport import IDEMPOTENCY_KEY_MAX_LENGTH, validate_idempotency_key

RequestId = Annotated[
    str,
    Field(
        min_length=1,
        max_length=IDEMPOTENCY_KEY_MAX_LENGTH,
        pattern=r"^[!-~]+$",
        description="Visible ASCII idempotency key without spaces.",
    ),
    AfterValidator(validate_idempotency_key),
]


class ReplayStart(MCPModel):
    """Start a replay-protected standalone replay."""

    operation: Literal["replay"]
    request_id: RequestId
    baseline_session_id: uuid.UUID
    agent_version_id: uuid.UUID | None = None
    override: ReplayOverride | None = None
    tool_policy: ToolPolicy | None = None
    evaluators: list[EvaluatorSelection] = Field(min_length=1, max_length=10)
    evaluate_baselines: bool = False


class SessionRunStart(MCPModel):
    """Start a replay-protected direct session run."""

    operation: Literal["session_run"]
    request_id: RequestId
    agent_version_id: uuid.UUID
    inputs: JsonValue
    name: str | None = None


class SessionImportStart(MCPModel):
    """Start a non-idempotent import from an existing payload blob."""

    operation: Literal["session_import"]
    payload_blob_id: uuid.UUID
    importer_id: uuid.UUID
    importer_version: int = Field(ge=1)
    agent_version_id: uuid.UUID
    params: dict[str, JsonValue] = Field(default_factory=dict)


class SessionEvaluationStart(MCPModel):
    """Start replay-protected evaluations for bounded session/evaluator pairs."""

    operation: Literal["session_evaluation"]
    request_id: RequestId
    session_ids: list[uuid.UUID] = Field(min_length=1, max_length=100)
    evaluators: list[EvaluatorSelection] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def _validate_batch(self) -> "SessionEvaluationStart":
        if len(set(self.session_ids)) != len(self.session_ids):
            raise ValueError("session_ids must be unique")
        if len(self.session_ids) * len(self.evaluators) > 100:
            raise ValueError("at most 100 session/evaluator pairs are allowed")
        return self


class ExperimentRunStart(MCPModel):
    """Start one replay-protected experiment run."""

    operation: Literal["experiment_run"]
    request_id: RequestId
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluate_baselines: bool = False


WorkflowStartRequest = Annotated[
    ReplayStart
    | SessionRunStart
    | SessionImportStart
    | SessionEvaluationStart
    | ExperimentRunStart,
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


class DeleteRequest(MCPModel):
    """Delete one allowlisted exact resource."""

    kind: Literal["cohort", "cohort_version", "experiment", "experiment_run"]
    id: uuid.UUID
