#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Shared strict MCP request and result models."""

import uuid
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.agent_version import AgentVersionResponse
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.cohort import CohortResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.evaluator import EvaluatorResponse, EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.experiment_run import ExperimentRunResponse
from kitaru.api_models.v1.importer import ImporterResponse, ImporterVersionResponse
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.api_models.v1.task import TaskResponse


class MCPModel(BaseModel):
    """Strict base for every MCP-owned model."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())


class PageOptions(MCPModel):
    """Bounded one-page request options."""

    cursor: str | None = None
    size: int = Field(default=20, ge=1, le=100)
    sort: str = Field(default="created:desc", pattern=r"^[a-z][a-z0-9_]*:(asc|desc)$")


class PageMetadata(MCPModel):
    """Opaque one-page cursor metadata."""

    size: int
    next_cursor: str | None
    has_more: bool


PageItemT = TypeVar("PageItemT", bound=BaseModel)


class PageData(MCPModel, Generic[PageItemT]):
    """Canonical bounded page result."""

    items: list[PageItemT]
    page: PageMetadata


class ToolError(MCPModel):
    """Stable, bounded error returned by a tool handler."""

    code: str
    message: str
    retryable: bool = False
    details: dict[str, JsonValue] | None = None
    recovery: str | None = None


class ToolResult(MCPModel):
    """Versioned result envelope shared by all seven tools."""

    schema_version: Literal["1"] = "1"
    ok: bool
    data: JsonValue | None = None
    warnings: list[str] = Field(default_factory=list)
    error: ToolError | None = None


RegistryItem = (
    CohortResponse
    | ExperimentResponse
    | ImporterResponse
    | EvaluatorResponse
    | AgentVersionResponse
    | CohortVersionResponse
    | ImporterVersionResponse
    | EvaluatorVersionResponse
    | AgentResponse
)


class RegistryReadResult(ToolResult):
    """Registry read result with domain-typed data."""

    data: RegistryItem | PageData[RegistryItem] | None = None


ActivityItem = (
    SessionResponse
    | ReplayResponse
    | EvaluationResponse
    | ExperimentRunResponse
    | SessionNodeResponse
    | TaskResponse
    | JobResponse
)


class ActivityReadResult(ToolResult):
    """Activity read result with domain-typed data."""

    data: ActivityItem | PageData[ActivityItem] | None = None


class CohortsManageResult(ToolResult):
    """Cohort management result."""

    data: CohortResponse | CohortVersionResponse | None = None


class ExperimentsManageResult(ToolResult):
    """Experiment management result."""

    data: ExperimentResponse | None = None


class SessionImportReceipt(MCPModel):
    """Receipt for a blob-backed import workflow."""

    operation: Literal["session_import"]
    idempotency: Literal["domain-deduplicated-only"]
    blob_id: uuid.UUID
    importer_id: uuid.UUID
    importer_version_id: uuid.UUID
    agent_id: uuid.UUID
    agent_version_id: uuid.UUID
    result: JobResponse


class SessionImportResult(ToolResult):
    """Session import result with an authoritative typed receipt."""

    data: SessionImportReceipt | None = None


class WorkflowCancellationReceipt(MCPModel):
    """Authoritative cancellation response."""

    operation: Literal["job", "experiment_run"]
    id: uuid.UUID
    cancellation_requested: Literal[True]
    result: ExperimentRunResponse | JobResponse


class WorkflowCancelResult(ToolResult):
    """Workflow cancellation result."""

    data: WorkflowCancellationReceipt | None = None


class DeleteReceipt(MCPModel):
    """Receipt for one exact allowlisted deletion."""

    kind: Literal["cohort", "cohort_version", "experiment", "experiment_run"]
    id: uuid.UUID
    deleted: Literal[True]


class DeleteResult(ToolResult):
    """Allowlisted deletion result."""

    data: DeleteReceipt | None = None
