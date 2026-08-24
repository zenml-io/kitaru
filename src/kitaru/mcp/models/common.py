#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Shared strict MCP request and result models."""

import uuid
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from kitaru.api_models.v1.agent import AgentResponse
from kitaru.api_models.v1.agent_version import AgentVersionResponse
from kitaru.api_models.v1.annotation import AnnotationResponse
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.cohort import CohortResponse
from kitaru.api_models.v1.cohort_version import CohortVersionResponse
from kitaru.api_models.v1.evaluation import EvaluationResponse
from kitaru.api_models.v1.evaluator import EvaluatorResponse, EvaluatorVersionResponse
from kitaru.api_models.v1.experiment import ExperimentResponse
from kitaru.api_models.v1.experiment_run import ExperimentRunResponse
from kitaru.api_models.v1.importer import ImporterResponse, ImporterVersionResponse
from kitaru.api_models.v1.investigation import (
    InvestigationResponse,
    InvestigationSessionResponse,
)
from kitaru.api_models.v1.job import JobResponse
from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.session import SessionResponse
from kitaru.api_models.v1.session_node import SessionNodeResponse
from kitaru.api_models.v1.tag import TagLinkResponse, TagResourceType, TagResponse
from kitaru.api_models.v1.task import TaskResponse
from kitaru.api_models.v1.worker import WorkerResponse


class MCPModel(BaseModel):
    """Strict base for every MCP-owned model."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())


IDEMPOTENCY_KEY_DESCRIPTION = (
    "Pass a fresh unique string per logical request. Reuse it only when "
    "retrying this exact call after a lost or failed response, so the retry "
    "returns the original result instead of acting twice."
)


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
    """Versioned result envelope shared by all public tools."""

    schema_version: Literal["1"] = "1"
    ok: bool
    data: JsonValue | None = None
    warnings: list[str] = Field(default_factory=list)
    error: ToolError | None = None


class ToolSuccessPayload(MCPModel):
    """Handler data plus success metadata for the public result envelope."""

    data: object
    warnings: list[str] = Field(default_factory=list)
    links: dict[str, str] = Field(default_factory=dict)


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
    | TagResponse
    | WorkerResponse
)


class RegistryReadResult(ToolResult):
    """Registry read result with domain-typed data."""

    data: RegistryItem | PageData[RegistryItem] | None = None


# Keep job activity output fully typed without repeating the API model's verbose
# field descriptions in the discovery schema. The literal mirrors JobKind while
# avoiding a separate enum definition in this already budget-constrained union.
class _MCPJob(JobResponse):
    id: uuid.UUID
    kind: Literal["session_run", "import", "evaluation", "replay"]


ActivityItem = (
    SessionResponse
    | ReplayResponse
    | EvaluationResponse
    | ExperimentRunResponse
    | SessionNodeResponse
    | TaskResponse
    | _MCPJob
)


class ActivityReadResult(ToolResult):
    """Typed activity read result."""

    data: ActivityItem | PageData[ActivityItem] | None = None


ReviewItem = InvestigationResponse | InvestigationSessionResponse | AnnotationResponse


class ReviewReadResult(ToolResult):
    """Typed investigation and annotation read result."""

    data: ReviewItem | PageData[ReviewItem] | None = None


class ReviewManageResult(ToolResult):
    """Typed investigation and annotation management result."""

    data: ReviewItem | TagResponse | TagLinkResponse | None = None
    links: dict[Literal["review"], str] = Field(default_factory=dict)


class CohortsManageResult(ToolResult):
    """Cohort management result."""

    data: CohortResponse | CohortVersionResponse | None = None


class ExperimentsManageResult(ToolResult):
    """Experiment management result."""

    data: ExperimentResponse | None = None


class EvaluatorsManageResult(ToolResult):
    """Evaluator parent or version management result."""

    data: EvaluatorResponse | EvaluatorVersionResponse | None = None


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


class EvaluationSelectionReceipt(MCPModel):
    """Resolved evaluator identity used by an evaluation batch."""

    evaluator_id: uuid.UUID
    evaluator_version_id: uuid.UUID
    version: int


class EvaluationStartReceipt(MCPModel):
    """Immediate receipt for a queued evaluation batch."""

    operation: Literal["evaluation"]
    input_session_ids: list[uuid.UUID]
    evaluators: list[EvaluationSelectionReceipt]
    result: JobResponse


class ExperimentRunStartReceipt(MCPModel):
    """Immediate receipt for a started experiment run."""

    operation: Literal["experiment_run"]
    experiment_id: uuid.UUID
    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluate_baselines: bool
    result: ExperimentRunResponse


class WorkflowStartResult(ToolResult):
    """Workflow start result with an authoritative typed receipt."""

    data: EvaluationStartReceipt | ExperimentRunStartReceipt | None = None


class WorkflowCancellationReceipt(MCPModel):
    """Authoritative cancellation response."""

    operation: Literal["job", "experiment_run"]
    id: uuid.UUID
    cancellation_requested: Literal[True]
    result: ExperimentRunResponse | JobResponse


class WorkflowCancelResult(ToolResult):
    """Workflow cancellation result."""

    data: WorkflowCancellationReceipt | None = None


DeleteKind = Literal[
    "cohort",
    "cohort_version",
    "experiment",
    "experiment_run",
    "investigation",
    "annotation",
    "evaluator",
    "tag",
]


class DeleteReceipt(MCPModel):
    """Receipt for one exact allowlisted deletion."""

    kind: DeleteKind
    id: uuid.UUID
    deleted: Literal[True]


class TagLinkDeleteReceipt(MCPModel):
    """Receipt for deleting one exact tag-to-resource link."""

    kind: Literal["tag_link"]
    tag_id: uuid.UUID
    resource_type: TagResourceType
    resource_id: uuid.UUID
    deleted: Literal[True]


class DeleteResult(ToolResult):
    """Allowlisted deletion result."""

    data: DeleteReceipt | TagLinkDeleteReceipt | None = None
