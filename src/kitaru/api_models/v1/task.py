"""Task lifecycle, claim, and process specification models."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    JsonValue,
    ListParams,
    RequestModel,
    ResponseModel,
    TimestampedResponseModel,
)
from kitaru.base import FrozenModel


class TaskKind(StrEnum):
    """Task kind."""

    AGENT = "agent"
    EVALUATOR = "evaluator"
    IMPORTER = "importer"


class TaskOnFailure(StrEnum):
    """Effect of a task failure on its job."""

    ABORT = "abort"
    CONTINUE = "continue"
    IGNORE = "ignore"


class TaskStatus(StrEnum):
    """Task status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"
    ABANDONED = "abandoned"


class LabelSelector(FrozenModel):
    """Worker label selector."""

    key: str = Field(description="Label key.")
    values: list[str] = Field(min_length=1, description="Accepted label values.")
    required: bool = Field(default=False, description="Whether the key must exist.")


class WorkerScope(FrozenModel):
    """Task claim constraints."""

    kinds: list[TaskKind] | None = Field(
        default=None, min_length=1, description="Accepted task kinds."
    )
    selectors: list[LabelSelector] | None = Field(
        default=None, min_length=1, description="Label selectors."
    )
    job_id: uuid.UUID | None = Field(default=None, description="Pinned job id.")

    @model_validator(mode="after")
    def _unique_selector_keys(self) -> "WorkerScope":
        if self.selectors is not None:
            keys = [selector.key for selector in self.selectors]
            if len(set(keys)) != len(keys):
                raise ValueError("selector keys must be unique")
        return self


class TaskResponse(TimestampedResponseModel):
    """Task response."""

    id: uuid.UUID = Field(description="Task id.")
    job_id: uuid.UUID = Field(description="Job id.")
    kind: TaskKind = Field(description="Task kind.")
    status: TaskStatus = Field(description="Task status.")
    on_failure: TaskOnFailure = Field(description="Failure behavior.")
    attempt: int = Field(description="Claim attempt.")
    labels: dict[str, str] = Field(description="Task labels.")
    agent_version_id: uuid.UUID | None = Field(description="Agent version id.")
    plugin_version_id: uuid.UUID | None = Field(description="Plugin version id.")
    payload_blob_id: uuid.UUID | None = Field(description="Payload blob id.")
    input_session_id: uuid.UUID | None = Field(description="Input session id.")
    agent_id: uuid.UUID | None = Field(description="Agent id.")
    worker_id: uuid.UUID | None = Field(description="Worker id.")
    result_session_id: uuid.UUID | None = Field(description="Result session id.")
    claimed_at: datetime | None = Field(description="Claim time.")
    heartbeat_at: datetime | None = Field(description="Last heartbeat time.")
    cancel_requested_at: datetime | None = Field(
        description="Cancellation request time."
    )
    started_at: datetime | None = Field(description="Start time.")
    ended_at: datetime | None = Field(description="End time.")
    error: str | None = Field(description="Failure detail.")
    result: JsonValue | None = Field(description="Task result.")


class TaskUpdateRequest(RequestModel):
    """Task executor update."""

    status: TaskStatus | None = Field(default=None, description="New task status.")
    attempt: int | None = Field(default=None, description="Claim attempt fence.")
    error: str | None = Field(default=None, description="Failure detail.")
    result: JsonValue | None = Field(default=None, description="Task result.")


class TaskListParams(ListParams):
    """Task list params."""

    job_id: uuid.UUID | None = Field(default=None, description="Filter on job id.")
    kind: TaskKind | None = Field(default=None, description="Filter on task kind.")
    status: TaskStatus | None = Field(
        default=None, description="Filter on task status."
    )
    worker_id: uuid.UUID | None = Field(
        default=None, description="Filter on worker id."
    )


class TaskClaimRequest(RequestModel):
    """Task claim request."""

    worker_id: uuid.UUID = Field(description="Claiming worker id.")
    max_tasks: int = Field(ge=1, le=100, description="Maximum tasks to claim.")


class TaskRunSpec(ResponseModel):
    """Resolved agent run specification."""

    command: str = Field(description="Process command.")
    working_dir: str | None = Field(description="Working directory.")
    env: dict[str, str] = Field(description="Process environment.")


class ScriptPluginSpec(ResponseModel):
    """Resolved script plugin specification."""

    type: Literal["script"]
    entrypoint: str = Field(description="Plugin entrypoint.")
    blob_id: uuid.UUID = Field(description="Source blob id.")
    sha256: str = Field(description="Source digest.")


class PackagePluginSpec(ResponseModel):
    """Resolved package plugin specification."""

    type: Literal["package"]
    entrypoint: str = Field(description="Plugin entrypoint.")
    requirement: str = Field(description="Pinned package requirement.")


PluginSpec = Annotated[
    ScriptPluginSpec | PackagePluginSpec, Field(discriminator="type")
]


class PayloadSpec(ResponseModel):
    """Resolved payload blob specification."""

    blob_id: uuid.UUID = Field(description="Payload blob id.")
    sha256: str = Field(description="Payload digest.")


class AgentTaskDetails(ResponseModel):
    """Agent task details."""

    kind: Literal["agent"]
    inputs: JsonValue = Field(description="Agent inputs.")


class EvaluationTaskDetails(ResponseModel):
    """Evaluator task details."""

    kind: Literal["evaluator"]
    evaluator_name: str = Field(description="Evaluator name.")
    params: dict[str, JsonValue] = Field(description="Evaluator parameters.")
    plugin: PluginSpec = Field(description="Resolved evaluator plugin.")
    input_session_id: uuid.UUID = Field(description="Input session id.")


class ImportTaskDetails(ResponseModel):
    """Importer task details."""

    kind: Literal["importer"]
    plugin: PluginSpec = Field(description="Resolved importer plugin.")
    payload: PayloadSpec = Field(description="Resolved payload.")
    provider: str | None = Field(description="Source provider.")
    agent_id: uuid.UUID = Field(description="Agent id.")
    params: dict[str, JsonValue] = Field(description="Importer parameters.")


TaskDetails = Annotated[
    AgentTaskDetails | EvaluationTaskDetails | ImportTaskDetails,
    Field(discriminator="kind"),
]


class TaskSpecResponse(ResponseModel):
    """Resolved task process specification."""

    task_id: uuid.UUID = Field(description="Task id.")
    kind: TaskKind = Field(description="Task kind.")
    timeout_seconds: int = Field(description="Process timeout in seconds.")
    run: TaskRunSpec | None = Field(description="Agent run specification.")
    env: dict[str, str] = Field(description="Creator environment values.")
    secret_env: dict[str, str] = Field(description="Resolved secret environment.")
    details: TaskDetails = Field(description="Kind-specific details.")


class TaskWithSpec(ResponseModel):
    """Claimed task and resolved specification."""

    task: TaskResponse = Field(description="Claimed task.")
    spec: TaskSpecResponse = Field(description="Resolved task specification.")


class TaskClaimResponse(ResponseModel):
    """Task claim response."""

    tasks: list[TaskWithSpec] = Field(description="Claimed tasks.")
