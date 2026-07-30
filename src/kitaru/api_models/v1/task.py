#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Task lifecycle, claim, and spec API models."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self

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
    """Kind of work a task runs."""

    AGENT = "agent"
    EVALUATOR = "evaluator"
    IMPORTER = "importer"


class TaskOnFailure(StrEnum):
    """What a task's hard failure does to the rest of its job."""

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


class TaskResponse(TimestampedResponseModel):
    """Task response."""

    id: uuid.UUID = Field(description="Task id.")
    job_id: uuid.UUID = Field(description="Owning job.")
    kind: TaskKind = Field(description="Kind of work the task runs.")
    status: TaskStatus = Field(description="Task status.")
    on_failure: TaskOnFailure = Field(
        description="Effect of a hard failure on the job."
    )
    attempt: int = Field(description="Current attempt number.")
    labels: dict[str, str] = Field(
        description="Labels matched by worker scope selectors."
    )
    agent_version_id: uuid.UUID | None = Field(
        default=None, description="Agent version run by an agent task."
    )
    plugin_version_id: uuid.UUID | None = Field(
        default=None, description="Plugin version run by an evaluator or importer task."
    )
    payload_blob_id: uuid.UUID | None = Field(
        default=None, description="Payload blob for an importer task."
    )
    input_session_id: uuid.UUID | None = Field(
        default=None, description="Input session for an evaluator task."
    )
    agent_id: uuid.UUID | None = Field(
        default=None, description="Agent an importer task creates sessions under."
    )
    worker_id: uuid.UUID | None = Field(
        default=None, description="Worker that claimed the task."
    )
    result_session_id: uuid.UUID | None = Field(
        default=None, description="Session an agent task produced."
    )
    claimed_at: datetime | None = Field(
        default=None, description="Time the task was claimed."
    )
    heartbeat_at: datetime | None = Field(
        default=None, description="Time of the worker's last heartbeat."
    )
    cancel_requested_at: datetime | None = Field(
        default=None, description="Time cancellation was requested."
    )
    started_at: datetime | None = Field(
        default=None, description="Time execution started."
    )
    ended_at: datetime | None = Field(default=None, description="Time execution ended.")
    error: str | None = Field(default=None, description="Error from a failed task.")
    result: JsonValue = Field(
        description="Task result, diagnostic output on a non-completed task."
    )


class TaskUpdateRequest(RequestModel):
    """Task update request."""

    status: TaskStatus | None = Field(default=None, description="New task status.")
    attempt: int | None = Field(
        default=None, description="Attempt this transition is fenced by."
    )
    error: str | None = Field(default=None, description="New error.")
    result: JsonValue | None = Field(default=None, description="New task result.")


class TaskListParams(ListParams):
    """Task list params."""

    job_id: uuid.UUID | None = Field(default=None, description="Filter on owning job.")
    kind: list[TaskKind] | None = Field(
        default=None,
        description="Filter on task kind, repeatable to match any of several.",
    )
    status: TaskStatus | None = Field(
        default=None, description="Filter on task status."
    )
    worker_id: uuid.UUID | None = Field(
        default=None, description="Filter on the claiming worker."
    )


class LabelSelector(FrozenModel):
    """Label selector."""

    key: str = Field(description="Label key.")
    values: list[str] = Field(min_length=1, description="Values the label may take.")
    required: bool = Field(
        default=False, description="Whether a task lacking the key fails the match."
    )


class WorkerScope(FrozenModel):
    """Worker scope."""

    kinds: list[TaskKind] | None = Field(
        default=None, description="Task kinds the worker claims."
    )
    selectors: list[LabelSelector] | None = Field(
        default=None,
        description="Label selectors the worker claims, combined by conjunction.",
    )
    job_id: uuid.UUID | None = Field(
        default=None, description="Job the worker claims tasks from."
    )

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        """Reject empty scope lists and duplicate selector keys.

        Raises:
            ValueError: kinds or selectors is set but empty, or two selectors
                share a key.

        Returns:
            The validated scope.
        """
        if self.kinds is not None and not self.kinds:
            raise ValueError("kinds must not be empty when set")
        if self.selectors is not None:
            if not self.selectors:
                raise ValueError("selectors must not be empty when set")
            keys = [selector.key for selector in self.selectors]
            if len(set(keys)) != len(keys):
                raise ValueError("selector keys must be unique")
        return self


class TaskClaimRequest(RequestModel):
    """Task claim request."""

    worker_id: uuid.UUID = Field(description="Claiming worker.")
    max_tasks: int = Field(
        ge=1, le=100, description="Maximum number of tasks to claim."
    )


class TaskRunSpec(ResponseModel):
    """Task run spec."""

    command: str = Field(description="Shell command to run.")
    working_dir: str | None = Field(default=None, description="Working directory.")
    env: dict[str, str] = Field(description="Process environment from the run spec.")


class ScriptPluginSpec(ResponseModel):
    """Script plugin spec."""

    type: Literal["script"] = Field(default="script")
    entrypoint: str = Field(description="Attribute in the file.")
    blob_id: uuid.UUID = Field(description="Blob holding the script.")
    sha256: str = Field(description="Blob content hash.")


class PackagePluginSpec(ResponseModel):
    """Package plugin spec."""

    type: Literal["package"] = Field(default="package")
    entrypoint: str = Field(description="Module and attribute, as module:attribute.")
    requirement: str = Field(description="Pinned PEP 508 requirement.")


PluginSpec = Annotated[
    ScriptPluginSpec | PackagePluginSpec, Field(discriminator="type")
]


class PayloadSpec(ResponseModel):
    """Payload spec."""

    blob_id: uuid.UUID = Field(description="Blob holding the payload.")
    sha256: str = Field(description="Blob content hash.")


class AgentTaskDetails(ResponseModel):
    """Agent task details."""

    kind: Literal["agent"] = Field(default="agent")
    inputs: JsonValue = Field(description="Inputs passed to the agent's command.")


class EvaluationTaskDetails(ResponseModel):
    """Evaluation task details."""

    kind: Literal["evaluator"] = Field(default="evaluator")
    evaluator_name: str = Field(description="Name the evaluator emits results under.")
    params: dict[str, JsonValue] = Field(
        description="Parameters passed to the evaluator."
    )
    plugin: PluginSpec = Field(description="Evaluator plugin to load.")
    input_session_id: uuid.UUID = Field(description="Session being scored.")


class ImportTaskDetails(ResponseModel):
    """Import task details."""

    kind: Literal["importer"] = Field(default="importer")
    plugin: PluginSpec = Field(description="Importer plugin to load.")
    payload: PayloadSpec = Field(description="Payload to parse.")
    provider: str | None = Field(
        default=None, description="Source system named on the import."
    )
    agent_id: uuid.UUID = Field(
        description="Agent imported sessions are created under."
    )
    params: dict[str, JsonValue] = Field(
        description="Parameters passed to the importer."
    )


TaskDetails = Annotated[
    AgentTaskDetails | EvaluationTaskDetails | ImportTaskDetails,
    Field(discriminator="kind"),
]


class TaskSpecResponse(ResponseModel):
    """Task spec response."""

    task_id: uuid.UUID = Field(description="Task the spec belongs to.")
    kind: TaskKind = Field(description="Kind of work the task runs.")
    timeout_seconds: int = Field(description="Process timeout.")
    run: TaskRunSpec | None = Field(
        default=None,
        description="Command to run, unset for evaluator and importer tasks.",
    )
    env: dict[str, str] = Field(description="Creator-set process environment extras.")
    secret_env: dict[str, str] = Field(
        description="Secrets merged into the process environment."
    )
    details: TaskDetails = Field(description="Kind-specific task details.")


class TaskWithSpec(ResponseModel):
    """Task with spec."""

    task: TaskResponse = Field(description="Task.")
    spec: TaskSpecResponse = Field(description="Task spec.")


class TaskClaimResponse(ResponseModel):
    """Task claim response."""

    tasks: list[TaskWithSpec] = Field(description="Claimed tasks.")
