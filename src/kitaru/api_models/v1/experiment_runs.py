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
"""Experiment run API models."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.agent_versions import ExecutionTarget
from kitaru.api_models.v1.base import RequestModel, ResponseModel


class ExperimentRunStatus(StrEnum):
    """Experiment run status."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELING = "canceling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ExperimentRunProgress(ResponseModel):
    """Experiment run progress."""

    pending: int = Field(description="Pending job count.")
    claimed: int = Field(description="Claimed job count.")
    running: int = Field(description="Running job count.")
    scoring: int = Field(description="Scoring job count.")
    completed: int = Field(description="Completed job count.")
    failed: int = Field(description="Failed job count.")
    timed_out: int = Field(description="Timed out job count.")
    canceled: int = Field(description="Canceled job count.")
    total: int = Field(description="Total job count.")


class ExperimentRunCreateRequest(RequestModel):
    """Experiment run create request."""

    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the agent version to execute, the latest runnable "
        "version when omitted.",
    )
    score_baselines: bool = Field(
        default=False,
        description="Whether the runner also scores originals missing scores.",
    )
    execution_target: ExecutionTarget | None = Field(
        default=None,
        description="Execution target, the agent version's default when omitted.",
    )


class ExperimentRunResponse(ResponseModel):
    """Experiment run response."""

    id: uuid.UUID = Field(description="Experiment run id.")
    owner_id: uuid.UUID = Field(description="Id of the owning account.")
    experiment_id: uuid.UUID = Field(description="Id of the experiment.")
    number: int = Field(description="Per-experiment run number.")
    status: ExperimentRunStatus = Field(description="Run status.")
    agent_version_id: uuid.UUID = Field(description="Id of the agent version.")
    score_baselines: bool = Field(
        description="Whether the runner also scores originals missing scores."
    )
    execution_target: ExecutionTarget = Field(description="Execution target.")
    executor_handle: str | None = Field(description="Executor handle.")
    started_at: datetime | None = Field(description="Execution start time.")
    ended_at: datetime | None = Field(description="Execution end time.")
    summary: dict[str, Any] | None = Field(
        description="Aggregate diff, written at completion."
    )
    error: str | None = Field(description="Error message.")
    progress: ExperimentRunProgress = Field(description="Job counts by status.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")
