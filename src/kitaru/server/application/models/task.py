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
"""Task filter and command models."""

import uuid
from typing import Any, NamedTuple

from pydantic import AwareDatetime

from kitaru.api_models.v1.task import TaskKind, TaskStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.domain.task import Task, TaskSpec


class ClaimedTask(NamedTuple):
    """Claimed task paired with its execution spec."""

    task: Task
    spec: TaskSpec


class TaskFilter(ListFilter):
    """Task list filter."""

    job_id: uuid.UUID | None = None
    kind: TaskKind | None = None
    status: TaskStatus | None = None
    worker_id: uuid.UUID | None = None
    stale_before: AwareDatetime | None = None


class TaskUpdate(FrozenModel):
    """Task update command."""

    status: TaskStatus | None = None
    attempt: int | None = None
    error: str | None = None
    result: Any = None


class TaskPolicy(FrozenModel):
    """Task execution policy."""

    heartbeat_timeout_seconds: int = 60
    retry_limit: int = 3
    sweep_batch_limit: int = 100
    evaluator_timeout_seconds: int = 300
    importer_timeout_seconds: int = 600
    max_result_bytes: int = 1024 * 1024
    evaluation_pair_limit: int = 100
