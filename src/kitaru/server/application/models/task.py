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
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, NamedTuple

from pydantic import AwareDatetime

from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter
from kitaru.server.domain.task import Task, TaskSpec
from kitaru.server.filtering import EQUALITY_OPS, NULLABLE_OPS, FilterField


class ClaimedTask(NamedTuple):
    """Claimed task paired with its execution spec and job owner."""

    task: Task
    spec: TaskSpec
    job_owner_id: uuid.UUID


class TaskSettlementStats(FrozenModel):
    """Task settlement stats."""

    total: int = 0
    non_terminal: int = 0
    canceled: int = 0
    counted_failures: int = 0
    abort_failures: int = 0
    first_failure_error: str | None = None
    kinds: tuple[TaskKind, ...] = ()

    @property
    def drained(self) -> bool:
        """Whether the job holds tasks and every one is terminal.

        Returns:
            Whether the job holds tasks and every one is terminal.
        """
        return self.total > 0 and self.non_terminal == 0

    @classmethod
    def from_tasks(cls, tasks: Sequence[Task]) -> "TaskSettlementStats":
        """Count tasks into settlement stats.

        Args:
            tasks: Every task of one job, in creation order.

        Returns:
            Task settlement stats.
        """
        counted = [task for task in tasks if task.counted_hard_failure]
        return cls(
            total=len(tasks),
            non_terminal=sum(1 for task in tasks if not task.terminal),
            canceled=sum(1 for task in tasks if task.status is TaskStatus.CANCELED),
            counted_failures=len(counted),
            abort_failures=sum(
                1 for task in counted if task.on_failure is TaskOnFailure.ABORT
            ),
            first_failure_error=counted[0].error if counted else None,
            kinds=tuple(dict.fromkeys(task.kind for task in tasks)),
        )


class TaskFilter(ListFilter):
    """Task list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "job_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
        "kind": FilterField(value_type=TaskKind, ops=EQUALITY_OPS),
        "status": FilterField(value_type=TaskStatus, ops=EQUALITY_OPS),
        "worker_id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS | NULLABLE_OPS),
    }

    job_id: uuid.UUID | None = None
    stale_before: AwareDatetime | None = None


class JobTasksFilter(TaskFilter):
    """Job tasks list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = {
        "kind": TaskFilter.filterable_fields["kind"],
        "status": TaskFilter.filterable_fields["status"],
    }


class TaskUpdate(FrozenModel):
    """Task update command."""

    status: TaskStatus | None = None
    error: str | None = None
    result: Any = None


class TaskPolicy(FrozenModel):
    """Task execution policy."""

    heartbeat_timeout_seconds: int = 60
    retry_limit: int = 3
    sweep_batch_limit: int = 100
    settlement_batch_limit: int = 100
    settlement_grace_seconds: int = 30
    evaluator_timeout_seconds: int = 300
    importer_timeout_seconds: int = 600
    max_result_bytes: int = 1024 * 1024
    evaluation_pair_limit: int = 100
