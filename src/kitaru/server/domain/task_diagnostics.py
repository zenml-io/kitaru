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
"""Pending-task worker coverage snapshots."""

import uuid

from kitaru.api_models.v1.task import TaskKind, TaskWaitReason
from kitaru.server.domain.base import DomainModel


class TaskDiagnostic(DomainModel):
    """Pending tasks sharing requirements and worker coverage."""

    kind: TaskKind
    agent_version_id: uuid.UUID | None
    labels: dict[str, str]
    reasons: list[TaskWaitReason]
    task_count: int = 0
    task_ids: list[uuid.UUID]
    job_ids: list[uuid.UUID]


class TaskDiagnostics(DomainModel):
    """Heartbeat-based snapshot of pending work."""

    live_worker_count: int
    pending_task_count: int
    groups: list[TaskDiagnostic]
    truncated: bool
