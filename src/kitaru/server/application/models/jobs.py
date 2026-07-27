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
"""Job filter and command models."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field, PositiveInt

from kitaru.server.base import FrozenModel
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import JobKind, JobStatus


class JobFilter(FrozenModel):
    """Job list filter."""

    experiment_run_id: uuid.UUID | None = None
    input_session_id: uuid.UUID | None = None
    kind: JobKind | None = None
    status: JobStatus | None = None
    standalone: bool | None = None
    worker_id: uuid.UUID | None = None
    execution_target: ExecutionTarget | None = None
    stale_before: datetime | None = None
    max_attempts: int | None = None
    page: PositiveInt = 1
    page_size: int = Field(default=20, ge=1, le=1000)


class SessionRunCreate(FrozenModel):
    """Session run create command."""

    agent_id: uuid.UUID | None = None
    agent_version_id: uuid.UUID | None = None
    inputs: Any = None
    name: str | None = None
    execution_target: ExecutionTarget | None = None


class ImportCreate(FrozenModel):
    """Import create command."""

    importer: str
    agent_id: uuid.UUID
    version: int | None = None
    payload_blob_id: uuid.UUID
    params: dict[str, Any] = Field(default_factory=dict)


class JobUpdate(FrozenModel):
    """Job update command."""

    status: JobStatus | None = None
    error: str | None = None
    result: Any = None
