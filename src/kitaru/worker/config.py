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
"""Worker configuration read from the environment."""

import uuid
from pathlib import Path
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.worker.heartbeat import DEFAULT_HEARTBEAT_INTERVAL_SECONDS

RUN_POLL_INTERVAL_SECONDS = 2.0

DEFAULT_CONCURRENCY = 10


class WorkerConfig(BaseSettings):
    """Worker configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_WORKER_", env_nested_delimiter="__", frozen=True
    )

    id: uuid.UUID | None = None
    name: str | None = None
    scope: WorkerScope = WorkerScope(
        claims=[WorkerClaim(kind=kind) for kind in TaskKind]
    )
    concurrency: int = Field(default=DEFAULT_CONCURRENCY, ge=1)
    claim_batch_size: int | None = Field(default=None, ge=1)
    poll_interval: float = Field(default=RUN_POLL_INTERVAL_SECONDS, gt=0)
    heartbeat_interval: float = Field(default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS, gt=0)
    timeout: float | None = Field(default=None, gt=0)
    drain_timeout: float | None = Field(default=None, gt=0)
    blob_cache_root: Path | None = None
    payload_cache_root: Path | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
