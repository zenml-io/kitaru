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

from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict

from kitaru.api_models.v1.task import WorkerScope
from kitaru.worker.heartbeat import DEFAULT_HEARTBEAT_INTERVAL_SECONDS

RUN_POLL_INTERVAL_SECONDS = 2.0


class WorkerConfig(BaseSettings):
    """Worker configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_WORKER_", env_nested_delimiter="__", frozen=True
    )

    name: str | None = None
    scope: WorkerScope = WorkerScope()
    concurrency: int = 1
    claim_batch_size: int | None = None
    poll_interval: float = RUN_POLL_INTERVAL_SECONDS
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    timeout: float | None = None
    blob_cache_root: Path | None = None
    payload_cache_root: Path | None = None
    metadata: dict[str, Any] = {}
