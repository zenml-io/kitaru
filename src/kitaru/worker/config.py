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
"""Worker configuration."""

from pathlib import Path
from typing import Any

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from kitaru.api_models.v1.jobs import WorkerScope
from kitaru.worker.heartbeat import DEFAULT_HEARTBEAT_INTERVAL_SECONDS

RUN_POLL_INTERVAL_SECONDS = 2.0

# Scope list fields accepted as a comma-separated env var, in addition to
# the JSON array pydantic-settings decodes by default.
_COMMA_SEPARATED_SCOPE_FIELDS = ("agent_version_ids", "kinds")


class WorkerConfig(BaseSettings):
    """Worker configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KITARU_WORKER_", env_nested_delimiter="__", frozen=True
    )

    name: str | None = None  # default: sanitized hostname-pid
    scope: WorkerScope = WorkerScope()
    concurrency: int = 1
    claim_batch_size: int | None = None  # default: free slots per claim
    poll_interval: float = RUN_POLL_INTERVAL_SECONDS
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    timeout: float | None = None  # wall clock lifetime, unbounded when unset
    blob_cache_root: Path | None = None
    payload_cache_root: Path | None = None

    @model_validator(mode="before")
    @classmethod
    def _split_comma_separated_scope_lists(cls, data: Any) -> Any:
        """Split comma-separated scope list env values left as raw strings.

        Args:
            data: Raw settings data merged from the constructor and the
                environment.

        Returns:
            Data with comma-separated scope list strings split into lists.
        """
        if not isinstance(data, dict):
            return data
        scope = data.get("scope")
        if not isinstance(scope, dict):
            return data
        for field in _COMMA_SEPARATED_SCOPE_FIELDS:
            value = scope.get(field)
            if isinstance(value, str):
                scope[field] = [
                    item.strip() for item in value.split(",") if item.strip()
                ]
        return data
