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
"""Worker pool API models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.base import OwnedResponseModel, RequestModel, ResponseModel
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.worker import WorkerScope


class WorkerPoolCreateRequest(RequestModel):
    """Worker pool create request."""

    name: str = Field(description="Worker pool name.")
    scope: WorkerScope = Field(
        default_factory=WorkerScope, description="Tasks this pool's workers claim."
    )


class WorkerPoolUpdateRequest(RequestModel):
    """Worker pool update request."""

    name: str | None = Field(default=None, description="New worker pool name.")
    scope: WorkerScope | None = Field(default=None, description="New claim scope.")


class WorkerPoolListParams(FilterableListParams):
    """Worker pool list params."""


class WorkerPoolResponse(OwnedResponseModel):
    """Worker pool response."""

    id: uuid.UUID = Field(description="Worker pool id.")
    name: str = Field(description="Worker pool name.")
    scope: WorkerScope = Field(description="Tasks this pool's workers claim.")


class WorkerPoolStatsResponse(ResponseModel):
    """Worker pool stats response."""

    pending_tasks: int = Field(description="Pending tasks the pool's scope matches.")
    in_flight_tasks: int = Field(
        description="Claimed or running tasks the pool's scope matches."
    )
    oldest_pending_seconds: float | None = Field(
        default=None, description="Age of the oldest matching pending task."
    )
    live_workers: int = Field(description="Pool workers inside the liveness window.")
