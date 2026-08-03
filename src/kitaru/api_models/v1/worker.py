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
"""Worker API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import (
    OwnedResponseModel,
    PlainSerializedSecretStr,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.task import WorkerScope


class WorkerRuntime(RequestModel):
    """Worker runtime."""

    platform: str = Field(
        description="Runtime platform, e.g. kubernetes, docker, bare."
    )
    hostname: str | None = Field(default=None, description="Reported hostname.")
    os: str | None = Field(default=None, description="Reported operating system.")
    arch: str | None = Field(default=None, description="Reported architecture.")
    python_version: str | None = Field(
        default=None, description="Reported Python version."
    )
    kitaru_version: str | None = Field(
        default=None, description="Reported Kitaru version."
    )
    namespace: str | None = Field(
        default=None, description="Reported Kubernetes namespace."
    )
    pod: str | None = Field(default=None, description="Reported Kubernetes pod name.")


class WorkerCreateRequest(RequestModel):
    """Worker create request."""

    name: str = Field(description="Worker name.")
    scope: WorkerScope = Field(description="Tasks this worker is willing to claim.")
    runtime: WorkerRuntime = Field(description="Runtime the worker reports.")
    metadata: dict[str, str] = Field(
        default_factory=dict, description="Arbitrary metadata."
    )


class WorkerListParams(FilterableListParams):
    """Worker list params."""


class WorkerHeartbeatRequest(RequestModel):
    """Worker heartbeat request."""

    task_ids: list[uuid.UUID] = Field(description="Tasks the worker currently holds.")


class WorkerHeartbeatResponse(ResponseModel):
    """Worker heartbeat response."""

    cancel_task_ids: list[uuid.UUID] = Field(
        description="Held tasks whose cancellation was requested."
    )


class WorkerResponse(OwnedResponseModel):
    """Worker response."""

    id: uuid.UUID = Field(description="Worker id.")
    name: str = Field(description="Worker name.")
    scope: WorkerScope = Field(description="Tasks this worker is willing to claim.")
    runtime: WorkerRuntime = Field(description="Runtime the worker reports.")
    last_seen_at: datetime = Field(description="Time of the worker's last heartbeat.")
    live: bool = Field(description="Whether the worker is considered alive.")
    metadata: dict[str, str] = Field(description="Arbitrary metadata.")


class WorkerRegistrationResponse(ResponseModel):
    """Worker registration response."""

    worker: WorkerResponse = Field(description="Registered worker.")
    token: PlainSerializedSecretStr = Field(
        description="Bearer token scoped to this worker."
    )
    token_expires_at: datetime = Field(description="Time the token expires.")
