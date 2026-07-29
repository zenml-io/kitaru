"""Worker API models."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.api_models.v1.base import (
    JsonValue,
    ListParams,
    OwnedResponseModel,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.task import WorkerScope


class WorkerRuntime(RequestModel):
    """Detected worker runtime."""

    platform: str = Field(description="Runtime platform.")
    hostname: str | None = Field(default=None, description="Host name.")
    os: str | None = Field(default=None, description="Operating system.")
    arch: str | None = Field(default=None, description="Machine architecture.")
    python_version: str | None = Field(default=None, description="Python version.")
    kitaru_version: str | None = Field(default=None, description="Kitaru version.")
    namespace: str | None = Field(default=None, description="Kubernetes namespace.")
    pod: str | None = Field(default=None, description="Kubernetes pod name.")


class WorkerCreateRequest(RequestModel):
    """Worker registration request."""

    name: str = Field(description="Worker name.")
    scope: WorkerScope = Field(default_factory=WorkerScope, description="Claim scope.")
    runtime: WorkerRuntime = Field(description="Detected runtime.")
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Worker metadata."
    )


class WorkerListParams(ListParams):
    """Worker list params."""

    name: str | None = Field(default=None, description="Filter on worker name.")


class WorkerHeartbeatRequest(RequestModel):
    """Worker heartbeat request."""

    task_ids: list[uuid.UUID] = Field(description="In-flight task ids.")


class WorkerHeartbeatResponse(ResponseModel):
    """Worker heartbeat response."""

    cancel_task_ids: list[uuid.UUID] = Field(description="Tasks to cancel.")


class WorkerResponse(OwnedResponseModel):
    """Worker response."""

    id: uuid.UUID = Field(description="Worker id.")
    name: str = Field(description="Worker name.")
    scope: WorkerScope = Field(description="Claim scope.")
    runtime: WorkerRuntime = Field(description="Detected runtime.")
    last_seen_at: datetime = Field(description="Last activity time.")
    live: bool = Field(description="Whether the worker is live.")
    metadata: dict[str, JsonValue] = Field(description="Worker metadata.")
