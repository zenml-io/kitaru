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
from typing import Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.base import (
    OwnedResponseModel,
    PlainSerializedSecretStr,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.filter import FilterableListParams
from kitaru.api_models.v1.task import TaskKind
from kitaru.base import FrozenModel

_ALL_TASK_KINDS = frozenset(TaskKind)


class LabelSelector(FrozenModel):
    """Label selector."""

    key: str = Field(description="Label key.")
    values: list[str] = Field(min_length=1, description="Values the label may take.")
    required: bool = Field(
        default=False, description="Whether a task lacking the key fails the match."
    )


class WorkerClaim(FrozenModel):
    """Worker claim."""

    kind: TaskKind = Field(description="Task kind the claim covers.")
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Agent version the claim covers, None claims every agent version.",
    )

    @model_validator(mode="after")
    def _validate_claim(self) -> Self:
        """Reject an agent version on a non-agent claim.

        Raises:
            ValueError: agent_version_id is set on a non-agent kind.

        Returns:
            The validated claim.
        """
        if self.agent_version_id is not None and self.kind is not TaskKind.AGENT:
            raise ValueError("agent_version_id requires the agent kind")
        return self


class WorkerScope(FrozenModel):
    """Worker scope."""

    claims: list[WorkerClaim] = Field(
        min_length=1,
        description="Claims the worker serves, combined by disjunction.",
    )
    selectors: list[LabelSelector] | None = Field(
        default=None,
        description="Label selectors the worker claims, combined by conjunction.",
    )
    job_id: uuid.UUID | None = Field(
        default=None, description="Job the worker claims tasks from."
    )

    @property
    def claims_everything(self) -> bool:
        """Whether the claims cover every kind with no agent pin.

        Returns:
            Whether the claims cover every kind with no agent pin.
        """
        return {claim.kind for claim in self.claims} == _ALL_TASK_KINDS and all(
            claim.agent_version_id is None for claim in self.claims
        )

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        """Reject redundant claims, empty selector lists, and duplicate selector keys.

        Raises:
            ValueError: Two claims are equal, an agent version claim is
                subsumed by an unversioned agent claim, selectors is set but
                empty, or two selectors share a key.

        Returns:
            The validated scope.
        """
        seen: set[tuple[TaskKind, uuid.UUID | None]] = set()
        for claim in self.claims:
            entry = (claim.kind, claim.agent_version_id)
            if entry in seen:
                raise ValueError("claims must be unique")
            seen.add(entry)
        if (TaskKind.AGENT, None) in seen and any(
            claim.agent_version_id is not None for claim in self.claims
        ):
            raise ValueError(
                "agent version claims are redundant with an unversioned agent claim"
            )
        if self.selectors is not None:
            if not self.selectors:
                raise ValueError("selectors must not be empty when set")
            keys = [selector.key for selector in self.selectors]
            if len(set(keys)) != len(keys):
                raise ValueError("selector keys must be unique")
        return self


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
