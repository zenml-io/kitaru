#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict activity-read tool inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.filter import Filter
from kitaru.mcp.models.common import MCPModel, PageOptions

ActivityKind = Literal["session", "replay", "evaluation", "experiment_run", "job"]


class ActivityListRequest(PageOptions):
    """List one page of activity records."""

    operation: Literal["list"]
    kind: ActivityKind
    filter: Filter | None = None


class ActivityGetRequest(MCPModel):
    """Get one activity record by exact UUID."""

    operation: Literal["get"]
    kind: ActivityKind
    id: uuid.UUID


class SessionNodesRequest(MCPModel):
    """List a session's child nodes."""

    operation: Literal["list_children"]
    kind: Literal["session_nodes"]
    parent_id: uuid.UUID
    cursor: str | None = None
    size: int = Field(default=20, ge=1, le=100)
    include_payloads: bool = False


class SortedChildrenRequest(PageOptions):
    """List an experiment run's jobs or a job's tasks, sorted."""

    operation: Literal["list_children"]
    kind: Literal["experiment_run_jobs", "job_tasks"]
    parent_id: uuid.UUID


ActivityChildrenRequest = Annotated[
    SessionNodesRequest | SortedChildrenRequest,
    Field(discriminator="kind"),
]


ActivityReadRequest = Annotated[
    ActivityListRequest | ActivityGetRequest | ActivityChildrenRequest,
    Field(discriminator="operation"),
]
