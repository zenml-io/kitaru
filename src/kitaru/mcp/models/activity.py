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
ChildKind = Literal["session_nodes", "experiment_run_jobs", "job_tasks"]


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


class ActivityChildrenRequest(PageOptions):
    """List one page of a bounded child collection."""

    operation: Literal["list_children"]
    kind: ChildKind
    parent_id: uuid.UUID
    include_payloads: bool = False


ActivityReadRequest = Annotated[
    ActivityListRequest | ActivityGetRequest | ActivityChildrenRequest,
    Field(discriminator="operation"),
]
