#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict workflow, cancellation, and deletion inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.base import JsonValue
from kitaru.mcp.models.common import MCPModel


class SessionImportRequest(MCPModel):
    """Import sessions from an existing payload blob."""

    payload_blob_id: uuid.UUID
    importer_id: uuid.UUID
    importer_version: int = Field(ge=1)
    agent_version_id: uuid.UUID
    params: dict[str, JsonValue] = Field(default_factory=dict)


class JobCancel(MCPModel):
    """Cancel one job."""

    operation: Literal["job"]
    id: uuid.UUID


class ExperimentRunCancel(MCPModel):
    """Cancel one experiment run."""

    operation: Literal["experiment_run"]
    id: uuid.UUID


WorkflowCancelRequest = Annotated[
    JobCancel | ExperimentRunCancel,
    Field(discriminator="operation"),
]


class DeleteRequest(MCPModel):
    """Delete one allowlisted exact resource."""

    kind: Literal["cohort", "cohort_version", "experiment", "experiment_run"]
    id: uuid.UUID
