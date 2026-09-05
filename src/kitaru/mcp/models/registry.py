#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict registry-read tool inputs."""

import uuid
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from kitaru.api_models.v1.filter import Filter
from kitaru.mcp.models.common import MCPModel, PageOptions
from kitaru.mcp.references import ParentKind

VersionedKind = Literal["agent", "cohort", "importer", "evaluator", "analyzer"]


class RegistryListRequest(PageOptions):
    """List one page of registry parents, tags, or workers."""

    operation: Literal["list"]
    kind: ParentKind | Literal["tag", "worker"]
    filter: Filter | None = None


class RegistryGetRequest(MCPModel):
    """Get a registry parent by UUID or exact case-sensitive name."""

    operation: Literal["get"]
    kind: ParentKind
    reference: str = Field(min_length=1)


class RegistryListVersionsRequest(PageOptions):
    """List one bounded page of versions for an exact parent reference."""

    operation: Literal["list_versions"]
    kind: VersionedKind
    parent_reference: str = Field(min_length=1)
    filter: Filter | None = None

    @model_validator(mode="after")
    def _validate_filter_support(self) -> Self:
        """Reject filters for plugin version endpoints that do not support them."""
        if (
            self.kind in {"importer", "evaluator", "analyzer"}
            and self.filter is not None
        ):
            raise ValueError("filter is supported only for agent and cohort versions")
        return self


class RegistryGetWorkerRequest(MCPModel):
    """Get one worker by its exact UUID."""

    operation: Literal["get_worker"]
    worker_id: uuid.UUID


class RegistryGetVersionRequest(MCPModel):
    """Get one exact asset or plugin version."""

    operation: Literal["get_version"]
    kind: VersionedKind
    version_id: uuid.UUID | None = None
    parent_reference: str | None = None
    version: int | None = Field(default=None, ge=1)


RegistryReadRequest = Annotated[
    RegistryListRequest
    | RegistryGetRequest
    | RegistryListVersionsRequest
    | RegistryGetWorkerRequest
    | RegistryGetVersionRequest,
    Field(discriminator="operation"),
]
