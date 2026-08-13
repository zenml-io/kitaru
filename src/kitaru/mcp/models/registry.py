#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Strict registry-read tool inputs."""

import uuid
from typing import Annotated, Literal

from pydantic import Field

from kitaru.api_models.v1.filter import Filter
from kitaru.mcp.models.common import MCPModel, PageOptions
from kitaru.mcp.references import ParentKind

VersionedKind = Literal["agent", "cohort", "importer", "evaluator"]


class RegistryListRequest(PageOptions):
    """List one page of registry parents."""

    operation: Literal["list"]
    kind: ParentKind
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
    | RegistryGetVersionRequest,
    Field(discriminator="operation"),
]
