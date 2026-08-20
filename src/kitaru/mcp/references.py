#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""MCP-local bounded reference resolution."""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, TypeVar, cast

from kitaru.api_models.v1.agent import AgentListParams, AgentResponse
from kitaru.api_models.v1.base import JsonValue, Page
from kitaru.api_models.v1.cohort import CohortListParams, CohortResponse
from kitaru.api_models.v1.evaluator import (
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorVersionResponse,
)
from kitaru.api_models.v1.experiment import ExperimentListParams, ExperimentResponse
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.importer import (
    ImporterListParams,
    ImporterResponse,
    ImporterVersionResponse,
)
from kitaru.client.resources.agents import AgentsResource
from kitaru.client.resources.cohorts import CohortsResource
from kitaru.client.resources.evaluators import EvaluatorsResource
from kitaru.client.resources.experiments import ExperimentsResource
from kitaru.client.resources.importers import ImportersResource

if TYPE_CHECKING:
    from kitaru.client.api_client import KitaruAPIClient


class ParentKind(StrEnum):
    """Parent resource families supporting UUID-or-name lookup."""

    AGENT = "agent"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    IMPORTER = "importer"
    EVALUATOR = "evaluator"


class PluginKind(StrEnum):
    """Plugin families addressed by parent and version number."""

    IMPORTER = "importer"
    EVALUATOR = "evaluator"


@dataclass(slots=True)
class ReferenceResolutionError(Exception):
    """Expected bounded-reference failure."""

    code: str
    message: str
    details: dict[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        """Initialize the `Exception` base with the error message."""
        Exception.__init__(self, self.message)


ParentResponse = (
    AgentResponse
    | CohortResponse
    | ExperimentResponse
    | ImporterResponse
    | EvaluatorResponse
)
ParentResource = (
    AgentsResource
    | CohortsResource
    | ExperimentsResource
    | ImportersResource
    | EvaluatorsResource
)
PluginVersionResponse = ImporterVersionResponse | EvaluatorVersionResponse
ParentT = TypeVar("ParentT", bound=ParentResponse)


async def resolve_parent(
    client: "KitaruAPIClient", kind: ParentKind, reference: str
) -> ParentResponse:
    """Resolve one parent with one direct get or one bounded list."""
    resource: ParentResource
    if kind is ParentKind.AGENT:
        resource = client.agents
    elif kind is ParentKind.COHORT:
        resource = client.cohorts
    elif kind is ParentKind.EXPERIMENT:
        resource = client.experiments
    elif kind is ParentKind.IMPORTER:
        resource = client.importers
    else:
        resource = client.evaluators
    return await _resolve_parent_resource(resource, kind, reference)


async def resolve_plugin_version(
    client: "KitaruAPIClient",
    kind: PluginKind,
    parent_id: uuid.UUID,
    version: int,
) -> PluginVersionResponse:
    """Resolve one plugin version through its direct endpoint."""
    if kind is PluginKind.IMPORTER:
        return await client.importers.get_version(parent_id, version)
    return await client.evaluators.get_version(parent_id, version)


async def _resolve_parent_resource(
    resource: ParentResource, kind: ParentKind, reference: str
) -> ParentResponse:
    normalized = reference.strip()
    if not normalized:
        raise ReferenceResolutionError(
            "invalid_arguments", f"{kind.value.title()} reference cannot be blank."
        )
    try:
        item_id = uuid.UUID(normalized)
    except ValueError:
        item_id = None
    if item_id is not None:
        return await resource.get(item_id)

    name_filter = FilterCondition(field="name", op=FilterOp.EQ, value=normalized)
    if kind is ParentKind.AGENT:
        page = await cast(AgentsResource, resource).list(
            AgentListParams(size=2, filter=name_filter)
        )
    elif kind is ParentKind.COHORT:
        page = await cast(CohortsResource, resource).list(
            CohortListParams(size=2, filter=name_filter)
        )
    elif kind is ParentKind.EXPERIMENT:
        page = await cast(ExperimentsResource, resource).list(
            ExperimentListParams(size=2, filter=name_filter)
        )
    elif kind is ParentKind.IMPORTER:
        page = await cast(ImportersResource, resource).list(
            ImporterListParams(size=2, filter=name_filter)
        )
    else:
        page = await cast(EvaluatorsResource, resource).list(
            EvaluatorListParams(size=2, filter=name_filter)
        )
    return _select_parent(page, kind, normalized)


def _select_parent(page: Page[ParentT], kind: ParentKind, normalized: str) -> ParentT:
    """Select one exact-name match from a bounded page."""
    matches = [item for item in page.items if item.name == normalized]
    if not matches:
        raise ReferenceResolutionError(
            "not_found", f"{kind.value.title()} {normalized!r} was not found."
        )
    if len(matches) > 1:
        raise ReferenceResolutionError(
            "conflict",
            f"More than one {kind.value} has the exact name {normalized!r}.",
            details={"ids": [str(item.id) for item in matches[:2]]},
        )
    return matches[0]
