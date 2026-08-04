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
"""Bounded, frontend-neutral reference resolution."""

import uuid
from collections.abc import Awaitable, Callable
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
    """Parent resource families supporting bounded UUID-or-name lookup."""

    AGENT = "agent"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    IMPORTER = "importer"
    EVALUATOR = "evaluator"


class PluginKind(StrEnum):
    """Plugin families with versions addressed by parent and number."""

    IMPORTER = "importer"
    EVALUATOR = "evaluator"


@dataclass(slots=True)
class ReferenceResolutionError(Exception):
    """Expected bounded-reference failure independent of frontend rendering."""

    code: str
    message: str
    details: dict[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        """Initialize the base exception message."""
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
ParamsT = TypeVar(
    "ParamsT",
    AgentListParams,
    CohortListParams,
    ExperimentListParams,
    ImporterListParams,
    EvaluatorListParams,
)


async def resolve_parent(
    client: "KitaruAPIClient", kind: ParentKind, reference: str
) -> ParentResponse:
    """Resolve one parent with at most one direct get or one bounded list."""
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
    return await resolve_parent_resource(resource, kind, reference)


async def resolve_parent_resource(
    resource: ParentResource, kind: ParentKind, reference: str
) -> ParentResponse:
    """Resolve one parent through an explicitly supplied typed resource."""
    normalized, item_id = _parse_reference(kind, reference)
    if item_id is not None:
        if kind is ParentKind.AGENT:
            return await cast(AgentsResource, resource).get(item_id)
        if kind is ParentKind.COHORT:
            return await cast(CohortsResource, resource).get(item_id)
        if kind is ParentKind.EXPERIMENT:
            return await cast(ExperimentsResource, resource).get(item_id)
        if kind is ParentKind.IMPORTER:
            return await cast(ImportersResource, resource).get(item_id)
        return await cast(EvaluatorsResource, resource).get(item_id)

    if kind is ParentKind.AGENT:
        agents = cast(AgentsResource, resource)
        return await _resolve_parent(
            agents.list,
            AgentListParams(size=2, filter=_get_name_filter(normalized)),
            kind,
            normalized,
        )
    if kind is ParentKind.COHORT:
        cohorts = cast(CohortsResource, resource)
        return await _resolve_parent(
            cohorts.list,
            CohortListParams(size=2, filter=_get_name_filter(normalized)),
            kind,
            normalized,
        )
    if kind is ParentKind.EXPERIMENT:
        experiments = cast(ExperimentsResource, resource)
        return await _resolve_parent(
            experiments.list,
            ExperimentListParams(size=2, filter=_get_name_filter(normalized)),
            kind,
            normalized,
        )
    if kind is ParentKind.IMPORTER:
        importers = cast(ImportersResource, resource)
        return await _resolve_parent(
            importers.list,
            ImporterListParams(size=2, filter=_get_name_filter(normalized)),
            kind,
            normalized,
        )
    evaluators = cast(EvaluatorsResource, resource)
    return await _resolve_parent(
        evaluators.list,
        EvaluatorListParams(size=2, filter=_get_name_filter(normalized)),
        kind,
        normalized,
    )


async def resolve_plugin_version(
    client: "KitaruAPIClient",
    kind: PluginKind,
    parent_id: uuid.UUID,
    version: int,
) -> PluginVersionResponse:
    """Resolve one plugin version through its direct parent/version endpoint."""
    if version < 1:
        raise ReferenceResolutionError(
            "invalid_arguments", "Plugin version must be a positive integer."
        )
    if kind is PluginKind.IMPORTER:
        return await client.importers.get_version(parent_id, version)
    return await client.evaluators.get_version(parent_id, version)


async def _resolve_parent(
    list_page: Callable[[ParamsT | None], Awaitable[Page[ParentT]]],
    params: ParamsT,
    kind: ParentKind,
    normalized: str,
) -> ParentT:
    """Resolve a name with exactly one bounded list request."""
    label = kind.value.title()
    page = await list_page(params)
    matches = [item for item in page.items if item.name == normalized]
    if not matches:
        raise ReferenceResolutionError(
            "not_found", f"{label} {normalized!r} was not found."
        )
    if len(matches) > 1:
        raise ReferenceResolutionError(
            "conflict",
            f"More than one {kind.value} has the exact name {normalized!r}.",
            details={"ids": [str(item.id) for item in matches[:2]]},
        )
    return matches[0]


def _parse_reference(kind: ParentKind, reference: str) -> tuple[str, uuid.UUID | None]:
    """Normalize a reference and classify the direct UUID path."""
    normalized = reference.strip()
    if not normalized:
        raise ReferenceResolutionError(
            "invalid_arguments",
            f"{kind.value.title()} reference cannot be blank.",
        )
    try:
        return normalized, uuid.UUID(normalized)
    except ValueError:
        return normalized, None


def _get_name_filter(reference: str) -> FilterCondition:
    """Build the exact-name filter shared by every parent family."""
    return FilterCondition(field="name", op=FilterOp.EQ, value=reference.strip())
