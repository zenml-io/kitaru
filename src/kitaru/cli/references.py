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
"""Bounded UUID-or-name reference lookup for CLI commands."""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.analyzer import AnalyzerListParams
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.importer import ImporterListParams


class ParentKind(StrEnum):
    """Parent resource families supporting bounded reference lookup."""

    AGENT = "agent"
    ANALYZER = "analyzer"
    COHORT = "cohort"
    EXPERIMENT = "experiment"
    IMPORTER = "importer"
    EVALUATOR = "evaluator"


@dataclass(slots=True)
class ReferenceResolutionError(Exception):
    """Expected bounded-reference failure before CLI rendering."""

    code: str
    message: str
    details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Initialize the base exception message."""
        Exception.__init__(self, self.message)


async def resolve_parent(client: Any, kind: ParentKind, reference: str) -> Any:
    """Resolve one parent through a client resource."""
    resource = getattr(client, f"{kind.value}s")
    return await resolve_parent_resource(resource, kind, reference)


async def resolve_parent_resource(
    resource: Any, kind: ParentKind, reference: str
) -> Any:
    """Resolve one parent with one direct get or one size-two list request."""
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

    params_type = {
        ParentKind.AGENT: AgentListParams,
        ParentKind.ANALYZER: AnalyzerListParams,
        ParentKind.COHORT: CohortListParams,
        ParentKind.EXPERIMENT: ExperimentListParams,
        ParentKind.IMPORTER: ImporterListParams,
        ParentKind.EVALUATOR: EvaluatorListParams,
    }[kind]
    params = params_type(
        size=2,
        filter=FilterCondition(field="name", op=FilterOp.EQ, value=normalized),
    )
    page = await resource.list(params)
    matches = [item for item in page.items if item.name == normalized]
    label = kind.value.title()
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
