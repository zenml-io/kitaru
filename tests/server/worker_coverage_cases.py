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
"""Coverage cases shared by the worker service and task repository tests."""

import uuid
from collections.abc import Callable
from typing import Any, NamedTuple

from conftest import UNSCOPED_WORKER_SCOPE
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import LabelSelector, WorkerClaim, WorkerScope
from kitaru.server.domain.task import AgentTask, EvaluationTask, ImportTask, Task


class CoverageIds(NamedTuple):
    """Coverage ids."""

    job_id: uuid.UUID
    other_job_id: uuid.UUID
    agent_version_id: uuid.UUID
    agent_version_id_2: uuid.UUID
    plugin_version_id: uuid.UUID
    import_id: uuid.UUID
    session_id: uuid.UUID


class CoverageCase(NamedTuple):
    """Coverage case."""

    name: str
    scope: Callable[[CoverageIds], WorkerScope]
    task: Callable[[CoverageIds], Task]
    covered: bool


def _agent_task(ids: CoverageIds, **overrides: Any) -> AgentTask:
    """Build an agent task pointed at the ids' primary agent version."""
    values: dict[str, Any] = {
        "job_id": ids.job_id,
        "agent_version_id": ids.agent_version_id,
    }
    values.update(overrides)
    return AgentTask(**values)


def _import_task(ids: CoverageIds, **overrides: Any) -> ImportTask:
    """Build an import task pointed at the ids' import."""
    values: dict[str, Any] = {
        "job_id": ids.job_id,
        "import_id": ids.import_id,
    }
    values.update(overrides)
    return ImportTask(**values)


def _evaluation_task(ids: CoverageIds) -> EvaluationTask:
    """Build an evaluation task pointed at the ids' plugin version and session."""
    return EvaluationTask(
        job_id=ids.job_id,
        plugin_version_id=ids.plugin_version_id,
        input_session_id=ids.session_id,
    )


COVERAGE_CASES: list[CoverageCase] = [
    CoverageCase(
        name="kind_claim_matches_import_task",
        scope=lambda ids: WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
        task=_import_task,
        covered=True,
    ),
    CoverageCase(
        name="kind_claim_rejects_other_kind",
        scope=lambda ids: WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
        task=_agent_task,
        covered=False,
    ),
    CoverageCase(
        name="pinned_agent_claim_matches_its_version",
        scope=lambda ids: WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.AGENT, agent_version_id=ids.agent_version_id)
            ]
        ),
        task=_agent_task,
        covered=True,
    ),
    CoverageCase(
        name="pinned_agent_claim_rejects_another_version",
        scope=lambda ids: WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.AGENT, agent_version_id=ids.agent_version_id)
            ]
        ),
        task=lambda ids: _agent_task(ids, agent_version_id=ids.agent_version_id_2),
        covered=False,
    ),
    CoverageCase(
        name="unversioned_agent_claim_spans_versions",
        scope=lambda ids: WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        task=lambda ids: _agent_task(ids, agent_version_id=ids.agent_version_id_2),
        covered=True,
    ),
    CoverageCase(
        name="job_pin_equal",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.IMPORTER)], job_id=ids.job_id
        ),
        task=_import_task,
        covered=True,
    ),
    CoverageCase(
        name="job_pin_different",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.IMPORTER)], job_id=ids.other_job_id
        ),
        task=_import_task,
        covered=False,
    ),
    CoverageCase(
        name="required_selector_satisfied",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=True)],
        ),
        task=lambda ids: _agent_task(ids, labels={"env": "prod"}),
        covered=True,
    ),
    CoverageCase(
        name="required_selector_missing_key",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=True)],
        ),
        task=_agent_task,
        covered=False,
    ),
    CoverageCase(
        name="required_selector_wrong_value",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=True)],
        ),
        task=lambda ids: _agent_task(ids, labels={"env": "dev"}),
        covered=False,
    ),
    CoverageCase(
        name="non_required_selector_missing_key",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=False)],
        ),
        task=_agent_task,
        covered=True,
    ),
    CoverageCase(
        name="non_required_selector_wrong_value",
        scope=lambda ids: WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT)],
            selectors=[LabelSelector(key="env", values=["prod"], required=False)],
        ),
        task=lambda ids: _agent_task(ids, labels={"env": "dev"}),
        covered=False,
    ),
    CoverageCase(
        name="full_scope_covers_evaluation_task",
        scope=lambda ids: UNSCOPED_WORKER_SCOPE,
        task=_evaluation_task,
        covered=True,
    ),
    CoverageCase(
        name="two_claims_combine_by_disjunction",
        scope=lambda ids: WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.EVALUATOR),
                WorkerClaim(kind=TaskKind.IMPORTER),
            ]
        ),
        task=_import_task,
        covered=True,
    ),
]
