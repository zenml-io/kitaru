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
"""Sample data models."""

import uuid

from pydantic import Field

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.base import FrozenModel
from kitaru.server.application.models.agent import AgentCreate
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.session import SessionCreate
from kitaru.server.application.models.session_node import SessionNodeUpsert
from kitaru.server.domain.replay_config import ToolPolicy


class SampleSession(FrozenModel):
    """Sample session."""

    session: SessionCreate
    nodes: list[SessionNodeUpsert]
    evaluations: list[EvaluationResult]


class SampleHighlight(FrozenModel):
    """Sample highlight."""

    node_index: int
    description: str


class SampleQuestion(FrozenModel):
    """Sample question."""

    key: str
    question: str
    highlights: list[SampleHighlight] = Field(default_factory=list)


class SampleInvestigationSession(FrozenModel):
    """Sample investigation session."""

    external_id: str
    questions: list[SampleQuestion]


class SampleInvestigation(FrozenModel):
    """Sample investigation."""

    name: str
    description: str | None = None
    sessions: list[SampleInvestigationSession]


class SampleCohort(FrozenModel):
    """Sample cohort."""

    name: str
    description: str | None = None
    display_version: str
    member_external_ids: list[str]


class SampleEvaluator(FrozenModel):
    """Sample evaluator."""

    name: str
    description: str | None = None
    display_version: str
    source: str


class SampleExperiment(FrozenModel):
    """Sample experiment."""

    name: str
    description: str | None = None
    tool_policy: ToolPolicy
    evaluators: list[EvaluatorConfigInput]


class SampleData(FrozenModel):
    """Sample data."""

    agent: AgentCreate
    sessions: list[SampleSession]
    session_tag: str
    cohort: SampleCohort
    evaluator: SampleEvaluator
    experiment: SampleExperiment
    investigation: SampleInvestigation


class SeededSession(FrozenModel):
    """Seeded session."""

    id: uuid.UUID
    node_ids: dict[int, uuid.UUID]
