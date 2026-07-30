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
"""Experiment run filter and command models."""

import uuid

from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.job import JobStatus
from kitaru.base import FrozenModel
from kitaru.server.base import ListFilter


class ExperimentRunFilter(ListFilter):
    """Experiment run list filter."""

    experiment_id: uuid.UUID | None = None
    status: ExperimentRunStatus | None = None
    tag: str | None = None


class ExperimentRunJobsFilter(ListFilter):
    """Experiment run jobs list filter."""

    status: JobStatus | None = None


class ExperimentRunCreate(FrozenModel):
    """Experiment run create command."""

    cohort_version_id: uuid.UUID
    agent_version_id: uuid.UUID
    evaluate_baselines: bool = False
