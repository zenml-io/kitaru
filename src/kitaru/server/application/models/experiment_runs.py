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
"""Experiment run filter models."""

import uuid

from pydantic import Field, PositiveInt

from kitaru.server.base import FrozenModel
from kitaru.server.domain.experiment_run import ExperimentRunStatus
from kitaru.server.domain.replay import ReplayStatus


class ExperimentRunFilter(FrozenModel):
    """Experiment run list filter."""

    experiment_id: uuid.UUID | None = None
    status: ExperimentRunStatus | None = None
    tag: str | None = None
    page: PositiveInt = 1
    page_size: int = Field(default=20, ge=1, le=1000)


class ExperimentRunReplaysFilter(FrozenModel):
    """Experiment run replay list filter."""

    status: ReplayStatus | None = None
    page: PositiveInt = 1
    page_size: int = Field(default=20, ge=1, le=1000)
