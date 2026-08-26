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
"""Experiment entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class ExperimentNotFound(NotFoundError):
    """Raised when an experiment lookup does not resolve."""

    def __init__(self, experiment_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            experiment_id: Id of the missing experiment.
        """
        super().__init__(f"Experiment {experiment_id} was not found")


class DuplicateExperimentName(ConflictError):
    """Raised when an experiment name is already registered."""

    def __init__(self, name: str) -> None:
        """Initialize the error.

        Args:
            name: Name that is already registered.
        """
        super().__init__(f"Experiment name '{name}' is already registered")


class ExperimentFrozen(ConflictError):
    """Raised when an experiment's replay config is touched while it has runs."""

    def __init__(self, experiment_id: uuid.UUID) -> None:
        """Initialize the error.

        Args:
            experiment_id: Id of the experiment.
        """
        super().__init__(
            f"Experiment {experiment_id} has runs and its replay config "
            "cannot be changed"
        )


class Experiment(DomainModel):
    """Experiment."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    agent_id: uuid.UUID
    replay_config_id: uuid.UUID
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set a new experiment name.

        Args:
            name: New name.
        """
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set a new experiment description.

        Args:
            description: New description.
        """
        self.description = description

    def update_replay_config_id(
        self, replay_config_id: uuid.UUID, has_runs: bool
    ) -> None:
        """Point the experiment at a different replay config.

        Args:
            replay_config_id: Id of the new replay config.
            has_runs: Whether the experiment already has runs.

        Raises:
            ExperimentFrozen: ``has_runs`` is set.
        """
        if has_runs:
            raise ExperimentFrozen(self.id)
        self.replay_config_id = replay_config_id
