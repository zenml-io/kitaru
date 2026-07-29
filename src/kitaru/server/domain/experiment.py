"""Experiment entity and errors."""

import uuid
from datetime import datetime

from pydantic import Field

from kitaru.server.domain.base import ConflictError, DomainModel, NotFoundError
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.names import Name


class ExperimentNotFound(NotFoundError):
    """Raised when an experiment lookup does not resolve."""

    def __init__(self, experiment: uuid.UUID | str) -> None:
        super().__init__(f"Experiment {experiment} was not found")


class DuplicateExperimentName(ConflictError):
    """Raised when an experiment name is already registered."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Experiment name '{name}' is already registered")


class Experiment(DomainModel):
    """Reusable replay experiment."""

    id: uuid.UUID = Field(default_factory=uuid7)
    owner_id: uuid.UUID
    name: Name
    description: str | None = None
    replay_config_id: uuid.UUID
    created: datetime | None = None
    updated: datetime | None = None

    def update_name(self, name: str) -> None:
        """Set the experiment name."""
        self.name = name

    def update_description(self, description: str | None) -> None:
        """Set the experiment description."""
        self.description = description

    def update_replay_config_id(
        self, replay_config_id: uuid.UUID, frozen: bool
    ) -> None:
        """Replace the replay configuration before the experiment is used."""
        if frozen and replay_config_id != self.replay_config_id:
            raise ConflictError(f"Experiment {self.id} configuration is frozen")
        self.replay_config_id = replay_config_id
