"""Experiment and run repository interfaces."""

import uuid
from typing import Protocol

from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunProgress,
)
from kitaru.server.domain.replay_config import ReplayConfig


class ExperimentRepository(Protocol):
    """Experiment persistence operations."""

    async def create(
        self, experiment: Experiment, config: ReplayConfig
    ) -> tuple[Experiment, ReplayConfig]: ...
    async def get(self, experiment_id: uuid.UUID) -> Experiment: ...
    async def get_config(self, config_id: uuid.UUID) -> ReplayConfig: ...
    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]: ...
    async def update(
        self, experiment: Experiment, config: ReplayConfig
    ) -> tuple[Experiment, ReplayConfig]: ...
    async def delete(self, experiment_id: uuid.UUID) -> None: ...
    async def next_run_number(self, experiment_id: uuid.UUID) -> int: ...


class ExperimentRunRepository(Protocol):
    """Experiment-run persistence operations."""

    async def create(self, run: ExperimentRun) -> ExperimentRun: ...
    async def get(
        self, run_id: uuid.UUID, exclusive: bool = False
    ) -> ExperimentRun: ...
    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]: ...
    async def update(self, run: ExperimentRun) -> ExperimentRun: ...
    async def delete(self, run_id: uuid.UUID) -> None: ...
    async def progress(self, run_id: uuid.UUID) -> ExperimentRunProgress: ...
