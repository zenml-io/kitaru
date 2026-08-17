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
"""SQL experiment and replay config repository."""

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.experiment import (
    EXPERIMENT_AGENT_ID_FOREIGN_KEY,
    EXPERIMENT_NAME_UNIQUE_CONSTRAINT,
    ExperimentORM,
    ReplayConfigORM,
)
from kitaru.server.adapters.db.orm.experiment_run import (
    EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.replay import REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentInUse,
    ExperimentNotFound,
)
from kitaru.server.domain.replay_config import (
    ReplayConfig,
    ReplayConfigInUse,
    ReplayConfigNotFound,
)

EXPERIMENT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": ExperimentORM.id,
    "agent_id": ExperimentORM.agent_id,
    "name": ExperimentORM.name,
    "tag": build_tag_condition_binding(TagResourceType.EXPERIMENT, ExperimentORM.id),
}


class SQLExperimentRepository(BaseSQLRepository[ExperimentORM]):
    """Experiment and replay config repository backed by the application database."""

    orm_class = ExperimentORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return ExperimentNotFound(entity_id)

    async def create(self, experiment: Experiment) -> Experiment:
        """Persist a new experiment.

        Args:
            experiment: Experiment to store.

        Raises:
            DuplicateExperimentName: The experiment name is already
                registered.
            AgentNotFound: No agent has the experiment's agent id.

        Returns:
            Stored experiment with timestamps set.
        """
        row = ExperimentORM.from_domain(experiment)
        await self._add(
            row,
            {
                EXPERIMENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateExperimentName(
                    experiment.name
                ),
                EXPERIMENT_AGENT_ID_FOREIGN_KEY: lambda: AgentNotFound(
                    experiment.agent_id
                ),
            },
        )
        return row.to_domain()

    async def get(
        self, experiment_id: uuid.UUID, exclusive: bool = False
    ) -> Experiment:
        """Load an experiment by id.

        Args:
            experiment_id: Id of the experiment.
            exclusive: Whether to lock the row for the duration of the
                transaction.

        Raises:
            ExperimentNotFound: No experiment has this id.

        Returns:
            Stored experiment.
        """
        row = await self._get_row(experiment_id, exclusive=exclusive)
        return row.to_domain()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]:
        """Query experiments matching a filter.

        Args:
            experiment_filter: Filter and pagination parameters.

        Returns:
            Page of matching experiments and the next cursor.
        """
        statement = select(ExperimentORM)
        if experiment_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    experiment_filter.expression, EXPERIMENT_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, experiment_filter, id_column=ExperimentORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, experiment: Experiment) -> Experiment:
        """Persist changes to an existing experiment.

        Args:
            experiment: Experiment with modified fields.

        Raises:
            ExperimentNotFound: No experiment has this id.
            DuplicateExperimentName: The experiment name is already
                registered.

        Returns:
            Stored experiment with the updated timestamp renewed.
        """
        row = await self._get_row(experiment.id)
        row.name = experiment.name
        row.description = experiment.description
        row.replay_config_id = experiment.replay_config_id
        await self._flush(
            {
                EXPERIMENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateExperimentName(
                    experiment.name
                )
            }
        )
        return row.to_domain()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        """Delete an experiment by id.

        Args:
            experiment_id: Id of the experiment.

        Raises:
            ExperimentNotFound: No experiment has this id.
            ExperimentInUse: The experiment has runs.
        """
        await self._delete_row(
            experiment_id,
            {
                EXPERIMENT_RUN_EXPERIMENT_ID_FOREIGN_KEY: lambda: ExperimentInUse(
                    experiment_id
                )
            },
        )

    async def create_replay_config(self, config: ReplayConfig) -> ReplayConfig:
        """Persist a new replay config.

        Args:
            config: Replay config to store.

        Returns:
            Stored replay config with timestamps set.
        """
        row = ReplayConfigORM.from_domain(config)
        await self._add(row)
        return row.to_domain()

    async def _get_replay_config_row(self, config_id: uuid.UUID) -> ReplayConfigORM:
        """Load a replay config row by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored row.
        """
        row = await self._session.get(ReplayConfigORM, config_id)
        if row is None:
            raise ReplayConfigNotFound(config_id)
        return row

    async def get_replay_config(self, config_id: uuid.UUID) -> ReplayConfig:
        """Load a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.

        Returns:
            Stored replay config.
        """
        row = await self._get_replay_config_row(config_id)
        return row.to_domain()

    async def get_many_replay_configs(
        self, config_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, ReplayConfig]:
        """Load replay configs by id, in one bulk fetch.

        Args:
            config_ids: Ids of the replay configs.

        Returns:
            Replay configs keyed by id, missing ids omitted.
        """
        if not config_ids:
            return {}
        statement = select(ReplayConfigORM).where(ReplayConfigORM.id.in_(config_ids))
        rows = (await self._session.scalars(statement)).all()
        return {row.id: row.to_domain() for row in rows}

    async def delete_replay_config(self, config_id: uuid.UUID) -> None:
        """Delete a replay config by id.

        Args:
            config_id: Id of the replay config.

        Raises:
            ReplayConfigNotFound: No replay config has this id.
            ReplayConfigInUse: A replay references the replay config.
        """
        row = await self._get_replay_config_row(config_id)
        try:
            async with self._session.begin_nested():
                await self._session.delete(row)
                await self._session.flush()
        except IntegrityError as exc:
            self._raise_translated(
                exc,
                {
                    REPLAY_REPLAY_CONFIG_ID_FOREIGN_KEY: lambda: ReplayConfigInUse(
                        config_id
                    )
                },
            )
