"""SQL experiment and run repositories."""

import uuid

from sqlalchemy import func, select

from kitaru.server.adapters.db.orm.experiment import (
    EXPERIMENT_NAME_UNIQUE_CONSTRAINT,
    ExperimentORM,
)
from kitaru.server.adapters.db.orm.experiment_run import ExperimentRunORM
from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.replay_config import ReplayConfigORM
from kitaru.server.adapters.db.orm.tag import TagLinkORM, TagORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentNotFound,
)
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunNotFound,
    ExperimentRunProgress,
)
from kitaru.server.domain.replay_config import ReplayConfig


class SQLExperimentRepository(BaseSQLRepository[ExperimentORM]):
    """Experiment repository backed by PostgreSQL."""

    orm_class = ExperimentORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return ExperimentNotFound(entity_id)

    async def create(
        self, experiment: Experiment, config: ReplayConfig
    ) -> tuple[Experiment, ReplayConfig]:
        config_row = ReplayConfigORM.from_domain(config)
        self._session.add(config_row)
        await self._session.flush()
        row = ExperimentORM.from_domain(experiment)
        await self._add(
            row,
            {
                EXPERIMENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateExperimentName(
                    experiment.name
                )
            },
        )
        return row.to_domain(), config_row.to_domain()

    async def get(self, experiment_id: uuid.UUID) -> Experiment:
        return (await self._get_row(experiment_id)).to_domain()

    async def get_config(self, config_id: uuid.UUID) -> ReplayConfig:
        row = await self._session.get(ReplayConfigORM, config_id)
        if row is None:
            raise ExperimentNotFound(config_id)
        return row.to_domain()

    async def query(
        self, experiment_filter: ExperimentFilter
    ) -> tuple[list[Experiment], str | None]:
        statement = select(ExperimentORM)
        if experiment_filter.name is not None:
            statement = statement.where(ExperimentORM.name == experiment_filter.name)
        if experiment_filter.tag is not None:
            statement = (
                statement.join(
                    TagLinkORM,
                    (TagLinkORM.resource_id == ExperimentORM.id)
                    & (TagLinkORM.resource_type == "experiment"),
                )
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(TagORM.name == experiment_filter.tag)
            )
        rows, cursor = await paginate(
            self._session, statement, experiment_filter, ExperimentORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(
        self, experiment: Experiment, config: ReplayConfig
    ) -> tuple[Experiment, ReplayConfig]:
        row = await self._get_row(experiment.id)
        row.name = experiment.name
        row.description = experiment.description
        config_row = await self._session.get(ReplayConfigORM, config.id)
        if config_row is None:
            raise ExperimentNotFound(config.id)
        source = ReplayConfigORM.from_domain(config)
        config_row.override = source.override
        config_row.tool_policy = source.tool_policy
        config_row.evaluators = source.evaluators
        await self._flush(
            {
                EXPERIMENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateExperimentName(
                    experiment.name
                )
            }
        )
        return row.to_domain(), config_row.to_domain()

    async def delete(self, experiment_id: uuid.UUID) -> None:
        await self._delete_row(experiment_id)

    async def next_run_number(self, experiment_id: uuid.UUID) -> int:
        row = await self._get_row(experiment_id)
        await self._session.execute(
            select(ExperimentORM.id).where(ExperimentORM.id == row.id).with_for_update()
        )
        current = await self._session.scalar(
            select(func.max(ExperimentRunORM.number)).where(
                ExperimentRunORM.experiment_id == experiment_id
            )
        )
        return int(current or 0) + 1


class SQLExperimentRunRepository(BaseSQLRepository[ExperimentRunORM]):
    """Experiment-run repository backed by PostgreSQL."""

    orm_class = ExperimentRunORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return ExperimentRunNotFound(entity_id)

    async def create(self, run: ExperimentRun) -> ExperimentRun:
        row = ExperimentRunORM.from_domain(run)
        await self._add(row)
        return row.to_domain()

    async def get(self, run_id: uuid.UUID, exclusive: bool = False) -> ExperimentRun:
        statement = select(ExperimentRunORM).where(ExperimentRunORM.id == run_id)
        if exclusive:
            statement = statement.with_for_update()
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise ExperimentRunNotFound(run_id)
        return row.to_domain()

    async def query(
        self, run_filter: ExperimentRunFilter
    ) -> tuple[list[ExperimentRun], str | None]:
        statement = select(ExperimentRunORM)
        if run_filter.experiment_id is not None:
            statement = statement.where(
                ExperimentRunORM.experiment_id == run_filter.experiment_id
            )
        if run_filter.status is not None:
            statement = statement.where(
                ExperimentRunORM.status == run_filter.status.value
            )
        if run_filter.tag is not None:
            statement = (
                statement.join(
                    TagLinkORM,
                    (TagLinkORM.resource_id == ExperimentRunORM.id)
                    & (TagLinkORM.resource_type == "experiment_run"),
                )
                .join(TagORM, TagORM.id == TagLinkORM.tag_id)
                .where(TagORM.name == run_filter.tag)
            )
        rows, cursor = await paginate(
            self._session, statement, run_filter, ExperimentRunORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, run: ExperimentRun) -> ExperimentRun:
        row = await self._get_row(run.id)
        source = ExperimentRunORM.from_domain(run)
        for name in ("status", "started_at", "ended_at", "error"):
            setattr(row, name, getattr(source, name))
        await self._session.flush()
        return row.to_domain()

    async def delete(self, run_id: uuid.UUID) -> None:
        await self._delete_row(run_id)

    async def progress(self, run_id: uuid.UUID) -> ExperimentRunProgress:
        rows = (
            await self._session.execute(
                select(ReplayORM.status, func.count())
                .where(ReplayORM.experiment_run_id == run_id)
                .group_by(ReplayORM.status)
            )
        ).all()
        counts = {status: count for status, count in rows}
        return ExperimentRunProgress(
            pending=counts.get("pending", 0),
            evaluating=counts.get("evaluating", 0),
            completed=counts.get("completed", 0),
            failed=counts.get("failed", 0),
            canceled=counts.get("canceled", 0),
            total=sum(counts.values()),
        )
