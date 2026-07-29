"""SQL replay repository."""

import uuid

from sqlalchemy import func, select

from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.replay_config import ReplayConfigORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.replay import ReplayFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.replay import Replay, ReplayNotFound, ReplayStatus
from kitaru.server.domain.replay_config import ReplayConfig


class SQLReplayRepository(BaseSQLRepository[ReplayORM]):
    """Replay repository backed by PostgreSQL."""

    orm_class = ReplayORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return ReplayNotFound(entity_id)

    async def create(
        self, replay: Replay, config: ReplayConfig
    ) -> tuple[Replay, ReplayConfig]:
        config_row = ReplayConfigORM.from_domain(config)
        self._session.add(config_row)
        await self._session.flush()
        row = ReplayORM.from_domain(replay)
        await self._add(row)
        return row.to_domain(), config_row.to_domain()

    async def get(self, replay_id: uuid.UUID, exclusive: bool = False) -> Replay:
        statement = select(ReplayORM).where(ReplayORM.id == replay_id)
        if exclusive:
            statement = statement.with_for_update()
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise ReplayNotFound(replay_id)
        return row.to_domain()

    async def get_by_job(self, job_id: uuid.UUID) -> Replay | None:
        row = (
            await self._session.scalars(
                select(ReplayORM).where(ReplayORM.job_id == job_id)
            )
        ).one_or_none()
        return row.to_domain() if row is not None else None

    async def get_config(self, config_id: uuid.UUID) -> ReplayConfig:
        row = await self._session.get(ReplayConfigORM, config_id)
        if row is None:
            raise ReplayNotFound(config_id)
        return row.to_domain()

    async def query(
        self, replay_filter: ReplayFilter
    ) -> tuple[list[Replay], str | None]:
        statement = select(ReplayORM)
        for name, column in (
            ("experiment_run_id", ReplayORM.experiment_run_id),
            ("baseline_session_id", ReplayORM.baseline_session_id),
        ):
            value = getattr(replay_filter, name)
            if value is not None:
                statement = statement.where(column == value)
        if replay_filter.status is not None:
            statement = statement.where(ReplayORM.status == replay_filter.status.value)
        rows, cursor = await paginate(
            self._session, statement, replay_filter, ReplayORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, replay: Replay) -> Replay:
        row = await self._get_row(replay.id)
        row.status = replay.status.value
        row.error = replay.error
        await self._session.flush()
        return row.to_domain()

    async def count_unsettled(self, run_id: uuid.UUID) -> int:
        return int(
            await self._session.scalar(
                select(func.count())
                .select_from(ReplayORM)
                .where(
                    ReplayORM.experiment_run_id == run_id,
                    ReplayORM.status.not_in(
                        [status.value for status in ReplayStatus if status.terminal]
                    ),
                )
            )
            or 0
        )

    async def count_statuses(self, run_id: uuid.UUID) -> dict[ReplayStatus, int]:
        rows = (
            await self._session.execute(
                select(ReplayORM.status, func.count())
                .where(ReplayORM.experiment_run_id == run_id)
                .group_by(ReplayORM.status)
            )
        ).all()
        return {ReplayStatus(status): count for status, count in rows}

    async def list_job_ids(self, run_id: uuid.UUID) -> list[uuid.UUID]:
        return list(
            (
                await self._session.scalars(
                    select(ReplayORM.job_id).where(
                        ReplayORM.experiment_run_id == run_id
                    )
                )
            ).all()
        )
