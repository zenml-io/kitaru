"""SQL evaluation repository."""

import uuid

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from kitaru.server.adapters.db.orm.evaluation import EvaluationORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.evaluation import Evaluation, EvaluationNotFound


class SQLEvaluationRepository(BaseSQLRepository[EvaluationORM]):
    """Evaluation repository backed by PostgreSQL."""

    orm_class = EvaluationORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return EvaluationNotFound(entity_id)

    async def create_many(self, evaluations: list[Evaluation]) -> list[Evaluation]:
        rows = [EvaluationORM.from_domain(evaluation) for evaluation in evaluations]
        self._session.add_all(rows)
        await self._session.flush()
        return [row.to_domain() for row in rows]

    async def upsert_manual(self, evaluations: list[Evaluation]) -> list[Evaluation]:
        stored: list[Evaluation] = []
        for evaluation in evaluations:
            row = EvaluationORM.from_domain(evaluation)
            values = {
                column.name: getattr(
                    row, "metadata_" if column.name == "metadata" else column.name
                )
                for column in EvaluationORM.__table__.columns
                if column.name not in {"created", "updated"}
            }
            statement = (
                insert(EvaluationORM)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[
                        EvaluationORM.session_id,
                        EvaluationORM.name,
                    ],
                    index_where=EvaluationORM.task_id.is_(None),
                    set_={
                        "data_type": row.data_type,
                        "numerical_value": row.numerical_value,
                        "string_value": row.string_value,
                        "explanation": row.explanation,
                    },
                )
                .returning(EvaluationORM.id)
            )
            stored_id = (await self._session.execute(statement)).scalar_one()
            stored.append((await self._get_row(stored_id)).to_domain())
        return stored

    async def get(self, evaluation_id: uuid.UUID) -> Evaluation:
        return (await self._get_row(evaluation_id)).to_domain()

    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[Evaluation], str | None]:
        statement = select(EvaluationORM)
        for name, column in (
            ("session_id", EvaluationORM.session_id),
            ("task_id", EvaluationORM.task_id),
            ("evaluator_version_id", EvaluationORM.evaluator_version_id),
            ("name", EvaluationORM.name),
        ):
            value = getattr(evaluation_filter, name)
            if value is not None:
                statement = statement.where(column == value)
        if evaluation_filter.data_type is not None:
            statement = statement.where(
                EvaluationORM.data_type == evaluation_filter.data_type.value
            )
        rows, cursor = await paginate(
            self._session, statement, evaluation_filter, EvaluationORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def delete_for_task(self, task_id: uuid.UUID) -> None:
        await self._session.execute(
            delete(EvaluationORM).where(EvaluationORM.task_id == task_id)
        )
