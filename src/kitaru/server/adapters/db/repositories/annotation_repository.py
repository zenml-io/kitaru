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
"""SQL annotation repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import ColumnElement, select

from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.annotation import AnnotationORM
from kitaru.server.adapters.db.orm.investigation_session import InvestigationSessionORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.annotation import AnnotationFilter
from kitaru.server.domain.annotation import Annotation, AnnotationNotFound
from kitaru.server.domain.base import NotFoundError
from kitaru.server.filtering import FilterCondition


def _compile_investigation_id_condition(
    condition: FilterCondition,
) -> ColumnElement[bool]:
    """Compile an investigation id condition into an EXISTS predicate.

    Annotation rows only carry an investigation_session_id, so the
    investigation id is resolved through a join to investigation_session.

    Args:
        condition: Validated investigation id condition.

    Returns:
        SQL predicate.
    """
    ids = condition.value if condition.op is FilterOp.IN else (condition.value,)
    investigation_match = (
        select(InvestigationSessionORM.id)
        .where(
            InvestigationSessionORM.id == AnnotationORM.investigation_session_id,
            InvestigationSessionORM.investigation_id.in_(ids),
        )
        .correlate(AnnotationORM)
    )
    matched = investigation_match.exists()
    return ~matched if condition.op is FilterOp.NE else matched


ANNOTATION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": AnnotationORM.id,
    "session_id": AnnotationORM.session_id,
    "investigation_session_id": AnnotationORM.investigation_session_id,
    "investigation_id": _compile_investigation_id_condition,
}


class SQLAnnotationRepository(BaseSQLRepository[AnnotationORM]):
    """Annotation repository backed by the application database."""

    orm_class = AnnotationORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return AnnotationNotFound(entity_id)

    async def create(self, annotation: Annotation) -> Annotation:
        """Persist a new annotation.

        Args:
            annotation: Annotation to store.

        Returns:
            Stored annotation with timestamps set.
        """
        row = AnnotationORM.from_domain(annotation)
        self._session.add(row)
        await self._flush()
        return row.to_domain()

    async def get(self, annotation_id: uuid.UUID) -> Annotation:
        """Load an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation.
        """
        row = await self._get_row(annotation_id)
        return row.to_domain()

    async def query(
        self, annotation_filter: AnnotationFilter
    ) -> tuple[list[Annotation], str | None]:
        """Query annotations matching a filter.

        Args:
            annotation_filter: Filter and pagination parameters.

        Returns:
            Page of matching annotations and the next cursor.
        """
        statement = select(AnnotationORM)
        if annotation_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    annotation_filter.expression, ANNOTATION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, annotation_filter, id_column=AnnotationORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, annotation: Annotation) -> Annotation:
        """Persist changes to an existing annotation.

        Args:
            annotation: Annotation with modified fields.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation with the updated timestamp renewed.
        """
        row = await self._get_row(annotation.id)
        row.value = annotation.value
        await self._flush()
        return row.to_domain()

    async def delete(self, annotation_id: uuid.UUID) -> None:
        """Delete an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.
        """
        await self._delete_row(annotation_id)
