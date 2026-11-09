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
"""In-memory fakes for investigations and annotations."""

import uuid
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.models.annotation import AnnotationFilter
from kitaru.server.application.models.investigation import (
    InvestigationFilter,
    InvestigationSessionFilter,
)
from kitaru.server.domain.annotation import Annotation, AnnotationNotFound
from kitaru.server.domain.investigation import (
    Investigation,
    InvestigationNotFound,
    InvestigationSession,
    InvestigationSessionNotFound,
)
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.session_nodes import _paginate_fake_by_index

# Named only in string annotations; a runtime import could be circular.
if TYPE_CHECKING:
    from kitaru_test_support.sessions import FakeSessionRepository


class FakeInvestigationRepository:
    """In-memory investigation repository."""

    def __init__(
        self, session_repository: "FakeSessionRepository | None" = None
    ) -> None:
        """Initialize the repository.

        Args:
            session_repository: Fake session repository, wired back onto the
                session repository so its delete can check for investigation
                membership.
        """
        self._investigations: dict[uuid.UUID, Investigation] = {}
        self._sessions: dict[uuid.UUID, InvestigationSession] = {}
        self._annotations: FakeAnnotationRepository | None = None
        if session_repository is not None:
            session_repository._investigations = self

    def _counts(self, investigation_id: uuid.UUID) -> tuple[int, int]:
        """Count an investigation's linked sessions and non-null verdicts.

        Args:
            investigation_id: Id of the investigation.

        Returns:
            Total and verdict-set linked session counts.
        """
        links = [
            session
            for session in self._sessions.values()
            if session.investigation_id == investigation_id
        ]
        completed = sum(1 for session in links if session.verdict is not None)
        return len(links), completed

    def _with_counts(self, investigation: Investigation) -> Investigation:
        """Attach freshly computed progress counts to an investigation.

        Args:
            investigation: Investigation to annotate.

        Returns:
            Investigation with total_sessions and completed_sessions set.
        """
        total, completed = self._counts(investigation.id)
        return investigation.model_copy(
            update={"total_sessions": total, "completed_sessions": completed}
        )

    async def create(
        self, investigation: Investigation, sessions: Sequence[InvestigationSession]
    ) -> Investigation:
        """Persist a new investigation with its linked sessions.

        Args:
            investigation: Investigation to store.
            sessions: Ordered investigation_session rows to link.

        Returns:
            Stored investigation with timestamps set.
        """
        now = datetime.now(UTC)
        stored = investigation.model_copy(
            update={
                "created": now,
                "updated": now,
                "total_sessions": 0,
                "completed_sessions": 0,
            }
        )
        self._investigations[stored.id] = stored
        for session in sessions:
            self._sessions[session.id] = session.model_copy(
                update={"created": now, "updated": now}
            )
        return self._with_counts(stored)

    async def get(
        self, investigation_id: uuid.UUID, exclusive: bool = False
    ) -> Investigation:
        """Load an investigation by id.

        Args:
            investigation_id: Id of the investigation.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation.
        """
        _ = exclusive
        stored = self._investigations.get(investigation_id)
        if stored is None:
            raise InvestigationNotFound(investigation_id)
        return self._with_counts(stored)

    async def query(
        self, investigation_filter: InvestigationFilter
    ) -> tuple[list[Investigation], str | None]:
        """Query investigations matching a filter.

        Args:
            investigation_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigations and the next cursor.
        """
        investigations = list(self._investigations.values())
        if investigation_filter.expression is not None:
            investigations = [
                investigation
                for investigation in investigations
                if _evaluate_filter_expression(
                    investigation, investigation_filter.expression
                )
            ]
        page, next_cursor = _paginate_fake(investigations, investigation_filter)
        return [self._with_counts(investigation) for investigation in page], next_cursor

    async def update(self, investigation: Investigation) -> Investigation:
        """Persist changes to an existing investigation.

        Args:
            investigation: Investigation with modified fields.

        Raises:
            InvestigationNotFound: No investigation has this id.

        Returns:
            Stored investigation with the updated timestamp renewed.
        """
        stored = self._investigations.get(investigation.id)
        if stored is None:
            raise InvestigationNotFound(investigation.id)
        now = _renewed_timestamp(stored.updated)
        updated = investigation.model_copy(
            update={
                "created": stored.created,
                "updated": now,
                "total_sessions": 0,
                "completed_sessions": 0,
            }
        )
        self._investigations[investigation.id] = updated
        return self._with_counts(updated)

    async def delete(self, investigation_id: uuid.UUID) -> None:
        """Delete an investigation by id, cascading its links and answers.

        Args:
            investigation_id: Id of the investigation.

        Raises:
            InvestigationNotFound: No investigation has this id.
        """
        if investigation_id not in self._investigations:
            raise InvestigationNotFound(investigation_id)
        del self._investigations[investigation_id]
        cascaded_session_ids = {
            session.id
            for session in self._sessions.values()
            if session.investigation_id == investigation_id
        }
        for session_id in cascaded_session_ids:
            del self._sessions[session_id]
        if self._annotations is not None:
            self._annotations.cascade_investigation_session_delete(cascaded_session_ids)

    async def get_session(
        self, investigation_session_id: uuid.UUID, exclusive: bool = False
    ) -> InvestigationSession:
        """Load an investigation session by id.

        Args:
            investigation_session_id: Id of the investigation session.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session.
        """
        _ = exclusive
        stored = self._sessions.get(investigation_session_id)
        if stored is None:
            raise InvestigationSessionNotFound(investigation_session_id)
        return stored.model_copy()

    async def get_session_by_session_id(
        self,
        investigation_id: uuid.UUID,
        session_id: uuid.UUID,
        exclusive: bool = False,
    ) -> InvestigationSession:
        """Load an investigation session by investigation id and session id.

        Args:
            investigation_id: Id of the investigation.
            session_id: Id of the linked session.
            exclusive: Ignored, the fake holds no rows to lock.

        Raises:
            InvestigationSessionNotFound: No investigation session links this
                investigation and session.

        Returns:
            Stored investigation session.
        """
        _ = exclusive
        for stored in self._sessions.values():
            if (
                stored.investigation_id == investigation_id
                and stored.session_id == session_id
            ):
                return stored.model_copy()
        raise InvestigationSessionNotFound(session_id)

    async def query_sessions(
        self, session_filter: InvestigationSessionFilter
    ) -> tuple[list[InvestigationSession], str | None]:
        """Query an investigation's sessions, ordered by position ascending.

        Args:
            session_filter: Filter and pagination parameters.

        Returns:
            Page of matching investigation sessions and the next cursor.
        """
        sessions = [
            session
            for session in self._sessions.values()
            if session.investigation_id == session_filter.investigation_id
        ]
        if session_filter.expression is not None:
            sessions = [
                session
                for session in sessions
                if _evaluate_filter_expression(session, session_filter.expression)
            ]
        page, next_cursor = _paginate_fake_by_index(
            sessions, session_filter, lambda session: session.position
        )
        return [session.model_copy() for session in page], next_cursor

    async def update_session(
        self, session: InvestigationSession
    ) -> InvestigationSession:
        """Persist changes to an existing investigation session.

        Args:
            session: Investigation session with modified fields.

        Raises:
            InvestigationSessionNotFound: No investigation session has this
                id.

        Returns:
            Stored investigation session with the updated timestamp renewed.
        """
        stored = self._sessions.get(session.id)
        if stored is None:
            raise InvestigationSessionNotFound(session.id)
        now = _renewed_timestamp(stored.updated)
        updated = session.model_copy(update={"created": stored.created, "updated": now})
        self._sessions[session.id] = updated
        return updated.model_copy()


class FakeAnnotationRepository:
    """In-memory annotation repository."""

    def __init__(
        self, investigations: FakeInvestigationRepository | None = None
    ) -> None:
        """Initialize the repository.

        Args:
            investigations: Fake investigation repository sharing session
                links, consulted by the investigation_id filter and wired
                back onto the investigation repository to cascade its
                deletes.
        """
        self._annotations: dict[uuid.UUID, Annotation] = {}
        self._investigations = investigations
        if investigations is not None:
            investigations._annotations = self

    async def create(self, annotation: Annotation) -> Annotation:
        """Persist a new annotation.

        Args:
            annotation: Annotation to store.

        Returns:
            Stored annotation with timestamps set.
        """
        now = datetime.now(UTC)
        stored = annotation.model_copy(update={"created": now, "updated": now})
        self._annotations[stored.id] = stored
        return stored.model_copy()

    async def get(self, annotation_id: uuid.UUID) -> Annotation:
        """Load an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation.
        """
        stored = self._annotations.get(annotation_id)
        if stored is None:
            raise AnnotationNotFound(annotation_id)
        return stored.model_copy()

    def _evaluate_investigation_id_condition(
        self, annotation: Annotation, condition: FilterCondition
    ) -> bool:
        """Evaluate an investigation id condition against an annotation.

        Annotation rows only carry an investigation_session_id, so the
        investigation id is resolved through the investigation repository's
        session links.

        Args:
            annotation: Annotation to evaluate.
            condition: Validated investigation id condition.

        Returns:
            Whether the annotation's linked investigation matches.
        """
        matched = False
        if (
            self._investigations is not None
            and annotation.investigation_session_id is not None
        ):
            session = self._investigations._sessions.get(
                annotation.investigation_session_id
            )
            if session is not None:
                ids = (
                    condition.value
                    if condition.op is FilterOp.IN
                    else (condition.value,)
                )
                matched = session.investigation_id in ids
        return not matched if condition.op is FilterOp.NE else matched

    async def query(
        self, annotation_filter: AnnotationFilter
    ) -> tuple[list[Annotation], str | None]:
        """Query annotations matching a filter.

        Args:
            annotation_filter: Filter and pagination parameters.

        Returns:
            Page of matching annotations and the next cursor.
        """
        annotations = list(self._annotations.values())
        if annotation_filter.expression is not None:
            resolvers = {"investigation_id": self._evaluate_investigation_id_condition}
            annotations = [
                annotation
                for annotation in annotations
                if _evaluate_filter_expression(
                    annotation, annotation_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(annotations, annotation_filter)
        return [annotation.model_copy() for annotation in page], next_cursor

    async def update(self, annotation: Annotation) -> Annotation:
        """Persist changes to an existing annotation.

        Args:
            annotation: Annotation with modified fields.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation with the updated timestamp renewed.
        """
        stored = self._annotations.get(annotation.id)
        if stored is None:
            raise AnnotationNotFound(annotation.id)
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(update={"value": annotation.value, "updated": now})
        self._annotations[annotation.id] = updated
        return updated.model_copy()

    async def delete(self, annotation_id: uuid.UUID) -> None:
        """Delete an annotation by id.

        Args:
            annotation_id: Id of the annotation.

        Raises:
            AnnotationNotFound: No annotation has this id.
        """
        if annotation_id not in self._annotations:
            raise AnnotationNotFound(annotation_id)
        del self._annotations[annotation_id]

    def cascade_investigation_session_delete(
        self, investigation_session_ids: Iterable[uuid.UUID]
    ) -> None:
        """Drop the answers of deleted investigation sessions.

        Manual annotations, whose investigation_session_id is null, are
        unaffected.

        Args:
            investigation_session_ids: Ids of the deleted investigation
                sessions.
        """
        deleted = set(investigation_session_ids)
        for annotation_id, annotation in list(self._annotations.items()):
            if annotation.investigation_session_id in deleted:
                del self._annotations[annotation_id]
