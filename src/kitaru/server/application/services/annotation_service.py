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
"""Annotation use cases."""

import uuid
from datetime import UTC, datetime
from typing import Any

from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.investigation import InvestigationStatus
from kitaru.server.application.interfaces.annotation_repository import (
    AnnotationRepository,
)
from kitaru.server.application.interfaces.investigation_repository import (
    InvestigationRepository,
)
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.annotation import (
    AnnotationFilter,
    InvestigationAnswerCreate,
    ManualAnnotationCreate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.annotation import Annotation
from kitaru.server.domain.base import ValidationError


class AnnotationService:
    """Annotation use cases."""

    def __init__(
        self,
        repository: AnnotationRepository,
        investigation_repository: InvestigationRepository,
        session_repository: SessionRepository,
        session_node_repository: SessionNodeRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Annotation repository.
            investigation_repository: Investigation repository, to resolve
                the linked session and validate its question key for
                investigation answers.
            session_repository: Session repository, to validate the
                annotated session exists.
            session_node_repository: Session node repository, to validate a
                selector's node id belongs to the annotated session.
        """
        self._repository = repository
        self._investigations = investigation_repository
        self._sessions = session_repository
        self._nodes = session_node_repository

    async def _check_selector(
        self, session_id: uuid.UUID, selector: AnnotationSelector | None
    ) -> None:
        """Validate a selector's node id belongs to the annotated session.

        Args:
            session_id: Id of the session the selector targets.
            selector: Selector to validate, None always passes.

        Raises:
            ValidationError: The selector names a node outside the session.
        """
        if selector is None or selector.node_id is None:
            return
        index_by_id = await self._nodes.get_index_by_id(session_id)
        if selector.node_id not in index_by_id:
            raise ValidationError(
                f"Node {selector.node_id} does not belong to session {session_id}"
            )

    async def create_manual_annotation(
        self, command: ManualAnnotationCreate, actor: AuthContext
    ) -> Annotation:
        """Create a manual annotation on a session, outside any investigation.

        Args:
            command: Session, selector, and value for the new annotation.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has the command's session id.
            ValidationError: The selector names a node outside the session.

        Returns:
            Created annotation.
        """
        await self._sessions.get(command.session_id)
        await self._check_selector(command.session_id, command.selector)
        annotation = Annotation(
            owner_id=actor.account.id,
            session_id=command.session_id,
            selector=command.selector,
            value=command.value,
        )
        return await self._repository.create(annotation)

    async def create_investigation_answer(
        self, command: InvestigationAnswerCreate, actor: AuthContext
    ) -> Annotation:
        """Answer one question of an investigation's linked session.

        Answering the same question twice replaces the earlier value. The
        investigation moves from pending to in_progress on its first answer.

        Args:
            command: Linked session, question, selector, and value for the
                answer.
            actor: Caller context.

        Raises:
            InvestigationSessionNotFound: No investigation session has the
                command's investigation session id.
            InvestigationNotFound: No investigation links the session.
            UnknownQuestionKey: The command's question key does not name one
                of the investigation's questions.
            ValidationError: The selector names a node outside the session.

        Returns:
            Stored annotation.
        """
        link = await self._investigations.get_session(command.investigation_session_id)
        # Locked because a racing first answer on the same investigation
        # could otherwise both observe pending and both flip the status.
        investigation = await self._investigations.get(
            link.investigation_id, exclusive=True
        )
        investigation.check_question_key(command.question_key)
        if investigation.status is InvestigationStatus.PENDING:
            investigation.start(datetime.now(UTC))
            await self._investigations.update(investigation)
        await self._check_selector(link.session_id, command.selector)
        annotation = Annotation(
            owner_id=actor.account.id,
            session_id=link.session_id,
            investigation_session_id=link.id,
            question_key=command.question_key,
            selector=command.selector,
            value=command.value,
        )
        return await self._repository.create(annotation)

    async def get_annotation(
        self, annotation_id: uuid.UUID, actor: AuthContext
    ) -> Annotation:
        """Get an annotation by id.

        Args:
            annotation_id: Id of the annotation.
            actor: Caller context.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Stored annotation.
        """
        _ = actor
        return await self._repository.get(annotation_id)

    async def list_annotations(
        self, annotation_filter: AnnotationFilter, actor: AuthContext
    ) -> tuple[list[Annotation], str | None]:
        """List annotations matching a filter.

        Args:
            annotation_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching annotations and the next cursor.
        """
        _ = actor
        return await self._repository.query(annotation_filter)

    async def update_annotation(
        self, annotation_id: uuid.UUID, value: Any, actor: AuthContext
    ) -> Annotation:
        """Set a new value on an annotation.

        Args:
            annotation_id: Id of the annotation.
            value: New annotation value.
            actor: Caller context.

        Raises:
            AnnotationNotFound: No annotation has this id.

        Returns:
            Updated annotation.
        """
        _ = actor
        annotation = await self._repository.get(annotation_id)
        annotation.update_value(value)
        return await self._repository.update(annotation)

    async def delete_annotation(
        self, annotation_id: uuid.UUID, actor: AuthContext
    ) -> None:
        """Delete an annotation.

        Args:
            annotation_id: Id of the annotation.
            actor: Caller context.

        Raises:
            AnnotationNotFound: No annotation has this id.
        """
        _ = actor
        await self._repository.delete(annotation_id)
