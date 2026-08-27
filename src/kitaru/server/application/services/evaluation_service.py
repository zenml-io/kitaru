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
"""Evaluation use cases."""

import uuid

from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
    EvaluationWithEvaluator,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.evaluation import (
    EvaluationFilter,
    EvaluationMerge,
)
from kitaru.server.application.services.resource_access import check_task_session_read
from kitaru.server.domain.evaluation import DuplicateEvaluationNameInBatch, Evaluation


class EvaluationService:
    """Evaluation use cases."""

    def __init__(
        self,
        repository: EvaluationRepository,
        session_repository: SessionRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Evaluation repository.
            session_repository: Session repository, for the merge existence
                check.
        """
        self._repository = repository
        self._sessions = session_repository

    async def get_evaluation(
        self, evaluation_id: uuid.UUID, actor: AuthContext
    ) -> EvaluationWithEvaluator:
        """Get an evaluation by id.

        Args:
            evaluation_id: Id of the evaluation.
            actor: Caller context.

        Raises:
            EvaluationNotFound: No evaluation has this id.

        Returns:
            Stored evaluation paired with its evaluator name and version.
        """
        _ = actor
        return await self._repository.get(evaluation_id)

    async def list_evaluations(
        self, evaluation_filter: EvaluationFilter, actor: AuthContext
    ) -> tuple[list[EvaluationWithEvaluator], str | None]:
        """List evaluations matching a filter.

        Args:
            evaluation_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching evaluations and the next cursor.
        """
        _ = actor
        return await self._repository.query(evaluation_filter)

    async def merge_evaluations(
        self,
        session_id: uuid.UUID,
        commands: list[EvaluationMerge],
        actor: AuthContext,
    ) -> list[Evaluation]:
        """Upsert manual evaluations into a session on (session, name).

        A resent name overwrites its data type, score, value, explanation,
        and pass flag. The stored rows carry no evaluator_version_id or
        task_id. A task principal merges into a session it owns or holds as
        its task's input session.

        Args:
            session_id: Id of the session to merge evaluations into.
            commands: Evaluations to merge, in request order.
            actor: Caller context.

        Raises:
            SessionNotFound: No session has this id.
            SessionAccessDenied: A task principal owns neither the session nor
                holds it as its task's input session.
            SessionNotEvaluatable: The session is in progress.
            DuplicateEvaluationNameInBatch: The request names the same
                evaluation twice.

        Returns:
            Stored evaluations in request order.
        """
        # Lock the session so a concurrent delete cannot land between this
        # read and the merge insert, whose foreign key would otherwise fail.
        session = await self._sessions.get(session_id, exclusive=True)
        check_task_session_read(session_id, session.task_id, actor)
        session.check_evaluate()
        seen: set[str] = set()
        for command in commands:
            if command.name in seen:
                raise DuplicateEvaluationNameInBatch(command.name)
            seen.add(command.name)
        evaluations = [
            Evaluation(
                owner_id=actor.account.id,
                session_id=session_id,
                name=command.name,
                data_type=command.data_type,
                score=command.score,
                value=command.value,
                explanation=command.explanation,
                passed=command.passed,
            )
            for command in commands
        ]
        return await self._repository.merge_session_evaluations(session_id, evaluations)
