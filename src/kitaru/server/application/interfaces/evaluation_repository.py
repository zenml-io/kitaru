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
"""Evaluation repository interface."""

import uuid
from typing import NamedTuple, Protocol

from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.evaluation import Evaluation


class EvaluationWithEvaluator(NamedTuple):
    """Evaluation paired with its denormalized evaluator name and version."""

    evaluation: Evaluation
    evaluator_name: str | None
    evaluator_version: int | None


class EvaluationRepository(Protocol):
    """Evaluation persistence operations."""

    async def get(self, evaluation_id: uuid.UUID) -> EvaluationWithEvaluator:
        """Load an evaluation by id, joined with its evaluator name and version.

        Args:
            evaluation_id: Id of the evaluation.

        Raises:
            EvaluationNotFound: No evaluation has this id.

        Returns:
            Stored evaluation paired with its evaluator name and version,
            both ``None`` on a manual evaluation.
        """
        ...

    async def query(
        self, evaluation_filter: EvaluationFilter
    ) -> tuple[list[EvaluationWithEvaluator], str | None]:
        """Query evaluations matching a filter.

        Args:
            evaluation_filter: Filter and pagination parameters.

        Returns:
            Page of matching evaluations, each paired with its evaluator name
            and version, and the next cursor.
        """
        ...

    async def merge_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert or replace manual evaluations upserted on (session, name).

        A resent name overwrites its data type, score, value, and
        explanation. ``evaluator_version_id`` and ``task_id`` stay null for
        every row this writes.

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Returns:
            Stored evaluations in request order.
        """
        ...

    async def create_task_evaluations(
        self, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.

        Returns:
            Stored evaluations in result order.
        """
        ...
