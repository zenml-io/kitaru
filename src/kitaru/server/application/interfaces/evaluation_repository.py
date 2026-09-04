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
from collections.abc import Sequence
from typing import NamedTuple, Protocol

from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.evaluation import Evaluation

EvaluationIdentity = tuple[uuid.UUID, uuid.UUID, str]


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

    async def create_session_evaluations(
        self, session_id: uuid.UUID, evaluations: list[Evaluation]
    ) -> list[Evaluation]:
        """Insert manual evaluations into a session.

        ``evaluator_version_id`` and ``task_id`` stay null for every row this
        writes.

        Args:
            session_id: Id of the session the evaluations belong to.
            evaluations: Fully resolved evaluations to store, in request
                order.

        Raises:
            EvaluationNameConflict: A name in the batch already exists for
                the session.

        Returns:
            Stored evaluations in request order.
        """
        ...

    async def create_task_evaluations(
        self, evaluations: list[Evaluation], replay_id: uuid.UUID | None
    ) -> list[Evaluation]:
        """Insert evaluation rows produced by a completed evaluator task.

        Args:
            evaluations: Fully resolved evaluations to store, in result order.
            replay_id: Replay to link each stored row to, ``None`` for a
                standalone evaluation batch.

        Raises:
            PluginVersionIdNotFound: No plugin version has the evaluator
                version id.

        Returns:
            Stored evaluations in result order.
        """
        ...

    async def get_latest_evaluation_ids_by_identity(
        self, session_ids: Sequence[uuid.UUID]
    ) -> dict[EvaluationIdentity, list[uuid.UUID]]:
        """Read the latest invocation's evaluation ids per identity.

        An identity is (session, evaluator version, params hash). Only rows
        carrying an evaluator version id, a params hash, and an invocation
        id are considered, and every row sharing the latest invocation id
        for an identity is returned, not just one.

        Args:
            session_ids: Ids of the candidate sessions.

        Returns:
            Evaluation ids of the latest invocation keyed by (session_id,
            evaluator_version_id, params_hash), identities without a match
            omitted.
        """
        ...

    async def add_replay_links(
        self, links: Sequence[tuple[uuid.UUID, uuid.UUID]]
    ) -> None:
        """Link replays to evaluations they adopted instead of re-running.

        Args:
            links: (replay_id, evaluation_id) pairs to link.
        """
        ...

    async def list_replay_evaluations(
        self, replay_ids: Sequence[uuid.UUID]
    ) -> list[tuple[uuid.UUID, EvaluationWithEvaluator]]:
        """Load the evaluations linked to a set of replays.

        Args:
            replay_ids: Ids of the replays.

        Returns:
            (replay_id, evaluation) pairs, each evaluation paired with its
            evaluator name and version.
        """
        ...
