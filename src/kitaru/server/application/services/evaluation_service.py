#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Evaluation read and manual-upsert use cases."""

import uuid
from typing import Any

from kitaru.server.application.interfaces.evaluation_repository import (
    EvaluationRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.evaluation import Evaluation, EvaluationDataType
from kitaru.server.domain.plugin import PluginVersion

EnrichedEvaluation = tuple[Evaluation, str | None, PluginVersion | None]


class EvaluationService:
    """Evaluation read and manual-upsert use cases."""

    def __init__(
        self,
        repository: EvaluationRepository,
        session_repository: SessionRepository,
        plugin_repository: PluginRepository,
    ) -> None:
        self._repository = repository
        self._session_repository = session_repository
        self._plugin_repository = plugin_repository

    async def _enrich(self, evaluations: list[Evaluation]) -> list[EnrichedEvaluation]:
        version_ids = {
            item.evaluator_version_id
            for item in evaluations
            if item.evaluator_version_id is not None
        }
        versions = (
            await self._plugin_repository.get_many_versions(list(version_ids))
            if version_ids
            else {}
        )
        plugins = {
            version.plugin_id: await self._plugin_repository.get(version.plugin_id)
            for version in versions.values()
        }
        return [
            (
                item,
                (
                    plugins[versions[item.evaluator_version_id].plugin_id].name
                    if item.evaluator_version_id in versions
                    else None
                ),
                versions.get(item.evaluator_version_id),
            )
            for item in evaluations
        ]

    async def get_evaluation(
        self, evaluation_id: uuid.UUID, actor: AuthContext
    ) -> EnrichedEvaluation:
        """Get an evaluation."""
        _ = actor
        evaluation = await self._repository.get(evaluation_id)
        return (await self._enrich([evaluation]))[0]

    async def list_evaluations(
        self, evaluation_filter: EvaluationFilter, actor: AuthContext
    ) -> tuple[list[EnrichedEvaluation], str | None]:
        """List evaluations."""
        _ = actor
        evaluations, cursor = await self._repository.query(evaluation_filter)
        return await self._enrich(evaluations), cursor

    async def merge_evaluations(
        self,
        session_id: uuid.UUID,
        evaluations: list[dict[str, Any]],
        actor: AuthContext,
    ) -> list[Evaluation]:
        """Upsert manual evaluations by session and name."""
        await self._session_repository.get(session_id)
        manual: list[Evaluation] = []
        for result in evaluations:
            score = result.get("score")
            value = result.get("value")
            if score is not None and value is not None:
                data_type = EvaluationDataType.CATEGORICAL
            elif isinstance(score, bool):
                data_type = EvaluationDataType.BOOL
            elif score is not None:
                data_type = EvaluationDataType.FLOAT
            elif value is not None:
                data_type = EvaluationDataType.STR
            else:
                raise ValidationError("Evaluation score or value is required")
            manual.append(
                Evaluation(
                    owner_id=actor.account.id,
                    session_id=session_id,
                    name=result["name"],
                    data_type=data_type,
                    score=score,
                    value=value,
                    explanation=result.get("explanation"),
                )
            )
        return await self._repository.upsert_manual(manual)
