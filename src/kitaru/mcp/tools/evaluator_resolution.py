#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Shared exact evaluator-selection resolution."""

import asyncio
import uuid
from dataclasses import dataclass

from kitaru.api_models.v1.evaluator import EvaluatorResponse, EvaluatorVersionResponse
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.management import EvaluatorSelection


@dataclass(frozen=True, slots=True)
class ResolvedEvaluatorSelections:
    """Evaluator configs and the exact identities that produced them."""

    configs: list[EvaluatorConfig]
    versions: list[EvaluatorVersionResponse]


async def resolve_evaluator_selections(
    state: MCPServerState, selections: list[EvaluatorSelection]
) -> ResolvedEvaluatorSelections:
    """Resolve bounded exact parent/version selections for a mutation."""
    identities = [(item.evaluator_id, item.version) for item in selections]
    if len(set(identities)) != len(identities):
        raise MCPToolError("invalid_arguments", "Evaluator selections must be unique.")
    parent_ids = list(dict.fromkeys(item.evaluator_id for item in selections))
    concurrency = min(
        state.settings.pool_size,
        len(parent_ids) + len(selections),
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def get_parent(evaluator_id: uuid.UUID) -> EvaluatorResponse:
        async with semaphore:
            return await state.client.evaluators.get(evaluator_id)

    async def get_version(
        evaluator_id: uuid.UUID, version: int
    ) -> EvaluatorVersionResponse:
        async with semaphore:
            return await state.client.evaluators.get_version(evaluator_id, version)

    parent_tasks = [asyncio.create_task(get_parent(item_id)) for item_id in parent_ids]
    version_tasks = [
        asyncio.create_task(get_version(selection.evaluator_id, selection.version))
        for selection in selections
    ]
    tasks = [*parent_tasks, *version_tasks]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    parent_results = [task.result() for task in parent_tasks]
    versions = [task.result() for task in version_tasks]
    parents = dict(zip(parent_ids, parent_results, strict=True))
    configs: list[EvaluatorConfig] = []
    for selection, version in zip(selections, versions, strict=True):
        parent = parents[selection.evaluator_id]
        if (
            parent.id != selection.evaluator_id
            or version.evaluator_id != selection.evaluator_id
            or version.version != selection.version
        ):
            raise MCPToolError(
                "conflict",
                "Evaluator selection resolved to a different parent or version.",
            )
        configs.append(
            EvaluatorConfig(
                evaluator=parent.name,
                version=version.version,
                params=selection.params,
            )
        )
    return ResolvedEvaluatorSelections(configs=configs, versions=versions)
