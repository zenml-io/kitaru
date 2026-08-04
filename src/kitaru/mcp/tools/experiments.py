#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Experiment management handler."""

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentUpdateRequest,
)
from kitaru.api_models.v1.replay_config import EvaluatorConfig
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.management import (
    EvaluatorSelection,
    ExperimentCreate,
    ExperimentsManageRequest,
)


async def handle_experiments_manage(
    state: MCPServerState, request: ExperimentsManageRequest
) -> object:
    """Perform one exact-ID experiment mutation."""
    evaluators = await _get_evaluator_configs(state, request.evaluators)
    if isinstance(request, ExperimentCreate):
        dto = ExperimentCreateRequest(
            name=request.name,
            description=request.description,
            override=request.override,
            tool_policy=request.tool_policy,
            evaluators=evaluators or [],
        )
        return await state.client.experiments.create(dto)
    values = request.model_dump(
        include={"name", "description", "override", "tool_policy"},
        exclude_unset=True,
    )
    for field, clear in (
        ("description", request.clear_description),
        ("override", request.clear_override),
        ("tool_policy", request.clear_tool_policy),
    ):
        if clear:
            values[field] = None
    if evaluators is not None:
        values["evaluators"] = [config.model_dump(mode="json") for config in evaluators]
    return await state.client.experiments.update(
        request.experiment_id, ExperimentUpdateRequest.model_validate(values)
    )


async def _get_evaluator_configs(
    state: MCPServerState, selections: list[EvaluatorSelection] | None
) -> list[EvaluatorConfig] | None:
    if selections is None:
        return None
    identities = [(item.evaluator_id, item.version) for item in selections]
    if len(set(identities)) != len(identities):
        raise MCPToolError("invalid_arguments", "Evaluator selections must be unique.")
    configs: list[EvaluatorConfig] = []
    for selection in selections:
        version = await state.client.evaluators.get_version(
            selection.evaluator_id, selection.version
        )
        if version.evaluator_id != selection.evaluator_id:
            raise MCPToolError(
                "conflict", "Evaluator version resolved to a different parent."
            )
        configs.append(
            EvaluatorConfig(evaluator_version_id=version.id, params=selection.params)
        )
    return configs
