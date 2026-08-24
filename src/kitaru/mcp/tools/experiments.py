#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Experiment management handler."""

from kitaru.api_models.v1.experiment import (
    ExperimentCreateRequest,
    ExperimentUpdateRequest,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.management import (
    ExperimentCreate,
    ExperimentsManageRequest,
)
from kitaru.mcp.tools.evaluator_resolution import resolve_evaluator_selections


async def handle_experiments_manage(
    state: MCPServerState, request: ExperimentsManageRequest
) -> object:
    """Perform one exact-ID experiment mutation."""
    resolved = (
        None
        if request.evaluators is None
        else await resolve_evaluator_selections(state, request.evaluators)
    )
    evaluators = None if resolved is None else resolved.configs
    if isinstance(request, ExperimentCreate):
        dto = ExperimentCreateRequest(
            name=request.name,
            description=request.description,
            agent_id=request.agent_id,
            override=request.override,
            tool_policy=request.tool_policy,
            evaluators=evaluators or [],
        )
        return await state.client.experiments.create(
            dto, idempotency_key=request.idempotency_key
        )
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
