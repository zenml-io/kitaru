#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Evaluation and experiment-run workflow starts."""

from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.workflows import (
    EvaluationStart,
    WorkflowStartRequest,
)
from kitaru.mcp.tools.evaluator_resolution import resolve_evaluator_selections


async def handle_workflow_start(
    state: MCPServerState, request: WorkflowStartRequest
) -> object:
    """Start one workflow and return immediately without polling."""
    if isinstance(request, EvaluationStart):
        resolved = await resolve_evaluator_selections(state, request.evaluators)
        job = await state.client.evaluations.create(
            EvaluationBatchCreateRequest(
                input_session_ids=request.session_ids,
                evaluators=resolved.configs,
            ),
            idempotency_key=request.idempotency_key,
        )
        return {
            "operation": "evaluation",
            "input_session_ids": [str(item_id) for item_id in request.session_ids],
            "evaluators": [
                {
                    "evaluator_id": str(selection.evaluator_id),
                    "evaluator_version_id": str(version.id),
                    "version": version.version,
                }
                for selection, version in zip(
                    request.evaluators, resolved.versions, strict=True
                )
            ],
            "result": job.model_dump(mode="json"),
        }
    dto = ExperimentRunCreateRequest(
        cohort_version_id=request.cohort_version_id,
        agent_version_id=request.agent_version_id,
        baseline_evaluation_mode=request.baseline_evaluation_mode,
    )
    run = await state.client.experiments.start_run(
        request.experiment_id, dto, idempotency_key=request.idempotency_key
    )
    if (
        run.experiment_id != request.experiment_id
        or run.cohort_version_id != request.cohort_version_id
        or run.agent_version_id != request.agent_version_id
    ):
        raise MCPToolError(
            "conflict", "Experiment run resolved to different exact resources."
        )
    return {
        "operation": "experiment_run",
        "experiment_id": str(request.experiment_id),
        "cohort_version_id": str(request.cohort_version_id),
        "agent_version_id": str(request.agent_version_id),
        "baseline_evaluation_mode": request.baseline_evaluation_mode,
        "result": run.model_dump(mode="json"),
    }
