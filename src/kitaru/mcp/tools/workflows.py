#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Immediate asynchronous workflow-start handler."""

from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.session_run import SessionRunCreateRequest
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.workflows import (
    ReplayStart,
    SessionEvaluationStart,
    SessionImportStart,
    SessionRunStart,
    WorkflowStartRequest,
)
from kitaru.mcp.tools.experiments import _get_evaluator_configs

IDEMPOTENCY_FEATURE = "idempotency.v1"


async def handle_workflow_start(
    state: MCPServerState, request: WorkflowStartRequest
) -> object:
    """Start one workflow and return immediately without polling."""
    if isinstance(request, SessionImportStart):
        return await _start_import(state, request)
    await state.require_feature(IDEMPOTENCY_FEATURE)
    if isinstance(request, ReplayStart):
        evaluators = await _get_evaluator_configs(state, request.evaluators)
        dto = ReplayCreateRequest(
            baseline_session_id=request.baseline_session_id,
            agent_version_id=request.agent_version_id,
            override=request.override,
            tool_policy=request.tool_policy,
            evaluators=evaluators or [],
            evaluate_baselines=request.evaluate_baselines,
        )
        response = await state.client.replays.create(
            dto, idempotency_key=request.request_id
        )
    elif isinstance(request, SessionRunStart):
        dto = SessionRunCreateRequest(
            agent_version_id=request.agent_version_id,
            inputs=request.inputs,
            name=request.name,
        )
        response = await state.client.session_runs.create(
            dto, idempotency_key=request.request_id
        )
    elif isinstance(request, SessionEvaluationStart):
        evaluators = await _get_evaluator_configs(state, request.evaluators)
        dto = EvaluationBatchCreateRequest(
            input_session_ids=request.session_ids, evaluators=evaluators or []
        )
        response = await state.client.evaluations.create(
            dto, idempotency_key=request.request_id
        )
    else:
        dto = ExperimentRunCreateRequest(
            cohort_version_id=request.cohort_version_id,
            agent_version_id=request.agent_version_id,
            evaluate_baselines=request.evaluate_baselines,
        )
        response = await state.client.experiments.start_run(
            request.experiment_id, dto, idempotency_key=request.request_id
        )
    return {
        "operation": request.operation,
        "request_id": request.request_id,
        "idempotency": "server-enforced",
        "result": response.model_dump(mode="json"),
    }


async def _start_import(state: MCPServerState, request: SessionImportStart) -> object:
    blob = await state.client.blobs.get(request.payload_blob_id)
    importer_version = await state.client.importers.get_version(
        request.importer_id, request.importer_version
    )
    importer = await state.client.importers.get(request.importer_id)
    agent_version = await state.client.agent_versions.get(request.agent_version_id)
    dto = ImportCreateRequest(
        importer=importer.name,
        version=importer_version.version,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        payload_blob_id=blob.id,
        params=request.params,
    )
    job = await state.client.imports.create(dto)
    return {
        "operation": request.operation,
        "idempotency": "domain-deduplicated-only",
        "blob_id": str(blob.id),
        "importer_id": str(importer.id),
        "importer_version_id": str(importer_version.id),
        "agent_id": str(agent_version.agent_id),
        "agent_version_id": str(agent_version.id),
        "result": job.model_dump(mode="json"),
    }
