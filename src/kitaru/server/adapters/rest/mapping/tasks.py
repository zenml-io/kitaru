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
"""Task DTO conversions."""

import uuid

from kitaru.api_models.v1.hook import (
    CommandHook,
    CopyWorkdirHook,
    TaskHook,
)
from kitaru.api_models.v1.task import (
    AgentTaskDetails,
    EvaluationTaskDetails,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    PluginSpec,
    ScriptPluginSpec,
    TaskClaimResponse,
    TaskDetails,
    TaskListParams,
    TaskResponse,
    TaskRunSpec,
    TaskSpecResponse,
    TaskUpdateRequest,
    TaskWithSpec,
)
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.task import ClaimedTask, TaskFilter, TaskUpdate
from kitaru.server.domain.hook import (
    CopyWorkdirHook as DomainCopyWorkdirHook,
)
from kitaru.server.domain.hook import (
    TaskHook as DomainTaskHook,
)
from kitaru.server.domain.task import (
    AgentTask,
    EvaluationTask,
    ImportTask,
    Task,
    TaskSpec,
)
from kitaru.server.domain.task import (
    AgentTaskDetails as DomainAgentTaskDetails,
)
from kitaru.server.domain.task import (
    EvaluationTaskDetails as DomainEvaluationTaskDetails,
)
from kitaru.server.domain.task import (
    ImportTaskDetails as DomainImportTaskDetails,
)
from kitaru.server.domain.task import (
    PayloadSpec as DomainPayloadSpec,
)
from kitaru.server.domain.task import (
    PluginSpec as DomainPluginSpec,
)
from kitaru.server.domain.task import (
    ScriptPluginSpec as DomainScriptPluginSpec,
)
from kitaru.server.domain.task import (
    TaskRunSpec as DomainTaskRunSpec,
)


def task_to_response(task: Task) -> TaskResponse:
    """Convert a task entity to its response DTO.

    Args:
        task: Stored task.

    Returns:
        Task response.
    """
    assert task.created is not None
    assert task.updated is not None
    return TaskResponse(
        id=task.id,
        job_id=task.job_id,
        kind=task.kind,
        status=task.status,
        on_failure=task.on_failure,
        attempt=task.attempt,
        labels=task.labels,
        agent_version_id=(
            task.agent_version_id if isinstance(task, AgentTask) else None
        ),
        plugin_version_id=(
            task.plugin_version_id
            if isinstance(task, EvaluationTask | ImportTask)
            else None
        ),
        payload_blob_id=task.payload_blob_id if isinstance(task, ImportTask) else None,
        input_session_id=(
            task.input_session_id if isinstance(task, EvaluationTask) else None
        ),
        agent_id=task.agent_id if isinstance(task, ImportTask) else None,
        worker_id=task.worker_id,
        claimed_at=task.claimed_at,
        heartbeat_at=task.heartbeat_at,
        cancel_requested_at=task.cancel_requested_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
        error=task.error,
        result=task.result,
        created=task.created,
        updated=task.updated,
    )


def _plugin_spec_to_response(plugin: DomainPluginSpec) -> PluginSpec:
    """Convert a plugin spec value object to its response DTO.

    Args:
        plugin: Plugin spec the task process loads its code from.

    Returns:
        Plugin spec DTO.
    """
    if isinstance(plugin, DomainScriptPluginSpec):
        return ScriptPluginSpec(
            entrypoint=plugin.entrypoint, blob_id=plugin.blob_id, sha256=plugin.sha256
        )
    return PackagePluginSpec(
        entrypoint=plugin.entrypoint, requirement=plugin.requirement
    )


def _payload_spec_to_response(payload: DomainPayloadSpec) -> PayloadSpec:
    """Convert a payload spec value object to its response DTO.

    Args:
        payload: Payload the importer parses.

    Returns:
        Payload spec DTO.
    """
    return PayloadSpec(blob_id=payload.blob_id, sha256=payload.sha256)


def _run_spec_to_response(run_spec: DomainTaskRunSpec) -> TaskRunSpec:
    """Convert a run spec value object to its response DTO.

    Args:
        run_spec: Command the worker runs.

    Returns:
        Run spec DTO.
    """
    return TaskRunSpec(
        command=run_spec.command, working_dir=run_spec.working_dir, env=run_spec.env
    )


def _hook_to_response(hook: DomainTaskHook) -> TaskHook:
    """Convert a task hook value object to its response DTO.

    Args:
        hook: Hook the worker runs around the task process.

    Returns:
        Task hook DTO.
    """
    if isinstance(hook, DomainCopyWorkdirHook):
        return CopyWorkdirHook()
    return CommandHook(
        command=hook.command, when=hook.when, run_on_failure=hook.run_on_failure
    )


def _details_to_response(spec: TaskSpec) -> TaskDetails:
    """Convert the kind-specific details of a task spec to their response DTO.

    Args:
        spec: Execution spec.

    Raises:
        ValueError: The details are not one of the three known kinds.

    Returns:
        Task details DTO.
    """
    details = spec.details
    if isinstance(details, DomainAgentTaskDetails):
        return AgentTaskDetails(inputs=details.inputs, replay_id=details.replay_id)
    if isinstance(details, DomainEvaluationTaskDetails):
        return EvaluationTaskDetails(
            evaluator_name=details.evaluator_name,
            params=details.params,
            plugin=_plugin_spec_to_response(details.plugin),
            input_session_id=details.input_session_id,
        )
    if isinstance(details, DomainImportTaskDetails):
        return ImportTaskDetails(
            plugin=_plugin_spec_to_response(details.plugin),
            payload=_payload_spec_to_response(details.payload),
            provider=details.provider,
            agent_id=details.agent_id,
            params=details.params,
        )
    raise ValueError(f"Task {spec.task_id} details have no response mapping")


def spec_to_response(spec: TaskSpec) -> TaskSpecResponse:
    """Convert a task spec value object to its response DTO.

    Args:
        spec: Execution spec.

    Returns:
        Task spec response.
    """
    return TaskSpecResponse(
        task_id=spec.task_id,
        kind=spec.kind,
        timeout_seconds=spec.timeout_seconds,
        run=(
            _run_spec_to_response(spec.run_spec) if spec.run_spec is not None else None
        ),
        env=spec.env,
        secret_env=spec.secret_env,
        hooks=[_hook_to_response(hook) for hook in spec.hooks],
        details=_details_to_response(spec),
    )


def claimed_tasks_to_response(
    claimed: list[ClaimedTask], tokens: dict[uuid.UUID, str]
) -> TaskClaimResponse:
    """Convert claimed tasks and their specs to the claim response DTO.

    Args:
        claimed: Claimed tasks paired with their execution specs.
        tokens: Task token by task id, one per claimed task.

    Returns:
        Task claim response.
    """
    return TaskClaimResponse(
        tasks=[
            TaskWithSpec(
                task=task_to_response(item.task),
                spec=spec_to_response(item.spec),
                token=tokens[item.task.id],
            )
            for item in claimed
        ]
    )


def task_list_params_to_filter(params: TaskListParams) -> TaskFilter:
    """Convert task list params to the application filter.

    Args:
        params: Task list params.

    Returns:
        Task filter.
    """
    return TaskFilter(
        expression=filter_to_expression(params.filter)
        if params.filter is not None
        else None,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def task_update_to_command(body: TaskUpdateRequest) -> TaskUpdate:
    """Convert a task update request to its command.

    Args:
        body: Task update request.

    Returns:
        Task update command carrying only the request's set fields.
    """
    return TaskUpdate(**body.model_dump(exclude_unset=True))
