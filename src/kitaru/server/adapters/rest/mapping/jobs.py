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
"""Job DTO conversions."""

from kitaru.api_models.v1.agent_versions import (
    ExecutionTarget as ExecutionTargetModel,
)
from kitaru.api_models.v1.jobs import HistoryPolicy as HistoryPolicyModel
from kitaru.api_models.v1.jobs import HistoryScope as HistoryScopeModel
from kitaru.api_models.v1.jobs import JobKind as JobKindModel
from kitaru.api_models.v1.jobs import (
    JobResponse,
    JobSpecImporter,
    JobSpecPayload,
    JobSpecPlugin,
    JobSpecResponse,
    JobSpecRun,
    JobSpecScorer,
    JobUpdateRequest,
    ToolLookupResponse,
)
from kitaru.api_models.v1.jobs import JobStatus as JobStatusModel
from kitaru.api_models.v1.jobs import LLMPolicy as LLMPolicyModel
from kitaru.api_models.v1.jobs import (
    PassthroughPolicy as PassthroughPolicyModel,
)
from kitaru.api_models.v1.jobs import (
    RegistryScorerConfig as RegistryScorerConfigModel,
)
from kitaru.api_models.v1.jobs import ReplayOverride as ReplayOverrideModel
from kitaru.api_models.v1.jobs import ScorerConfig as ScorerConfigModel
from kitaru.api_models.v1.jobs import ScoringPolicy as ScoringPolicyModel
from kitaru.api_models.v1.jobs import (
    SourceScorerConfig as SourceScorerConfigModel,
)
from kitaru.api_models.v1.jobs import StaticCase as StaticCaseModel
from kitaru.api_models.v1.jobs import StaticMatchMode as StaticMatchModeModel
from kitaru.api_models.v1.jobs import StaticPolicy as StaticPolicyModel
from kitaru.api_models.v1.jobs import ToolPolicy as ToolPolicyModel
from kitaru.api_models.v1.jobs import (
    ToolPolicyConfig as ToolPolicyConfigModel,
)
from kitaru.api_models.v1.jobs import (
    ToolPolicyOnMiss as ToolPolicyOnMissModel,
)
from kitaru.api_models.v1.jobs import WorkerScope as WorkerScopeModel
from kitaru.api_models.v1.plugins import PluginFormat as PluginFormatModel
from kitaru.api_models.v1.sessions import (
    SessionProvider as SessionProviderModel,
)
from kitaru.server.application.models.jobs import JobUpdate
from kitaru.server.domain.job import (
    Import,
    ImporterSpec,
    Job,
    JobKind,
    JobSpec,
    JobStatus,
    PluginSpec,
    ReplayJob,
    Score,
    ScorerSpec,
    SessionRun,
    WorkerScope,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    LLMPolicy,
    PassthroughPolicy,
    RegistryScorerConfig,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
    SourceScorerConfig,
    StaticCase,
    StaticMatchMode,
    StaticPolicy,
    ToolPolicy,
    ToolPolicyConfig,
    ToolPolicyOnMiss,
)
from kitaru.server.domain.session_node import SessionNode


def override_to_domain(override: ReplayOverrideModel | None) -> ReplayOverride | None:
    """Convert an optional override DTO to its domain value object.

    Args:
        override: Override DTO.

    Returns:
        Domain override, ``None`` for ``None``.
    """
    if override is None:
        return None
    return ReplayOverride(
        model=override.model,
        system_prompt=override.system_prompt,
        prompt=override.prompt,
        model_params=override.model_params,
    )


def override_to_response(override: ReplayOverride | None) -> ReplayOverrideModel | None:
    """Convert an optional domain override to its DTO.

    Args:
        override: Domain override.

    Returns:
        Override DTO, ``None`` for ``None``.
    """
    if override is None:
        return None
    return ReplayOverrideModel(
        model=override.model,
        system_prompt=override.system_prompt,
        prompt=override.prompt,
        model_params=override.model_params,
    )


def tool_policy_to_domain(policy: ToolPolicyModel) -> ToolPolicy:
    """Convert a tool policy DTO to its domain value object.

    Args:
        policy: Tool policy DTO.

    Returns:
        Domain tool policy.
    """
    if isinstance(policy, PassthroughPolicyModel):
        return PassthroughPolicy()
    if isinstance(policy, HistoryPolicyModel):
        return HistoryPolicy(
            scope=HistoryScope(policy.scope.value),
            on_miss=ToolPolicyOnMiss(policy.on_miss.value),
        )
    if isinstance(policy, StaticPolicyModel):
        return StaticPolicy(
            cases=[
                StaticCase(
                    match=case.match,
                    match_mode=StaticMatchMode(case.match_mode.value),
                    result=case.result,
                )
                for case in policy.cases
            ],
            on_miss=ToolPolicyOnMiss(policy.on_miss.value),
        )
    return LLMPolicy(model=policy.model, instructions=policy.instructions)


def tool_policy_to_response(policy: ToolPolicy) -> ToolPolicyModel:
    """Convert a domain tool policy to its DTO.

    Args:
        policy: Domain tool policy.

    Returns:
        Tool policy DTO.
    """
    if isinstance(policy, PassthroughPolicy):
        return PassthroughPolicyModel()
    if isinstance(policy, HistoryPolicy):
        return HistoryPolicyModel(
            scope=HistoryScopeModel(policy.scope.value),
            on_miss=ToolPolicyOnMissModel(policy.on_miss.value),
        )
    if isinstance(policy, StaticPolicy):
        return StaticPolicyModel(
            cases=[
                StaticCaseModel(
                    match=case.match,
                    match_mode=StaticMatchModeModel(case.match_mode.value),
                    result=case.result,
                )
                for case in policy.cases
            ],
            on_miss=ToolPolicyOnMissModel(policy.on_miss.value),
        )
    return LLMPolicyModel(model=policy.model, instructions=policy.instructions)


def tool_policy_config_to_domain(
    config: ToolPolicyConfigModel | None,
) -> ToolPolicyConfig | None:
    """Convert an optional tool policy config DTO to its domain value object.

    Args:
        config: Tool policy config DTO.

    Returns:
        Domain tool policy config, ``None`` for ``None``.
    """
    if config is None:
        return None
    return ToolPolicyConfig(
        default=tool_policy_to_domain(config.default),
        tools={
            name: tool_policy_to_domain(policy) for name, policy in config.tools.items()
        },
    )


def tool_policy_config_to_response(config: ToolPolicyConfig) -> ToolPolicyConfigModel:
    """Convert a domain tool policy config to its DTO.

    Args:
        config: Domain tool policy config.

    Returns:
        Tool policy config DTO.
    """
    return ToolPolicyConfigModel(
        default=tool_policy_to_response(config.default),
        tools={
            name: tool_policy_to_response(policy)
            for name, policy in config.tools.items()
        },
    )


def scorer_config_to_domain(config: ScorerConfigModel) -> ScorerConfig:
    """Convert a scorer config DTO to its domain value object.

    Args:
        config: Scorer config DTO.

    Returns:
        Domain scorer config.
    """
    if isinstance(config, SourceScorerConfigModel):
        return SourceScorerConfig(
            name=config.name,
            source=SourceRef.parse(config.source),
            params=config.params,
            weight=config.weight,
            fail_below=config.fail_below,
        )
    return RegistryScorerConfig(
        name=config.name,
        version=config.version,
        params=config.params,
        weight=config.weight,
        fail_below=config.fail_below,
    )


def scorer_config_to_response(config: ScorerConfig) -> ScorerConfigModel:
    """Convert a domain scorer config to its DTO.

    Args:
        config: Domain scorer config.

    Returns:
        Scorer config DTO.
    """
    if isinstance(config, SourceScorerConfig):
        return SourceScorerConfigModel(
            name=config.name,
            source=config.source.render(),
            params=config.params,
            weight=config.weight,
            fail_below=config.fail_below,
        )
    return RegistryScorerConfigModel(
        name=config.name,
        version=config.version,
        params=config.params,
        weight=config.weight,
        fail_below=config.fail_below,
    )


def scoring_policy_to_domain(policy: ScoringPolicyModel) -> ScoringPolicy:
    """Convert a scoring policy DTO to its domain value object.

    Args:
        policy: Scoring policy DTO.

    Returns:
        Domain scoring policy.
    """
    return ScoringPolicy(
        scorers=[scorer_config_to_domain(scorer) for scorer in policy.scorers],
        pass_threshold=policy.pass_threshold,
    )


def scoring_policy_to_response(policy: ScoringPolicy) -> ScoringPolicyModel:
    """Convert a domain scoring policy to its DTO.

    Args:
        policy: Domain scoring policy.

    Returns:
        Scoring policy DTO.
    """
    return ScoringPolicyModel(
        scorers=[scorer_config_to_response(scorer) for scorer in policy.scorers],
        pass_threshold=policy.pass_threshold,
    )


def job_status_to_domain(status: JobStatusModel | None) -> JobStatus | None:
    """Convert an optional job status DTO to its domain enum.

    Args:
        status: Job status DTO.

    Returns:
        Domain job status, ``None`` for ``None``.
    """
    if status is None:
        return None
    return JobStatus(status.value)


def job_kind_to_domain(kind: JobKindModel | None) -> JobKind | None:
    """Convert an optional job kind DTO to its domain enum.

    Args:
        kind: Job kind DTO.

    Returns:
        Domain job kind, ``None`` for ``None``.
    """
    if kind is None:
        return None
    return JobKind(kind.value)


def worker_scope_to_domain(scope: WorkerScopeModel) -> WorkerScope:
    """Convert a worker scope DTO to its domain value object.

    Args:
        scope: Worker scope DTO.

    Returns:
        Domain worker scope.
    """
    return WorkerScope(
        agent_version_ids=scope.agent_version_ids,
        kinds=None
        if scope.kinds is None
        else [JobKind(kind.value) for kind in scope.kinds],
        experiment_run_id=scope.experiment_run_id,
        job_id=scope.job_id,
    )


def worker_scope_to_response(scope: WorkerScope) -> WorkerScopeModel:
    """Convert a domain worker scope to its DTO.

    Args:
        scope: Domain worker scope.

    Returns:
        Worker scope DTO.
    """
    return WorkerScopeModel(
        agent_version_ids=scope.agent_version_ids,
        kinds=None
        if scope.kinds is None
        else [JobKindModel(kind.value) for kind in scope.kinds],
        experiment_run_id=scope.experiment_run_id,
        job_id=scope.job_id,
    )


def job_to_response(job: Job) -> JobResponse:
    """Convert a job entity to its response DTO.

    Args:
        job: Stored job.

    Returns:
        Job response.
    """
    assert job.created is not None
    assert job.updated is not None
    replay = job if isinstance(job, ReplayJob) else None
    session_run = job if isinstance(job, SessionRun) else None
    score = job if isinstance(job, Score) else None
    import_job = job if isinstance(job, Import) else None
    input_session_id = None
    if replay is not None:
        input_session_id = replay.input_session_id
    elif score is not None:
        input_session_id = score.input_session_id
    inputs = None
    if session_run is not None:
        inputs = session_run.inputs
    elif import_job is not None:
        inputs = import_job.inputs
    plugin_version_id = None
    if score is not None:
        plugin_version_id = score.plugin_version_id
    elif import_job is not None:
        plugin_version_id = import_job.plugin_version_id
    return JobResponse(
        id=job.id,
        kind=JobKindModel(job.kind.value),
        experiment_run_id=None if replay is None else replay.experiment_run_id,
        agent_version_id=job.agent_version_id,
        agent_id=None if import_job is None else import_job.agent_id,
        parent_job_id=None if score is None else score.parent_job_id,
        input_session_id=input_session_id,
        result_session_id=job.result_session_id,
        scorer=None
        if score is None
        else scorer_config_to_response(score.scorer_config),
        plugin_version_id=plugin_version_id,
        payload_blob_id=None if import_job is None else import_job.payload_blob_id,
        status=JobStatusModel(job.status.value),
        attempt=job.attempt,
        worker_id=job.worker_id,
        execution_target=ExecutionTargetModel(job.execution_target.value),
        executor_handle=job.executor_handle,
        inputs=inputs,
        name=None if session_run is None else session_run.name,
        claimed_at=job.claimed_at,
        heartbeat_at=job.heartbeat_at,
        started_at=job.started_at,
        ended_at=job.ended_at,
        error=job.error,
        result=job.result,
        created=job.created,
        updated=job.updated,
    )


def job_update_to_command(body: JobUpdateRequest) -> JobUpdate:
    """Convert a job update request to its command.

    Args:
        body: Job update request.

    Returns:
        Job update command.
    """
    return JobUpdate(
        status=None if body.status is None else JobStatus(body.status.value),
        error=body.error,
        result=body.result,
    )


def _plugin_spec_to_response(plugin: PluginSpec) -> JobSpecPlugin:
    """Convert a domain plugin spec to its DTO.

    Args:
        plugin: Domain plugin spec.

    Returns:
        Job spec plugin DTO.
    """
    return JobSpecPlugin(
        format=PluginFormatModel(plugin.format.value),
        entrypoint=plugin.entrypoint,
        blob_id=plugin.blob_id,
        sha256=plugin.sha256,
    )


def _scorer_spec_to_response(scorer: ScorerSpec) -> JobSpecScorer:
    """Convert a domain scorer spec to its DTO.

    Args:
        scorer: Domain scorer spec.

    Returns:
        Job spec scorer DTO.
    """
    return JobSpecScorer(
        config=scorer_config_to_response(scorer.config),
        plugin=None
        if scorer.plugin is None
        else _plugin_spec_to_response(scorer.plugin),
        input_session_id=scorer.input_session_id,
    )


def _importer_spec_to_response(importer: ImporterSpec) -> JobSpecImporter:
    """Convert a domain importer spec to its DTO.

    Args:
        importer: Domain importer spec.

    Returns:
        Job spec importer DTO.
    """
    return JobSpecImporter(
        plugin=_plugin_spec_to_response(importer.plugin),
        payload=JobSpecPayload(
            blob_id=importer.payload.blob_id, sha256=importer.payload.sha256
        ),
        provider=SessionProviderModel(importer.provider.value),
        agent_id=importer.agent_id,
        params=importer.params,
    )


def job_spec_to_response(spec: JobSpec) -> JobSpecResponse:
    """Convert a job spec to its response DTO.

    Args:
        spec: Resolved job spec.

    Returns:
        Job spec response.
    """
    return JobSpecResponse(
        job_id=spec.job_id,
        kind=JobKindModel(spec.kind.value),
        inputs=spec.inputs,
        override=override_to_response(spec.override),
        tool_policy=None
        if spec.tool_policy is None
        else tool_policy_config_to_response(spec.tool_policy),
        scorer=None if spec.scorer is None else _scorer_spec_to_response(spec.scorer),
        importer=None
        if spec.importer is None
        else _importer_spec_to_response(spec.importer),
        run=None
        if spec.run_spec is None
        else JobSpecRun(
            command=spec.run_spec.command,
            working_dir=spec.run_spec.working_dir,
            env=spec.run_spec.env,
            timeout_seconds=spec.run_spec.timeout_seconds,
        ),
        secret_env={
            name: value.get_secret_value() for name, value in spec.secret_env.items()
        },
        input_session_id=spec.input_session_id,
        name=spec.name,
    )


def tool_lookup_to_response(node: SessionNode | None) -> ToolLookupResponse:
    """Convert a tool lookup result to its response DTO.

    Args:
        node: Matched tool call node, ``None`` on a miss.

    Returns:
        Tool lookup response.
    """
    if node is None:
        return ToolLookupResponse(found=False, result=None)
    return ToolLookupResponse(found=True, result=node.outputs)
