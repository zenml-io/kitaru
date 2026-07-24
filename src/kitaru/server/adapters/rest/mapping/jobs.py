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

from kitaru.api_models.v1.jobs import DiffNode as DiffNodeModel
from kitaru.api_models.v1.jobs import DiffValue as DiffValueModel
from kitaru.api_models.v1.jobs import HistoryPolicy as HistoryPolicyModel
from kitaru.api_models.v1.jobs import HistoryScope as HistoryScopeModel
from kitaru.api_models.v1.jobs import (
    JobResponse,
    JobSpecResponse,
    JobSpecRun,
    JobUpdateRequest,
    ReplayCreateRequest,
    ReplayDiffResponse,
    ToolLookupResponse,
)
from kitaru.api_models.v1.jobs import JobStatus as JobStatusModel
from kitaru.api_models.v1.jobs import LLMPolicy as LLMPolicyModel
from kitaru.api_models.v1.jobs import NodePairDiff as NodePairDiffModel
from kitaru.api_models.v1.jobs import (
    PassthroughPolicy as PassthroughPolicyModel,
)
from kitaru.api_models.v1.jobs import (
    ReplayInputDiff as ReplayInputDiffModel,
)
from kitaru.api_models.v1.jobs import ReplayOverride as ReplayOverrideModel
from kitaru.api_models.v1.jobs import ScoreDelta as ScoreDeltaModel
from kitaru.api_models.v1.jobs import ScorerConfig as ScorerConfigModel
from kitaru.api_models.v1.jobs import ScoringPolicy as ScoringPolicyModel
from kitaru.api_models.v1.jobs import StaticCase as StaticCaseModel
from kitaru.api_models.v1.jobs import StaticMatchMode as StaticMatchModeModel
from kitaru.api_models.v1.jobs import StaticPolicy as StaticPolicyModel
from kitaru.api_models.v1.jobs import TokenDeltas as TokenDeltasModel
from kitaru.api_models.v1.jobs import ToolPolicy as ToolPolicyModel
from kitaru.api_models.v1.jobs import (
    ToolPolicyConfig as ToolPolicyConfigModel,
)
from kitaru.api_models.v1.jobs import (
    ToolPolicyOnMiss as ToolPolicyOnMissModel,
)
from kitaru.api_models.v1.session_nodes import NodeType as NodeTypeModel
from kitaru.server.application.models.jobs import JobUpdate, ReplayCreate
from kitaru.server.domain.job import Job, JobSpec, JobStatus
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    HistoryScope,
    LLMPolicy,
    PassthroughPolicy,
    ReplayConfig,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
    StaticCase,
    StaticMatchMode,
    StaticPolicy,
    ToolPolicy,
    ToolPolicyConfig,
    ToolPolicyOnMiss,
)
from kitaru.server.domain.replay_diff import (
    DiffNode,
    DiffValue,
    NodePairDiff,
    ReplayDiff,
    ReplayInputDiff,
    ScoreDelta,
    TokenDeltas,
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


def scoring_policy_to_domain(policy: ScoringPolicyModel) -> ScoringPolicy:
    """Convert a scoring policy DTO to its domain value object.

    Args:
        policy: Scoring policy DTO.

    Returns:
        Domain scoring policy.
    """
    return ScoringPolicy(
        scorers=[
            ScorerConfig(
                name=scorer.name,
                source=SourceRef.parse(scorer.source),
                params=scorer.params,
                weight=scorer.weight,
                fail_below=scorer.fail_below,
            )
            for scorer in policy.scorers
        ],
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
        scorers=[
            ScorerConfigModel(
                name=scorer.name,
                source=scorer.source.render(),
                params=scorer.params,
                weight=scorer.weight,
                fail_below=scorer.fail_below,
            )
            for scorer in policy.scorers
        ],
        pass_threshold=policy.pass_threshold,
    )


def replay_create_to_command(body: ReplayCreateRequest) -> ReplayCreate:
    """Convert a replay create request to its command.

    Args:
        body: Replay create request.

    Returns:
        Replay create command.
    """
    return ReplayCreate(
        original_session_id=body.original_session_id,
        agent_version_id=body.agent_version_id,
        override=override_to_domain(body.override),
        tool_policy=tool_policy_config_to_domain(body.tool_policy),
        scoring_policy=scoring_policy_to_domain(body.scoring_policy),
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


def job_to_response(job: Job, config: ReplayConfig) -> JobResponse:
    """Convert a job entity to its response DTO.

    Args:
        job: Stored job.
        config: Replay config of the job.

    Returns:
        Job response.
    """
    assert job.created is not None
    assert job.updated is not None
    return JobResponse(
        id=job.id,
        experiment_run_id=job.experiment_run_id,
        agent_version_id=job.agent_version_id,
        original_session_id=job.original_session_id,
        result_session_id=job.result_session_id,
        status=JobStatusModel(job.status.value),
        attempt=job.attempt,
        worker_id=job.worker_id,
        claimed_at=job.claimed_at,
        heartbeat_at=job.heartbeat_at,
        started_at=job.started_at,
        ended_at=job.ended_at,
        error=job.error,
        passed=job.passed,
        score=job.score,
        scores=job.scores,
        diff=job.diff,
        override=override_to_response(config.override),
        tool_policy=tool_policy_config_to_response(config.tool_policy),
        scoring_policy=scoring_policy_to_response(config.scoring_policy),
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
        status=JobStatus(body.status.value),
        error=body.error,
        passed=body.passed,
        score=body.score,
        scores=body.scores,
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
        inputs=spec.inputs,
        override=override_to_response(spec.override),
        tool_policy=tool_policy_config_to_response(spec.tool_policy),
        scoring_policy=scoring_policy_to_response(spec.scoring_policy),
        score_baselines=spec.score_baselines,
        run=JobSpecRun(
            command=spec.run_spec.command,
            working_dir=spec.run_spec.working_dir,
            env=spec.run_spec.env,
            timeout_seconds=spec.run_spec.timeout_seconds,
        ),
        secret_env={
            name: value.get_secret_value() for name, value in spec.secret_env.items()
        },
        original_session_id=spec.original_session_id,
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


def _diff_value_to_response(value: DiffValue) -> DiffValueModel:
    """Convert a domain diff value to its DTO.

    Args:
        value: Domain diff value.

    Returns:
        Diff value DTO.
    """
    return DiffValueModel(original=value.original, effective=value.effective)


def _input_diff_to_response(diff: ReplayInputDiff) -> ReplayInputDiffModel:
    """Convert a domain input diff to its DTO.

    Args:
        diff: Domain input diff.

    Returns:
        Input diff DTO.
    """
    return ReplayInputDiffModel(
        inputs=_diff_value_to_response(diff.inputs),
        model=_diff_value_to_response(diff.model),
        system_prompt=_diff_value_to_response(diff.system_prompt),
    )


def _token_deltas_to_response(deltas: TokenDeltas) -> TokenDeltasModel:
    """Convert domain token deltas to their DTO.

    Args:
        deltas: Domain token deltas.

    Returns:
        Token deltas DTO.
    """
    return TokenDeltasModel(
        input_tokens=deltas.input_tokens,
        output_tokens=deltas.output_tokens,
        cached_input_tokens=deltas.cached_input_tokens,
        reasoning_tokens=deltas.reasoning_tokens,
    )


def _node_pair_to_response(pair: NodePairDiff) -> NodePairDiffModel:
    """Convert a domain node pair diff to its DTO.

    Args:
        pair: Domain node pair diff.

    Returns:
        Node pair diff DTO.
    """
    return NodePairDiffModel(
        key=pair.key,
        node_type=NodeTypeModel(pair.node_type.value),
        original_node_id=pair.original_node_id,
        result_node_id=pair.result_node_id,
        cost_delta=pair.cost_delta,
        token_deltas=_token_deltas_to_response(pair.token_deltas),
        duration_delta=pair.duration_delta,
        outputs_equal=pair.outputs_equal,
        mocked=pair.mocked,
        cache_key_changed=pair.cache_key_changed,
    )


def _diff_node_to_response(node: DiffNode) -> DiffNodeModel:
    """Convert a domain unmatched diff node to its DTO.

    Args:
        node: Domain diff node.

    Returns:
        Diff node DTO.
    """
    return DiffNodeModel(
        id=node.id,
        key=node.key,
        node_type=NodeTypeModel(node.node_type.value),
        name=node.name,
    )


def _score_delta_to_response(delta: ScoreDelta) -> ScoreDeltaModel:
    """Convert a domain score delta to its DTO.

    Args:
        delta: Domain score delta.

    Returns:
        Score delta DTO.
    """
    return ScoreDeltaModel(
        original=delta.original, replay=delta.replay, delta=delta.delta
    )


def replay_diff_to_response(diff: ReplayDiff) -> ReplayDiffResponse:
    """Convert a domain replay diff to its response DTO.

    Args:
        diff: Computed replay diff.

    Returns:
        Replay diff response.
    """
    return ReplayDiffResponse(
        replay_id=diff.replay_id,
        original_session_id=diff.original_session_id,
        result_session_id=diff.result_session_id,
        input_diff=_input_diff_to_response(diff.input_diff),
        node_pairs=[_node_pair_to_response(pair) for pair in diff.node_pairs],
        added_nodes=[_diff_node_to_response(node) for node in diff.added_nodes],
        removed_nodes=[_diff_node_to_response(node) for node in diff.removed_nodes],
        score_deltas={
            name: _score_delta_to_response(delta)
            for name, delta in diff.score_deltas.items()
        },
    )
