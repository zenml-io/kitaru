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
"""Replay DTO conversions."""

from kitaru.api_models.v1.replays import DiffNode as DiffNodeModel
from kitaru.api_models.v1.replays import DiffValue as DiffValueModel
from kitaru.api_models.v1.replays import NodePairDiff as NodePairDiffModel
from kitaru.api_models.v1.replays import (
    ReplayCreateRequest,
    ReplayDiffResponse,
    ReplayResponse,
)
from kitaru.api_models.v1.replays import ReplayInputDiff as ReplayInputDiffModel
from kitaru.api_models.v1.replays import ScoreDelta as ScoreDeltaModel
from kitaru.api_models.v1.replays import TokenDeltas as TokenDeltasModel
from kitaru.api_models.v1.session_nodes import NodeType as NodeTypeModel
from kitaru.server.adapters.rest.mapping.jobs import (
    override_to_domain,
    override_to_response,
    scoring_policy_to_domain,
    scoring_policy_to_response,
    tool_policy_config_to_domain,
    tool_policy_config_to_response,
)
from kitaru.server.application.models.replays import ReplayCreate
from kitaru.server.domain.job import ReplayJob
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig
from kitaru.server.domain.replay_diff import (
    DiffNode,
    DiffValue,
    NodePairDiff,
    ReplayDiff,
    ReplayInputDiff,
    ScoreDelta,
    TokenDeltas,
)


def replay_create_to_command(body: ReplayCreateRequest) -> ReplayCreate:
    """Convert a replay create request to its command.

    Args:
        body: Replay create request.

    Returns:
        Replay create command.
    """
    return ReplayCreate(
        input_session_id=body.input_session_id,
        agent_version_id=body.agent_version_id,
        override=override_to_domain(body.override),
        tool_policy=tool_policy_config_to_domain(body.tool_policy),
        scoring_policy=scoring_policy_to_domain(body.scoring_policy),
    )


def replay_to_response(
    replay: Replay, job: ReplayJob, config: ReplayConfig
) -> ReplayResponse:
    """Convert a replay entity to its response DTO.

    Args:
        replay: Stored replay.
        job: Job executing the replay.
        config: Replay config of the replay.

    Returns:
        Replay response.
    """
    assert replay.created is not None
    assert replay.updated is not None
    return ReplayResponse(
        id=replay.id,
        job_id=replay.job_id,
        experiment_run_id=replay.experiment_run_id,
        input_session_id=replay.input_session_id,
        result_session_id=job.result_session_id,
        override=override_to_response(config.override),
        tool_policy=tool_policy_config_to_response(config.tool_policy),
        scoring_policy=scoring_policy_to_response(config.scoring_policy),
        passed=replay.passed,
        score=replay.score,
        scores=replay.scores,
        error=replay.error,
        created=replay.created,
        updated=replay.updated,
    )


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
