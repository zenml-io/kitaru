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
"""Replay API models."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from kitaru.api_models.v1.base import RequestModel, ResponseModel
from kitaru.api_models.v1.jobs import ReplayOverride, ScoringPolicy, ToolPolicyConfig
from kitaru.api_models.v1.session_nodes import NodeType


class ReplayCreateRequest(RequestModel):
    """Replay create request."""

    input_session_id: uuid.UUID = Field(description="Id of the session to replay.")
    agent_version_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the agent version to execute, the latest runnable "
        "version when omitted.",
    )
    override: ReplayOverride | None = Field(
        default=None, description="Execution override."
    )
    tool_policy: ToolPolicyConfig | None = Field(
        default=None,
        description="Tool policy, a history policy scoped to the original "
        "session when omitted.",
    )
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")


class ReplayResponse(ResponseModel):
    """Replay response."""

    id: uuid.UUID = Field(description="Replay id.")
    job_id: uuid.UUID = Field(description="Id of the job executing the replay.")
    experiment_run_id: uuid.UUID | None = Field(
        description="Id of the experiment run, null for standalone replays."
    )
    input_session_id: uuid.UUID = Field(description="Id of the session replayed.")
    result_session_id: uuid.UUID | None = Field(
        description="Id of the result session, null until the job completes."
    )
    override: ReplayOverride | None = Field(description="Execution override.")
    tool_policy: ToolPolicyConfig | None = Field(description="Tool policy.")
    scoring_policy: ScoringPolicy = Field(description="Scoring policy.")
    passed: bool | None = Field(description="Scoring outcome, null until scored.")
    score: float | None = Field(
        description="Weighted average of the scores, null until scored."
    )
    scores: dict[str, float] | None = Field(
        description="Scores by scorer name, null until scored."
    )
    error: str | None = Field(description="Error message.")
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class DiffValue(ResponseModel):
    """Original and effective value pair."""

    original: Any = Field(description="Original value.")
    effective: Any = Field(description="Effective value under the override.")


class ReplayInputDiff(ResponseModel):
    """Replay input diff."""

    inputs: DiffValue = Field(description="Session inputs.")
    model: DiffValue = Field(description="Models of the LLM calls.")
    system_prompt: DiffValue = Field(
        description="System prompt, original is null since it is not recorded."
    )


class TokenDeltas(ResponseModel):
    """Token count deltas."""

    input_tokens: int | None = Field(description="Input token delta.")
    output_tokens: int | None = Field(description="Output token delta.")
    cached_input_tokens: int | None = Field(description="Cached input token delta.")
    reasoning_tokens: int | None = Field(description="Reasoning token delta.")


class NodePairDiff(ResponseModel):
    """Node pair diff."""

    key: str = Field(description="Node key matched on.")
    node_type: NodeType = Field(description="Node type.")
    original_node_id: uuid.UUID = Field(description="Id of the original node.")
    result_node_id: uuid.UUID = Field(description="Id of the result node.")
    cost_delta: float | None = Field(description="Cost delta.")
    token_deltas: TokenDeltas = Field(description="Token count deltas.")
    duration_delta: float | None = Field(description="Duration delta in seconds.")
    outputs_equal: bool = Field(description="Whether the outputs are equal.")
    mocked: bool = Field(description="Whether the result node was mocked.")
    cache_key_changed: bool | None = Field(
        description="Whether the tool arguments drifted, null for non-tool nodes."
    )


class DiffNode(ResponseModel):
    """Unmatched diff node."""

    id: uuid.UUID = Field(description="Node id.")
    key: str = Field(description="Node key.")
    node_type: NodeType = Field(description="Node type.")
    name: str = Field(description="Display name.")


class ScoreDelta(ResponseModel):
    """Score delta."""

    original: float | None = Field(description="Original session score.")
    replay: float | None = Field(description="Replay score.")
    delta: float | None = Field(description="Replay minus original.")


class ReplayDiffResponse(ResponseModel):
    """Replay diff response."""

    replay_id: uuid.UUID = Field(description="Replay id.")
    original_session_id: uuid.UUID = Field(description="Id of the original session.")
    result_session_id: uuid.UUID = Field(description="Id of the result session.")
    input_diff: ReplayInputDiff = Field(description="Input diff.")
    node_pairs: list[NodePairDiff] = Field(description="Node pairs matched by key.")
    added_nodes: list[DiffNode] = Field(
        description="Result nodes without an original counterpart."
    )
    removed_nodes: list[DiffNode] = Field(
        description="Original nodes without a result counterpart."
    )
    score_deltas: dict[str, ScoreDelta] = Field(
        description="Score deltas by scorer name."
    )
