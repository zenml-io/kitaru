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
"""Job API models."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field

from kitaru.api_models.v1.agent_versions import ExecutionTarget
from kitaru.api_models.v1.base import (
    FiniteFloat,
    JsonValue,
    RequestModel,
    ResponseModel,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.session_nodes import NodeType
from kitaru.api_models.v1.sessions import SessionProvider

MAX_IMPORT_FAILURES = 20

SOURCE_REF_PATTERN = r"^[^:\s]+:[^:\s]+$"


class JobKind(StrEnum):
    """Job kind."""

    REPLAY = "replay"
    SESSION_RUN = "session_run"
    SCORE = "score"
    IMPORT = "import"


class JobStatus(StrEnum):
    """Job status."""

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    SCORING = "scoring"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"


class HistoryScope(StrEnum):
    """History policy lookup scope."""

    ORIGINAL_SESSION = "original_session"
    COHORT = "cohort"
    AGENT = "agent"


class ToolPolicyOnMiss(StrEnum):
    """Tool policy miss behavior."""

    FAIL = "fail"
    PASSTHROUGH = "passthrough"
    ERROR_RESULT = "error_result"


class StaticMatchMode(StrEnum):
    """Static case match mode."""

    EXACT = "exact"
    SUBSET = "subset"


class ReplayOverride(RequestModel):
    """Execution override."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: str | dict[str, str] | None = Field(
        default=None,
        description="Replacement model, or an old to new model map.",
    )
    system_prompt: str | None = Field(
        default=None, description="Replacement system prompt."
    )
    prompt: str | None = Field(default=None, description="Replacement session inputs.")
    model_params: dict[str, JsonValue] | None = Field(
        default=None, description="Replacement model parameters."
    )


class ScorerConfigBase(RequestModel):
    """Scorer config union member base."""

    def model_post_init(self, context: Any) -> None:
        """Mark the type discriminator as set so exclude_unset dumps keep it.

        Args:
            context: Pydantic validation context.
        """
        _ = context
        self.model_fields_set.add("type")


class SourceScorerConfig(ScorerConfigBase):
    """Source scorer configuration."""

    type: Literal["source"] = "source"
    name: str = Field(description="Scorer name, unique within the policy.")
    source: str = Field(
        pattern=SOURCE_REF_PATTERN,
        description="Scoring function reference as 'module:attribute'.",
    )
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Keyword arguments for the function."
    )
    weight: FiniteFloat = Field(default=1.0, ge=0, description="Weight in the average.")
    fail_below: FiniteFloat | None = Field(
        default=None,
        description="Score at or below which the job fails outright.",
    )


class RegistryScorerConfig(ScorerConfigBase):
    """Registry scorer configuration."""

    type: Literal["scorer"] = "scorer"
    name: str = Field(description="Name of the registered scorer.")
    version: int | None = Field(
        default=None,
        description="Registered version to run, the latest one when omitted.",
    )
    params: dict[str, JsonValue] = Field(
        default_factory=dict, description="Keyword arguments for the function."
    )
    weight: FiniteFloat = Field(default=1.0, ge=0, description="Weight in the average.")
    fail_below: FiniteFloat | None = Field(
        default=None,
        description="Score at or below which the job fails outright.",
    )


ScorerConfig = Annotated[
    SourceScorerConfig | RegistryScorerConfig,
    Field(discriminator="type"),
]


class ScoringPolicy(RequestModel):
    """Scoring policy."""

    scorers: list[ScorerConfig] = Field(min_length=1, description="Scorers to run.")
    pass_threshold: FiniteFloat = Field(
        ge=0, le=1, description="Weighted average required to pass."
    )


class StaticCase(RequestModel):
    """Static tool result case."""

    match: dict[str, JsonValue] | None = Field(
        default=None, description="Inputs to match, any inputs when omitted."
    )
    match_mode: StaticMatchMode = Field(
        default=StaticMatchMode.EXACT, description="Match mode."
    )
    result: JsonValue = Field(default=None, description="Tool result to return.")


class ToolPolicyBase(RequestModel):
    """Tool policy union member base."""

    def model_post_init(self, context: Any) -> None:
        """Mark the type discriminator as set so exclude_unset dumps keep it.

        Args:
            context: Pydantic validation context.
        """
        _ = context
        self.model_fields_set.add("type")


class PassthroughPolicy(ToolPolicyBase):
    """Passthrough tool policy."""

    type: Literal["passthrough"] = "passthrough"


class HistoryPolicy(ToolPolicyBase):
    """History tool policy."""

    type: Literal["history"] = "history"
    scope: HistoryScope = Field(
        default=HistoryScope.ORIGINAL_SESSION, description="Lookup scope."
    )
    on_miss: ToolPolicyOnMiss = Field(
        default=ToolPolicyOnMiss.FAIL, description="Miss behavior."
    )


class StaticPolicy(ToolPolicyBase):
    """Static tool policy."""

    type: Literal["static"] = "static"
    cases: list[StaticCase] = Field(
        description="Cases evaluated in order, first match wins."
    )
    on_miss: ToolPolicyOnMiss = Field(
        default=ToolPolicyOnMiss.FAIL, description="Miss behavior."
    )


class LLMPolicy(ToolPolicyBase):
    """LLM tool policy."""

    type: Literal["llm"] = "llm"
    model: str = Field(description="Model generating the tool results.")
    instructions: str | None = Field(
        default=None, description="Generation instructions."
    )


ToolPolicy = Annotated[
    PassthroughPolicy | HistoryPolicy | StaticPolicy | LLMPolicy,
    Field(discriminator="type"),
]


class ToolPolicyConfig(RequestModel):
    """Tool policy configuration."""

    default: ToolPolicy = Field(description="Policy for tools without an override.")
    tools: dict[str, ToolPolicy] = Field(
        default_factory=dict, description="Per-tool override by tool name."
    )


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


class ImportFailure(RequestModel):
    """Import failure."""

    line: int = Field(
        ge=0,
        description="Payload line the failure occurred on, the stream "
        "position for ingest failures.",
    )
    external_id: str | None = Field(
        default=None, description="External id of the failed session."
    )
    error: str = Field(description="Error message.")


class ImportStats(RequestModel):
    """Import stats."""

    created: int = Field(ge=0, description="Number of imported sessions.")
    skipped: int = Field(ge=0, description="Number of already imported sessions.")
    failed: int = Field(ge=0, description="Number of sessions that failed to parse.")
    failures: list[ImportFailure] = Field(
        default_factory=list,
        max_length=MAX_IMPORT_FAILURES,
        description="Sample of the recorded failures.",
    )


class JobResponse(ResponseModel):
    """Job response."""

    id: uuid.UUID = Field(description="Job id.")
    kind: JobKind = Field(description="Job kind.")
    experiment_run_id: uuid.UUID | None = Field(
        description="Id of the experiment run, null for standalone jobs."
    )
    agent_version_id: uuid.UUID | None = Field(
        description="Id of the agent version, null for registry score jobs."
    )
    agent_id: uuid.UUID | None = Field(
        description="Id of the agent, null outside import jobs."
    )
    parent_job_id: uuid.UUID | None = Field(
        description="Id of the job this job was fanned out from."
    )
    input_session_id: uuid.UUID | None = Field(
        description="Id of the session the job reads, null for session runs."
    )
    result_session_id: uuid.UUID | None = Field(description="Id of the result session.")
    scorer: ScorerConfig | None = Field(
        description="Scorer configuration, null outside score jobs."
    )
    plugin_version_id: uuid.UUID | None = Field(
        description="Id of the pinned plugin version."
    )
    payload_blob_id: uuid.UUID | None = Field(
        description="Id of the payload blob, null outside import jobs."
    )
    status: JobStatus = Field(description="Job status.")
    attempt: int = Field(description="Attempt counter.")
    worker_id: uuid.UUID | None = Field(description="Id of the claiming worker.")
    execution_target: ExecutionTarget = Field(description="Execution target.")
    executor_handle: str | None = Field(description="Executor handle.")
    inputs: Any = Field(
        description="Session inputs or importer params, null for replay jobs."
    )
    name: str | None = Field(description="Session run name.")
    claimed_at: datetime | None = Field(description="Claim time.")
    heartbeat_at: datetime | None = Field(description="Last heartbeat time.")
    started_at: datetime | None = Field(description="Execution start time.")
    ended_at: datetime | None = Field(description="Execution end time.")
    error: str | None = Field(description="Error message.")
    passed: bool | None = Field(description="Scoring outcome, null until scored.")
    score: float | None = Field(
        description="Weighted average of a replay or the value a score job "
        "produced, null until scored."
    )
    scores: dict[str, float] | None = Field(
        description="Scores by scorer name, null until scored."
    )
    diff: dict[str, Any] | None = Field(
        description="Diff summary, written at completion."
    )
    stats: ImportStats | None = Field(
        description="Import results, null until the importer reports them."
    )
    override: ReplayOverride | None = Field(description="Execution override.")
    tool_policy: ToolPolicyConfig | None = Field(
        description="Tool policy, null for session runs."
    )
    scoring_policy: ScoringPolicy | None = Field(
        description="Scoring policy, null for session runs."
    )
    created: datetime = Field(description="Creation time.")
    updated: datetime = Field(description="Last modification time.")


class JobUpdateRequest(RequestModel):
    """Job update request."""

    status: JobStatus | None = Field(default=None, description="Target status.")
    error: str | None = Field(
        default=None, description="Error message, required for failed and timed out."
    )
    score: FiniteFloat | None = Field(
        default=None, description="Value the scorer produced, score jobs only."
    )
    stats: ImportStats | None = Field(
        default=None, description="Results the importer produced, import jobs only."
    )


class JobClaimRequest(RequestModel):
    """Job claim request."""

    worker_id: uuid.UUID = Field(description="Id of the claiming worker.")
    max_jobs: int = Field(ge=1, le=100, description="Maximum jobs to claim.")
    agent_ids: list[uuid.UUID] | None = Field(
        default=None,
        description="Ids of the agents to claim for, any agent when omitted.",
    )
    experiment_run_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the experiment run to claim for, pool-target work "
        "when omitted.",
    )
    parent_job_id: uuid.UUID | None = Field(
        default=None,
        description="Id of the job whose fanned out jobs to claim for.",
    )


class StandaloneJobClaimRequest(RequestModel):
    """Standalone job claim request."""

    worker_id: uuid.UUID = Field(description="Id of the claiming worker.")


class JobSpecRun(ResponseModel):
    """Job spec run command."""

    command: str = Field(description="Bash command starting the agent.")
    working_dir: str | None = Field(description="Working directory for the command.")
    env: dict[str, str] = Field(description="Literal environment variables.")
    timeout_seconds: int = Field(description="Wall clock limit.")


class JobSpecPlugin(ResponseModel):
    """Job spec plugin code."""

    format: PluginFormat = Field(description="Code format.")
    entrypoint: str = Field(description="Attribute implementing the plugin.")
    blob_id: uuid.UUID = Field(description="Id of the code blob.")
    sha256: str = Field(description="Hash of the code blob content.")


class JobSpecScorer(ResponseModel):
    """Job spec scorer."""

    config: ScorerConfig = Field(description="Scorer configuration.")
    plugin: JobSpecPlugin | None = Field(
        description="Registered code, null for source scorers."
    )
    input_session_id: uuid.UUID = Field(description="Id of the session to score.")


class JobSpecPayload(ResponseModel):
    """Job spec payload."""

    blob_id: uuid.UUID = Field(description="Id of the payload blob.")
    sha256: str = Field(description="Hash of the payload blob content.")


class JobSpecImporter(ResponseModel):
    """Job spec importer."""

    plugin: JobSpecPlugin = Field(description="Registered code.")
    payload: JobSpecPayload = Field(description="Payload to import.")
    provider: SessionProvider = Field(description="Provider of the imported sessions.")
    agent_id: uuid.UUID = Field(description="Id of the agent the sessions bind to.")
    params: dict[str, JsonValue] = Field(
        description="Keyword arguments for the importer."
    )


class JobSpecResponse(ResponseModel):
    """Job spec response."""

    job_id: uuid.UUID = Field(description="Job id.")
    kind: JobKind = Field(description="Job kind.")
    inputs: Any = Field(
        description="Effective session inputs, with any prompt override applied."
    )
    override: ReplayOverride | None = Field(description="Execution override.")
    tool_policy: ToolPolicyConfig | None = Field(
        description="Tool policy, null outside replays."
    )
    scorer: JobSpecScorer | None = Field(
        description="Scorer to run, null outside score jobs."
    )
    importer: JobSpecImporter | None = Field(
        description="Importer to run, null outside import jobs."
    )
    run: JobSpecRun | None = Field(
        description="Run command of the agent version, null for registry score jobs."
    )
    secret_env: dict[str, str] = Field(
        description="Resolved secret environment variables."
    )
    input_session_id: uuid.UUID | None = Field(
        description="Id of the session the job reads, null for session runs."
    )
    name: str | None = Field(description="Session run name.")


class ClaimedJobResponse(ResponseModel):
    """Claimed job."""

    job: JobResponse = Field(description="Claimed job.")
    spec: JobSpecResponse = Field(description="Spec the runner executes the job with.")


class JobClaimResponse(ResponseModel):
    """Job claim response."""

    jobs: list[ClaimedJobResponse] = Field(description="Claimed jobs.")


class ToolLookupRequest(RequestModel):
    """Tool lookup request."""

    tool_name: str = Field(max_length=255, description="Name of the called tool.")
    inputs: JsonValue = Field(default=None, description="Tool call inputs.")
    cache_key: str = Field(
        min_length=64, max_length=64, description="Cache key of the tool call."
    )


class ToolLookupResponse(ResponseModel):
    """Tool lookup response."""

    found: bool = Field(description="Whether a recorded result matched.")
    result: Any = Field(description="Recorded tool result, null on a miss.")


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
