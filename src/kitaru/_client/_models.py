"""Internal client-facing data models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from kitaru._llm_usage import (
    LLM_USAGE_SUMMARY_METADATA_KEY,
    aggregate_usage_records,
    aggregate_usage_records_with_cost_completeness,
    parse_usage_summary,
    strip_usage_record_bookkeeping,
    usage_records_from_metadata,
    usage_reuse_classification,
)
from kitaru.config import FrozenExecutionSpec
from kitaru.errors import FailureOrigin, KitaruUsageError

if TYPE_CHECKING:
    from kitaru._experiments import Experiment
    from kitaru.client import KitaruClient
    from kitaru.replay import ReplaySubmission


def _record_identity(record: Mapping[str, Any]) -> tuple[str | None, str | None] | None:
    """Return the stable identity used to avoid duplicate usage records."""
    record_id = record.get("record_id")
    event_id = record.get("event_id")
    if record_id is None and event_id is None:
        return None
    return (
        str(record_id) if record_id is not None else None,
        str(event_id) if event_id is not None else None,
    )


class ExecutionStatus(StrEnum):
    """Simplified public execution status taxonomy."""

    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_finished(self) -> bool:
        """Whether the execution is in a terminal state."""
        return self in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }

    @property
    def is_successful(self) -> bool:
        """Whether the execution finished successfully."""
        return self is ExecutionStatus.COMPLETED


class ExecutionStatisticsDimension(StrEnum):
    """Public dimensions that execution statistics can group by."""

    STATUS = "status"
    FLOW = "flow"
    STACK = "stack"
    TAG = "tag"
    TIME = "time"
    METADATA = "metadata"


class ExecutionStatisticsTimeGranularity(StrEnum):
    """Supported time bucket sizes for execution statistics."""

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class ExecutionStatisticsMetricSource(StrEnum):
    """Numeric value sources that execution statistics can aggregate."""

    DURATION = "duration"
    STEP_COUNT = "step_count"
    CACHED_STEP_COUNT = "cached_step_count"
    OUTPUT_ARTIFACT_COUNT = "output_artifact_count"
    METADATA = "metadata"


class ExecutionStatisticsMetricAggregation(StrEnum):
    """Supported aggregation operators for execution statistics metrics."""

    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


@dataclass(frozen=True)
class ExecutionStatisticsGrouping:
    """One public grouping dimension for execution statistics.

    Simple dimensions such as ``status`` and ``tag`` only need ``dimension``.
    Time groupings must provide ``time_granularity``. Metadata groupings must
    provide ``metadata_key``. ``name`` optionally overrides the output key.
    """

    dimension: ExecutionStatisticsDimension | str
    name: str | None = None
    time_granularity: ExecutionStatisticsTimeGranularity | str | None = None
    metadata_key: str | None = None

    def __post_init__(self) -> None:
        """Normalize enum inputs and validate dimension-specific fields."""
        try:
            dimension = ExecutionStatisticsDimension(
                str(self.dimension).strip().lower()
            )
        except ValueError as exc:
            expected = ", ".join(item.value for item in ExecutionStatisticsDimension)
            raise KitaruUsageError(
                f"Unsupported execution statistics dimension {self.dimension!r}. "
                f"Expected one of: {expected}."
            ) from exc

        name = self.name
        if name is not None:
            name = name.strip()
            if not name:
                raise KitaruUsageError(
                    "Execution statistics grouping name cannot be empty."
                )

        time_granularity: ExecutionStatisticsTimeGranularity | None = None
        if self.time_granularity is not None:
            try:
                time_granularity = ExecutionStatisticsTimeGranularity(
                    str(self.time_granularity).strip().lower()
                )
            except ValueError as exc:
                expected = ", ".join(
                    item.value for item in ExecutionStatisticsTimeGranularity
                )
                raise KitaruUsageError(
                    "Unsupported execution statistics time granularity "
                    f"{self.time_granularity!r}. Expected one of: {expected}."
                ) from exc

        metadata_key = self.metadata_key
        if metadata_key is not None:
            metadata_key = metadata_key.strip()
            if not metadata_key:
                raise KitaruUsageError("Metadata grouping key cannot be empty.")

        if dimension is ExecutionStatisticsDimension.TIME:
            if time_granularity is None:
                raise KitaruUsageError(
                    "Time statistics groupings require time_granularity."
                )
            if metadata_key is not None:
                raise KitaruUsageError(
                    "Time statistics groupings cannot use metadata_key."
                )
            default_name = time_granularity.value
        elif dimension is ExecutionStatisticsDimension.METADATA:
            if metadata_key is None:
                raise KitaruUsageError(
                    "Metadata statistics groupings require metadata_key."
                )
            if time_granularity is not None:
                raise KitaruUsageError(
                    "Metadata statistics groupings cannot use time_granularity."
                )
            default_name = metadata_key
        else:
            if time_granularity is not None:
                raise KitaruUsageError(
                    f"{dimension.value!r} statistics groupings cannot use "
                    "time_granularity."
                )
            if metadata_key is not None:
                raise KitaruUsageError(
                    f"{dimension.value!r} statistics groupings cannot use metadata_key."
                )
            default_name = {
                ExecutionStatisticsDimension.STATUS: "status",
                ExecutionStatisticsDimension.FLOW: "flow_id",
                ExecutionStatisticsDimension.STACK: "stack_id",
                ExecutionStatisticsDimension.TAG: "tag",
            }[dimension]

        object.__setattr__(self, "dimension", dimension)
        object.__setattr__(self, "name", name or default_name)
        object.__setattr__(self, "time_granularity", time_granularity)
        object.__setattr__(self, "metadata_key", metadata_key)


@dataclass(frozen=True)
class ExecutionStatisticsMetric:
    """One numeric metric to compute for each execution-statistics group.

    ``name`` is the output key under each group's ``metrics`` mapping. Simple
    metrics aggregate built-in execution values such as duration or step
    counts. Metadata metrics aggregate one top-level numeric execution metadata
    key.
    """

    name: str
    source: ExecutionStatisticsMetricSource | str
    aggregation: ExecutionStatisticsMetricAggregation | str
    metadata_key: str | None = None

    def __post_init__(self) -> None:
        """Normalize enum inputs and validate source-specific fields."""
        name = self.name.strip() if isinstance(self.name, str) else self.name
        if not isinstance(name, str) or not name:
            raise KitaruUsageError("Execution statistics metric name cannot be empty.")

        try:
            source = ExecutionStatisticsMetricSource(str(self.source).strip().lower())
        except ValueError as exc:
            expected = ", ".join(item.value for item in ExecutionStatisticsMetricSource)
            raise KitaruUsageError(
                f"Unsupported execution statistics metric source {self.source!r}. "
                f"Expected one of: {expected}."
            ) from exc

        try:
            aggregation = ExecutionStatisticsMetricAggregation(
                str(self.aggregation).strip().lower()
            )
        except ValueError as exc:
            expected = ", ".join(
                item.value for item in ExecutionStatisticsMetricAggregation
            )
            raise KitaruUsageError(
                "Unsupported execution statistics metric aggregation "
                f"{self.aggregation!r}. Expected one of: {expected}."
            ) from exc

        metadata_key = self.metadata_key
        if metadata_key is not None:
            metadata_key = metadata_key.strip()
            if not metadata_key:
                raise KitaruUsageError("Metadata metric key cannot be empty.")

        if source is ExecutionStatisticsMetricSource.METADATA:
            if metadata_key is None:
                raise KitaruUsageError(
                    "Metadata statistics metrics require metadata_key."
                )
        elif metadata_key is not None:
            raise KitaruUsageError(
                f"{source.value!r} statistics metrics cannot use metadata_key."
            )

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "aggregation", aggregation)
        object.__setattr__(self, "metadata_key", metadata_key)


@dataclass(frozen=True)
class ExecutionStatisticsGroup:
    """One aggregate execution-statistics row."""

    keys: dict[str, str | int | float | bool | None]
    execution_count: int
    metrics: dict[str, float | None] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionStatistics:
    """Grouped execution statistics with counts and optional numeric metrics."""

    groups: list[ExecutionStatisticsGroup]
    truncated: bool


@dataclass(frozen=True)
class AuthServiceAccount:
    """Public metadata view of a Kitaru service account."""

    service_account_id: str
    name: str
    full_name: str
    description: str
    active: bool
    created_at: datetime | None
    updated_at: datetime | None
    avatar_url: str | None = None


@dataclass(frozen=True)
class AuthAPIKey:
    """Metadata-only view of a Kitaru service-account API key."""

    api_key_id: str
    name: str
    service_account_id: str
    service_account_name: str
    description: str
    active: bool
    created_at: datetime | None
    updated_at: datetime | None
    last_login: datetime | None
    last_rotated: datetime | None
    retain_period_minutes: int


@dataclass(frozen=True)
class AuthAPIKeyWithValue:
    """One-time API-key result returned only by create and rotate operations."""

    api_key: AuthAPIKey
    key: str = field(repr=False)
    local_key_activation_requested: bool = False
    local_key_activation_succeeded: bool | None = None
    local_key_activation_error: str | None = None
    local_key_rollback_attempted: bool = False
    local_key_rollback_succeeded: bool | None = None
    local_key_rollback_error: str | None = None
    local_key_rollback_reason: str | None = None


@dataclass(frozen=True)
class Deployment:
    """Public view of a versioned Kitaru deployment snapshot."""

    deployment_id: str
    flow: str
    version: int
    tags: dict[str, bool]
    commit_sha: str | None
    commit_dirty: bool | None
    image_digest: str | None
    created_at: datetime | None
    schema: dict[str, Any] | None
    stack: str | None


@dataclass(frozen=True)
class PendingWait:
    """Public view of an active wait condition."""

    wait_id: str
    name: str
    question: str | None
    schema: dict[str, Any] | None
    metadata: dict[str, Any]
    entered_waiting_at: datetime | None


@dataclass(frozen=True)
class FailureInfo:
    """Structured failure details for executions/checkpoints."""

    message: str
    exception_type: str | None
    traceback: str | None
    origin: FailureOrigin


@dataclass(frozen=True)
class LogEntry:
    """One runtime log entry retrieved for an execution."""

    message: str
    level: str | None = None
    timestamp: str | None = None
    source: str | None = None
    checkpoint_name: str | None = None
    module: str | None = None
    filename: str | None = None
    lineno: int | None = None


@dataclass(frozen=True)
class ExecutionEvent:
    """One live event observed for an execution."""

    exec_id: str
    kind: str
    payload: dict[str, Any]
    correlation_id: str | None
    index: int | None
    cursor: str | None
    checkpoint_id: str | None
    checkpoint_name: str | None
    step_name: str | None


@dataclass(frozen=True)
class CheckpointAttempt:
    """One checkpoint attempt in retry/failure journaling history."""

    attempt_id: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    failure: FailureInfo | None
    _checkpoint_name: str | None = field(default=None, repr=False, compare=False)
    _raw_status: str | None = field(default=None, repr=False, compare=False)
    _replay_reused: bool = field(default=False, repr=False, compare=False)

    def _llm_usage_records_with_bookkeeping(self) -> list[dict[str, Any]]:
        reused, reused_cache_status = usage_reuse_classification(
            replay_reused=self._replay_reused,
            checkpoint_status=self._raw_status or self.status,
        )
        return usage_records_from_metadata(
            self.metadata,
            source_attempt_id=self.attempt_id,
            default_checkpoint_id=self.attempt_id,
            default_checkpoint_name=self._checkpoint_name,
            reused=reused,
            reused_cache_status=reused_cache_status,
        )

    @property
    def llm_usage_records(self) -> list[dict[str, Any]]:
        """Canonical usage records attributed to this physical attempt.

        Missing checkpoint identity fields default to this attempt's step-run ID
        and normalized checkpoint name. Explicit producer identity is preserved.
        """
        return [
            strip_usage_record_bookkeeping(record)
            for record in self._llm_usage_records_with_bookkeeping()
        ]


@dataclass(frozen=True)
class ArtifactRef:
    """Reference to an artifact produced by an execution."""

    artifact_id: str
    name: str
    kind: str | None
    save_type: str
    producing_call: str | None
    metadata: dict[str, Any]
    _client: KitaruClient = field(repr=False, compare=False)
    direction: Literal["input", "output"] = "output"
    input_type: str | None = None

    def load(self) -> Any:
        """Load and materialize this artifact value."""
        artifact = self._client._get_artifact_version(
            self.artifact_id,
            hydrate=True,
        )
        return artifact.load()


@dataclass(frozen=True)
class CheckpointCall:
    """Public view of a checkpoint call inside an execution."""

    call_id: str
    name: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    original_call_id: str | None
    parent_call_ids: list[str]
    failure: FailureInfo | None
    attempts: list[CheckpointAttempt]
    artifacts: list[ArtifactRef]
    checkpoint_type: str | None = None
    checkpoint_origin: Literal["user", "adapter"] = "user"
    adapter: str | None = None
    adapter_checkpoint_kind: str | None = None
    replay_input_slots: list[str] = field(default_factory=list)
    replay_output_slots: list[str] = field(default_factory=list)

    def _llm_usage_records_with_bookkeeping(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for attempt in self.attempts:
            records.extend(attempt._llm_usage_records_with_bookkeeping())
        if records:
            return records
        return usage_records_from_metadata(
            self.metadata,
            default_checkpoint_id=self.call_id,
            default_checkpoint_name=self.name,
        )

    @property
    def llm_usage_records(self) -> list[dict[str, Any]]:
        """Canonical usage records for this checkpoint and its attempts.

        Attempt records retain physical-attempt identity. If no attempt records
        exist, missing identity fields on call metadata default to this call.
        """
        return [
            strip_usage_record_bookkeeping(record)
            for record in self._llm_usage_records_with_bookkeeping()
        ]

    @property
    def aggregated_llm_usage_summary(self) -> dict[str, Any]:
        """Aggregate usage while retaining retry-attempt identities."""
        return aggregate_usage_records(self._llm_usage_records_with_bookkeeping())

    def aggregated_llm_usage_with_cost_completeness(
        self,
    ) -> tuple[dict[str, Any], bool]:
        """Aggregate usage and retain internal cost-completeness state."""
        return aggregate_usage_records_with_cost_completeness(
            self._llm_usage_records_with_bookkeeping()
        )


@dataclass(frozen=True)
class Execution:
    """Public view of a Kitaru execution."""

    exec_id: str
    flow_id: str | None
    flow_name: str | None
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    stack_name: str | None
    metadata: dict[str, Any]
    status_reason: str | None
    failure: FailureInfo | None
    pending_wait: PendingWait | None
    frozen_execution_spec: FrozenExecutionSpec | None
    original_exec_id: str | None
    checkpoints: list[CheckpointCall]
    artifacts: list[ArtifactRef]
    _client: KitaruClient = field(repr=False, compare=False)
    project_id: str | None = None
    project_name: str | None = None

    @property
    def llm_usage_summary(self) -> dict[str, Any] | None:
        """Execution-level LLM usage summary, when terminal aggregation ran."""
        return parse_usage_summary(self.metadata.get(LLM_USAGE_SUMMARY_METADATA_KEY))

    @property
    def llm_usage_records(self) -> list[dict[str, Any]]:
        """Canonical usage records from execution and checkpoint metadata.

        Checkpoint-owned records carry attempt-aware identity when producer
        identity is absent. Execution-level records remain unattributed unless
        their producer supplied checkpoint identity explicitly.
        """
        checkpoint_records: list[dict[str, Any]] = []
        checkpoint_identities: set[tuple[str | None, str | None]] = set()
        for checkpoint in self.checkpoints:
            checkpoint_records.extend(checkpoint.llm_usage_records)
        for record in checkpoint_records:
            identity = _record_identity(record)
            if identity is not None:
                checkpoint_identities.add(identity)

        run_records = usage_records_from_metadata(
            self.metadata,
            source_attempt_id=f"run:{self.exec_id}",
        )
        unique_run_records = [
            record
            for record in run_records
            if (identity := _record_identity(record)) is None
            or identity not in checkpoint_identities
        ]
        return [
            strip_usage_record_bookkeeping(record)
            for record in [*unique_run_records, *checkpoint_records]
        ]

    @property
    def experiments(self) -> list[Experiment]:
        """Return attempts whose verified frozen targets contain this execution."""
        return self._client.agents.experiments.list_for_execution(
            self.exec_id,
            agent=self.project_id,
        )

    @property
    def replays(self) -> list[Execution]:
        """Return tagged replay descendants across this execution's attempts."""
        return self._client.executions._list_experiment_replays(self.exec_id)

    @property
    def original(self) -> Execution | None:
        """Return the authoritative immediate replay parent, when present."""
        if self.original_exec_id is None:
            return None
        return self._client.executions.get(self.original_exec_id)

    @property
    def root_exec_id(self) -> str | None:
        """Return the verified stored replay root ID, or None for unknown ancestry."""
        from kitaru.replay import (
            EXPERIMENT_ID_METADATA_KEY,
            EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY,
            EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY,
        )

        experiment_id = self.metadata.get(EXPERIMENT_ID_METADATA_KEY)
        parent_id = self.metadata.get(EXPERIMENT_PARENT_EXECUTION_ID_METADATA_KEY)
        root_id = self.metadata.get(EXPERIMENT_ROOT_EXECUTION_ID_METADATA_KEY)
        if (
            not isinstance(experiment_id, str)
            or not experiment_id.strip()
            or not isinstance(parent_id, str)
            or parent_id != self.original_exec_id
            or not isinstance(root_id, str)
            or not root_id.strip()
        ):
            return None
        return root_id

    @property
    def root(self) -> Execution | None:
        """Return the verified stored root execution, or None when unknown."""
        root_id = self.root_exec_id
        if root_id is None:
            return None
        return self._client.executions.get(root_id)

    def refresh(self) -> Execution:
        """Fetch the latest execution state."""
        return self._client.executions.get(self.exec_id)

    def retry(self) -> Execution:
        """Retry this failed execution as a same-execution recovery."""
        return self._client.executions.retry(self.exec_id)

    def resume(self) -> Execution:
        """Resume this paused execution after wait input is resolved."""
        return self._client.executions.resume(self.exec_id)

    def cancel(self) -> Execution:
        """Cancel this execution."""
        return self._client.executions.cancel(self.exec_id)

    def replay(
        self,
        *,
        at: str,
        flow_overrides: Mapping[str, Any] | None = None,
        checkpoint_overrides: Mapping[str, Any] | None = None,
        invocation_overrides: Mapping[str, Any] | None = None,
        skip: Sequence[str] | None = None,
        tag: str | None = None,
        wait: bool | None = None,
        on_error: Literal["collect", "fail"] | None = None,
    ) -> ReplaySubmission:
        """Replay this execution from a checkpoint cut point."""
        return self._client.executions.replay(
            self.exec_id,
            at=at,
            flow_overrides=flow_overrides,
            checkpoint_overrides=checkpoint_overrides,
            invocation_overrides=invocation_overrides,
            skip=skip,
            tag=tag,
            wait=wait,
            on_error=on_error,
        )

    def list_checkpoints(self) -> list[CheckpointCall]:
        """Return checkpoint calls for this execution."""
        return list(self.checkpoints)

    def list_artifacts(self) -> list[ArtifactRef]:
        """Return artifact refs for this execution."""
        return list(self.artifacts)


__all__ = [
    "ArtifactRef",
    "AuthAPIKey",
    "AuthAPIKeyWithValue",
    "AuthServiceAccount",
    "CheckpointAttempt",
    "CheckpointCall",
    "Deployment",
    "Execution",
    "ExecutionEvent",
    "ExecutionStatistics",
    "ExecutionStatisticsDimension",
    "ExecutionStatisticsGroup",
    "ExecutionStatisticsGrouping",
    "ExecutionStatisticsMetric",
    "ExecutionStatisticsMetricAggregation",
    "ExecutionStatisticsMetricSource",
    "ExecutionStatisticsTimeGranularity",
    "ExecutionStatus",
    "FailureInfo",
    "LogEntry",
    "PendingWait",
]
