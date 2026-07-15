"""Internal helpers that map ZenML responses into client-facing models."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import ValidationError
from zenml.models import PipelineRunResponse, StepRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru._checkpoint_metadata import (
    ADAPTER_CHECKPOINT_KIND_KEY,
    ADAPTER_KEY,
    CHECKPOINT_ORIGIN_ADAPTER,
    CHECKPOINT_ORIGIN_KEY,
    CHECKPOINT_ORIGIN_USER,
    REPLAY_INPUT_SLOTS_KEY,
    REPLAY_OUTPUT_SLOTS_KEY,
    CheckpointOrigin,
    checkpoint_metadata_from_step,
    checkpoint_metadata_slots,
    checkpoint_metadata_str,
)
from kitaru._checkpoint_steps import visible_checkpoint_step_for_lineage
from kitaru._client._models import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
    FailureInfo,
    PendingWait,
)
from kitaru._run_identity import extract_run_project_identity
from kitaru._source_aliases import (
    CHECKPOINT_SOURCE_ALIAS_PREFIX as _CHECKPOINT_SOURCE_ALIAS_PREFIX,
)
from kitaru._source_aliases import (
    PIPELINE_SOURCE_ALIAS_PREFIX as _PIPELINE_SOURCE_ALIAS_PREFIX,
)
from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru._source_aliases import normalize_flow_name as _normalize_flow_name
from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    FrozenExecutionSpec,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    classify_failure_origin,
    traceback_exception_type,
    traceback_last_line,
)
from kitaru.replay import (
    parse_replay_skipped_steps_metadata,
    replay_step_invocation_id,
)

if TYPE_CHECKING:
    from kitaru.client import KitaruClient

_WAIT_CONDITION_STATUS_PENDING = "pending"

_RAW_STATUSES_BY_PUBLIC_STATUS: dict[ExecutionStatus, tuple[str, ...]] = {
    ExecutionStatus.RUNNING: (
        "cancelling",
        "initializing",
        "provisioning",
        "queued",
        "running",
        "retrying",
        "resuming",
        "stopping",
    ),
    ExecutionStatus.WAITING: ("paused",),
    ExecutionStatus.COMPLETED: (
        "completed",
        "cached",
        "skipped",
        "replay_reused",
        "reused",
        "reused_not_incurred",
    ),
    ExecutionStatus.FAILED: ("failed", "retried"),
    ExecutionStatus.CANCELLED: ("cancelled", "stopped"),
}
_RAW_STATUS_TO_PUBLIC_STATUS: dict[str, ExecutionStatus] = {
    raw_status: public_status
    for public_status, raw_statuses in _RAW_STATUSES_BY_PUBLIC_STATUS.items()
    for raw_status in raw_statuses
}


def _backend_filter_value(values: Sequence[str]) -> str:
    """Format values for ZenML backend string-filter fields."""
    if len(values) == 1:
        return values[0]
    return f"oneof:{json.dumps(list(values), separators=(',', ':'))}"


def _status_filter_value(public_status: ExecutionStatus | None) -> str | None:
    """Map a public status filter to the backend string-filter syntax."""
    if public_status is None:
        return None
    return _backend_filter_value(_RAW_STATUSES_BY_PUBLIC_STATUS[public_status])


def _to_plain_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    """Convert metadata mappings to plain dictionaries."""
    return {str(key): value for key, value in values.items()}


def _to_public_status(status: Any) -> ExecutionStatus:
    """Map ZenML execution states to Kitaru public states."""
    status_value = str(getattr(status, "value", status))

    try:
        return _RAW_STATUS_TO_PUBLIC_STATUS[status_value]
    except KeyError as exc:
        message = (
            f"Unsupported execution status mapping: {status!r} "
            f"(value={status_value!r})."
        )
        raise KitaruRuntimeError(message) from exc


def _coerce_status_filter(
    status: ExecutionStatus | str | None,
) -> ExecutionStatus | None:
    """Normalize status filter input."""
    if status is None:
        return None
    if isinstance(status, ExecutionStatus):
        return status

    normalized = status.strip().lower()
    try:
        return ExecutionStatus(normalized)
    except ValueError as exc:
        expected = ", ".join(item.value for item in ExecutionStatus)
        raise KitaruUsageError(
            f"Unsupported status filter {status!r}. Expected one of: {expected}."
        ) from exc


def _parse_frozen_execution_spec(raw_value: Any) -> FrozenExecutionSpec | None:
    """Parse frozen execution metadata when available."""
    if raw_value is None:
        return None
    if not isinstance(raw_value, Mapping):
        return None

    normalized_raw_value = dict(raw_value)
    for field_name in ("resolved_execution", "flow_defaults"):
        field_value = normalized_raw_value.get(field_name)
        if not isinstance(field_value, Mapping):
            continue
        if "stack" in field_value or "runner" not in field_value:
            continue
        normalized_raw_value[field_name] = {
            **field_value,
            "stack": field_value["runner"],
        }

    try:
        return FrozenExecutionSpec.model_validate(normalized_raw_value)
    except ValidationError:
        return None


def _map_failure_info(
    *,
    status_reason: str | None,
    exception_info: Any,
    default_origin: FailureOrigin,
    fallback_message: str | None = None,
) -> FailureInfo | None:
    """Build structured failure info from status + exception payloads."""
    traceback_text: str | None = None
    if exception_info is not None:
        traceback_text = getattr(exception_info, "traceback", None)

    traceback_tail = traceback_last_line(traceback_text)
    message = status_reason or traceback_tail or fallback_message
    if message is None:
        return None

    origin = classify_failure_origin(
        status_reason=status_reason,
        traceback=traceback_text,
        default=default_origin,
    )
    return FailureInfo(
        message=message,
        exception_type=traceback_exception_type(traceback_text),
        traceback=traceback_text,
        origin=origin,
    )


def _checkpoint_lineage_key(step: StepRunResponse) -> str:
    """Return the run-local invocation name used for retry grouping."""
    return step.name


def _list_checkpoint_attempts_for_run_with_zenml_client(
    *,
    run: PipelineRunResponse,
    zenml_client: Any,
    project: Any | None = None,
) -> dict[str, list[StepRunResponse]]:
    """Fetch all step attempts for one execution with a ZenML client."""
    grouped_attempts: defaultdict[str, list[StepRunResponse]] = defaultdict(list)
    page = 1
    page_size = 200

    try:
        while True:
            step_page = zenml_client.list_run_steps(
                sort_by="asc:created",
                page=page,
                size=page_size,
                pipeline_run_id=run.id,
                project=project,
                exclude_retried=False,
                hydrate=True,
            )
            step_items = list(step_page.items)
            if not step_items:
                break

            for step in step_items:
                grouped_attempts[_checkpoint_lineage_key(step)].append(step)

            if len(step_items) < page_size:
                break
            page += 1
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to fetch checkpoint attempts for execution {run.id}: {exc}"
        ) from exc

    for attempts in grouped_attempts.values():
        attempts.sort(key=lambda step: step.version)
    return dict(grouped_attempts)


def _list_checkpoint_attempts_for_run(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> dict[str, list[StepRunResponse]]:
    """Fetch all step attempts for one execution, including retried runs."""
    return _list_checkpoint_attempts_for_run_with_zenml_client(
        run=run,
        zenml_client=client._client(),
        project=client._project,
    )


def _map_checkpoint_attempt(
    step: StepRunResponse,
    *,
    replay_skipped_steps: set[str] | None = None,
) -> CheckpointAttempt:
    """Map one step run model into a checkpoint-attempt entry."""
    public_status = _to_public_status(step.status)
    checkpoint_name = _normalize_checkpoint_name(step.name)

    failure = _map_failure_info(
        status_reason=None,
        exception_info=getattr(step, "exception_info", None),
        default_origin=FailureOrigin.USER_CODE,
        fallback_message=(
            f"Checkpoint '{checkpoint_name}' failed."
            if public_status == ExecutionStatus.FAILED
            else None
        ),
    )

    return CheckpointAttempt(
        attempt_id=str(step.id),
        status=public_status,
        started_at=step.start_time,
        ended_at=step.end_time,
        metadata=_to_plain_dict(step.run_metadata),
        failure=failure,
        _raw_status=str(getattr(step.status, "value", step.status)).strip().lower(),
        _replay_reused=(
            replay_skipped_steps is not None
            and replay_step_invocation_id(step) in replay_skipped_steps
        ),
    )


def _map_artifact_ref(
    *,
    artifact: ArtifactVersionResponse,
    client: KitaruClient,
    producing_call: str | None,
    name: str | None = None,
    direction: Literal["input", "output"] = "output",
    input_type: str | None = None,
    fallback_kind: str | None = None,
) -> ArtifactRef:
    """Map a ZenML artifact response into a Kitaru artifact reference."""
    metadata = _to_plain_dict(artifact.run_metadata)
    raw_kind = metadata.get("kitaru_artifact_type")
    kind = raw_kind if isinstance(raw_kind, str) else fallback_kind
    artifact_name = name or artifact.name

    return ArtifactRef(
        artifact_id=str(artifact.id),
        name=artifact_name,
        kind=kind,
        save_type=artifact.save_type.value,
        producing_call=producing_call,
        metadata=metadata,
        _client=client,
        direction=direction,
        input_type=input_type,
    )


def _checkpoint_origin(metadata: Mapping[str, Any]) -> CheckpointOrigin:
    origin = checkpoint_metadata_str(metadata, CHECKPOINT_ORIGIN_KEY)
    if origin == CHECKPOINT_ORIGIN_ADAPTER:
        return CHECKPOINT_ORIGIN_ADAPTER
    return CHECKPOINT_ORIGIN_USER


def _adapter_structural_input_kind(
    *,
    checkpoint_type: str | None,
    input_name: str,
) -> str | None:
    """Return public kind for adapter structural inputs, if exposable."""
    if checkpoint_type == "llm_call" and input_name == "input":
        return "prompt"
    if checkpoint_type == "tool_call" and input_name == "tool_args":
        return "input"
    return None


def _map_checkpoint_call(
    *,
    step: StepRunResponse,
    client: KitaruClient,
    attempts_by_lineage: Mapping[str, list[StepRunResponse]],
    replay_skipped_steps: set[str] | None = None,
    visible_call_id_by_attempt_id: Mapping[str, str] | None = None,
) -> CheckpointCall:
    """Map a ZenML step run into a Kitaru checkpoint call."""
    producing_call = _normalize_checkpoint_name(step.name)
    checkpoint_type = step.type.value if step.type else None
    metadata = _to_plain_dict(step.run_metadata)
    contract_metadata = checkpoint_metadata_from_step(step)
    seen_artifact_ids: set[str] = set()
    artifacts: list[ArtifactRef] = []

    for input_name, input_artifacts in step.inputs.items():
        input_kind = _adapter_structural_input_kind(
            checkpoint_type=checkpoint_type,
            input_name=input_name,
        )
        if input_kind is None:
            continue
        for artifact in input_artifacts:
            artifact_id = str(artifact.id)
            if artifact_id in seen_artifact_ids:
                continue
            seen_artifact_ids.add(artifact_id)
            input_type = getattr(getattr(artifact, "input_type", None), "value", None)
            artifacts.append(
                _map_artifact_ref(
                    artifact=artifact,
                    client=client,
                    producing_call=None,
                    name=input_name,
                    direction="input",
                    input_type=input_type,
                    fallback_kind=input_kind,
                )
            )

    for output_name, output_artifacts in step.outputs.items():
        for artifact in output_artifacts:
            artifact_id = str(artifact.id)
            if artifact_id in seen_artifact_ids:
                continue
            seen_artifact_ids.add(artifact_id)
            output_kind = None
            if output_name == "output":
                if checkpoint_type == "llm_call":
                    output_kind = "response"
                elif checkpoint_type == "tool_call":
                    output_kind = "output"
            artifacts.append(
                _map_artifact_ref(
                    artifact=artifact,
                    client=client,
                    producing_call=producing_call,
                    fallback_kind=output_kind,
                )
            )

    lineage_key = _checkpoint_lineage_key(step)
    attempt_steps = attempts_by_lineage.get(lineage_key, [step])
    attempts = [
        _map_checkpoint_attempt(
            attempt,
            replay_skipped_steps=replay_skipped_steps,
        )
        for attempt in attempt_steps
    ]

    failure = None
    if attempts:
        failure = attempts[-1].failure

    original_call_id: str | None = None
    if step.original_step_run_id is not None:
        original_call_id = str(step.original_step_run_id)

    parent_call_ids = [str(parent_id) for parent_id in step.parent_step_ids]
    if visible_call_id_by_attempt_id is not None:
        parent_call_ids = [
            visible_call_id_by_attempt_id.get(parent_id, parent_id)
            for parent_id in parent_call_ids
        ]

    return CheckpointCall(
        call_id=str(step.id),
        name=producing_call,
        status=_to_public_status(step.status),
        started_at=step.start_time,
        ended_at=step.end_time,
        metadata=metadata,
        original_call_id=original_call_id,
        parent_call_ids=parent_call_ids,
        failure=failure,
        attempts=attempts,
        artifacts=artifacts,
        checkpoint_type=checkpoint_type,
        checkpoint_origin=_checkpoint_origin(contract_metadata),
        adapter=checkpoint_metadata_str(contract_metadata, ADAPTER_KEY),
        adapter_checkpoint_kind=checkpoint_metadata_str(
            contract_metadata, ADAPTER_CHECKPOINT_KIND_KEY
        ),
        replay_input_slots=checkpoint_metadata_slots(
            contract_metadata, REPLAY_INPUT_SLOTS_KEY
        ),
        replay_output_slots=checkpoint_metadata_slots(
            contract_metadata, REPLAY_OUTPUT_SLOTS_KEY
        ),
    )


def _map_pending_wait(wait_condition: Any) -> PendingWait:
    """Map a wait condition response to the public pending wait model."""
    data_schema = wait_condition.data_schema
    schema: dict[str, Any] | None = None
    if data_schema is not None:
        schema = dict(data_schema)

    return PendingWait(
        wait_id=str(wait_condition.id),
        name=wait_condition.name,
        question=wait_condition.question,
        schema=schema,
        metadata=dict(wait_condition.run_metadata),
        entered_waiting_at=getattr(wait_condition, "created", None),
    )


def _get_active_wait_condition(run: PipelineRunResponse) -> Any | None:
    """Return the active wait condition attached to run resources, if any."""
    resources = cast(Any, run.get_resources())
    return getattr(resources, "active_wait_condition", None)


def _list_run_wait_conditions(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    status: str | None = None,
) -> list[Any]:
    """Return wait-condition models for an execution."""
    try:
        wait_conditions_page = cast(Any, client._client()).list_run_wait_conditions(
            pipeline_run=run.id,
            project=client._project,
            status=status,
            hydrate=True,
            sort_by="asc:created",
            size=200,
        )
    except AttributeError:
        return []
    except Exception as exc:
        operation = (
            "pending waits" if status == _WAIT_CONDITION_STATUS_PENDING else "waits"
        )
        raise KitaruBackendError(
            f"Failed to list {operation} for execution {run.id}: {exc}"
        ) from exc

    return list(wait_conditions_page.items)


def _list_pending_wait_conditions(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> list[Any]:
    """Return pending wait-condition models for an execution."""
    pending_conditions: list[Any] = []

    active_wait = _get_active_wait_condition(run)
    if active_wait is not None:
        pending_conditions.append(active_wait)

    listed_pending = _list_run_wait_conditions(
        run=run,
        client=client,
        status=_WAIT_CONDITION_STATUS_PENDING,
    )

    existing_ids = {str(condition.id) for condition in pending_conditions}
    for condition in listed_pending:
        condition_id = str(condition.id)
        if condition_id in existing_ids:
            continue
        pending_conditions.append(condition)
        existing_ids.add(condition_id)

    return pending_conditions


def _first_pending_wait(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> PendingWait | None:
    """Resolve the first pending wait condition for a run."""
    active_wait = _get_active_wait_condition(run)
    if active_wait is not None:
        return _map_pending_wait(active_wait)

    try:
        pending_conditions = _list_pending_wait_conditions(run=run, client=client)
    except KitaruBackendError:
        return None

    if not pending_conditions:
        return None
    return _map_pending_wait(pending_conditions[0])


def _select_pending_wait_condition(
    *,
    run: PipelineRunResponse,
    wait: str,
    pending_conditions: list[Any],
) -> Any:
    """Resolve a wait selector to exactly one pending wait condition."""
    wait_selector = wait.strip()
    if not wait_selector:
        raise KitaruUsageError("`wait` must be a non-empty string.")

    key_matches = [
        condition for condition in pending_conditions if condition.name == wait_selector
    ]
    if len(key_matches) == 1:
        return key_matches[0]
    if len(key_matches) > 1:
        raise KitaruStateError(
            f"Multiple pending waits match '{wait_selector}' for execution '{run.id}'."
        )

    id_matches = [
        condition
        for condition in pending_conditions
        if str(condition.id) == wait_selector
    ]
    if len(id_matches) == 1:
        return id_matches[0]

    available_waits = ", ".join(
        sorted({condition.name for condition in pending_conditions})
    )
    raise KitaruStateError(
        f"Execution '{run.id}' has no pending wait '{wait_selector}'. "
        f"Available waits: {available_waits}."
    )


def _map_execution(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    include_details: bool,
    resolve_wait_status: bool = False,
) -> Execution:
    """Map a ZenML pipeline run into a Kitaru execution model."""
    status = _to_public_status(run.status)

    pending_wait: PendingWait | None = None
    if status == ExecutionStatus.WAITING:
        pending_wait = _first_pending_wait(run=run, client=client)
    elif status == ExecutionStatus.RUNNING:
        active_wait = _get_active_wait_condition(run)
        if active_wait is not None:
            pending_wait = _map_pending_wait(active_wait)
        elif include_details or resolve_wait_status:
            # Some ZenML responses only expose pending waits through
            # list_run_wait_conditions(...), not through active_wait_condition.
            pending_wait = _first_pending_wait(run=run, client=client)

    if pending_wait is not None:
        status = ExecutionStatus.WAITING

    status_reason = getattr(run, "status_reason", None)
    run_exception_info = getattr(run, "exception_info", None)

    failure: FailureInfo | None = None
    if status == ExecutionStatus.FAILED:
        failure = _map_failure_info(
            status_reason=status_reason,
            exception_info=run_exception_info,
            default_origin=(
                FailureOrigin.USER_CODE
                if run_exception_info is not None
                else FailureOrigin.UNKNOWN
            ),
            fallback_message=f"Execution {run.id} failed.",
        )

    metadata = _to_plain_dict(run.run_metadata)
    replay_skipped_steps = parse_replay_skipped_steps_metadata(metadata)

    checkpoints: list[CheckpointCall] = []
    artifacts: list[ArtifactRef] = []
    if include_details:
        attempts_by_lineage: dict[str, list[StepRunResponse]] = {}
        try:
            attempts_by_lineage = _list_checkpoint_attempts_for_run(
                run=run,
                client=client,
            )
        except KitaruBackendError:
            attempts_by_lineage = {}

        attempt_ids_by_lineage = {
            lineage_key: {str(attempt.id) for attempt in attempts}
            for lineage_key, attempts in attempts_by_lineage.items()
        }
        for step in run.steps.values():
            lineage_key = _checkpoint_lineage_key(step)
            attempts = attempts_by_lineage.setdefault(lineage_key, [])
            attempt_ids = attempt_ids_by_lineage.setdefault(lineage_key, set())
            step_id = str(step.id)
            if step_id not in attempt_ids:
                attempts.append(step)
                attempt_ids.add(step_id)

        latest_steps_by_lineage: dict[str, StepRunResponse] = {}
        for lineage_key, attempts in attempts_by_lineage.items():
            attempts.sort(key=lambda step: step.version)
            visible_step = visible_checkpoint_step_for_lineage(attempts)
            if visible_step is not None:
                latest_steps_by_lineage[lineage_key] = visible_step

        visible_call_id_by_attempt_id = {
            str(attempt.id): str(visible_step.id)
            for lineage_key, visible_step in latest_steps_by_lineage.items()
            for attempt in attempts_by_lineage[lineage_key]
        }

        for step in latest_steps_by_lineage.values():
            checkpoints.append(
                _map_checkpoint_call(
                    step=step,
                    client=client,
                    attempts_by_lineage=attempts_by_lineage,
                    replay_skipped_steps=replay_skipped_steps,
                    visible_call_id_by_attempt_id=visible_call_id_by_attempt_id,
                )
            )

        artifact_indexes_by_id: dict[str, int] = {}
        for checkpoint in checkpoints:
            for artifact in checkpoint.artifacts:
                existing_index = artifact_indexes_by_id.get(artifact.artifact_id)
                if existing_index is None:
                    artifact_indexes_by_id[artifact.artifact_id] = len(artifacts)
                    artifacts.append(artifact)
                    continue

                existing = artifacts[existing_index]
                if existing.direction == "input" and artifact.direction == "output":
                    artifacts[existing_index] = artifact

    flow_id: str | None = None
    flow_name: str | None = None
    if run.pipeline is not None:
        raw_id = getattr(run.pipeline, "id", None)
        flow_id = str(raw_id) if raw_id is not None else None
        flow_name = _normalize_flow_name(run.pipeline.name)

    original_exec_id: str | None = None
    if run.original_run is not None:
        original_exec_id = str(run.original_run.id)

    stack_name: str | None = None
    if run.stack is not None:
        stack_name = run.stack.name

    project_identity = extract_run_project_identity(run)

    return Execution(
        exec_id=str(run.id),
        flow_id=flow_id,
        flow_name=flow_name,
        status=status,
        started_at=run.start_time,
        ended_at=run.end_time,
        stack_name=stack_name,
        metadata=metadata,
        status_reason=status_reason,
        failure=failure,
        pending_wait=pending_wait,
        frozen_execution_spec=_parse_frozen_execution_spec(
            metadata.get(FROZEN_EXECUTION_SPEC_METADATA_KEY)
        ),
        original_exec_id=original_exec_id,
        checkpoints=checkpoints,
        artifacts=artifacts,
        _client=client,
        project_id=project_identity.project_id,
        project_name=project_identity.project_name,
    )


__all__ = [
    "_CHECKPOINT_SOURCE_ALIAS_PREFIX",
    "_PIPELINE_SOURCE_ALIAS_PREFIX",
    "_RAW_STATUSES_BY_PUBLIC_STATUS",
    "_RAW_STATUS_TO_PUBLIC_STATUS",
    "_WAIT_CONDITION_STATUS_PENDING",
    "_adapter_structural_input_kind",
    "_checkpoint_lineage_key",
    "_coerce_status_filter",
    "_first_pending_wait",
    "_get_active_wait_condition",
    "_list_checkpoint_attempts_for_run",
    "_list_pending_wait_conditions",
    "_list_run_wait_conditions",
    "_map_artifact_ref",
    "_map_checkpoint_attempt",
    "_map_checkpoint_call",
    "_map_execution",
    "_map_failure_info",
    "_map_pending_wait",
    "_normalize_checkpoint_name",
    "_normalize_flow_name",
    "_parse_frozen_execution_spec",
    "_select_pending_wait_condition",
    "_to_plain_dict",
    "_to_public_status",
]
