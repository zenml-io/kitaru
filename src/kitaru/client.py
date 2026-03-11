"""Kitaru client for execution and artifact management.

`KitaruClient` provides a programmatic API for inspecting and managing
executions outside flow bodies.

Example::

    from kitaru import KitaruClient

    client = KitaruClient()
    execution = client.executions.get("exec-123")
    print(execution.status)
"""

from __future__ import annotations

import builtins
import importlib
import sys
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, cast, runtime_checkable

from pydantic import ValidationError
from zenml.client import Client
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.models import PipelineRunResponse, StepRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse
from zenml.utils.run_utils import stop_run
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru.config import (
    FROZEN_EXECUTION_SPEC_METADATA_KEY,
    FrozenExecutionSpec,
    active_stack_log_store,
    resolve_connection_config,
    resolve_log_store,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruLogRetrievalError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    KitaruWaitValidationError,
    classify_failure_origin,
    execution_error_from_failure,
    traceback_exception_type,
    traceback_last_line,
)
from kitaru.replay import build_replay_plan

_CHECKPOINT_SOURCE_ALIAS_PREFIX = "__kitaru_checkpoint_source_"
_PIPELINE_SOURCE_ALIAS_PREFIX = "__kitaru_pipeline_source_"
_WAIT_CONDITION_STATUS_PENDING = "pending"
_WAIT_CONDITION_RESOLUTION_CONTINUE = "continue"


class ExecutionStatus(StrEnum):
    """Simplified public execution status taxonomy."""

    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@runtime_checkable
class _ReplayFlowLike(Protocol):
    """Flow wrapper protocol used by client-side replay resolution."""

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Any: ...


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
class CheckpointAttempt:
    """One checkpoint attempt in retry/failure journaling history."""

    attempt_id: str
    status: ExecutionStatus
    started_at: datetime | None
    ended_at: datetime | None
    metadata: dict[str, Any]
    failure: FailureInfo | None


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


@dataclass(frozen=True)
class Execution:
    """Public view of a Kitaru execution."""

    exec_id: str
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
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay this execution from a prior checkpoint boundary."""
        return self._client.executions.replay(
            self.exec_id,
            from_=from_,
            overrides=overrides,
            **flow_inputs,
        )

    def list_checkpoints(self) -> list[CheckpointCall]:
        """Return checkpoint calls for this execution."""
        return list(self.checkpoints)

    def list_artifacts(self) -> list[ArtifactRef]:
        """Return artifact refs for this execution."""
        return list(self.artifacts)


@contextmanager
def _temporary_active_stack(stack_name_or_id: str | None) -> Iterator[None]:
    """Temporarily activate a stack while running an operation."""
    if not stack_name_or_id:
        yield
        return

    client = Client()
    old_stack_id = client.active_stack_model.id
    client.activate_stack(stack_name_or_id)
    try:
        yield
    finally:
        client.activate_stack(old_stack_id)


def _normalize_checkpoint_name(step_name: str) -> str:
    """Normalize internal checkpoint source alias names."""
    if step_name.startswith(_CHECKPOINT_SOURCE_ALIAS_PREFIX):
        return step_name.removeprefix(_CHECKPOINT_SOURCE_ALIAS_PREFIX)
    return step_name


def _normalize_flow_name(flow_name: str | None) -> str | None:
    """Normalize internal flow source alias names."""
    if flow_name is None:
        return None
    if flow_name.startswith(_PIPELINE_SOURCE_ALIAS_PREFIX):
        return flow_name.removeprefix(_PIPELINE_SOURCE_ALIAS_PREFIX)
    return flow_name


def _to_plain_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    """Convert metadata mappings to plain dictionaries."""
    return {str(key): value for key, value in values.items()}


def _to_public_status(status: Any) -> ExecutionStatus:
    """Map ZenML execution states to Kitaru public states."""
    status_value = str(getattr(status, "value", status))

    if status_value in {
        "initializing",
        "provisioning",
        "running",
        "retrying",
    }:
        return ExecutionStatus.RUNNING
    if status_value == "paused":
        return ExecutionStatus.WAITING
    if status_value in {
        "completed",
        "cached",
        "skipped",
    }:
        return ExecutionStatus.COMPLETED
    if status_value in {
        "failed",
        "retried",
    }:
        return ExecutionStatus.FAILED
    if status_value in {
        "stopped",
        "stopping",
    }:
        return ExecutionStatus.CANCELLED

    raise KitaruRuntimeError(
        f"Unsupported execution status mapping: {status!r} (value={status_value!r})."
    )


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


def _normalize_log_source(source: str) -> str:
    """Normalize a runtime log source selector."""
    normalized = source.strip().lower()
    if not normalized:
        raise KitaruUsageError("`source` must be a non-empty string.")
    return normalized


def _parse_log_timestamp(value: str | None) -> datetime | None:
    """Parse an optional log timestamp for sorting."""
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _log_sort_key(entry: LogEntry, fallback_index: int) -> tuple[int, float, int]:
    """Build a stable sort key for runtime log entries."""
    parsed_timestamp = _parse_log_timestamp(entry.timestamp)
    if parsed_timestamp is None:
        return (1, float("inf"), fallback_index)
    return (0, parsed_timestamp.timestamp(), fallback_index)


def _sort_log_entries(entries: list[LogEntry]) -> list[LogEntry]:
    """Sort runtime log entries chronologically with stable fallback order."""
    indexed = list(enumerate(entries))
    indexed.sort(key=lambda item: _log_sort_key(item[1], item[0]))
    return [entry for _, entry in indexed]


def _step_log_fetch_order_key(step: StepRunResponse) -> tuple[float, str, str]:
    """Order step runs deterministically for sequential log retrieval."""
    start_time = step.start_time
    if start_time is None:
        start_key = float("inf")
    else:
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=UTC)
        start_key = start_time.timestamp()

    return (start_key, _normalize_checkpoint_name(step.name), str(step.id))


def _coerce_log_level(value: Any) -> str | None:
    """Coerce a log level value from API payloads to a string."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, Mapping):
        nested = value.get("value")
        if isinstance(nested, str):
            normalized_nested = nested.strip()
            return normalized_nested or None
    return str(value)


def _coerce_log_text(value: Any) -> str | None:
    """Coerce optional log text fields to stripped strings."""
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return str(value)


def _coerce_log_lineno(value: Any) -> int | None:
    """Coerce an optional log line number value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _map_runtime_log_entry(
    raw_entry: Mapping[str, Any],
    *,
    source: str,
    checkpoint_name: str | None,
) -> LogEntry:
    """Map one raw REST log payload entry into a public `LogEntry`."""
    message_value = raw_entry.get("message")
    if isinstance(message_value, str):
        message = message_value
    elif message_value is None:
        message = ""
    else:
        message = str(message_value)

    timestamp_value = raw_entry.get("timestamp")
    timestamp: str | None
    if isinstance(timestamp_value, datetime):
        timestamp = timestamp_value.isoformat()
    elif isinstance(timestamp_value, str):
        stripped_timestamp = timestamp_value.strip()
        timestamp = stripped_timestamp or None
    else:
        timestamp = None

    return LogEntry(
        message=message,
        level=_coerce_log_level(raw_entry.get("level")),
        timestamp=timestamp,
        source=source,
        checkpoint_name=checkpoint_name,
        module=_coerce_log_text(raw_entry.get("module")),
        filename=_coerce_log_text(raw_entry.get("filename")),
        lineno=_coerce_log_lineno(raw_entry.get("lineno")),
    )


def _is_empty_log_result_error(message: str) -> bool:
    """Return whether an error message indicates an empty log collection."""
    lowered = message.lower()
    return "no logs found" in lowered


def _is_otel_log_retrieval_error(message: str) -> bool:
    """Return whether an error message points to OTEL export-only retrieval."""
    lowered = message.lower()
    if "notimplementederror" in lowered:
        return True
    return "otel" in lowered and "not implemented" in lowered


def _parse_frozen_execution_spec(raw_value: Any) -> FrozenExecutionSpec | None:
    """Parse frozen execution metadata when available."""
    if raw_value is None:
        return None
    if not isinstance(raw_value, Mapping):
        return None

    try:
        return FrozenExecutionSpec.model_validate(raw_value)
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
    """Return the stable lineage key for checkpoint retry grouping."""
    if step.original_step_run_id is not None:
        return str(step.original_step_run_id)
    return str(step.id)


def _list_checkpoint_attempts_for_run(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
) -> dict[str, list[StepRunResponse]]:
    """Fetch all step attempts for one execution, including retried runs."""
    grouped_attempts: defaultdict[str, list[StepRunResponse]] = defaultdict(list)
    page = 1
    page_size = 200

    try:
        while True:
            step_page = client._client().list_run_steps(
                sort_by="asc:created",
                page=page,
                size=page_size,
                pipeline_run_id=run.id,
                project=client._project,
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

    return dict(grouped_attempts)


def _map_checkpoint_attempt(step: StepRunResponse) -> CheckpointAttempt:
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
    )


def _map_artifact_ref(
    *,
    artifact: ArtifactVersionResponse,
    client: KitaruClient,
    producing_call: str | None,
) -> ArtifactRef:
    """Map a ZenML artifact response into a Kitaru artifact reference."""
    metadata = _to_plain_dict(artifact.run_metadata)
    raw_kind = metadata.get("kitaru_artifact_type")
    kind = raw_kind if isinstance(raw_kind, str) else None

    return ArtifactRef(
        artifact_id=str(artifact.id),
        name=artifact.name,
        kind=kind,
        save_type=artifact.save_type.value,
        producing_call=producing_call,
        metadata=metadata,
        _client=client,
    )


def _map_checkpoint_call(
    *,
    step: StepRunResponse,
    client: KitaruClient,
    attempts_by_lineage: Mapping[str, list[StepRunResponse]],
) -> CheckpointCall:
    """Map a ZenML step run into a Kitaru checkpoint call."""
    producing_call = _normalize_checkpoint_name(step.name)
    seen_artifact_ids: set[str] = set()
    artifacts: list[ArtifactRef] = []

    for output_artifacts in step.outputs.values():
        for artifact in output_artifacts:
            artifact_id = str(artifact.id)
            if artifact_id in seen_artifact_ids:
                continue
            seen_artifact_ids.add(artifact_id)
            artifacts.append(
                _map_artifact_ref(
                    artifact=artifact,
                    client=client,
                    producing_call=producing_call,
                )
            )

    lineage_key = _checkpoint_lineage_key(step)
    attempt_steps = attempts_by_lineage.get(lineage_key, [step])
    attempts = [_map_checkpoint_attempt(attempt) for attempt in attempt_steps]

    failure = None
    if attempts:
        failure = attempts[-1].failure

    original_call_id: str | None = None
    if step.original_step_run_id is not None:
        original_call_id = str(step.original_step_run_id)

    return CheckpointCall(
        call_id=str(step.id),
        name=producing_call,
        status=_to_public_status(step.status),
        started_at=step.start_time,
        ended_at=step.end_time,
        metadata=_to_plain_dict(step.run_metadata),
        original_call_id=original_call_id,
        parent_call_ids=[str(parent_id) for parent_id in step.parent_step_ids],
        failure=failure,
        attempts=attempts,
        artifacts=artifacts,
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
        metadata=dict(wait_condition.wait_metadata),
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


def _snapshot_source_parts(run: PipelineRunResponse) -> tuple[str, str | None]:
    """Return `(module, attribute)` from a run snapshot source."""
    snapshot = run.snapshot
    pipeline_spec = getattr(snapshot, "pipeline_spec", None)
    source = getattr(pipeline_spec, "source", None)
    if source is None:
        raise KitaruRuntimeError(
            "Replay requires pipeline source metadata on the source execution."
        )

    module = getattr(source, "module", None)
    attribute = getattr(source, "attribute", None)

    import_path = getattr(source, "import_path", None)
    if isinstance(import_path, str) and import_path:
        import_module, _, import_attribute = import_path.rpartition(".")
        if not module and import_module:
            module = import_module
        if attribute is None and import_attribute:
            attribute = import_attribute

    if not isinstance(module, str) or not module:
        raise KitaruRuntimeError(
            "Replay source metadata is missing a module import path."
        )

    if attribute is not None and not isinstance(attribute, str):
        attribute = None

    return module, attribute


def _import_module_for_replay(module_name: str, run_id: str | Any) -> Any:
    """Import a module by name, falling back to ``sys.modules`` search.

    ZenML records the pipeline source module relative to the archived source
    root (e.g. ``replay_with_overrides``), but in the running process the
    module may be loaded under a different path.  Three fallback strategies:

    1. Direct ``importlib.import_module`` (exact match).
    2. Search ``sys.modules`` for a suffix match (e.g. the module is loaded
       as ``examples.replay_with_overrides``).
    3. Return ``__main__`` — when invoked via ``python -m pkg.mod``, the
       module is loaded as ``__main__`` and won't appear under its dotted
       name in ``sys.modules``.
    """
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        pass

    # Search already-loaded modules for a suffix match.
    suffix = f".{module_name}"
    for loaded_name, loaded_module in sys.modules.items():
        if (
            loaded_name == module_name or loaded_name.endswith(suffix)
        ) and loaded_module is not None:
            return loaded_module

    # When run via `python -m`, the module is __main__.
    main_module = sys.modules.get("__main__")
    if main_module is not None:
        return main_module

    raise KitaruRuntimeError(
        f"Failed to import replay source module '{module_name}' for "
        f"execution '{run_id}': no module named '{module_name}' and no "
        "matching module found in sys.modules."
    )


def _resolve_flow_for_replay(run: PipelineRunResponse) -> _ReplayFlowLike:
    """Resolve the original flow wrapper object for a replay source run."""
    module_name, source_attribute = _snapshot_source_parts(run)
    module = _import_module_for_replay(module_name, run.id)

    selectors: list[str] = []
    if run.pipeline is not None:
        flow_name = _normalize_flow_name(run.pipeline.name)
        if flow_name:
            selectors.append(flow_name)

    if source_attribute and source_attribute.startswith(_PIPELINE_SOURCE_ALIAS_PREFIX):
        selectors.append(source_attribute.removeprefix(_PIPELINE_SOURCE_ALIAS_PREFIX))

    if source_attribute:
        selectors.append(source_attribute)

    deduped_selectors = list(
        dict.fromkeys(selector for selector in selectors if selector)
    )
    for selector in deduped_selectors:
        candidate = getattr(module, selector, None)
        if isinstance(candidate, _ReplayFlowLike):
            return candidate

    tried_selectors = ", ".join(deduped_selectors) or "none"
    raise KitaruRuntimeError(
        "Unable to resolve a replay-capable flow object from source module "
        f"'{module_name}' for execution '{run.id}'. "
        f"Tried: {tried_selectors}."
    )


def _resolve_pipeline_for_replay(run: PipelineRunResponse) -> Any:
    """Resolve the underlying pipeline object for replay fallback."""
    module_name, source_attribute = _snapshot_source_parts(run)
    if not source_attribute:
        raise KitaruRuntimeError(
            "Replay fallback could not determine pipeline source attribute for "
            f"execution '{run.id}'."
        )

    module = _import_module_for_replay(module_name, run.id)

    pipeline_obj = getattr(module, source_attribute, None)
    if pipeline_obj is None or not hasattr(pipeline_obj, "replay"):
        raise KitaruRuntimeError(
            "Replay fallback expected a pipeline object with `.replay(...)` at "
            f"'{module_name}.{source_attribute}'."
        )
    return pipeline_obj


def _restart_run_from_snapshot(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    operation_name: str,
) -> None:
    """Restart an execution from its stored snapshot metadata."""
    snapshot = run.snapshot
    if snapshot is None:
        raise KitaruRuntimeError(
            f"Unable to {operation_name} execution because snapshot metadata "
            "is missing."
        )
    if snapshot.stack is None:
        raise KitaruRuntimeError(
            f"Unable to {operation_name} execution because snapshot stack "
            "metadata is missing."
        )

    try:
        with _temporary_active_stack(str(snapshot.stack.id)):
            active_stack = client._client().active_stack
            orchestrator = cast(Any, active_stack.orchestrator)
            orchestrator.restart(
                snapshot=snapshot,
                run=run,
                stack=active_stack,
            )
    except Exception as exc:
        raise KitaruBackendError(
            f"Failed to {operation_name} execution '{run.id}': {exc}"
        ) from exc


def _map_execution(
    *,
    run: PipelineRunResponse,
    client: KitaruClient,
    include_details: bool,
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
        elif include_details:
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
            # Degrade gracefully: execution inspection should still work even
            # when deep attempt history cannot be fetched.
            attempts_by_lineage = {}

        latest_steps_by_lineage: dict[str, StepRunResponse] = {}
        for lineage_key, attempts in attempts_by_lineage.items():
            if attempts:
                latest_steps_by_lineage[lineage_key] = attempts[-1]

        for step in run.steps.values():
            lineage_key = _checkpoint_lineage_key(step)
            latest_steps_by_lineage.setdefault(lineage_key, step)
            attempts_by_lineage.setdefault(lineage_key, [step])

        for step in latest_steps_by_lineage.values():
            checkpoints.append(
                _map_checkpoint_call(
                    step=step,
                    client=client,
                    attempts_by_lineage=attempts_by_lineage,
                )
            )

        seen_artifact_ids: set[str] = set()
        for checkpoint in checkpoints:
            for artifact in checkpoint.artifacts:
                if artifact.artifact_id in seen_artifact_ids:
                    continue
                seen_artifact_ids.add(artifact.artifact_id)
                artifacts.append(artifact)

    metadata = _to_plain_dict(run.run_metadata)

    flow_name: str | None = None
    if run.pipeline is not None:
        flow_name = _normalize_flow_name(run.pipeline.name)

    original_exec_id: str | None = None
    if run.original_run is not None:
        original_exec_id = str(run.original_run.id)

    stack_name: str | None = None
    if run.stack is not None:
        stack_name = run.stack.name

    return Execution(
        exec_id=str(run.id),
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
    )


class _ExecutionsAPI:
    """Namespace for execution lifecycle and inspection operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def _rest_store(self) -> RestZenStore:
        """Return a REST-backed zen store required for runtime log retrieval."""
        zen_store = self._client_ref._client().zen_store
        if isinstance(zen_store, RestZenStore):
            return zen_store

        raise KitaruLogRetrievalError(
            "Runtime log retrieval requires a server-backed connection. "
            "Local database mode does not expose execution log endpoints."
        )

    def _resolve_log_endpoint_hint(self) -> str | None:
        """Resolve a best-effort endpoint hint for log-retrieval errors."""
        active_log_store = active_stack_log_store()
        if active_log_store is not None and active_log_store.endpoint:
            return active_log_store.endpoint

        try:
            preferred_log_store = resolve_log_store()
        except ValueError:
            return None

        return preferred_log_store.endpoint

    def _fetch_log_payload(
        self,
        *,
        path: str,
        source: str,
    ) -> builtins.list[Mapping[str, Any]]:
        """Call a log endpoint and normalize the response payload shape."""
        store = self._rest_store()

        try:
            payload = store.get(path, params={"source": source})
        except Exception as exc:
            error_message = str(exc)
            if _is_empty_log_result_error(error_message):
                return []

            if _is_otel_log_retrieval_error(error_message):
                endpoint_hint = self._resolve_log_endpoint_hint()
                message = (
                    "Logs for this execution are stored in an OTEL backend and "
                    "cannot be fetched via the Kitaru log retrieval API."
                )
                if endpoint_hint:
                    message += f" View them in your OTEL backend at: {endpoint_hint}."
                raise KitaruLogRetrievalError(message) from exc

            raise KitaruLogRetrievalError(
                f"Failed to retrieve runtime logs for source '{source}': {exc}"
            ) from exc

        if not isinstance(payload, list):
            raise KitaruLogRetrievalError(
                "Unexpected response while retrieving runtime logs: "
                "expected a list payload."
            )

        normalized_payload: builtins.list[Mapping[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, Mapping):
                raise KitaruLogRetrievalError(
                    "Unexpected log entry payload type returned by the server."
                )
            normalized_payload.append(entry)

        return normalized_payload

    def logs(
        self,
        exec_id: str,
        *,
        checkpoint: str | None = None,
        source: str = "step",
        limit: int | None = None,
    ) -> builtins.list[LogEntry]:
        """Fetch runtime log entries for an execution."""
        normalized_source = _normalize_log_source(source)
        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        normalized_checkpoint: str | None = None
        if checkpoint is not None:
            normalized_checkpoint = checkpoint.strip()
            if not normalized_checkpoint:
                raise KitaruUsageError("`checkpoint` must be non-empty when provided.")

        if normalized_source == "runner" and normalized_checkpoint is not None:
            raise KitaruUsageError(
                "`checkpoint` cannot be combined with `source='runner'`."
            )

        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)

        if normalized_source == "runner":
            run_payload = self._fetch_log_payload(
                path=f"/runs/{run.id}/logs",
                source=normalized_source,
            )
            run_entries = [
                _map_runtime_log_entry(
                    raw_entry,
                    source=normalized_source,
                    checkpoint_name=None,
                )
                for raw_entry in run_payload
            ]
            sorted_run_entries = _sort_log_entries(run_entries)
            if limit is not None:
                return sorted_run_entries[:limit]
            return sorted_run_entries

        step_runs = sorted(run.steps.values(), key=_step_log_fetch_order_key)
        if normalized_checkpoint is not None:
            step_runs = [
                step
                for step in step_runs
                if _normalize_checkpoint_name(step.name) == normalized_checkpoint
            ]

        if not step_runs:
            return []

        entries: list[LogEntry] = []
        for step in step_runs:
            checkpoint_name = _normalize_checkpoint_name(step.name)
            step_payload = self._fetch_log_payload(
                path=f"/steps/{step.id}/logs",
                source=normalized_source,
            )
            entries.extend(
                _map_runtime_log_entry(
                    raw_entry,
                    source=normalized_source,
                    checkpoint_name=checkpoint_name,
                )
                for raw_entry in step_payload
            )

            if limit is not None and len(entries) >= limit:
                break

        sorted_entries = _sort_log_entries(entries)
        if limit is not None:
            return sorted_entries[:limit]
        return sorted_entries

    def input(self, exec_id: str, *, wait: str, value: Any) -> Execution:
        """Provide input to a waiting execution."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        pending_conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        if not pending_conditions:
            raise KitaruStateError(
                f"Execution '{exec_id}' has no pending waits to resolve."
            )

        condition = _select_pending_wait_condition(
            run=run,
            wait=wait,
            pending_conditions=pending_conditions,
        )

        try:
            cast(Any, self._client_ref._client()).resolve_run_wait_condition(
                run_wait_condition_id=condition.id,
                resolution=cast(Any, _WAIT_CONDITION_RESOLUTION_CONTINUE),
                result=value,
            )
        except (ValidationError, TypeError, ValueError) as exc:
            raise KitaruWaitValidationError(
                "Wait input failed validation for "
                f"'{condition.name}' on execution '{exec_id}': {exc}"
            ) from exc
        except Exception as exc:
            raise KitaruBackendError(
                "Failed to resolve wait condition "
                f"'{condition.name}' for execution '{exec_id}': {exc}"
            ) from exc

        return self.get(exec_id)

    def retry(self, exec_id: str) -> Execution:
        """Retry a failed execution as same-execution recovery."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        run_status_value = str(getattr(run.status, "value", run.status))
        if run_status_value != ZenMLExecutionStatus.FAILED.value:
            raise KitaruStateError(
                "Only failed executions can be retried. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        _restart_run_from_snapshot(
            run=run,
            client=self._client_ref,
            operation_name="retry",
        )
        return self.get(exec_id)

    def resume(self, exec_id: str) -> Execution:
        """Resume a paused execution after all waits are resolved."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        pending_conditions = _list_pending_wait_conditions(
            run=run,
            client=self._client_ref,
        )
        if pending_conditions:
            raise KitaruStateError(
                f"Resolve pending wait input before resuming execution '{exec_id}'."
            )

        run_status_value = str(getattr(run.status, "value", run.status))
        if run_status_value != "paused":
            raise KitaruStateError(
                "Only paused executions can be resumed. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        _restart_run_from_snapshot(
            run=run,
            client=self._client_ref,
            operation_name="resume",
        )
        return self.get(exec_id)

    def replay(
        self,
        exec_id: str,
        *,
        from_: str,
        overrides: dict[str, Any] | None = None,
        **flow_inputs: Any,
    ) -> Execution:
        """Replay an execution from a checkpoint boundary."""
        source_run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)

        run_status_value = str(getattr(source_run.status, "value", source_run.status))
        if run_status_value in {
            "initializing",
            "provisioning",
            "running",
            "retrying",
            "stopping",
        }:
            raise KitaruStateError(
                "Replay requires a non-running source execution. "
                f"Execution '{exec_id}' is currently '{run_status_value}'."
            )

        replay_flow: _ReplayFlowLike | None = None
        try:
            replay_flow = _resolve_flow_for_replay(source_run)
        except KitaruRuntimeError:
            replay_flow = None

        if replay_flow is not None:
            handle = replay_flow.replay(
                exec_id,
                from_=from_,
                overrides=overrides,
                **flow_inputs,
            )
            replay_exec_id = getattr(handle, "exec_id", None)
            if not replay_exec_id:
                raise KitaruRuntimeError(
                    "Resolved flow replay call did not return a valid execution handle."
                )
            return self.get(str(replay_exec_id))

        replay_pipeline = _resolve_pipeline_for_replay(source_run)
        replay_plan = build_replay_plan(
            run=source_run,
            from_=from_,
            overrides=overrides,
            flow_inputs=flow_inputs,
        )

        try:
            replayed_run = replay_pipeline.replay(
                pipeline_run=source_run.id,
                skip=replay_plan.steps_to_skip,
                skip_successful_steps=False,
                input_overrides=replay_plan.input_overrides or None,
                step_input_overrides=replay_plan.step_input_overrides or None,
            )
        except Exception as exc:
            failure_origin = classify_failure_origin(
                status_reason=str(exc),
                traceback=None,
                default=FailureOrigin.BACKEND,
            )
            if failure_origin == FailureOrigin.DIVERGENCE:
                raise execution_error_from_failure(
                    f"Replay divergence detected for execution '{exec_id}': {exc}",
                    exec_id=str(source_run.id),
                    status="failed",
                    origin=failure_origin,
                ) from exc
            raise KitaruBackendError(
                f"Failed to replay execution '{exec_id}': {exc}"
            ) from exc

        replayed_exec_id = str(getattr(replayed_run, "id", ""))
        if not replayed_exec_id:
            raise KitaruRuntimeError("Replay did not produce a pipeline run ID.")

        return self.get(replayed_exec_id)

    def get(self, exec_id: str) -> Execution:
        """Get and map one execution by ID."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        return _map_execution(run=run, client=self._client_ref, include_details=True)

    def list(
        self,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
        limit: int | None = None,
    ) -> builtins.list[Execution]:
        """List executions with optional flow/status filters."""
        status_filter = _coerce_status_filter(status)

        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        results: list[Execution] = []
        page = 1
        page_size = 50 if limit is None else max(50, limit)

        while True:
            run_page = self._client_ref._client().list_pipeline_runs(
                sort_by="desc:created",
                page=page,
                size=page_size,
                project=self._client_ref._project,
                hydrate=True,
            )
            runs = list(run_page.items)
            if not runs:
                break

            for run in runs:
                execution = _map_execution(
                    run=run,
                    client=self._client_ref,
                    include_details=False,
                )

                if flow is not None and execution.flow_name != flow:
                    continue
                if status_filter is not None and execution.status != status_filter:
                    continue

                results.append(execution)
                if limit is not None and len(results) >= limit:
                    return results

            if len(runs) < page_size:
                break
            page += 1

        return results

    def latest(
        self,
        *,
        flow: str | None = None,
        status: ExecutionStatus | str | None = None,
    ) -> Execution:
        """Return the most recent execution for a filter set."""
        executions = self.list(flow=flow, status=status, limit=1)
        if not executions:
            filters: list[str] = []
            if flow is not None:
                filters.append(f"flow={flow!r}")
            if status is not None:
                filters.append(f"status={str(status)!r}")
            where = " and ".join(filters) if filters else "the current project"
            raise LookupError(f"No executions found for {where}.")
        return executions[0]

    def cancel(self, exec_id: str) -> Execution:
        """Cancel an execution if supported by the backend state."""
        run = self._client_ref._get_pipeline_run(exec_id, hydrate=True)
        stop_run(run=run, graceful=False)
        return self.get(exec_id)


class _ArtifactsAPI:
    """Namespace for artifact browsing operations."""

    def __init__(self, client: KitaruClient) -> None:
        self._client_ref = client

    def list(
        self,
        exec_id: str,
        *,
        name: str | None = None,
        kind: str | None = None,
        producing_call: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[ArtifactRef]:
        """List artifacts for an execution with optional filters."""
        if limit is not None and limit < 1:
            raise KitaruUsageError("`limit` must be >= 1 when provided.")

        execution = self._client_ref.executions.get(exec_id)
        artifacts = execution.artifacts

        if name is not None:
            artifacts = [artifact for artifact in artifacts if artifact.name == name]
        if kind is not None:
            artifacts = [artifact for artifact in artifacts if artifact.kind == kind]
        if producing_call is not None:
            artifacts = [
                artifact
                for artifact in artifacts
                if artifact.producing_call == producing_call
            ]

        if limit is not None:
            return artifacts[:limit]
        return artifacts

    def get(self, artifact_id: str) -> ArtifactRef:
        """Get one artifact by ID."""
        artifact = self._client_ref._get_artifact_version(
            artifact_id,
            hydrate=True,
        )

        producing_call: str | None = None
        if artifact.producer_step_run_id is not None:
            step = self._client_ref._client().get_run_step(
                artifact.producer_step_run_id,
                hydrate=True,
            )
            producing_call = _normalize_checkpoint_name(step.name)

        return _map_artifact_ref(
            artifact=artifact,
            client=self._client_ref,
            producing_call=producing_call,
        )


class KitaruClient:
    """Client for managing Kitaru executions and artifacts."""

    def __init__(
        self,
        *,
        server_url: str | None = None,
        auth_token: str | None = None,
        project: str | None = None,
    ) -> None:
        """Initialize a Kitaru client.

        Args:
            server_url: Optional per-client server override (not yet supported).
            auth_token: Optional per-client auth token override (not yet
                supported).
            project: Optional per-client project override (not yet supported).

        Raises:
            KitaruFeatureNotAvailableError: If per-client connection overrides
                are provided.
        """
        explicit_overrides: dict[str, str] = {}
        if server_url is not None:
            explicit_overrides["server_url"] = server_url
        if auth_token is not None:
            explicit_overrides["auth_token"] = auth_token
        if project is not None:
            explicit_overrides["project"] = project

        if explicit_overrides:
            supplied = ", ".join(sorted(explicit_overrides))
            raise KitaruFeatureNotAvailableError(
                "Per-client connection overrides are not implemented yet "
                f"(received: {supplied}). Use kitaru.connect(...) and active "
                "project settings for now."
            )

        resolved_connection = resolve_connection_config(validate_for_use=True)
        self._project = resolved_connection.project

        self.executions = _ExecutionsAPI(self)
        self.artifacts = _ArtifactsAPI(self)

    def _client(self) -> Client:
        """Return a ZenML client instance."""
        return Client()

    def _get_pipeline_run(
        self,
        exec_id: str,
        *,
        hydrate: bool,
    ) -> PipelineRunResponse:
        """Fetch a run by execution ID with strict ID matching."""
        try:
            return self._client().get_pipeline_run(
                name_id_or_prefix=exec_id,
                allow_name_prefix_match=False,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load execution '{exec_id}': {exc}"
            ) from exc

    def _get_artifact_version(
        self,
        artifact_id: str,
        *,
        hydrate: bool,
    ) -> ArtifactVersionResponse:
        """Fetch an artifact version by ID."""
        try:
            return self._client().get_artifact_version(
                name_id_or_prefix=artifact_id,
                project=self._project,
                hydrate=hydrate,
            )
        except Exception as exc:
            raise KitaruBackendError(
                f"Failed to load artifact '{artifact_id}': {exc}"
            ) from exc


__all__ = [
    "ArtifactRef",
    "CheckpointAttempt",
    "CheckpointCall",
    "Execution",
    "ExecutionStatus",
    "FailureInfo",
    "KitaruClient",
    "LogEntry",
    "PendingWait",
]
