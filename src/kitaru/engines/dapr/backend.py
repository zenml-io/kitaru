"""Dapr execution engine backend for Kitaru.

Owns checkpoint-as-activity registration, the activity execution wrapper,
retry-policy mapping, and a runtime session that routes ``save``/``load``/
``log`` into the Dapr execution ledger store.

This module is lazily imported by the engine registry on first backend
access. Importing ``kitaru.engines`` does not import this file, and this
file does not import the Dapr SDK at module level.
"""

from __future__ import annotations

import hashlib
import threading
import traceback
from collections.abc import Callable
from contextlib import ExitStack
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Protocol

from kitaru.engines._types import ExecutionGraphSnapshot
from kitaru.engines.dapr.models import (
    ArtifactRecord,
    CheckpointAttemptRecord,
    CheckpointCallRecord,
    FailureRecord,
)
from kitaru.engines.dapr.store import ExecutionLedgerStore
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruDivergenceError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    classify_failure_origin,
    traceback_exception_type,
)

# ---------------------------------------------------------------------------
# Retry policy mapping
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DaprRetryPolicySpec:
    """Backend-neutral retry spec, testable without Dapr SDK types.

    ``max_attempts`` is total attempts (first run + retries).
    Kitaru's ``retries`` parameter means extra tries *after* the first.
    """

    max_attempts: int
    initial_retry_interval_seconds: int = 1
    backoff_coefficient: float = 1.0


def _to_retry_policy(retries: int) -> DaprRetryPolicySpec | None:
    """Map Kitaru retry count to a Dapr retry policy spec."""
    if retries <= 0:
        return None
    return DaprRetryPolicySpec(max_attempts=retries + 1)


# ---------------------------------------------------------------------------
# Activity request (wire contract between workflow interpreter and activity)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DaprCheckpointActivityRequest:
    """Input to a Dapr checkpoint activity invocation.

    Created by the workflow interpreter (Phase 8) or directly in tests.
    """

    exec_id: str
    flow_name: str | None
    call_id: str
    invocation_id: str
    checkpoint_name: str
    checkpoint_type: str | None = None
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    attempt_number: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    original_call_id: str | None = None
    upstream_call_ids: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Activity registrar protocol
# ---------------------------------------------------------------------------


class DaprActivityRegistrar(Protocol):
    """Registers activity callables with the Dapr workflow runtime."""

    def register_activity(
        self,
        *,
        name: str,
        fn: Callable[[DaprCheckpointActivityRequest], Any],
    ) -> None: ...


# ---------------------------------------------------------------------------
# Activity binding (per-invocation mutable state, contextvar-scoped)
# ---------------------------------------------------------------------------


@dataclass
class _PendingManualArtifact:
    """Buffered manual artifact waiting for flush on activity success."""

    name: str
    value: Any
    kind: str
    tags: tuple[str, ...]


@dataclass
class _DaprActivityBinding:
    """Per-activity-invocation mutable state.

    Stored in a contextvar so concurrent activities do not share buffers.
    """

    store: ExecutionLedgerStore
    request: DaprCheckpointActivityRequest
    pending_manual_artifacts: dict[str, _PendingManualArtifact] = field(
        default_factory=dict
    )


_CURRENT_ACTIVITY_BINDING: ContextVar[_DaprActivityBinding | None] = ContextVar(
    "kitaru_dapr_activity_binding",
    default=None,
)


# ---------------------------------------------------------------------------
# Deterministic artifact IDs
# ---------------------------------------------------------------------------


def _step_output_artifact_id(call_id: str) -> str:
    return f"{call_id}:step_output"


def _manual_artifact_id(call_id: str, name: str) -> str:
    name_hash = hashlib.sha256(name.encode()).hexdigest()[:16]
    return f"{call_id}:manual:{name_hash}"


# ---------------------------------------------------------------------------
# Activity wrapper
# ---------------------------------------------------------------------------

_PHASE8_FLOW_MSG = (
    "Dapr flow execution is not yet implemented. "
    "This will be available in a future Kitaru release."
)
_PHASE8_CHECKPOINT_MSG = (
    "Dapr checkpoint invocation methods (call/submit/map/product) are not "
    "yet implemented. The Dapr workflow interpreter will wire these in a "
    "future Kitaru release."
)


def _classify_exception_origin(exc: BaseException, tb_text: str) -> FailureOrigin:
    """Classify a caught exception into a FailureOrigin."""
    if isinstance(exc, KitaruBackendError):
        return FailureOrigin.BACKEND
    if isinstance(exc, KitaruDivergenceError):
        return FailureOrigin.DIVERGENCE
    if isinstance(exc, (KitaruRuntimeError, KitaruStateError)):
        return FailureOrigin.RUNTIME
    return classify_failure_origin(
        status_reason=str(exc),
        traceback=tb_text,
        default=FailureOrigin.USER_CODE,
    )


def _build_failure_record(exc: BaseException) -> FailureRecord:
    """Build a structured failure record from an exception."""
    tb_text = traceback.format_exc()
    message = str(exc) or type(exc).__name__
    return FailureRecord(
        message=message,
        exception_type=traceback_exception_type(tb_text) or type(exc).__name__,
        traceback=tb_text,
        origin=_classify_exception_origin(exc, tb_text),
    )


def _run_checkpoint_activity(
    request: DaprCheckpointActivityRequest,
    *,
    entrypoint: Callable[..., Any],
    store: ExecutionLedgerStore,
) -> Any:
    """Execute a single checkpoint activity invocation.

    This is the core activity wrapper that:
    1. Creates/updates the checkpoint call record
    2. Journals the attempt
    3. Enters runtime scopes
    4. Runs user code
    5. Flushes artifacts on success / records failure
    """
    from kitaru.runtime import _checkpoint_scope, _flow_scope

    now = datetime.now(UTC)

    # Explicit attempt_number (for replay idempotency) or auto-incremented
    try:
        existing = store.get_execution(request.exec_id)
        existing_call = next(
            (c for c in existing.checkpoints if c.call_id == request.call_id),
            None,
        )
    except KitaruRuntimeError:
        existing_call = None

    attempt_number = request.attempt_number
    if attempt_number is None:
        existing_attempts = existing_call.attempts if existing_call else ()
        attempt_number = len(existing_attempts) + 1

    attempt_id = f"{request.call_id}:attempt:{attempt_number}"

    call_metadata = dict(existing_call.metadata) if existing_call else {}
    call_metadata.update(request.metadata)

    call_record = CheckpointCallRecord(
        call_id=request.call_id,
        invocation_id=request.invocation_id,
        name=request.checkpoint_name,
        checkpoint_type=request.checkpoint_type,
        status="running",
        started_at=(existing_call.started_at if existing_call else None) or now,
        ended_at=None,
        metadata=call_metadata,
        original_call_id=request.original_call_id
        or (existing_call.original_call_id if existing_call else None),
        upstream_call_ids=request.upstream_call_ids
        or (existing_call.upstream_call_ids if existing_call else ()),
        failure=None,
        attempts=existing_call.attempts if existing_call else (),
        artifacts=existing_call.artifacts if existing_call else (),
    )
    store.upsert_checkpoint_call(request.exec_id, call_record)

    attempt = CheckpointAttemptRecord(
        attempt_id=attempt_id,
        attempt_number=attempt_number,
        status="running",
        started_at=now,
    )
    store.append_checkpoint_attempt(request.exec_id, request.call_id, attempt)

    binding = _DaprActivityBinding(store=store, request=request)
    binding_token = _CURRENT_ACTIVITY_BINDING.set(binding)

    # Pre-install DaprRuntimeSession so _flow_scope() does not create a
    # ZenML session. _flow_scope() only installs when none is active.
    from kitaru.runtime import _CURRENT_RUNTIME_SESSION

    session = DaprRuntimeSession()
    session_token = _CURRENT_RUNTIME_SESSION.set(session)

    try:
        with ExitStack() as scope_stack:
            scope_stack.enter_context(
                _flow_scope(
                    name=request.flow_name,
                    execution_id=request.exec_id,
                )
            )
            scope_stack.enter_context(
                _checkpoint_scope(
                    name=request.checkpoint_name,
                    checkpoint_type=request.checkpoint_type,
                    execution_id=request.exec_id,
                    checkpoint_id=request.invocation_id,
                )
            )
            result = entrypoint(*request.args, **request.kwargs)

        end_time = datetime.now(UTC)
        for pending in binding.pending_manual_artifacts.values():
            artifact = ArtifactRecord(
                artifact_id=_manual_artifact_id(request.call_id, pending.name),
                name=pending.name,
                kind=pending.kind,
                save_type="manual",
                producing_call_id=request.call_id,
                metadata={"tags": list(pending.tags)} if pending.tags else {},
            )
            store.store_artifact(request.exec_id, artifact, pending.value)

        output_artifact = ArtifactRecord(
            artifact_id=_step_output_artifact_id(request.call_id),
            name="output",
            kind="output",
            save_type="step_output",
            producing_call_id=request.call_id,
        )
        store.store_artifact(request.exec_id, output_artifact, result)

        completed_attempt = CheckpointAttemptRecord(
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            status="completed",
            started_at=now,
            ended_at=end_time,
        )
        store.append_checkpoint_attempt(
            request.exec_id, request.call_id, completed_attempt
        )

        _update_call_status(
            store,
            exec_id=request.exec_id,
            call_id=request.call_id,
            status="completed",
            ended_at=end_time,
            failure=None,
        )

        return result

    except BaseException as exc:
        end_time = datetime.now(UTC)
        failure = _build_failure_record(exc)

        failed_attempt = CheckpointAttemptRecord(
            attempt_id=attempt_id,
            attempt_number=attempt_number,
            status="failed",
            started_at=now,
            ended_at=end_time,
            failure=failure,
        )
        store.append_checkpoint_attempt(
            request.exec_id, request.call_id, failed_attempt
        )

        _update_call_status(
            store,
            exec_id=request.exec_id,
            call_id=request.call_id,
            status="failed",
            ended_at=end_time,
            failure=failure,
        )

        # Buffered manual artifacts are intentionally not flushed here:
        # checkpoint failure should be atomic (no partial artifact leakage).
        raise

    finally:
        _CURRENT_RUNTIME_SESSION.reset(session_token)
        _CURRENT_ACTIVITY_BINDING.reset(binding_token)


def _update_call_status(
    store: ExecutionLedgerStore,
    *,
    exec_id: str,
    call_id: str,
    status: str,
    ended_at: datetime | None,
    failure: FailureRecord | None,
) -> None:
    """Re-read the latest call record and update its status fields."""
    record = store.get_execution(exec_id)
    for cp in record.checkpoints:
        if cp.call_id == call_id:
            updated = replace(
                cp,
                status=status,
                ended_at=ended_at,
                failure=failure,
            )
            store.upsert_checkpoint_call(exec_id, updated)
            return


# ---------------------------------------------------------------------------
# Runtime session
# ---------------------------------------------------------------------------


class DaprRuntimeSession:
    """Dapr-backed runtime session for in-flow primitive dispatch.

    Routes ``save``/``load``/``log`` to the Dapr execution ledger store
    via the current activity binding context.
    """

    def wait(
        self,
        *,
        schema: Any = None,
        name: str | None = None,
        question: str | None = None,
        timeout: int,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        raise KitaruFeatureNotAvailableError(
            "kitaru.wait() inside Dapr activities is not yet implemented. "
            "Wait orchestration will be available in a future release."
        )

    def save_artifact(
        self,
        name: str,
        value: Any,
        *,
        type: str,
        tags: list[str] | None = None,
    ) -> None:
        """Buffer a manual artifact for flush on activity success."""
        binding = _CURRENT_ACTIVITY_BINDING.get()
        if binding is None:
            raise KitaruRuntimeError(
                "kitaru.save() called outside a Dapr activity context."
            )
        binding.pending_manual_artifacts[name] = _PendingManualArtifact(
            name=name,
            value=value,
            kind=type,
            tags=tuple(tags) if tags else (),
        )

    def load_artifact(self, exec_id: str, name: str) -> Any:
        """Load an artifact from the ledger, checking local buffer first."""
        binding = _CURRENT_ACTIVITY_BINDING.get()
        if binding is None:
            raise KitaruRuntimeError(
                "kitaru.load() called outside a Dapr activity context."
            )
        store = binding.store
        request = binding.request

        # Check local buffer for same-execution, same-name manual artifact
        if exec_id == request.exec_id and name in binding.pending_manual_artifacts:
            return binding.pending_manual_artifacts[name].value

        # Search the execution record
        record = store.get_execution(exec_id)
        matches: list[ArtifactRecord] = []

        for art in record.artifacts:
            is_manual_match = art.save_type == "manual" and art.name == name
            is_output_match = art.save_type == "step_output" and (
                art.name == name or _checkpoint_name_matches(record, art, name)
            )
            if is_manual_match or is_output_match:
                matches.append(art)

        if not matches:
            raise KitaruRuntimeError(
                f"No artifact named {name!r} found in execution {exec_id!r}."
            )

        if len(matches) > 1:
            details = ", ".join(
                f"artifact_id={m.artifact_id!r}, save_type={m.save_type!r}"
                for m in matches
            )
            raise KitaruRuntimeError(
                f"Multiple artifacts named {name!r} found in execution "
                f"{exec_id!r}. Disambiguate by using a unique name. "
                f"Matches: {details}"
            )

        _, value = store.load_artifact(matches[0].artifact_id)
        return value

    def log_metadata(self, metadata: dict[str, Any]) -> None:
        """Merge metadata into the checkpoint or execution via the ledger."""
        from kitaru.runtime import _is_inside_checkpoint

        binding = _CURRENT_ACTIVITY_BINDING.get()
        if binding is None:
            raise KitaruRuntimeError(
                "kitaru.log() called outside a Dapr activity context."
            )
        store = binding.store
        request = binding.request

        if _is_inside_checkpoint():
            store.merge_checkpoint_metadata(request.exec_id, request.call_id, metadata)
        else:
            store.merge_execution_metadata(request.exec_id, metadata)


def _checkpoint_name_matches(
    record: Any,
    artifact: ArtifactRecord,
    name: str,
) -> bool:
    """Check if a step-output artifact's producing checkpoint matches name."""
    if artifact.producing_call_id is None:
        return False
    for cp in record.checkpoints:
        if cp.call_id == artifact.producing_call_id and cp.name == name:
            return True
    return False


# ---------------------------------------------------------------------------
# Definition wrappers
# ---------------------------------------------------------------------------


class DaprFlowDefinition:
    """Flow definition placeholder — real execution deferred to Phase 8."""

    __slots__ = ("_entrypoint", "_registration_name")

    def __init__(self, entrypoint: Callable[..., Any], registration_name: str) -> None:
        self._entrypoint = entrypoint
        self._registration_name = registration_name

    @property
    def source_object(self) -> Any:
        return self._entrypoint

    def run(self, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_FLOW_MSG)

    def replay(self, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_FLOW_MSG)


class DaprCheckpointDefinition:
    """Checkpoint definition backed by a registered Dapr activity."""

    __slots__ = (
        "_checkpoint_type",
        "_entrypoint",
        "_registration_name",
        "_retry_policy",
        "_runtime",
    )

    def __init__(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
        checkpoint_type: str | None,
        retry_policy: DaprRetryPolicySpec | None,
        runtime: Any,
    ) -> None:
        self._entrypoint = entrypoint
        self._registration_name = registration_name
        self._checkpoint_type = checkpoint_type
        self._retry_policy = retry_policy
        self._runtime = runtime

    @property
    def source_object(self) -> Any:
        return self._entrypoint

    @property
    def registration_name(self) -> str:
        return self._registration_name

    @property
    def retry_policy(self) -> DaprRetryPolicySpec | None:
        return self._retry_policy

    def make_activity_callable(
        self,
        store_provider: Callable[[], ExecutionLedgerStore],
    ) -> Callable[[DaprCheckpointActivityRequest], Any]:
        """Build the activity callable for Dapr runtime registration."""
        entrypoint = self._entrypoint

        def activity(request: DaprCheckpointActivityRequest) -> Any:
            return _run_checkpoint_activity(
                request,
                entrypoint=entrypoint,
                store=store_provider(),
            )

        activity.__name__ = self._registration_name
        activity.__qualname__ = self._registration_name
        return activity

    def call(self, *args: Any, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_CHECKPOINT_MSG)

    def submit(self, *args: Any, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_CHECKPOINT_MSG)

    def map(self, *args: Any, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_CHECKPOINT_MSG)

    def product(self, *args: Any, **kwargs: Any) -> Any:
        raise KitaruFeatureNotAvailableError(_PHASE8_CHECKPOINT_MSG)


# ---------------------------------------------------------------------------
# Backend class
# ---------------------------------------------------------------------------


class DaprExecutionEngineBackend:
    """Dapr engine backend with checkpoint activity registration."""

    def __init__(self) -> None:
        self._checkpoint_definitions: dict[str, DaprCheckpointDefinition] = {}
        self._ledger_store_provider: Callable[[], ExecutionLedgerStore] | None = None
        self._lock = threading.RLock()

    @property
    def name(self) -> str:
        return "dapr"

    def execution_graph_from_run(self, run: Any) -> ExecutionGraphSnapshot:
        raise KitaruFeatureNotAvailableError(
            "Execution graph mapping for Dapr is not yet implemented."
        )

    def create_flow_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
    ) -> DaprFlowDefinition:
        return DaprFlowDefinition(entrypoint, registration_name)

    def create_checkpoint_definition(
        self,
        *,
        entrypoint: Callable[..., Any],
        registration_name: str,
        retries: int,
        checkpoint_type: str | None,
        runtime: Any,
    ) -> DaprCheckpointDefinition:
        defn = DaprCheckpointDefinition(
            entrypoint=entrypoint,
            registration_name=registration_name,
            checkpoint_type=checkpoint_type,
            retry_policy=_to_retry_policy(retries),
            runtime=runtime,
        )
        with self._lock:
            self._checkpoint_definitions[registration_name] = defn
        return defn

    def create_runtime_session(self) -> DaprRuntimeSession:
        return DaprRuntimeSession()

    def bind_ledger_store_provider(
        self,
        provider: Callable[[], ExecutionLedgerStore],
    ) -> None:
        """Set the store provider used by activity callables."""
        with self._lock:
            self._ledger_store_provider = provider

    def register_checkpoint_activities(
        self,
        registrar: DaprActivityRegistrar,
    ) -> None:
        """Register all known checkpoint definitions as Dapr activities."""
        with self._lock:
            if self._ledger_store_provider is None:
                raise KitaruRuntimeError(
                    "Cannot register activities without a ledger store provider. "
                    "Call bind_ledger_store_provider() first."
                )
            provider = self._ledger_store_provider
            for reg_name in sorted(self._checkpoint_definitions):
                defn = self._checkpoint_definitions[reg_name]
                registrar.register_activity(
                    name=reg_name,
                    fn=defn.make_activity_callable(provider),
                )

    def get_checkpoint_definitions(
        self,
    ) -> dict[str, DaprCheckpointDefinition]:
        """Return a snapshot of registered checkpoint definitions."""
        with self._lock:
            return dict(self._checkpoint_definitions)
