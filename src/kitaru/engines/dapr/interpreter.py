"""Dapr wrapper workflow interpreter.

Implements the deterministic wrapper orchestrator pattern: the user's
``@flow`` function is re-executed from the top on each cycle. Checkpoint
calls, waits, and future resolutions are intercepted as suspend signals
and converted into activity invocations or event waits.

This module is imported lazily by the Dapr backend; it does not import
the Dapr SDK at module level.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

from kitaru.engines.dapr.backend import (
    DaprCheckpointActivityRequest,
)
from kitaru.engines.dapr.store import ExecutionLedgerStore
from kitaru.errors import (
    FailureOrigin,
    KitaruDivergenceError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
)

# ---------------------------------------------------------------------------
# Suspend exception
# ---------------------------------------------------------------------------


class FlowSuspendRequested(BaseException):
    """Signal from user flow code to the wrapper orchestrator.

    Raised when a Kitaru primitive inside the flow body needs a Dapr
    operation (checkpoint call, future join, wait).
    """

    __slots__ = ("kind", "op_index", "payload")

    def __init__(
        self,
        *,
        op_index: int,
        kind: str,
        payload: Any = None,
    ) -> None:
        super().__init__(f"FlowSuspendRequested(op={op_index}, kind={kind})")
        self.op_index = op_index
        self.kind = kind
        self.payload = payload


# ---------------------------------------------------------------------------
# Operation fingerprint for divergence detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _OpFingerprint:
    """Fingerprint for one operation in the call sequence."""

    kind: str
    name: str
    call_id: str
    checkpoint_type: str | None = None


def _fingerprint_key(fp: _OpFingerprint) -> str:
    return f"{fp.kind}|{fp.name}|{fp.call_id}|{fp.checkpoint_type or ''}"


# ---------------------------------------------------------------------------
# Suspend payloads
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CheckpointSuspendPayload:
    """Data carried by a checkpoint suspend signal."""

    checkpoint_name: str
    checkpoint_type: str | None
    call_id: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    retry_policy: Any


@dataclass(frozen=True)
class _WaitSuspendPayload:
    """Data carried by a wait suspend signal."""

    wait_id: str
    name: str
    schema: Any
    question: str | None
    timeout: int
    metadata: dict[str, Any] | None


@dataclass(frozen=True)
class _FutureJoinSuspendPayload:
    """Data carried by a future-join suspend signal."""

    call_ids: tuple[str, ...]


# ---------------------------------------------------------------------------
# Replay seed
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplaySeed:
    """Cross-execution replay seed consumed by the interpreter."""

    source_exec_id: str
    seeded_results: dict[str, Any]


@dataclass(frozen=True)
class FlowIterationOutcome:
    """Result of one replay iteration through a flow function."""

    kind: Literal["completed", "suspended", "unresolved_futures"]
    result: Any | None = None
    suspend: FlowSuspendRequested | None = None
    unresolved: tuple[_ScheduledFuture, ...] = ()
    buffered_metadata: tuple[dict[str, Any], ...] = ()


# ---------------------------------------------------------------------------
# Scheduled future tracking
# ---------------------------------------------------------------------------


@dataclass
class _ScheduledFuture:
    """Tracks a submitted-but-possibly-unresolved checkpoint task."""

    call_id: str
    checkpoint_name: str
    checkpoint_type: str | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    retry_policy: Any


# ---------------------------------------------------------------------------
# Orchestrator session
# ---------------------------------------------------------------------------


_CURRENT_ORCHESTRATOR_SESSION: ContextVar[DaprOrchestratorSession | None] = ContextVar(
    "kitaru_dapr_orchestrator_session",
    default=None,
)


class DaprOrchestratorSession:
    """Mutable interpreter state for a single Dapr flow execution.

    Tracks completed operations, scheduled tasks, resolved values,
    pending waits, metadata buffer, and call sequence fingerprints.
    """

    __slots__ = (
        "_call_fingerprints",
        "_checkpoint_occurrence",
        "_metadata_buffer",
        "_op_counter",
        "_replay_seed",
        "_resolved_values",
        "_resolved_waits",
        "_scheduled_futures",
        "_step_input_overrides",
        "_store",
        "_wait_occurrence",
        "exec_id",
        "flow_name",
    )

    def __init__(
        self,
        *,
        exec_id: str,
        flow_name: str | None,
        store: ExecutionLedgerStore,
        replay_seed: ReplaySeed | None = None,
        step_input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self.exec_id = exec_id
        self.flow_name = flow_name
        self._store = store
        self._replay_seed = replay_seed
        self._step_input_overrides = {
            call_id: dict(overrides)
            for call_id, overrides in (step_input_overrides or {}).items()
        }

        # Persistent across replay iterations
        self._resolved_values: dict[str, Any] = {}
        self._resolved_waits: dict[str, Any] = {}
        self._scheduled_futures: dict[str, _ScheduledFuture] = {}
        self._metadata_buffer: list[dict[str, Any]] = []
        self._call_fingerprints: list[_OpFingerprint] = []

        # Reset per replay iteration
        self._op_counter: int = 0
        self._checkpoint_occurrence: dict[str, int] = {}
        self._wait_occurrence: int = 0

    def reset_for_replay(self) -> None:
        """Reset per-iteration counters for the next flow re-execution."""
        self._op_counter = 0
        self._checkpoint_occurrence = {}
        self._wait_occurrence = 0

    def next_op_index(self) -> int:
        idx = self._op_counter
        self._op_counter += 1
        return idx

    # -- Deterministic identity ---------------------------------------------

    def _deterministic_call_id(self, name: str, user_id: str | None) -> str:
        if user_id is not None:
            return user_id
        count = self._checkpoint_occurrence.get(name, 0)
        self._checkpoint_occurrence[name] = count + 1
        return f"{name}:{count}"

    def _deterministic_wait_id(self, name: str | None) -> str:
        if name is not None:
            return name
        wait_id = f"wait:{self._wait_occurrence}"
        self._wait_occurrence += 1
        return wait_id

    def _apply_step_input_override(
        self,
        *,
        call_id: str,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        overrides = self._step_input_overrides.get(call_id)
        if not overrides:
            return kwargs
        merged = dict(kwargs)
        merged.update(overrides)
        return merged

    # -- Divergence detection -----------------------------------------------

    def _check_divergence(self, op_index: int, fingerprint: _OpFingerprint) -> None:
        key = _fingerprint_key(fingerprint)
        if op_index < len(self._call_fingerprints):
            expected = self._call_fingerprints[op_index]
            if _fingerprint_key(expected) != key:
                raise KitaruDivergenceError(
                    f"Flow divergence detected at operation {op_index}: "
                    f"expected {expected.kind}:{expected.name} "
                    f"but got {fingerprint.kind}:{fingerprint.name}",
                    exec_id=self.exec_id,
                    status="failed",
                    failure_origin=FailureOrigin.DIVERGENCE,
                )
        else:
            self._call_fingerprints.append(fingerprint)

    # -- Checkpoint call (sync) ---------------------------------------------

    def call_checkpoint(
        self,
        *,
        checkpoint_name: str,
        checkpoint_type: str | None,
        retry_policy: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        call_id: str | None = None,
    ) -> Any:
        """Intercept a sync checkpoint call.

        Returns cached result if already resolved or seeded.
        Otherwise raises ``FlowSuspendRequested``.
        """
        resolved_call_id = self._deterministic_call_id(checkpoint_name, call_id)
        resolved_kwargs = self._apply_step_input_override(
            call_id=resolved_call_id,
            kwargs=kwargs,
        )
        op_index = self.next_op_index()

        fingerprint = _OpFingerprint(
            kind="checkpoint",
            name=checkpoint_name,
            call_id=resolved_call_id,
            checkpoint_type=checkpoint_type,
        )
        self._check_divergence(op_index, fingerprint)

        if resolved_call_id in self._resolved_values:
            return self._resolved_values[resolved_call_id]

        if self._replay_seed and resolved_call_id in self._replay_seed.seeded_results:
            result = self._replay_seed.seeded_results[resolved_call_id]
            self._resolved_values[resolved_call_id] = result
            return result

        raise FlowSuspendRequested(
            op_index=op_index,
            kind="checkpoint",
            payload=_CheckpointSuspendPayload(
                checkpoint_name=checkpoint_name,
                checkpoint_type=checkpoint_type,
                call_id=resolved_call_id,
                args=args,
                kwargs=resolved_kwargs,
                retry_policy=retry_policy,
            ),
        )

    # -- Checkpoint submit (async) ------------------------------------------

    def submit_checkpoint(
        self,
        *,
        checkpoint_name: str,
        checkpoint_type: str | None,
        retry_policy: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        call_id: str | None = None,
    ) -> DaprNativeStepFuture:
        """Schedule a checkpoint for concurrent execution, return a future."""
        resolved_call_id = self._deterministic_call_id(checkpoint_name, call_id)
        resolved_kwargs = self._apply_step_input_override(
            call_id=resolved_call_id,
            kwargs=kwargs,
        )
        op_index = self.next_op_index()

        fingerprint = _OpFingerprint(
            kind="submit",
            name=checkpoint_name,
            call_id=resolved_call_id,
            checkpoint_type=checkpoint_type,
        )
        self._check_divergence(op_index, fingerprint)

        if resolved_call_id in self._resolved_values:
            return DaprNativeStepFuture(
                invocation_id=resolved_call_id,
                call_id=resolved_call_id,
                session=self,
            )

        if self._replay_seed and resolved_call_id in self._replay_seed.seeded_results:
            result = self._replay_seed.seeded_results[resolved_call_id]
            self._resolved_values[resolved_call_id] = result
            return DaprNativeStepFuture(
                invocation_id=resolved_call_id,
                call_id=resolved_call_id,
                session=self,
            )

        self._scheduled_futures[resolved_call_id] = _ScheduledFuture(
            call_id=resolved_call_id,
            checkpoint_name=checkpoint_name,
            checkpoint_type=checkpoint_type,
            args=args,
            kwargs=resolved_kwargs,
            retry_policy=retry_policy,
        )
        return DaprNativeStepFuture(
            invocation_id=resolved_call_id,
            call_id=resolved_call_id,
            session=self,
        )

    # -- Map / product ------------------------------------------------------

    def map_checkpoint(
        self,
        *,
        checkpoint_name: str,
        checkpoint_type: str | None,
        retry_policy: Any,
        mapped_args: tuple[Any, ...],
        kwargs: dict[str, Any],
        call_id_prefix: str | None = None,
    ) -> DaprNativeMapFuture:
        """Expand mapped inputs and schedule N concurrent checkpoints."""
        if not mapped_args:
            return DaprNativeMapFuture(futures=[])

        items = list(mapped_args[0])
        extra_args = mapped_args[1:]

        return self._fan_out(
            checkpoint_name=checkpoint_name,
            checkpoint_type=checkpoint_type,
            retry_policy=retry_policy,
            item_args=[(item, *extra_args) for item in items],
            kwargs=kwargs,
            call_id_prefix=call_id_prefix,
        )

    def product_checkpoint(
        self,
        *,
        checkpoint_name: str,
        checkpoint_type: str | None,
        retry_policy: Any,
        product_args: tuple[Any, ...],
        kwargs: dict[str, Any],
        call_id_prefix: str | None = None,
    ) -> DaprNativeMapFuture:
        """Expand cartesian product of inputs and schedule N checkpoints."""
        if not product_args:
            return DaprNativeMapFuture(futures=[])

        iterables = [list(arg) for arg in product_args]

        return self._fan_out(
            checkpoint_name=checkpoint_name,
            checkpoint_type=checkpoint_type,
            retry_policy=retry_policy,
            item_args=list(itertools.product(*iterables)),
            kwargs=kwargs,
            call_id_prefix=call_id_prefix,
        )

    def _fan_out(
        self,
        *,
        checkpoint_name: str,
        checkpoint_type: str | None,
        retry_policy: Any,
        item_args: list[tuple[Any, ...]],
        kwargs: dict[str, Any],
        call_id_prefix: str | None,
    ) -> DaprNativeMapFuture:
        """Shared fan-out loop for map and product."""
        futures: list[DaprNativeStepFuture] = []
        for i, combo in enumerate(item_args):
            child_id = f"{call_id_prefix}:{i}" if call_id_prefix else None
            future = self.submit_checkpoint(
                checkpoint_name=checkpoint_name,
                checkpoint_type=checkpoint_type,
                retry_policy=retry_policy,
                args=tuple(combo),
                kwargs=kwargs,
                call_id=child_id,
            )
            futures.append(future)
        return DaprNativeMapFuture(futures=futures)

    # -- Wait ---------------------------------------------------------------

    def wait_for_input(
        self,
        *,
        schema: Any = None,
        name: str | None = None,
        question: str | None = None,
        timeout: int,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Intercept a wait call. Returns cached or raises suspend."""
        wait_id = self._deterministic_wait_id(name)
        op_index = self.next_op_index()

        fingerprint = _OpFingerprint(
            kind="wait",
            name=wait_id,
            call_id=wait_id,
        )
        self._check_divergence(op_index, fingerprint)

        if wait_id in self._resolved_waits:
            return self._resolved_waits[wait_id]

        raise FlowSuspendRequested(
            op_index=op_index,
            kind="wait",
            payload=_WaitSuspendPayload(
                wait_id=wait_id,
                name=name or wait_id,
                schema=schema,
                question=question,
                timeout=timeout,
                metadata=metadata,
            ),
        )

    # -- Metadata buffering -------------------------------------------------

    def buffer_metadata(self, metadata: dict[str, Any]) -> None:
        """Buffer metadata for later flush via activity."""
        self._metadata_buffer.append(metadata)

    def drain_metadata(self) -> list[dict[str, Any]]:
        """Remove and return all buffered metadata."""
        drained = list(self._metadata_buffer)
        self._metadata_buffer.clear()
        return drained

    def has_buffered_metadata(self) -> bool:
        return bool(self._metadata_buffer)

    # -- Future resolution --------------------------------------------------

    def has_unresolved_futures(self) -> bool:
        return any(
            f.call_id not in self._resolved_values
            for f in self._scheduled_futures.values()
        )

    def unresolved_future_specs(self) -> list[_ScheduledFuture]:
        """Return specs for all unresolved futures."""
        return [
            f
            for f in self._scheduled_futures.values()
            if f.call_id not in self._resolved_values
        ]

    def get_scheduled_future(self, call_id: str) -> _ScheduledFuture | None:
        """Look up a scheduled future by call_id."""
        return self._scheduled_futures.get(call_id)

    def record_resolution(self, call_id: str, value: Any) -> None:
        self._resolved_values[call_id] = value

    def record_wait_resolution(self, wait_id: str, value: Any) -> None:
        self._resolved_waits[wait_id] = value

    # -- Introspection (for tests) ------------------------------------------

    @property
    def resolved_values(self) -> dict[str, Any]:
        return dict(self._resolved_values)

    @property
    def fingerprints(self) -> list[_OpFingerprint]:
        return list(self._call_fingerprints)


# ---------------------------------------------------------------------------
# Dapr-native future types (duck-typed for KitaruStepFuture wrapping)
# ---------------------------------------------------------------------------


class DaprNativeStepFuture:
    """Dapr-backed step future satisfying the KitaruStepFuture duck contract.

    When ``result()`` or ``load()`` is called on an unresolved future, it
    raises ``FlowSuspendRequested`` to trigger resolution by the interpreter.
    """

    __slots__ = ("_call_id", "_session", "invocation_id")

    def __init__(
        self,
        *,
        invocation_id: str,
        call_id: str,
        session: DaprOrchestratorSession,
    ) -> None:
        self.invocation_id = invocation_id
        self._call_id = call_id
        self._session = session

    def running(self) -> bool:
        return self._call_id not in self._session._resolved_values

    def wait(self) -> None:
        if self._call_id in self._session._resolved_values:
            return
        raise FlowSuspendRequested(
            op_index=self._session.next_op_index(),
            kind="future_join",
            payload=_FutureJoinSuspendPayload(call_ids=(self._call_id,)),
        )

    def result(self) -> Any:
        if self._call_id in self._session._resolved_values:
            return self._session._resolved_values[self._call_id]
        raise FlowSuspendRequested(
            op_index=self._session.next_op_index(),
            kind="future_join",
            payload=_FutureJoinSuspendPayload(call_ids=(self._call_id,)),
        )

    def load(self, *, disable_cache: bool = False) -> Any:
        return self.result()

    def artifacts(self) -> Any:
        return self.result()

    def get_artifact(self, key: str) -> DaprNativeArtifactFuture:
        return DaprNativeArtifactFuture(
            invocation_id=self.invocation_id,
            call_id=self._call_id,
            output_name=key,
            session=self._session,
        )

    def __getitem__(self, key: int | slice) -> Any:
        val = self.result()
        if isinstance(val, (list, tuple)):
            items = val[key]
            if isinstance(items, list):
                return tuple(
                    DaprNativeArtifactFuture(
                        invocation_id=self.invocation_id,
                        call_id=self._call_id,
                        output_name=str(i),
                        session=self._session,
                    )
                    for i in range(len(items))
                )
            return DaprNativeArtifactFuture(
                invocation_id=self.invocation_id,
                call_id=self._call_id,
                output_name=str(key),
                session=self._session,
            )
        return val

    def __iter__(self) -> Iterator[DaprNativeArtifactFuture]:
        yield DaprNativeArtifactFuture(
            invocation_id=self.invocation_id,
            call_id=self._call_id,
            output_name="output",
            session=self._session,
        )

    def __len__(self) -> int:
        return 1


class DaprNativeArtifactFuture:
    """Single artifact future satisfying the KitaruArtifactFuture duck contract."""

    __slots__ = ("_call_id", "_output_name", "_session", "invocation_id")

    def __init__(
        self,
        *,
        invocation_id: str,
        call_id: str,
        output_name: str,
        session: DaprOrchestratorSession,
    ) -> None:
        self.invocation_id = invocation_id
        self._call_id = call_id
        self._output_name = output_name
        self._session = session

    def running(self) -> bool:
        return self._call_id not in self._session._resolved_values

    def result(self) -> Any:
        if self._call_id in self._session._resolved_values:
            return self._session._resolved_values[self._call_id]
        raise FlowSuspendRequested(
            op_index=self._session.next_op_index(),
            kind="future_join",
            payload=_FutureJoinSuspendPayload(call_ids=(self._call_id,)),
        )

    def load(self, *, disable_cache: bool = False) -> Any:
        return self.result()

    def chunk(self, index: int) -> Any:
        raise KitaruFeatureNotAvailableError(
            "Chunked artifacts are not supported on the Dapr backend."
        )


class DaprNativeMapFuture:
    """Map future satisfying the KitaruMapFuture duck contract."""

    __slots__ = ("futures",)

    def __init__(self, *, futures: list[DaprNativeStepFuture]) -> None:
        self.futures = futures

    def running(self) -> bool:
        return any(f.running() for f in self.futures)

    def result(self) -> list[Any]:
        return [f.result() for f in self.futures]

    def load(self, *, disable_cache: bool = False) -> list[Any]:
        return [f.load(disable_cache=disable_cache) for f in self.futures]

    def unpack(self) -> tuple[list[DaprNativeArtifactFuture], ...]:
        return (
            [
                DaprNativeArtifactFuture(
                    invocation_id=f.invocation_id,
                    call_id=f._call_id,
                    output_name="output",
                    session=f._session,
                )
                for f in self.futures
            ],
        )

    def __getitem__(self, key: int | slice) -> Any:
        if isinstance(key, int):
            return self.futures[key]
        return list(self.futures[key])

    def __iter__(self) -> Iterator[DaprNativeStepFuture]:
        yield from self.futures

    def __len__(self) -> int:
        return len(self.futures)


# ---------------------------------------------------------------------------
# Interpreter loop
# ---------------------------------------------------------------------------

# Type alias for the activity execution callback.
# In tests this calls _run_checkpoint_activity directly.
# In real Dapr this would yield ctx.call_activity().
ActivityExecutor = Callable[[DaprCheckpointActivityRequest], Any]

# Type alias for wait resolution callback.
# In tests this is a simple dict lookup or callable.
WaitResolver = Callable[[_WaitSuspendPayload], Any]

_MAX_INTERPRETER_ITERATIONS = 500


def run_flow_iteration(
    *,
    flow_func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    session: DaprOrchestratorSession,
) -> FlowIterationOutcome:
    """Run one replay iteration without resolving suspends."""
    from kitaru.engines.dapr.backend import DaprRuntimeSession
    from kitaru.runtime import _CURRENT_RUNTIME_SESSION, _flow_scope

    session.reset_for_replay()

    session_obj = DaprRuntimeSession()
    session_token = _CURRENT_RUNTIME_SESSION.set(session_obj)
    orchestrator_token = _CURRENT_ORCHESTRATOR_SESSION.set(session)

    try:
        with ExitStack() as scope_stack:
            scope_stack.enter_context(_dapr_flow_dispatch_scope())
            scope_stack.enter_context(
                _flow_scope(
                    name=session.flow_name,
                    execution_id=session.exec_id,
                )
            )
            result = flow_func(*args, **kwargs)
    except FlowSuspendRequested as suspend:
        return FlowIterationOutcome(
            kind="suspended",
            suspend=suspend,
            buffered_metadata=tuple(session.drain_metadata()),
        )
    finally:
        _CURRENT_ORCHESTRATOR_SESSION.reset(orchestrator_token)
        _CURRENT_RUNTIME_SESSION.reset(session_token)

    unresolved = session.unresolved_future_specs()
    if unresolved:
        return FlowIterationOutcome(
            kind="unresolved_futures",
            result=result,
            unresolved=tuple(unresolved),
            buffered_metadata=tuple(session.drain_metadata()),
        )

    return FlowIterationOutcome(
        kind="completed",
        result=result,
        buffered_metadata=tuple(session.drain_metadata()),
    )


def interpret_flow(
    *,
    flow_func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    session: DaprOrchestratorSession,
    activity_executor: ActivityExecutor,
    wait_resolver: WaitResolver | None = None,
) -> Any:
    """Run the wrapper interpreter loop.

    Re-executes ``flow_func`` until it completes without suspension.
    Checkpoint suspensions are resolved by calling ``activity_executor``.
    Wait suspensions are resolved by calling ``wait_resolver``.
    After the flow completes, flushes pending futures and metadata.
    """
    for _iteration in range(_MAX_INTERPRETER_ITERATIONS):
        outcome = run_flow_iteration(
            flow_func=flow_func,
            args=args,
            kwargs=kwargs,
            session=session,
        )

        if outcome.buffered_metadata:
            merged: dict[str, Any] = {}
            for meta in outcome.buffered_metadata:
                merged.update(meta)
            session._store.merge_execution_metadata(session.exec_id, merged)

        if outcome.kind == "suspended":
            assert outcome.suspend is not None
            _handle_suspend(
                session=session,
                suspend=outcome.suspend,
                activity_executor=activity_executor,
                wait_resolver=wait_resolver,
            )
            continue

        if outcome.kind == "unresolved_futures":
            for spec in outcome.unresolved:
                _execute_and_record(
                    session=session,
                    spec=spec,
                    activity_executor=activity_executor,
                )
            continue

        return outcome.result

    raise KitaruRuntimeError(
        f"Interpreter exceeded {_MAX_INTERPRETER_ITERATIONS} iterations. "
        "This likely indicates an infinite suspend loop."
    )


def _handle_suspend(
    *,
    session: DaprOrchestratorSession,
    suspend: FlowSuspendRequested,
    activity_executor: ActivityExecutor,
    wait_resolver: WaitResolver | None,
) -> None:
    """Handle a single suspend signal from the flow."""
    if suspend.kind == "checkpoint":
        payload: _CheckpointSuspendPayload = suspend.payload
        spec = _ScheduledFuture(
            call_id=payload.call_id,
            checkpoint_name=payload.checkpoint_name,
            checkpoint_type=payload.checkpoint_type,
            args=payload.args,
            kwargs=payload.kwargs,
            retry_policy=payload.retry_policy,
        )
        _execute_and_record(
            session=session,
            spec=spec,
            activity_executor=activity_executor,
        )

    elif suspend.kind == "future_join":
        payload_fj: _FutureJoinSuspendPayload = suspend.payload
        for cid in payload_fj.call_ids:
            if cid in session._resolved_values:
                continue
            spec_fj = session._scheduled_futures.get(cid)
            if spec_fj is None:
                raise KitaruRuntimeError(
                    f"Future join requested for unknown call_id {cid!r}."
                )
            _execute_and_record(
                session=session,
                spec=spec_fj,
                activity_executor=activity_executor,
            )

    elif suspend.kind == "wait":
        if wait_resolver is None:
            raise KitaruFeatureNotAvailableError(
                "Wait resolution is not available in this execution context."
            )
        payload_w: _WaitSuspendPayload = suspend.payload
        value = wait_resolver(payload_w)
        session.record_wait_resolution(payload_w.wait_id, value)

    else:
        raise KitaruRuntimeError(f"Unknown suspend kind {suspend.kind!r}.")


def _execute_and_record(
    *,
    session: DaprOrchestratorSession,
    spec: _ScheduledFuture,
    activity_executor: ActivityExecutor,
) -> None:
    """Build an activity request, execute it, and record the result."""
    request = DaprCheckpointActivityRequest(
        exec_id=session.exec_id,
        flow_name=session.flow_name,
        call_id=spec.call_id,
        invocation_id=str(uuid4()),
        checkpoint_name=spec.checkpoint_name,
        checkpoint_type=spec.checkpoint_type,
        args=spec.args,
        kwargs=spec.kwargs,
    )
    result = activity_executor(request)
    session.record_resolution(spec.call_id, result)


# ---------------------------------------------------------------------------
# Dapr flow dispatch scope
# ---------------------------------------------------------------------------

_DAPR_FLOW_DISPATCH_ALLOWED: ContextVar[bool] = ContextVar(
    "kitaru_dapr_flow_dispatch_allowed",
    default=False,
)


def _is_dapr_flow_dispatch_allowed() -> bool:
    return _DAPR_FLOW_DISPATCH_ALLOWED.get()


@contextmanager
def _dapr_flow_dispatch_scope() -> Iterator[None]:
    """Allow checkpoint dispatch from a Dapr orchestrator flow body."""
    token = _DAPR_FLOW_DISPATCH_ALLOWED.set(True)
    try:
        yield
    finally:
        _DAPR_FLOW_DISPATCH_ALLOWED.reset(token)
