"""Dapr workflow runtime host for the experimental Kitaru engine."""

from __future__ import annotations

import atexit
import threading
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from kitaru.engines.dapr._dependencies import require_dapr_workflow_sdk
from kitaru.engines.dapr.backend import (
    DaprCheckpointActivityRequest,
    DaprCheckpointDefinition,
    DaprExecutionEngineBackend,
    DaprFlowDefinition,
    DaprRetryPolicySpec,
    _build_failure_record,
    _step_output_artifact_id,
)
from kitaru.engines.dapr.interpreter import (
    _MAX_INTERPRETER_ITERATIONS,
    DaprOrchestratorSession,
    FlowSuspendRequested,
    ReplaySeed,
    _CheckpointSuspendPayload,
    _FutureJoinSuspendPayload,
    _ScheduledFuture,
    _WaitSuspendPayload,
    run_flow_iteration,
)
from kitaru.engines.dapr.models import (
    DAPR_ABORT_RESOLUTION_KEY,
    DAPR_ABORT_RESOLUTION_VALUE,
    DAPR_METADATA_NAMESPACE,
    DAPR_TRANSPORT_EVENT_KEY,
    DAPR_TRANSPORT_MAX_BYTES,
    FLOW_RESULT_ARTIFACT_ID_KEY,
    FLOW_RESULT_ARTIFACT_NAME,
    FLOW_RESULT_SAVE_TYPE,
    INTERNAL_ARTIFACT_FLAG,
    ArtifactRecord,
    CheckpointActivityPayload,
    CheckpointActivityResultPayload,
    FailureRecord,
    FinalizeExecutionPayload,
    WaitRecord,
    WorkflowStartPayload,
    _iso_to_dt,
    decode_transport_value,
    encode_transport_value,
)
from kitaru.errors import KitaruBackendError, KitaruRuntimeError

if TYPE_CHECKING:
    from dapr.ext.workflow import DaprWorkflowContext

_INTERNAL_FINALIZE_ACTIVITY = "__kitaru_finalize"


def _replace_execution_status(
    backend: DaprExecutionEngineBackend,
    *,
    exec_id: str,
    status: str,
    status_reason: str | None = None,
) -> None:
    """Update execution status fields with an idempotent ledger replace."""
    store = backend.get_store()
    record = store.get_execution(exec_id)
    now = datetime.now(UTC)
    updated = replace(
        record,
        status=status,
        status_reason=status_reason,
        updated_at=now,
    )
    store.replace_execution(exec_id, updated)


def _replace_wait(
    backend: DaprExecutionEngineBackend,
    *,
    exec_id: str,
    wait: WaitRecord,
) -> None:
    backend.get_store().upsert_wait(exec_id, wait)


def _to_retry_policy(spec: DaprRetryPolicySpec | None) -> Any:
    if spec is None:
        return None
    require_dapr_workflow_sdk()
    from dapr.ext import workflow

    return workflow.RetryPolicy(
        first_retry_interval=timedelta(seconds=spec.initial_retry_interval_seconds),
        max_number_of_attempts=spec.max_attempts,
        backoff_coefficient=spec.backoff_coefficient,
    )


def _normalize_resolution_event(data: Any) -> tuple[bool, Any]:
    """Return ``(is_abort, resolved_value)`` from a Dapr external event payload."""
    if (
        isinstance(data, Mapping)
        and data.get(DAPR_ABORT_RESOLUTION_KEY) == DAPR_ABORT_RESOLUTION_VALUE
    ):
        return True, data
    if isinstance(data, Mapping) and DAPR_TRANSPORT_EVENT_KEY in data:
        return False, decode_transport_value(data[DAPR_TRANSPORT_EVENT_KEY])
    return False, data


def _build_replay_seed(payload: Mapping[str, Any]) -> ReplaySeed | None:
    raw = payload.get("replay_seed")
    if not isinstance(raw, Mapping):
        return None
    source_exec_id = raw.get("source_exec_id")
    seeded_results = raw.get("seeded_results", {})
    if not isinstance(source_exec_id, str) or not isinstance(seeded_results, Mapping):
        return None
    return ReplaySeed(
        source_exec_id=source_exec_id,
        seeded_results=dict(seeded_results),
    )


class DaprWorkflowClientBridge:
    """Small adapter that normalizes the Dapr workflow client surface."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._lock = threading.RLock()

    def get_workflow_state(self, instance_id: str) -> Any | None:
        with self._lock:
            try:
                return self._client.get_workflow_state(
                    instance_id, fetch_payloads=False
                )
            except LookupError:
                return None
            except Exception as exc:
                text = str(exc).lower()
                if "not found" in text or "no workflow" in text:
                    return None
                raise

    def raise_workflow_event(
        self, instance_id: str, event_name: str, data: Any
    ) -> None:
        with self._lock:
            self._client.raise_workflow_event(instance_id, event_name, data=data)

    def terminate_workflow(self, instance_id: str) -> None:
        with self._lock:
            self._client.terminate_workflow(instance_id)

    def resume_workflow(self, instance_id: str) -> None:
        with self._lock:
            self._client.resume_workflow(instance_id)

    def schedule_new_workflow(
        self,
        workflow_name: str,
        *,
        input: Any,
        instance_id: str | None = None,
    ) -> str:
        with self._lock:
            # The real DaprWorkflowClient.schedule_new_workflow() may not
            # accept string workflow names (older SDK versions). Use the
            # underlying orchestration client when available, otherwise
            # fall back to schedule_new_workflow() for fakes/test doubles.
            inner = getattr(self._client, "_DaprWorkflowClient__obj", None)
            if inner is not None:
                scheduled_id = inner.schedule_new_orchestration(
                    workflow_name,
                    input=input,
                    instance_id=instance_id,
                )
            else:
                scheduled_id = self._client.schedule_new_workflow(
                    workflow_name,
                    input=input,
                    instance_id=instance_id,
                )
        if instance_id is not None and scheduled_id != instance_id:
            raise KitaruBackendError(
                "Dapr returned a different workflow instance ID than requested: "
                f"requested {instance_id!r}, got {scheduled_id!r}."
            )
        return scheduled_id


def create_dapr_workflow_client() -> DaprWorkflowClientBridge:
    """Create a Dapr workflow client without starting a workflow runtime."""
    require_dapr_workflow_sdk()
    from dapr.ext import workflow

    return DaprWorkflowClientBridge(workflow.DaprWorkflowClient())


class DaprRuntimeHost:
    """Owns the process-local Dapr workflow runtime and workflow client."""

    def __init__(self, *, backend: DaprExecutionEngineBackend) -> None:
        self._backend = backend
        self._runtime: Any | None = None
        self._workflow_client: DaprWorkflowClientBridge | None = None
        self._started = False
        self._registered_flows: set[str] = set()
        self._registered_activities: set[str] = set()
        self._lock = threading.RLock()
        self._atexit_registered = False
        self._finalize_activity = self._make_finalize_activity()

    def ensure_started(self) -> None:
        """Start the Dapr workflow runtime lazily and idempotently."""
        with self._lock:
            if self._started:
                return
            require_dapr_workflow_sdk()
            from dapr.ext import workflow

            runtime = workflow.WorkflowRuntime()
            self._runtime = runtime
            self._register_known_definitions_locked()
            runtime.start()
            self._workflow_client = DaprWorkflowClientBridge(
                workflow.DaprWorkflowClient()
            )
            self._started = True
            if not self._atexit_registered:
                atexit.register(self.shutdown)
                self._atexit_registered = True

    def shutdown(self) -> None:
        """Shut down the Dapr workflow runtime if it was started."""
        with self._lock:
            if self._runtime is None or not self._started:
                return
            self._runtime.shutdown()
            self._runtime = None
            self._workflow_client = None
            self._started = False
            self._registered_flows.clear()
            self._registered_activities.clear()

    def workflow_client_if_started(self) -> DaprWorkflowClientBridge | None:
        with self._lock:
            return self._workflow_client if self._started else None

    def ensure_flow_registered(self, name: str, definition: DaprFlowDefinition) -> None:
        with self._lock:
            if name in self._registered_flows:
                return
            if self._runtime is None:
                return
            self._register_flow_locked(name, definition)

    def ensure_checkpoint_registered(
        self,
        name: str,
        definition: DaprCheckpointDefinition,
    ) -> None:
        with self._lock:
            if name in self._registered_activities:
                return
            if self._runtime is None:
                return
            self._register_checkpoint_locked(name, definition)

    def schedule_execution(self, *, workflow_name: str, exec_id: str) -> None:
        self.ensure_started()
        with self._lock:
            if self._workflow_client is None:
                raise KitaruRuntimeError("Dapr workflow client is not available.")
            self._workflow_client.schedule_new_workflow(
                workflow_name,
                input=WorkflowStartPayload(exec_id=exec_id).to_dict(),
                instance_id=exec_id,
            )

    def _register_known_definitions_locked(self) -> None:
        if self._runtime is None:
            raise KitaruRuntimeError("Runtime host has no Dapr WorkflowRuntime.")
        for name, definition in self._backend.get_flow_definitions().items():
            if name not in self._registered_flows:
                self._register_flow_locked(name, definition)
        for name, definition in self._backend.get_checkpoint_definitions().items():
            if name not in self._registered_activities:
                self._register_checkpoint_locked(name, definition)
        if _INTERNAL_FINALIZE_ACTIVITY not in self._registered_activities:
            self._runtime.register_activity(
                self._finalize_activity,
                name=_INTERNAL_FINALIZE_ACTIVITY,
            )
            self._registered_activities.add(_INTERNAL_FINALIZE_ACTIVITY)

    def _register_flow_locked(self, name: str, definition: DaprFlowDefinition) -> None:
        assert self._runtime is not None
        try:
            self._runtime.register_workflow(
                self._make_workflow_body(definition),
                name=name,
            )
        except Exception as exc:
            raise KitaruRuntimeError(
                "Failed to register Dapr workflow after the runtime was initialized. "
                "Import all flow and checkpoint modules before the first Dapr run(). "
                f"Workflow: {name!r}. Error: {exc}"
            ) from exc
        self._registered_flows.add(name)

    def _register_checkpoint_locked(
        self,
        name: str,
        definition: DaprCheckpointDefinition,
    ) -> None:
        assert self._runtime is not None
        try:
            self._runtime.register_activity(
                self._make_activity_shim(definition),
                name=name,
            )
        except Exception as exc:
            raise KitaruRuntimeError(
                "Failed to register Dapr checkpoint activity after the runtime "
                "was initialized. "
                "Import all flow and checkpoint modules before the first Dapr run(). "
                f"Activity: {name!r}. Error: {exc}"
            ) from exc
        self._registered_activities.add(name)

    def _make_activity_shim(
        self,
        definition: DaprCheckpointDefinition,
    ) -> Callable[[Any, dict[str, Any]], dict[str, Any]]:
        activity = definition.make_activity_callable(self._backend.get_store)

        def _activity(_ctx: Any, payload: dict[str, Any]) -> dict[str, Any]:
            request_payload = CheckpointActivityPayload.from_dict(payload)
            request = DaprCheckpointActivityRequest(
                exec_id=request_payload.exec_id,
                flow_name=request_payload.flow_name,
                call_id=request_payload.call_id,
                invocation_id=request_payload.invocation_id,
                checkpoint_name=request_payload.checkpoint_name,
                checkpoint_type=request_payload.checkpoint_type,
                args=tuple(decode_transport_value(request_payload.args)),
                kwargs=dict(decode_transport_value(request_payload.kwargs)),
                attempt_number=request_payload.attempt_number,
                metadata=dict(request_payload.metadata),
                original_call_id=request_payload.original_call_id,
                upstream_call_ids=tuple(request_payload.upstream_call_ids),
            )
            activity(request)
            return CheckpointActivityResultPayload(
                call_id=request.call_id,
                output_artifact_id=_step_output_artifact_id(request.call_id),
            ).to_dict()

        _activity.__name__ = definition.registration_name
        _activity.__qualname__ = definition.registration_name
        return _activity

    def _make_workflow_body(
        self,
        definition: DaprFlowDefinition,
    ) -> Callable[[DaprWorkflowContext, dict[str, Any]], Any]:
        def _workflow_body(ctx: DaprWorkflowContext, payload: dict[str, Any]) -> Any:
            start = WorkflowStartPayload.from_dict(payload)
            store = self._backend.get_store()
            input_payload = store.load_execution_input(start.exec_id)
            if not isinstance(input_payload, Mapping):
                raise KitaruRuntimeError(
                    f"Execution input for {start.exec_id!r} is not a mapping payload."
                )
            input_data = dict(input_payload)
            record = store.get_execution(start.exec_id)
            session = DaprOrchestratorSession(
                exec_id=start.exec_id,
                flow_name=record.flow_name,
                store=store,
                replay_seed=_build_replay_seed(input_data),
                step_input_overrides=input_data.get("step_input_overrides"),
            )
            _replace_execution_status(
                self._backend,
                exec_id=start.exec_id,
                status="running",
                status_reason=None,
            )
            args = tuple(input_data.get("args", ()))
            kwargs = dict(input_data.get("kwargs", {}))

            try:
                for _ in range(_MAX_INTERPRETER_ITERATIONS):
                    outcome = run_flow_iteration(
                        flow_func=definition.source_object,
                        args=args,
                        kwargs=kwargs,
                        session=session,
                    )
                    if outcome.buffered_metadata:
                        merged: dict[str, Any] = {}
                        for meta in outcome.buffered_metadata:
                            merged.update(meta)
                        store.merge_execution_metadata(start.exec_id, merged)

                    if outcome.kind == "completed":
                        yield ctx.call_activity(
                            _INTERNAL_FINALIZE_ACTIVITY,
                            input=self._build_finalize_payload(
                                exec_id=start.exec_id,
                                result=outcome.result,
                            ).to_dict(),
                        )
                        return {"exec_id": start.exec_id}

                    if outcome.kind == "unresolved_futures":
                        yield from self._resolve_future_specs(
                            ctx=ctx,
                            exec_id=start.exec_id,
                            flow_name=record.flow_name,
                            session=session,
                            specs=list(outcome.unresolved),
                        )
                        continue

                    assert outcome.suspend is not None
                    yield from self._handle_suspend(
                        ctx=ctx,
                        exec_id=start.exec_id,
                        flow_name=record.flow_name,
                        session=session,
                        suspend=outcome.suspend,
                    )
                raise KitaruRuntimeError(
                    "Dapr workflow host exceeded 500 interpreter iterations."
                )
            except BaseException as exc:
                yield ctx.call_activity(
                    _INTERNAL_FINALIZE_ACTIVITY,
                    input=FinalizeExecutionPayload(
                        exec_id=start.exec_id,
                        status="failed",
                        ended_at=datetime.now(UTC).isoformat(),
                        failure=_build_failure_record(exc).to_dict(),
                    ).to_dict(),
                )
                raise

        _workflow_body.__name__ = definition.source_object.__name__
        _workflow_body.__qualname__ = definition.source_object.__qualname__
        return _workflow_body

    def _handle_suspend(
        self,
        *,
        ctx: DaprWorkflowContext,
        exec_id: str,
        flow_name: str | None,
        session: DaprOrchestratorSession,
        suspend: FlowSuspendRequested,
    ) -> Any:
        if suspend.kind == "checkpoint":
            payload = suspend.payload
            assert isinstance(payload, _CheckpointSuspendPayload)
            result_payload = yield ctx.call_activity(
                payload.checkpoint_name,
                input=self._checkpoint_payload(
                    exec_id=exec_id,
                    flow_name=flow_name,
                    spec=_ScheduledFuture(
                        call_id=payload.call_id,
                        checkpoint_name=payload.checkpoint_name,
                        checkpoint_type=payload.checkpoint_type,
                        args=payload.args,
                        kwargs=payload.kwargs,
                        retry_policy=payload.retry_policy,
                    ),
                ).to_dict(),
                retry_policy=_to_retry_policy(payload.retry_policy),
            )
            self._record_activity_resolution(
                exec_id=exec_id,
                session=session,
                result_payload=result_payload,
            )
            return

        if suspend.kind == "future_join":
            payload = suspend.payload
            assert isinstance(payload, _FutureJoinSuspendPayload)
            specs: list[_ScheduledFuture] = []
            for call_id in payload.call_ids:
                if call_id in session.resolved_values:
                    continue
                spec = session.get_scheduled_future(call_id)
                if spec is None:
                    raise KitaruRuntimeError(
                        f"Future join requested for unknown call_id {call_id!r}."
                    )
                specs.append(spec)
            yield from self._resolve_future_specs(
                ctx=ctx,
                exec_id=exec_id,
                flow_name=flow_name,
                session=session,
                specs=specs,
            )
            return

        if suspend.kind == "wait":
            payload = suspend.payload
            assert isinstance(payload, _WaitSuspendPayload)
            now = datetime.now(UTC)
            wait = WaitRecord(
                wait_id=payload.wait_id,
                name=payload.name,
                status="pending",
                question=payload.question,
                schema=payload.schema,
                metadata=dict(payload.metadata) if payload.metadata else {},
                entered_at=now,
            )
            _replace_wait(self._backend, exec_id=exec_id, wait=wait)
            _replace_execution_status(
                self._backend,
                exec_id=exec_id,
                status="suspended",
                status_reason=None,
            )
            data = yield ctx.wait_for_external_event(payload.wait_id)
            is_abort, resolved_value = _normalize_resolution_event(data)
            resolved_wait = replace(
                wait,
                status="aborted" if is_abort else "resolved",
                resolved_at=datetime.now(UTC),
            )
            _replace_wait(self._backend, exec_id=exec_id, wait=resolved_wait)
            _replace_execution_status(
                self._backend,
                exec_id=exec_id,
                status="running",
                status_reason=None,
            )
            session.record_wait_resolution(payload.wait_id, resolved_value)
            return

        raise KitaruRuntimeError(f"Unknown Dapr suspend kind {suspend.kind!r}.")

    def _resolve_future_specs(
        self,
        *,
        ctx: DaprWorkflowContext,
        exec_id: str,
        flow_name: str | None,
        session: DaprOrchestratorSession,
        specs: list[_ScheduledFuture],
    ) -> Any:
        if not specs:
            return
        require_dapr_workflow_sdk()
        from dapr.ext import workflow

        tasks = [
            ctx.call_activity(
                spec.checkpoint_name,
                input=self._checkpoint_payload(
                    exec_id=exec_id,
                    flow_name=flow_name,
                    spec=spec,
                ).to_dict(),
                retry_policy=_to_retry_policy(spec.retry_policy),
            )
            for spec in specs
        ]
        result_payloads = yield workflow.when_all(tasks)
        for spec, result_payload in zip(specs, result_payloads, strict=True):
            self._record_activity_resolution(
                exec_id=exec_id,
                session=session,
                result_payload=result_payload,
                expected_call_id=spec.call_id,
            )

    def _checkpoint_payload(
        self,
        *,
        exec_id: str,
        flow_name: str | None,
        spec: _ScheduledFuture,
    ) -> CheckpointActivityPayload:
        return CheckpointActivityPayload(
            exec_id=exec_id,
            flow_name=flow_name,
            call_id=spec.call_id,
            invocation_id=spec.call_id,
            checkpoint_name=spec.checkpoint_name,
            checkpoint_type=spec.checkpoint_type,
            args=encode_transport_value(
                list(spec.args),
                max_bytes=DAPR_TRANSPORT_MAX_BYTES,
                label=f"checkpoint args for {spec.checkpoint_name}",
            ),
            kwargs=encode_transport_value(
                spec.kwargs,
                max_bytes=DAPR_TRANSPORT_MAX_BYTES,
                label=f"checkpoint kwargs for {spec.checkpoint_name}",
            ),
        )

    def _record_activity_resolution(
        self,
        *,
        exec_id: str,
        session: DaprOrchestratorSession,
        result_payload: Any,
        expected_call_id: str | None = None,
    ) -> None:
        if not isinstance(result_payload, Mapping):
            raise KitaruRuntimeError(
                "Checkpoint activity for execution "
                f"{exec_id!r} returned a non-mapping payload."
            )
        payload = CheckpointActivityResultPayload.from_dict(result_payload)
        if expected_call_id is not None and payload.call_id != expected_call_id:
            raise KitaruRuntimeError(
                "Checkpoint activity resolution order drifted: "
                f"expected {expected_call_id!r}, got {payload.call_id!r}."
            )
        _, value = self._backend.get_store().load_artifact(payload.output_artifact_id)
        session.record_resolution(payload.call_id, value)

    def _build_finalize_payload(
        self,
        *,
        exec_id: str,
        result: Any,
    ) -> FinalizeExecutionPayload:
        return FinalizeExecutionPayload(
            exec_id=exec_id,
            status="completed",
            ended_at=datetime.now(UTC).isoformat(),
            result=encode_transport_value(
                result,
                max_bytes=DAPR_TRANSPORT_MAX_BYTES,
                label=f"flow result for {exec_id}",
            ),
        )

    def _make_finalize_activity(self) -> Callable[[Any, dict[str, Any]], None]:
        def _finalize(_ctx: Any, payload: dict[str, Any]) -> None:
            finalize = FinalizeExecutionPayload.from_dict(payload)
            store = self._backend.get_store()
            record = store.get_execution(finalize.exec_id)
            ended_at = _iso_to_dt(finalize.ended_at) or datetime.now(UTC)

            if record.status == "terminated":
                if record.ended_at is None:
                    store.replace_execution(
                        finalize.exec_id,
                        replace(record, ended_at=ended_at, updated_at=ended_at),
                    )
                return

            if finalize.status == "completed":
                if finalize.result is None:
                    raise KitaruRuntimeError(
                        "Finalize payload for "
                        f"{finalize.exec_id!r} is missing a result envelope."
                    )
                result_artifact_id = f"{finalize.exec_id}:flow_result"
                artifact = ArtifactRecord(
                    artifact_id=result_artifact_id,
                    name=FLOW_RESULT_ARTIFACT_NAME,
                    save_type=FLOW_RESULT_SAVE_TYPE,
                    metadata={INTERNAL_ARTIFACT_FLAG: True},
                    exec_id=finalize.exec_id,
                )
                store.store_artifact(
                    finalize.exec_id,
                    artifact,
                    decode_transport_value(finalize.result),
                )
                store.merge_execution_metadata(
                    finalize.exec_id,
                    {
                        DAPR_METADATA_NAMESPACE: {
                            FLOW_RESULT_ARTIFACT_ID_KEY: result_artifact_id,
                        }
                    },
                )
                record = store.get_execution(finalize.exec_id)
                store.replace_execution(
                    finalize.exec_id,
                    replace(
                        record,
                        status="completed",
                        ended_at=ended_at,
                        updated_at=ended_at,
                        failure=None,
                        status_reason=None,
                    ),
                )
                return

            failure = (
                FailureRecord.from_dict(finalize.failure)
                if finalize.failure is not None
                else None
            )
            store.replace_execution(
                finalize.exec_id,
                replace(
                    record,
                    status="failed",
                    ended_at=ended_at,
                    updated_at=ended_at,
                    failure=failure,
                    status_reason=(
                        failure.message if failure is not None else record.status_reason
                    ),
                ),
            )

        _finalize.__name__ = _INTERNAL_FINALIZE_ACTIVITY
        _finalize.__qualname__ = _INTERNAL_FINALIZE_ACTIVITY
        return _finalize
