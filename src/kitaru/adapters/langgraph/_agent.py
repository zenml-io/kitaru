"""Public runner wrapper for LangGraph graph-call checkpointing."""

import inspect
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from functools import lru_cache
from importlib import metadata
from typing import Any, cast

from kitaru._llm_usage import (
    build_usage_record,
    estimate_calculated_cost_usd,
    log_usage_record,
    token_usage_from_mapping,
)
from kitaru.adapters._result_identity import canonicalize_result_model
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from ._constants import LANGGRAPH_CONFIG_CHECKPOINTER_KEY
from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._policy import (
    LangGraphCallCheckpointPolicy,
    LangGraphCapturePolicy,
    LangGraphDurabilityPolicy,
    LangGraphStreamPolicy,
    resolve_summary_checkpoint_config,
)
from ._serialization import redact_config, to_cache_identity, to_json_safe
from ._streaming import (
    STREAM_VERSION,
    SUPPORTED_STREAM_MODES,
    LangGraphStreamOptions,
    LangGraphStreamPublisher,
    LangGraphStreamStats,
    resolve_stream_options,
    stream_part_mode,
    stream_part_output_candidate,
)
from ._tracking import EventTracker, tracker_scope
from ._types import (
    LangGraphInterruptSummary,
    LangGraphPendingState,
    LangGraphRunRequest,
    LangGraphRunResult,
    LangGraphStateSummary,
    LangGraphStreamMode,
    LangGraphUsageSummary,
)
from ._utils import (
    CheckpointConfig,
    GraphCheckpointStrategy,
    checkpoint_cache_key,
    merge_config,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
)


@lru_cache(maxsize=1)
def langgraph_version() -> str:
    """Resolve installed LangGraph version lazily."""
    try:
        return metadata.version("langgraph")
    except metadata.PackageNotFoundError:
        return "unknown"


class KitaruGraphRunner:
    """Wrap a LangGraph-compatible runnable with one Kitaru graph-call boundary.

    LangGraph still owns graph-internal replay, checkpointing, state, stores, and
    interrupts. Kitaru records and checkpoints the outer ``graph.invoke(...)`` /
    ``graph.ainvoke(...)`` call so a Kitaru flow can restart the same graph
    thread instead of silently creating a new one.
    """

    def __init__(
        self,
        graph: Any,
        *,
        name: str | None = None,
        checkpoint_strategy: GraphCheckpointStrategy = "graph_call",
        capture: LangGraphCapturePolicy | None = None,
        durability: LangGraphDurabilityPolicy | None = None,
        stream_policy: LangGraphStreamPolicy | None = None,
        run_checkpoint_config: CheckpointConfig | None = None,
        call_checkpoint_policy: LangGraphCallCheckpointPolicy | None = None,
        config_factory: Callable[[LangGraphRunRequest], dict[str, Any]] | None = None,
        context_factory: Callable[[LangGraphRunRequest], Any] | None = None,
        cost_calculator: Callable[[LangGraphUsageSummary], float | None] | None = None,
    ) -> None:
        self._graph = graph
        resolved_name = name or getattr(graph, "name", None)
        if not isinstance(resolved_name, str) or not resolved_name.strip():
            raise KitaruUsageError(
                "KitaruGraphRunner requires a stable `name`; pass `name=` or "
                "wrap a graph object that exposes a stable `.name`."
            )
        self._name = resolved_name
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._validate_strategy_config(
            run_checkpoint_config=run_checkpoint_config,
            call_checkpoint_policy=call_checkpoint_policy,
        )
        self._capture = capture or LangGraphCapturePolicy()
        self._capture_summary = self._capture.model_dump(mode="json")
        self._durability = durability or LangGraphDurabilityPolicy()
        self._stream_policy = stream_policy or LangGraphStreamPolicy()
        self._call_checkpoint_policy = (
            call_checkpoint_policy or LangGraphCallCheckpointPolicy()
        )
        self._run_checkpoint_config: CheckpointConfig = validate_checkpoint_config(
            run_checkpoint_config,
            context="run_checkpoint_config",
        ) or cast(CheckpointConfig, {})
        self._config_factory = config_factory
        self._context_factory = context_factory
        self._cost_calculator = cost_calculator
        self._checkpointer_saver_value = self._resolve_checkpointer_saver()
        self._checkpointer_label_value = self._resolve_checkpointer_label()
        self._store_label_value = self._resolve_store_label()
        self._graph_identity_value = self._build_graph_identity()
        self._method_allowed_kwargs: dict[str, set[str] | None] = {}

        track(
            AnalyticsEvent.LANGGRAPH_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "has_checkpointer": self._checkpointer_label_value is not None,
                "has_store": self._store_label_value is not None,
                "durability": self._durability.mode,
            },
        )

    @property
    def graph(self) -> Any:
        return self._graph

    @property
    def name(self) -> str:
        return self._name

    @property
    def checkpoint_strategy(self) -> GraphCheckpointStrategy:
        return self._checkpoint_strategy

    @property
    def capture(self) -> LangGraphCapturePolicy:
        return self._capture

    @property
    def durability(self) -> LangGraphDurabilityPolicy:
        return self._durability

    @property
    def stream_policy(self) -> LangGraphStreamPolicy:
        return self._stream_policy

    @property
    def call_checkpoint_policy(self) -> LangGraphCallCheckpointPolicy:
        return self._call_checkpoint_policy

    def invoke(self, request: LangGraphRunRequest) -> LangGraphRunResult:
        """Invoke the wrapped graph synchronously."""
        self._validate_request(request, required_method="invoke")

        config = self._prepared_config(request)
        self._validate_checkpointer_requirement(config)

        def _body() -> LangGraphRunResult:
            return self._invoke_graph_sync(request, config=config)

        if self._checkpoint_strategy == "calls":
            result = _body()
        elif is_inside_flow() and not is_inside_checkpoint():
            result = run_sync_in_checkpoint(
                config=self._graph_call_checkpoint_config(),
                step_name=f"{self._name}_langgraph_call",
                body=_body,
                cache_key=self._graph_call_cache_key(request),
            )
        else:
            result = _body()
        result = canonicalize_result_model(result, LangGraphRunResult)
        self._track_result("invoke", result, request=request, config=config)
        return result

    async def ainvoke(self, request: LangGraphRunRequest) -> LangGraphRunResult:
        """Invoke the wrapped graph asynchronously when the graph supports it."""
        self._validate_request(request, required_method="ainvoke")

        config = self._prepared_config(request)
        self._validate_checkpointer_requirement(config)

        async def _body() -> LangGraphRunResult:
            return await self._invoke_graph_async(request, config=config)

        if self._checkpoint_strategy == "calls":
            result = await _body()
        elif is_inside_flow() and not is_inside_checkpoint():
            result = await run_async_in_checkpoint(
                config=self._graph_call_checkpoint_config(),
                step_name=f"{self._name}_langgraph_call",
                body=_body,
                cache_key=self._graph_call_cache_key(request),
            )
        else:
            result = await _body()
        result = canonicalize_result_model(result, LangGraphRunResult)
        self._track_result("ainvoke", result, request=request, config=config)
        return result

    def stream(
        self,
        request: LangGraphRunRequest,
        *,
        stream_mode: LangGraphStreamMode | Sequence[LangGraphStreamMode] | None = None,
        subgraphs: bool = False,
    ) -> LangGraphRunResult:
        """Run the graph synchronously and forward best-effort live stream events."""
        self._require_streaming_graph_call()
        self._validate_request(request, required_method="stream")
        options = resolve_stream_options(
            stream_mode,
            policy=self._stream_policy,
            subgraphs=subgraphs,
        )
        config = self._prepared_config(request)
        self._validate_checkpointer_requirement(config)

        def _body() -> LangGraphRunResult:
            return self._stream_graph_sync(request, config=config, options=options)

        if is_inside_flow() and not is_inside_checkpoint():
            result = run_sync_in_checkpoint(
                config=self._graph_call_checkpoint_config(),
                step_name=f"{self._name}_langgraph_call",
                body=_body,
                cache_key=self._graph_call_cache_key(
                    request,
                    surface="stream",
                    stream_identity=self._stream_cache_identity(
                        options,
                        method_name="stream",
                    ),
                ),
            )
        else:
            result = _body()
        result = canonicalize_result_model(result, LangGraphRunResult)
        self._track_result("stream", result, request=request, config=config)
        return result

    async def astream(
        self,
        request: LangGraphRunRequest,
        *,
        stream_mode: LangGraphStreamMode | Sequence[LangGraphStreamMode] | None = None,
        subgraphs: bool = False,
    ) -> LangGraphRunResult:
        """Run the graph asynchronously and forward best-effort live stream events."""
        self._require_streaming_graph_call()
        self._validate_request(request, required_method="astream")
        options = resolve_stream_options(
            stream_mode,
            policy=self._stream_policy,
            subgraphs=subgraphs,
        )
        config = self._prepared_config(request)
        self._validate_checkpointer_requirement(config)

        async def _body() -> LangGraphRunResult:
            return await self._stream_graph_async(
                request, config=config, options=options
            )

        if is_inside_flow() and not is_inside_checkpoint():
            result = await run_async_in_checkpoint(
                config=self._graph_call_checkpoint_config(),
                step_name=f"{self._name}_langgraph_call",
                body=_body,
                cache_key=self._graph_call_cache_key(
                    request,
                    surface="stream",
                    stream_identity=self._stream_cache_identity(
                        options,
                        method_name="astream",
                    ),
                ),
            )
        else:
            result = await _body()
        result = canonicalize_result_model(result, LangGraphRunResult)
        self._track_result("astream", result, request=request, config=config)
        return result

    def _invoke_graph_sync(
        self, request: LangGraphRunRequest, *, config: dict[str, Any]
    ) -> LangGraphRunResult:
        context = self._prepared_context(request)
        kwargs = self._graph_call_kwargs(
            request,
            config=config,
            context=context,
            method_name="invoke",
        )
        input_or_command = request.input if request.kind == "start" else request.command
        warnings = self._checkpointer_warnings(config=config)

        with tracker_scope(
            self._name,
            call_checkpoint_policy=(
                self._effective_call_checkpoint_policy()
                if self._checkpoint_strategy == "calls"
                else None
            ),
            capture=self._capture if self._checkpoint_strategy == "calls" else None,
        ) as tracker:
            tracker.record(
                "graph_call_started",
                metadata=self._safe_event_metadata(request, config=config),
            )
            try:
                output = self._graph.invoke(input_or_command, config=config, **kwargs)
            except Exception as exc:
                self._handle_graph_call_failure(
                    error=exc,
                    tracker=tracker,
                    request=request,
                    config=config,
                    context=context,
                    warnings=warnings,
                )
                raise
            result = self._build_result(
                output,
                request=request,
                config=config,
                tracker=tracker,
                warnings=warnings,
            )
            self._persist_tracker(
                tracker,
                result,
                request=request,
                config=config,
                context=context,
            )
            return result

    async def _invoke_graph_async(
        self, request: LangGraphRunRequest, *, config: dict[str, Any]
    ) -> LangGraphRunResult:
        ainvoke = getattr(self._graph, "ainvoke", None)
        if not callable(ainvoke):
            raise KitaruUsageError(
                "Wrapped LangGraph object does not expose `ainvoke(...)`. Use "
                "`invoke(...)` or wrap a graph that supports async invocation."
            )

        context = self._prepared_context(request)
        kwargs = self._graph_call_kwargs(
            request,
            config=config,
            context=context,
            method_name="ainvoke",
        )
        input_or_command = request.input if request.kind == "start" else request.command
        warnings = self._checkpointer_warnings(config=config)

        with tracker_scope(
            self._name,
            call_checkpoint_policy=(
                self._effective_call_checkpoint_policy()
                if self._checkpoint_strategy == "calls"
                else None
            ),
            capture=self._capture if self._checkpoint_strategy == "calls" else None,
        ) as tracker:
            tracker.record(
                "graph_call_started",
                metadata=self._safe_event_metadata(request, config=config),
            )
            try:
                output = await ainvoke(input_or_command, config=config, **kwargs)
            except Exception as exc:
                self._handle_graph_call_failure(
                    error=exc,
                    tracker=tracker,
                    request=request,
                    config=config,
                    context=context,
                    warnings=warnings,
                )
                raise
            result = self._build_result(
                output,
                request=request,
                config=config,
                tracker=tracker,
                warnings=warnings,
            )
            self._persist_tracker(
                tracker,
                result,
                request=request,
                config=config,
                context=context,
            )
            return result

    def _stream_graph_sync(
        self,
        request: LangGraphRunRequest,
        *,
        config: dict[str, Any],
        options: LangGraphStreamOptions,
    ) -> LangGraphRunResult:
        context = self._prepared_context(request)
        kwargs = self._graph_stream_kwargs(
            request,
            config=config,
            context=context,
            method_name="stream",
            options=options,
        )
        input_or_command = request.input if request.kind == "start" else request.command
        warnings = self._checkpointer_warnings(config=config)
        publisher, stats = self._new_stream_publisher(request, options)

        with tracker_scope(self._name) as tracker:
            self._record_stream_call_started(
                tracker,
                request=request,
                config=config,
                options=options,
            )
            publisher.started()
            try:
                output = self._drain_stream_parts(
                    self._graph.stream(input_or_command, config=config, **kwargs),
                    options=options,
                    publisher=publisher,
                    stats=stats,
                )
                result = self._build_result(
                    output,
                    request=request,
                    config=config,
                    tracker=tracker,
                    warnings=warnings,
                )
            except Exception as exc:
                publisher.failed(exc, stats=stats)
                self._handle_graph_call_failure(
                    error=exc,
                    tracker=tracker,
                    request=request,
                    config=config,
                    context=context,
                    warnings=warnings,
                    stream_summary=self._stream_run_summary(options, stats),
                )
                raise
            try:
                self._persist_tracker(
                    tracker,
                    result,
                    request=request,
                    config=config,
                    context=context,
                    stream_summary=self._stream_run_summary(options, stats),
                )
            except Exception as exc:
                publisher.failed(exc, stats=stats)
                raise
            publisher.completed(status=result.status, stats=stats)
            return result

    async def _stream_graph_async(
        self,
        request: LangGraphRunRequest,
        *,
        config: dict[str, Any],
        options: LangGraphStreamOptions,
    ) -> LangGraphRunResult:
        astream = getattr(self._graph, "astream", None)
        if not callable(astream):
            raise KitaruUsageError(
                "Wrapped LangGraph object does not expose `astream(...)`. Use "
                "`stream(...)` or wrap a graph that supports async streaming."
            )

        context = self._prepared_context(request)
        kwargs = self._graph_stream_kwargs(
            request,
            config=config,
            context=context,
            method_name="astream",
            options=options,
        )
        input_or_command = request.input if request.kind == "start" else request.command
        warnings = self._checkpointer_warnings(config=config)
        publisher, stats = self._new_stream_publisher(request, options)

        with tracker_scope(self._name) as tracker:
            self._record_stream_call_started(
                tracker,
                request=request,
                config=config,
                options=options,
            )
            publisher.started()
            try:
                output = await self._drain_async_stream_parts(
                    astream(input_or_command, config=config, **kwargs),
                    options=options,
                    publisher=publisher,
                    stats=stats,
                )
                result = self._build_result(
                    output,
                    request=request,
                    config=config,
                    tracker=tracker,
                    warnings=warnings,
                )
            except Exception as exc:
                publisher.failed(exc, stats=stats)
                self._handle_graph_call_failure(
                    error=exc,
                    tracker=tracker,
                    request=request,
                    config=config,
                    context=context,
                    warnings=warnings,
                    stream_summary=self._stream_run_summary(options, stats),
                )
                raise
            try:
                self._persist_tracker(
                    tracker,
                    result,
                    request=request,
                    config=config,
                    context=context,
                    stream_summary=self._stream_run_summary(options, stats),
                )
            except Exception as exc:
                publisher.failed(exc, stats=stats)
                raise
            publisher.completed(status=result.status, stats=stats)
            return result

    def _new_stream_publisher(
        self,
        request: LangGraphRunRequest,
        options: LangGraphStreamOptions,
    ) -> tuple[LangGraphStreamPublisher, LangGraphStreamStats]:
        return (
            LangGraphStreamPublisher(
                graph_name=self._name,
                thread_id=request.thread_id,
                policy=self._stream_policy,
                options=options,
            ),
            LangGraphStreamStats(),
        )

    def _record_stream_call_started(
        self,
        tracker: EventTracker,
        *,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        options: LangGraphStreamOptions,
    ) -> None:
        tracker.record(
            "graph_call_started",
            metadata={
                **self._safe_event_metadata(request, config=config),
                "surface": "stream",
                **options.tracker_metadata(),
            },
        )

    def _drain_stream_parts(
        self,
        parts: Any,
        *,
        options: LangGraphStreamOptions,
        publisher: LangGraphStreamPublisher,
        stats: LangGraphStreamStats,
    ) -> Any:
        has_output = False
        output: Any = None
        for part in parts:
            has_candidate, candidate = self._handle_stream_part(
                part,
                options=options,
                publisher=publisher,
                stats=stats,
            )
            if has_candidate:
                has_output = True
                output = candidate
        if not has_output:
            raise KitaruUsageError(
                "LangGraph stream finished without a `values` part. Kitaru "
                'requires `.stream(..., version="v2")` values output to build '
                "the durable LangGraphRunResult without invoking the graph twice."
            )
        return output

    async def _drain_async_stream_parts(
        self,
        parts: Any,
        *,
        options: LangGraphStreamOptions,
        publisher: LangGraphStreamPublisher,
        stats: LangGraphStreamStats,
    ) -> Any:
        has_output = False
        output: Any = None
        async for part in parts:
            has_candidate, candidate = self._handle_stream_part(
                part,
                options=options,
                publisher=publisher,
                stats=stats,
            )
            if has_candidate:
                has_output = True
                output = candidate
        if not has_output:
            raise KitaruUsageError(
                "LangGraph stream finished without a `values` part. Kitaru "
                'requires `.astream(..., version="v2")` values output to build '
                "the durable LangGraphRunResult without invoking the graph twice."
            )
        return output

    def _handle_stream_part(
        self,
        part: Any,
        *,
        options: LangGraphStreamOptions,
        publisher: LangGraphStreamPublisher,
        stats: LangGraphStreamStats,
    ) -> tuple[bool, Any]:
        mode = stream_part_mode(part)
        stats.record(mode)
        if mode in options.published_modes or mode not in SUPPORTED_STREAM_MODES:
            publisher.part(part)
        return stream_part_output_candidate(part)

    def _build_result(
        self,
        output: Any,
        *,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        tracker: EventTracker,
        warnings: list[str],
    ) -> LangGraphRunResult:
        state_summary = self._inspect_state(config, warnings=warnings)
        if state_summary is not None:
            tracker.record(
                "checkpoint_state_captured",
                metadata={
                    "has_checkpoint_id": state_summary.latest_checkpoint_id is not None,
                    "next_node_count": len(state_summary.next_nodes),
                    "interrupt_count": len(state_summary.interrupts),
                },
            )

        output_interrupts = self._interrupts_from_output(output)
        interrupts = state_summary.interrupts if state_summary else []
        if output_interrupts:
            interrupts = output_interrupts
        status = (
            "interrupted"
            if interrupts or self._output_has_interrupt(output)
            else "completed"
        )

        latest_checkpoint_id = (
            state_summary.latest_checkpoint_id if state_summary is not None else None
        )
        next_nodes = state_summary.next_nodes if state_summary is not None else []
        pending_state = None
        if status == "interrupted":
            if not interrupts:
                interrupts = [
                    LangGraphInterruptSummary(
                        index=0, value=self._interrupt_value(output)
                    )
                ]
            pending_state = LangGraphPendingState(
                thread_id=request.thread_id,
                checkpoint_id=latest_checkpoint_id,
                checkpoint_ns=self._checkpoint_ns_from_state_or_request(
                    state_summary, request
                ),
                next_nodes=next_nodes,
                interrupts=interrupts,
                warnings=warnings,
            )
            tracker.record(
                "graph_interrupted",
                status="interrupted",
                metadata={"interrupt_count": len(interrupts)},
            )
        else:
            tracker.record("graph_call_completed")

        usage = self._usage_from_output(output) if self._capture.save_usage else None
        estimated_cost = (
            estimate_calculated_cost_usd(
                calculator=self._cost_calculator,
                usage=usage,
                warnings=warnings,
                adapter_name="LangGraph",
            )
            if self._capture.save_usage
            else None
        )
        if self._capture.save_usage:
            usage_record = build_usage_record(
                adapter="langgraph",
                surface="graph_call",
                call_name=self._name,
                event_id=tracker.run_label,
                record_id=tracker.run_label,
                usage=usage.model_dump(mode="json") if usage is not None else None,
                model=usage.model_name if usage is not None else None,
                estimated_cost_usd=estimated_cost,
                cost_source="calculator" if estimated_cost is not None else "none",
                cost_source_label="langgraph.cost_calculator",
                status=status,
                billing_effect="incurred" if status == "completed" else "unknown",
                cache_status="executed",
                warnings=warnings,
            )
            log_usage_record(usage_record)
        result = LangGraphRunResult(
            status=status,
            output=None if status == "interrupted" else output,
            thread_id=request.thread_id,
            latest_checkpoint_id=latest_checkpoint_id,
            next_nodes=next_nodes,
            interrupts=interrupts,
            pending_state=pending_state,
            state_summary=state_summary,
            state_artifact_name=None,
            output_artifact_name=None,
            event_log_artifact_name=(
                tracker.event_log_artifact_name
                if self._will_persist_run_artifacts()
                else None
            ),
            run_summary_artifact_name=(
                tracker.run_summary_artifact_name
                if self._will_persist_run_artifacts()
                else None
            ),
            usage=usage,
            estimated_cost_usd=estimated_cost,
            warnings=warnings,
        )
        return result

    def _inspect_state(
        self, config: dict[str, Any], *, warnings: list[str]
    ) -> LangGraphStateSummary | None:
        if (
            not self._durability.inspect_state_after_run
            or not self._capture.save_state_snapshot
        ):
            return None
        get_state = getattr(self._graph, "get_state", None)
        if not callable(get_state):
            warnings.append(
                "Wrapped graph does not expose `get_state(config)`; latest "
                "LangGraph checkpoint metadata could not be inspected."
            )
            return None
        try:
            snapshot = get_state(config)
        except Exception as exc:
            warnings.append(
                "LangGraph `get_state(config)` failed during adapter capture: "
                f"{type(exc).__name__}: {exc}"
            )
            return None
        return self._summarize_state(snapshot)

    def _summarize_state(self, snapshot: Any) -> LangGraphStateSummary:
        snapshot_config = getattr(snapshot, "config", None)
        configurable = _mapping_get(snapshot_config, "configurable") or {}
        latest_checkpoint_id = _string_or_none(
            _mapping_get(configurable, "checkpoint_id")
        )
        checkpoint_ns = _string_or_none(_mapping_get(configurable, "checkpoint_ns"))
        next_nodes = [str(node) for node in list(getattr(snapshot, "next", ()) or ())]
        interrupts = self._interrupts_from_snapshot(snapshot)
        values = (
            to_json_safe(getattr(snapshot, "values", None))
            if self._capture.save_state_values
            else None
        )
        tasks = self._summarize_tasks(snapshot)
        return LangGraphStateSummary(
            latest_checkpoint_id=latest_checkpoint_id,
            checkpoint_ns=checkpoint_ns,
            next_nodes=next_nodes,
            interrupts=interrupts,
            values=values,
            tasks=tasks,
        )

    def _summarize_tasks(self, snapshot: Any) -> Any | None:
        if not self._capture.save_state_tasks:
            return None
        tasks = getattr(snapshot, "tasks", ()) or ()
        if self._capture.capture_mode == "full":
            return to_json_safe(tasks)
        return [
            self._summarize_task_metadata(task, index=index)
            for index, task in enumerate(tasks)
        ]

    def _summarize_task_metadata(self, task: Any, *, index: int) -> dict[str, Any]:
        error = getattr(task, "error", None)
        result = getattr(task, "result", None)
        interrupts = getattr(task, "interrupts", ()) or ()
        return {
            "index": index,
            "id": _string_or_none(getattr(task, "id", None)),
            "name": _string_or_none(getattr(task, "name", None)),
            "path": _safe_string_list(getattr(task, "path", None)),
            "interrupt_count": _len_or_count(interrupts),
            "has_result": result is not None,
            "result_has_interrupt": self._output_has_interrupt(result),
            "has_error": error is not None,
            "error_type": type(error).__name__ if error is not None else None,
        }

    def _interrupts_from_snapshot(
        self, snapshot: Any
    ) -> list[LangGraphInterruptSummary]:
        summaries: list[LangGraphInterruptSummary] = []
        for task in list(getattr(snapshot, "tasks", ()) or ()):
            raw_interrupts = list(getattr(task, "interrupts", ()) or ())
            if not raw_interrupts:
                nested_result = getattr(task, "result", None)
                if self._output_has_interrupt(nested_result):
                    raw_interrupts = list(
                        _mapping_get(nested_result, "__interrupt__") or []
                    )
            for raw_interrupt in raw_interrupts:
                summaries.append(
                    self._summarize_interrupt(
                        raw_interrupt,
                        index=len(summaries),
                        task=task,
                    )
                )
        return summaries

    def _interrupts_from_output(self, output: Any) -> list[LangGraphInterruptSummary]:
        raw_interrupts = _mapping_get(output, "__interrupt__")
        if raw_interrupts is None:
            return []
        if not isinstance(raw_interrupts, list | tuple):
            raw_interrupts = [raw_interrupts]
        return [
            self._summarize_interrupt(raw_interrupt, index=index)
            for index, raw_interrupt in enumerate(raw_interrupts)
        ]

    def _summarize_interrupt(
        self, interrupt: Any, *, index: int, task: Any | None = None
    ) -> LangGraphInterruptSummary:
        value = getattr(interrupt, "value", None)
        if value is None and isinstance(interrupt, Mapping):
            value = interrupt.get("value")
        namespace = getattr(interrupt, "ns", None) or getattr(
            interrupt, "namespace", None
        )
        if namespace is None and isinstance(interrupt, Mapping):
            namespace = interrupt.get("ns") or interrupt.get("namespace")
        task_id = getattr(task, "id", None) if task is not None else None
        if task_id is None:
            task_id = _mapping_get(interrupt, "task_id")
        interrupt_id = getattr(interrupt, "id", None)
        if interrupt_id is None and isinstance(interrupt, Mapping):
            interrupt_id = interrupt.get("id") or interrupt.get("interrupt_id")
        node_name = getattr(task, "name", None) if task is not None else None
        if node_name is None and isinstance(interrupt, Mapping):
            node_name = interrupt.get("node_name")
        resumable = getattr(interrupt, "resumable", True)
        if isinstance(interrupt, Mapping):
            resumable = interrupt.get("resumable", resumable)
        return LangGraphInterruptSummary(
            index=index,
            interrupt_id=str(interrupt_id) if interrupt_id is not None else None,
            value=to_json_safe(value),
            resumable=bool(resumable),
            namespace=str(namespace) if namespace is not None else None,
            task_id=str(task_id) if task_id is not None else None,
            node_name=str(node_name) if node_name is not None else None,
        )

    def _prepared_config(self, request: LangGraphRunRequest) -> dict[str, Any]:
        if self._config_factory is None:
            return merge_config(request)
        factory_config = self._config_factory(request)
        if not isinstance(factory_config, dict):
            raise KitaruUsageError("config_factory must return a dictionary.")
        factory_configurable = factory_config.get("configurable", {})
        request_configurable = request.config.get("configurable", {})
        if not isinstance(factory_configurable, Mapping) or not isinstance(
            request_configurable, Mapping
        ):
            raise KitaruUsageError(
                "config_factory/request configurable must be mappings."
            )
        merged_request = request.model_copy(
            update={
                "config": {
                    **factory_config,
                    **request.config,
                    "configurable": {
                        **dict(factory_configurable),
                        **dict(request_configurable),
                    },
                }
            }
        )
        return merge_config(merged_request)

    def _prepared_context(self, request: LangGraphRunRequest) -> Any:
        if self._context_factory is None:
            return request.context
        return self._context_factory(request)

    def _resolved_durability(self, request: LangGraphRunRequest) -> str:
        return request.durability or self._durability.mode

    def _forwarded_durability(
        self,
        request: LangGraphRunRequest,
        *,
        config: Mapping[str, Any] | None = None,
    ) -> str | None:
        if not self._has_effective_checkpointer_saver(config):
            return None
        return self._resolved_durability(request)

    def _graph_call_kwargs(
        self,
        request: LangGraphRunRequest,
        *,
        config: Mapping[str, Any],
        context: Any | None,
        method_name: str,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        forwarded_durability = self._forwarded_durability(request, config=config)
        if forwarded_durability is not None:
            kwargs["durability"] = forwarded_durability
        if context is not None:
            kwargs["context"] = context
        return self._filter_kwargs_for_graph_method(method_name, kwargs)

    def _graph_stream_kwargs(
        self,
        request: LangGraphRunRequest,
        *,
        config: Mapping[str, Any],
        context: Any | None,
        method_name: str,
        options: LangGraphStreamOptions,
    ) -> dict[str, Any]:
        kwargs = self._graph_call_kwargs(
            request,
            config=config,
            context=context,
            method_name=method_name,
        )
        kwargs.update(
            {
                "stream_mode": list(options.upstream_modes),
                "subgraphs": options.subgraphs,
                "version": options.version,
            }
        )
        required_kwargs = {"stream_mode", "version"}
        if options.subgraphs:
            required_kwargs.add("subgraphs")
        return self._filter_stream_kwargs_for_graph_method(
            method_name,
            kwargs,
            required_kwargs=required_kwargs,
        )

    def _filter_kwargs_for_graph_method(
        self, method_name: str, kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        allowed_kwargs = self._allowed_kwargs_for_graph_method(method_name)
        if allowed_kwargs is None:
            return kwargs
        return {key: value for key, value in kwargs.items() if key in allowed_kwargs}

    def _filter_stream_kwargs_for_graph_method(
        self,
        method_name: str,
        kwargs: dict[str, Any],
        *,
        required_kwargs: set[str],
    ) -> dict[str, Any]:
        allowed_kwargs = self._allowed_kwargs_for_graph_method(method_name)
        if allowed_kwargs is None:
            return kwargs
        missing_required = sorted(required_kwargs - allowed_kwargs)
        if missing_required:
            arguments = ", ".join(f"`{name}`" for name in missing_required)
            raise KitaruUsageError(
                f"Wrapped LangGraph object cannot be streamed because "
                f"`{method_name}(...)` does not accept required streaming "
                f"keyword argument(s): {arguments}. Kitaru needs these "
                f"arguments before graph execution so it can request "
                f"LangGraph's v2 stream format and reconstruct the durable "
                f"result from root `values` events. Upgrade or wrap the graph "
                f"method so it accepts those keywords, or exposes `**kwargs`."
            )
        return {key: value for key, value in kwargs.items() if key in allowed_kwargs}

    def _allowed_kwargs_for_graph_method(self, method_name: str) -> set[str] | None:
        if method_name in self._method_allowed_kwargs:
            return self._method_allowed_kwargs[method_name]
        method = getattr(self._graph, method_name, None)
        if method is None:
            self._method_allowed_kwargs[method_name] = None
            return None
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            self._method_allowed_kwargs[method_name] = None
            return None
        if any(
            param.kind == param.VAR_KEYWORD for param in signature.parameters.values()
        ):
            self._method_allowed_kwargs[method_name] = None
            return None
        allowed_kwargs = set(signature.parameters)
        self._method_allowed_kwargs[method_name] = allowed_kwargs
        return allowed_kwargs

    def _require_streaming_graph_call(self) -> None:
        if self._checkpoint_strategy == "graph_call":
            return
        raise KitaruUsageError(
            "`KitaruGraphRunner.stream(...)` and `astream(...)` support only "
            "`checkpoint_strategy='graph_call'` today. Streaming with "
            "`checkpoint_strategy='calls'` would observe LangGraph trace chunks, "
            "but those chunks do not own the replay-safe model/tool handler "
            "calls, so calls-mode streaming is intentionally deferred."
        )

    def _validate_strategy_config(
        self,
        *,
        run_checkpoint_config: CheckpointConfig | None,
        call_checkpoint_policy: LangGraphCallCheckpointPolicy | None,
    ) -> None:
        if self._checkpoint_strategy == "calls" and run_checkpoint_config is not None:
            raise KitaruUsageError(
                "`run_checkpoint_config` applies only to "
                "`checkpoint_strategy='graph_call'`. Use `call_checkpoint_policy` "
                "for calls-mode model/tool/summary checkpoints."
            )
        if (
            self._checkpoint_strategy == "graph_call"
            and call_checkpoint_policy is not None
        ):
            raise KitaruUsageError(
                "`call_checkpoint_policy` applies only to "
                "`checkpoint_strategy='calls'`. Remove it or set "
                "`checkpoint_strategy='calls'`."
            )

    def _validate_request(
        self,
        request: LangGraphRunRequest,
        *,
        required_method: str,
    ) -> None:
        if self._durability.require_thread_id and not request.thread_id.strip():
            raise KitaruUsageError(
                "LangGraph requests require a stable non-empty `thread_id`."
            )
        if (
            self._checkpoint_strategy == "calls"
            and is_inside_checkpoint()
            and self._call_checkpoint_policy.nested_checkpoint_policy == "error"
        ):
            raise KitaruUsageError(
                "`checkpoint_strategy='calls'` opens model/tool checkpoints and "
                "must run from a flow body, not from inside another checkpoint. "
                "Set `call_checkpoint_policy.nested_checkpoint_policy` to "
                "'metadata_only' to record metadata without inner checkpoints."
            )
        if not callable(getattr(self._graph, required_method, None)):
            raise KitaruUsageError(
                f"Wrapped LangGraph object does not expose `{required_method}(...)`."
            )

    def _validate_checkpointer_requirement(self, config: Mapping[str, Any]) -> None:
        if (
            self._durability.require_checkpointer
            and not self._has_effective_checkpointer_saver(config)
        ):
            raise KitaruUsageError(
                "LangGraph durability policy requires a graph checkpointer, but "
                "none was detected on the wrapped graph or prepared config."
            )

    def _effective_call_checkpoint_policy(self) -> LangGraphCallCheckpointPolicy:
        if not (
            self._checkpoint_strategy == "calls"
            and is_inside_checkpoint()
            and self._call_checkpoint_policy.nested_checkpoint_policy == "metadata_only"
        ):
            return self._call_checkpoint_policy
        return self._call_checkpoint_policy.model_copy(
            update={
                "model_checkpoint_config": False,
                "tool_checkpoint_config": False,
                "tool_checkpoint_config_by_name": {
                    tool_name: False
                    for tool_name in (
                        self._call_checkpoint_policy.tool_checkpoint_config_by_name
                    )
                },
            }
        )

    def _graph_call_checkpoint_config(self) -> CheckpointConfig:
        return {
            **self._run_checkpoint_config,
            "retries": self._run_checkpoint_config.get("retries", 0),
            "cache": self._run_checkpoint_config.get("cache", False),
            "type": self._run_checkpoint_config.get("type", "graph_call"),
            "runtime": self._run_checkpoint_config.get("runtime", "inline"),
        }

    def _graph_call_cache_key(
        self,
        request: LangGraphRunRequest,
        *,
        surface: str | None = None,
        stream_identity: Mapping[str, object] | None = None,
    ) -> str:
        payload: dict[str, object] = {
            "adapter": "langgraph",
            "checkpoint_strategy": "graph_call",
            "langgraph_version": langgraph_version(),
            "graph": self._graph_identity(),
            "request": to_cache_identity(
                request.model_dump(
                    mode="python",
                    exclude={"command"} if request.kind == "resume" else set(),
                )
            ),
            "command": (
                self._command_cache_identity(request.command)
                if request.kind == "resume"
                else None
            ),
        }
        if surface is not None:
            payload["surface"] = surface
        if stream_identity is not None:
            payload["stream"] = to_cache_identity(stream_identity)
        return checkpoint_cache_key(payload)

    def _command_cache_identity(self, command: Any) -> Any:
        resume_payload = getattr(command, "resume", None)
        if resume_payload is not None:
            return {
                "python_type": _type_label(command),
                "resume": to_cache_identity(resume_payload),
            }
        return to_cache_identity(command)

    def _graph_identity(self) -> dict[str, Any]:
        return dict(self._graph_identity_value)

    def _build_graph_identity(self) -> dict[str, Any]:
        graph_type = type(self._graph)
        return {
            "name": self._name,
            "python_type": f"{graph_type.__module__}.{graph_type.__qualname__}",
            "fingerprint": getattr(self._graph, "fingerprint", None),
        }

    def _checkpointer_label(self) -> str | None:
        return self._checkpointer_label_value

    def _resolve_checkpointer_label(self) -> str | None:
        checkpointer = self._checkpointer_saver_value
        if checkpointer is None:
            return None
        return _type_label(checkpointer)

    def _has_checkpointer_saver(self) -> bool:
        return self._checkpointer_saver_value is not None

    def _config_checkpointer_saver(
        self, config: Mapping[str, Any] | None
    ) -> Any | None:
        configurable = (
            _mapping_get(config, "configurable") if config is not None else None
        )
        checkpointer = _mapping_get(configurable, LANGGRAPH_CONFIG_CHECKPOINTER_KEY)
        if checkpointer is None or isinstance(checkpointer, bool):
            return None
        return checkpointer

    def _has_effective_checkpointer_saver(
        self, config: Mapping[str, Any] | None = None
    ) -> bool:
        return (
            self._has_checkpointer_saver()
            or self._config_checkpointer_saver(config) is not None
        )

    def _effective_checkpointer_label(
        self, config: Mapping[str, Any] | None = None
    ) -> str | None:
        graph_label = self._checkpointer_label()
        if graph_label is not None:
            return graph_label
        config_checkpointer = self._config_checkpointer_saver(config)
        if config_checkpointer is None:
            return None
        return _type_label(config_checkpointer)

    def _resolve_checkpointer_saver(self) -> Any | None:
        checkpointer = getattr(self._graph, "checkpointer", None)
        if checkpointer is None or isinstance(checkpointer, bool):
            return None
        return checkpointer

    def _store_label(self) -> str | None:
        return self._store_label_value

    def _resolve_store_label(self) -> str | None:
        store = getattr(self._graph, "store", None)
        if store is None:
            return None
        return _type_label(store)

    def _checkpointer_warnings(
        self, *, config: Mapping[str, Any] | None = None
    ) -> list[str]:
        warnings: list[str] = []
        checkpointer_label = self._effective_checkpointer_label(config)
        if checkpointer_label is None:
            if self._durability.warn_without_checkpointer:
                warnings.append(
                    "No LangGraph checkpointer was detected. Kitaru will still "
                    "record the outer graph call, but LangGraph may not be able "
                    "to resume graph-internal state."
                )
            return warnings
        if (
            self._durability.warn_ephemeral_checkpointer
            and "InMemorySaver" in checkpointer_label
        ):
            warnings.append(
                "LangGraph is using InMemorySaver. This is useful for local tests, "
                "but it is ephemeral and will not survive process/container loss."
            )
        return warnings

    def _persist_tracker(
        self,
        tracker: EventTracker,
        result: LangGraphRunResult,
        *,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        context: Any | None,
        stream_summary: dict[str, object] | None = None,
    ) -> None:
        self._persist_tracker_summary(
            tracker,
            {
                **self._run_summary_metadata(
                    request=request,
                    config=config,
                    context=context,
                ),
                "status": result.status,
                "latest_checkpoint_id": result.latest_checkpoint_id,
                "checkpoint_ns": (
                    result.pending_state.checkpoint_ns
                    if result.pending_state is not None
                    else request.checkpoint_ns
                ),
                "output": self._captured_output(result),
                "warnings": result.warnings,
                **(stream_summary or {}),
            },
        )

    def _persist_failure_tracker(
        self,
        tracker: EventTracker,
        error: BaseException,
        *,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        context: Any | None,
        warnings: list[str],
        stream_summary: dict[str, object] | None = None,
    ) -> None:
        self._persist_tracker_summary(
            tracker,
            {
                **self._run_summary_metadata(
                    request=request,
                    config=config,
                    context=context,
                ),
                "status": "failed",
                "latest_checkpoint_id": None,
                "checkpoint_ns": request.checkpoint_ns,
                "output": None,
                "warnings": warnings,
                "error_type": type(error).__name__,
                "error_message": str(error),
                **(stream_summary or {}),
            },
        )

    def _persist_tracker_summary(
        self,
        tracker: EventTracker,
        extra_summary: dict[str, object],
    ) -> None:
        if not self._will_persist_run_artifacts():
            return
        fail_on_error = self._capture.fail_on_event_persistence_error
        if (
            self._checkpoint_strategy == "calls"
            and self._call_checkpoint_policy.persist_run_artifacts
            and is_inside_flow()
            and not is_inside_checkpoint()
        ):

            def _body() -> None:
                tracker.persist(extra_summary, fail_on_error=fail_on_error)

            summary_step_name = (
                f"langgraph_summary__{tracker.graph_name}_{tracker.run_label}"
            )
            summary_config = self._calls_summary_checkpoint_config()
            try:
                run_sync_in_checkpoint(
                    config=summary_config,
                    step_name=summary_step_name,
                    body=_body,
                    cache_key=self._calls_summary_cache_key(
                        tracker,
                        extra_summary,
                        enabled=summary_config.get("cache", False),
                    ),
                )
            except Exception as error:
                if fail_on_error:
                    raise
                previous_failures = extra_summary.get("persistence_failures")
                persistence_failures = (
                    [*previous_failures] if isinstance(previous_failures, list) else []
                )
                persistence_failures.append(
                    {
                        "operation": "save_run_summary",
                        "artifact_name": summary_step_name,
                        "exception_type": type(error).__name__,
                        "message": str(error),
                    }
                )
                fallback_summary = {
                    **extra_summary,
                    "persistence_failures": persistence_failures,
                    "summary_checkpoint_failed": True,
                }
                with suppress(Exception):
                    tracker.persist(fallback_summary, fail_on_error=False)
            return
        tracker.persist(extra_summary, fail_on_error=fail_on_error)

    def _will_persist_run_artifacts(self) -> bool:
        return not (
            self._checkpoint_strategy == "calls"
            and not self._call_checkpoint_policy.persist_run_artifacts
        )

    def _calls_summary_checkpoint_config(self) -> CheckpointConfig:
        config = resolve_summary_checkpoint_config(self._call_checkpoint_policy)
        return {
            **config,
            "retries": config.get("retries", 0),
            "cache": config.get("cache", False),
            "runtime": config.get("runtime", "inline"),
        }

    def _calls_summary_cache_key(
        self,
        tracker: EventTracker,
        extra_summary: dict[str, object],
        *,
        enabled: bool,
    ) -> str | None:
        if not enabled:
            return None
        return checkpoint_cache_key(
            {
                "adapter": "langgraph",
                "checkpoint_strategy": "calls",
                "summary": extra_summary,
                "event_ids": [event.event_id for event in tracker.events],
            }
        )

    def _handle_graph_call_failure(
        self,
        *,
        error: BaseException,
        tracker: EventTracker,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        context: Any | None,
        warnings: list[str],
        stream_summary: dict[str, object] | None = None,
    ) -> None:
        tracker.record("graph_call_failed", status="failed", error=error)
        try:
            self._persist_failure_tracker(
                tracker,
                error,
                request=request,
                config=config,
                context=context,
                warnings=warnings,
                stream_summary=stream_summary,
            )
        except Exception as persistence_error:
            note = (
                "LangGraph failure-summary persistence also failed: "
                f"{type(persistence_error).__name__}: {persistence_error}"
            )
            add_note = getattr(error, "add_note", None)
            if callable(add_note):
                add_note(note)

    def _stream_cache_identity(
        self,
        options: LangGraphStreamOptions,
        *,
        method_name: str,
    ) -> dict[str, object]:
        return {
            **options.cache_identity(),
            "method": method_name,
            "policy": self._stream_policy.model_dump(
                mode="json",
                include={
                    "default_modes",
                    "include_raw_payloads",
                    "include_message_text_deltas",
                    "include_custom_payload",
                    "allow_debug",
                    "max_display_chars",
                },
            ),
        }

    def _stream_run_summary(
        self,
        options: LangGraphStreamOptions,
        stats: LangGraphStreamStats,
    ) -> dict[str, object]:
        return {
            "surface": "stream",
            "stream_version": STREAM_VERSION,
            **options.tracker_metadata(),
            **stats.metadata(),
        }

    def _run_summary_metadata(
        self,
        *,
        request: LangGraphRunRequest,
        config: dict[str, Any],
        context: Any | None,
    ) -> dict[str, object]:
        return {
            "adapter_version": 1,
            "langgraph_version": langgraph_version(),
            "graph_name": self._name,
            "thread_id": request.thread_id,
            "thread_id_present": bool(request.thread_id),
            "checkpointer_type": self._effective_checkpointer_label(config),
            "store_type": self._store_label(),
            "durability": self._resolved_durability(request),
            "forwarded_durability": self._forwarded_durability(request, config=config),
            "capture": self._capture_summary,
            "config": redact_config(config) if self._capture.save_config else None,
            "context": redact_config(context) if self._capture.save_context else None,
            "input": self._captured_input(request),
        }

    def _captured_input(self, request: LangGraphRunRequest) -> Any | None:
        if not self._capture.save_input:
            return None
        if request.kind == "start":
            if self._checkpoint_strategy == "calls":
                return _calls_mode_input_capture(request.input)
            return to_json_safe(request.input)
        return self._command_capture(request.command)

    def _captured_output(self, result: LangGraphRunResult) -> Any | None:
        if not self._capture.save_output or result.status != "completed":
            return None
        if self._checkpoint_strategy == "calls":
            return _calls_mode_payload_capture(result.output)
        return to_json_safe(result.output)

    def _command_capture(self, command: Any) -> Any:
        resume_payload = getattr(command, "resume", None)
        if resume_payload is not None:
            return {
                "python_type": _type_label(command),
                "resume": to_json_safe(resume_payload),
            }
        return to_json_safe(command)

    def _safe_event_metadata(
        self, request: LangGraphRunRequest, *, config: dict[str, Any]
    ) -> dict[str, object]:
        return {
            "kind": request.kind,
            "durability": self._resolved_durability(request),
            "forwarded_durability": self._forwarded_durability(request, config=config),
            "has_checkpointer": self._has_effective_checkpointer_saver(config),
            "has_store": self._store_label() is not None,
            "thread_id_present": bool(request.thread_id),
            "configurable_keys": _safe_key_labels(
                _mapping_get(config, "configurable", {})
            ),
        }

    def _analytics_metadata(
        self,
        method: str,
        *,
        request: LangGraphRunRequest,
        config: Mapping[str, Any],
        status: str,
        captured_state: bool,
    ) -> dict[str, object]:
        return {
            "method": method,
            "status": status,
            "durability": self._resolved_durability(request),
            "forwarded_durability": self._forwarded_durability(request, config=config),
            "has_checkpointer": self._has_effective_checkpointer_saver(config),
            "has_store": self._store_label() is not None,
            "captured_state": captured_state,
        }

    def _track_result(
        self,
        method: str,
        result: LangGraphRunResult,
        *,
        request: LangGraphRunRequest,
        config: Mapping[str, Any],
    ) -> None:
        metadata = self._analytics_metadata(
            method,
            request=request,
            config=config,
            status=result.status,
            captured_state=result.state_summary is not None,
        )
        if result.status == "interrupted":
            track(AnalyticsEvent.LANGGRAPH_INTERRUPTED, metadata)
        track(
            AnalyticsEvent.LANGGRAPH_RUN_COMPLETED,
            metadata,
        )

    def _usage_from_output(self, output: Any) -> LangGraphUsageSummary | None:
        usage = _find_usage(output, max_depth=6)
        if usage is None:
            return None
        usage_json = to_json_safe(usage)
        if not isinstance(usage_json, dict):
            return LangGraphUsageSummary(raw={"value": usage_json})
        token_usage = token_usage_from_mapping(usage_json)
        return LangGraphUsageSummary(
            input_tokens=token_usage["input_tokens"],
            output_tokens=token_usage["output_tokens"],
            total_tokens=token_usage["total_tokens"],
            raw=usage_json,
        )

    def _output_has_interrupt(self, output: Any) -> bool:
        return _mapping_get(output, "__interrupt__") is not None

    def _interrupt_value(self, output: Any) -> Any:
        interrupts = _mapping_get(output, "__interrupt__")
        if isinstance(interrupts, list | tuple) and interrupts:
            return getattr(interrupts[0], "value", interrupts[0])
        return interrupts

    def _checkpoint_ns_from_state_or_request(
        self,
        state_summary: LangGraphStateSummary | None,
        request: LangGraphRunRequest,
    ) -> str | None:
        if state_summary is not None and state_summary.checkpoint_ns is not None:
            return state_summary.checkpoint_ns
        return request.checkpoint_ns


def _calls_mode_input_capture(value: Any) -> Any:
    """Capture graph input while omitting LangChain message free text."""
    return _calls_mode_payload_capture(value)


def _calls_mode_payload_capture(
    value: Any,
    *,
    seen: set[int] | None = None,
) -> Any:
    """Capture calls-mode payloads while omitting message free text.

    In calls mode, graph inputs and outputs often contain ``messages`` lists.
    Those messages can hold the same prompt text that model-call checkpoint
    inputs deliberately omit, so run summaries use structural message summaries
    for message-shaped values and leave non-message values unchanged.
    """
    seen = seen or set()
    if _is_message_like(value):
        return _message_input_summary(value)
    if isinstance(value, Mapping):
        value_id = id(value)
        if value_id in seen:
            return {"serialization_error": "cycle_detected"}
        seen.add(value_id)
        try:
            captured: dict[str, Any] = {}
            for key, nested in value.items():
                key_text = _safe_key_text(key)
                if key_text == "system_message":
                    captured[key_text] = _system_input_summary(nested)
                else:
                    captured[key_text] = _calls_mode_payload_capture(
                        nested,
                        seen=seen,
                    )
            return captured
        finally:
            seen.remove(value_id)
    if isinstance(value, list | tuple):
        value_id = id(value)
        if value_id in seen:
            return {"serialization_error": "cycle_detected"}
        seen.add(value_id)
        try:
            return [_calls_mode_payload_capture(item, seen=seen) for item in value]
        finally:
            seen.remove(value_id)
    return to_json_safe(value)


def _is_message_like(value: Any) -> bool:
    content = _mapping_get(value, "content")
    if content is None:
        content = getattr(value, "content", None)
    if content is None:
        return False
    return any(
        marker is not None
        for marker in (
            _mapping_get(value, "role"),
            _mapping_get(value, "type"),
            _mapping_get(value, "tool_calls"),
            getattr(value, "type", None),
            getattr(value, "tool_calls", None),
        )
    )


def _message_input_summary(message: Any) -> dict[str, Any]:
    return {
        "type": _short_type_label(message),
        "role": _string_or_none(
            _mapping_get(message, "role") or getattr(message, "type", None)
        ),
        "name": _string_or_none(
            _mapping_get(message, "name") or getattr(message, "name", None)
        ),
        "content": (
            "[OMITTED]"
            if _mapping_get(message, "content") is not None
            or getattr(message, "content", None) is not None
            else None
        ),
        "tool_call_count": _len_or_count(
            _mapping_get(message, "tool_calls")
            or getattr(message, "tool_calls", None)
            or ()
        ),
    }


def _system_input_summary(system_message: Any) -> dict[str, Any]:
    if system_message is None:
        return {"present": False, "content": None}
    return {
        "present": True,
        "type": _short_type_label(system_message),
        "content": "[OMITTED]",
    }


def _safe_key_text(key: Any) -> str:
    try:
        return str(key)
    except Exception:
        return f"<unprintable key {_type_label(key)}>"


def _mapping_get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(key, default)
    return default


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_USAGE_TOKEN_KEYS = frozenset(
    {
        "input_tokens",
        "prompt_tokens",
        "request_tokens",
        "tokens_input",
        "input_token_count",
        "output_tokens",
        "completion_tokens",
        "response_tokens",
        "tokens_output",
        "output_token_count",
        "total_tokens",
        "tokens_total",
        "total_token_count",
    }
)
_USAGE_CONTAINER_KEYS = ("usage", "token_usage", "usage_metadata")


def _looks_like_usage(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            key in value and _int_or_none(value[key]) is not None
            for key in _USAGE_TOKEN_KEYS
        )
    return any(
        _int_or_none(getattr(value, key, None)) is not None for key in _USAGE_TOKEN_KEYS
    )


def _len_or_count(value: Any) -> int:
    try:
        return len(value)
    except TypeError:
        return sum(1 for _ in value)


def _safe_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple):
        return [str(item) for item in value]
    return [str(value)]


def _safe_key_labels(value: Any) -> list[str]:
    """Return stable-ish mapping key labels without breaking observability."""
    if not isinstance(value, Mapping):
        return []
    try:
        keys = list(value)
    except Exception:
        return [f"<unavailable keys for {_type_label(value)}>"]
    labels: list[str] = []
    for key in keys:
        try:
            labels.append(str(key))
        except Exception:
            labels.append(f"<unprintable key {_type_label(key)}>")
    return sorted(labels)


def _short_type_label(value: Any) -> str:
    if value is None:
        return "NoneType"
    return type(value).__qualname__


def _type_label(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _find_usage(value: Any, *, max_depth: int, _depth: int = 0) -> Any | None:
    if value is None or _depth > max_depth:
        return None
    if _looks_like_usage(value):
        return value
    if isinstance(value, Mapping):
        for key in _USAGE_CONTAINER_KEYS:
            if key in value and _looks_like_usage(value[key]):
                return value[key]
        for nested in value.values():
            found = _find_usage(nested, max_depth=max_depth, _depth=_depth + 1)
            if found is not None:
                return found
    if isinstance(value, list | tuple):
        for item in value:
            found = _find_usage(item, max_depth=max_depth, _depth=_depth + 1)
            if found is not None:
                return found
    for key in ("usage_metadata", "usage"):
        usage = getattr(value, key, None)
        if _looks_like_usage(usage):
            return usage
    return None
