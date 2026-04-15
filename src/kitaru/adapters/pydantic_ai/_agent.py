import asyncio
import threading
import time
import uuid
import weakref
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any

import kitaru
from kitaru.analytics import AnalyticsEvent, track

from pydantic_ai import _utils, messages as _messages, models, usage as _usage
from pydantic_ai.agent import AbstractAgent, AgentRun, WrapperAgent
from pydantic_ai.agent.abstract import (
    AgentBuiltinTool,
    AgentInstructions,
    AgentMetadata,
    AgentModelSettings,
    EventStreamHandler,
)
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.output import OutputDataT, OutputSpec
from pydantic_ai.tools import AgentDepsT, DeferredToolResults
from pydantic_ai.toolsets import AbstractToolset

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._model import KitaruModel
from ._policy import CapturePolicy
from ._toolset import kitaruify_toolset
from ._tracking import get_current_tracker, tracker_scope
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverrides,
    materialize_step_output,
    reject_isolated_runtime,
    run_async_in_checkpoint,
)

_TRACKING_ACTIVE: ContextVar[bool] = ContextVar('kitaru_tracking_active', default=False)

# Auto-flow bodies keyed by uuid. The @kitaru.flow entrypoint must be module-
# level for ZenML dynamic-pipeline source resolution, so it can't close over
# its body — the registry bridges the gap. In-process only; remote stacks
# require an explicit @kitaru.flow.
_AUTO_FLOW_BODIES: dict[str, '_AutoFlowSlot'] = {}
_AUTO_FLOW_LOCK = threading.Lock()


class _AutoFlowSlot:
    __slots__ = ('body', 'error', 'has_result', 'result')

    def __init__(self, body: Callable[[], Any]) -> None:
        self.body = body
        self.result: Any = None
        self.error: BaseException | None = None
        self.has_result = False


@kitaru.flow
def _kitaru_pydantic_ai_auto_flow(run_id: str) -> None:
    with _AUTO_FLOW_LOCK:
        slot = _AUTO_FLOW_BODIES.get(run_id)
    if slot is None:
        raise RuntimeError(
            f'Kitaru auto-flow body {run_id!r} not found in registry. Auto-flow '
            'is local-only; wrap your agent call in an explicit `@kitaru.flow` '
            'for remote stacks.'
        )
    try:
        slot.result = slot.body()
        slot.has_result = True
    except BaseException as exc:
        slot.error = exc
        raise


def _track_run_completed(method: str, error: BaseException | None) -> None:
    payload: dict[str, Any] = {
        'method': method,
        'status': 'failed' if error is not None else 'completed',
    }
    if error is not None:
        payload['error_type'] = type(error).__name__
    track(AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED, payload)


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
        capture: CapturePolicy | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        turn_checkpoint_config: CheckpointConfig | None = None,
        granular_checkpoints: bool = False,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
        mcp_checkpoint_config: CheckpointConfig | None = None,
        persist_message_history: bool = False,
    ) -> None:
        """Wrap an agent so its runs become durable under Kitaru.

        Outside a flow, ``run()`` / ``run_sync()`` auto-open a ``@kitaru.flow``.

        **Turn mode (default):** each run opens one ``@kitaru.checkpoint`` named
        after the agent; model/tool/MCP calls are recorded as child events.

        **Granular mode** (``granular_checkpoints=True``): no turn checkpoint;
        each model/tool/MCP call opens its own checkpoint — true per-call retry
        and cache at the cost of losing the aggregating run artifact. The
        ``model_/tool_/mcp_checkpoint_config`` kwargs (and the per-tool
        ``tool_checkpoint_config_by_name`` map, where ``False`` opts a tool
        out entirely) are only honored in this mode.

        When ``persist_message_history=True``, the adapter remembers the final
        ``result.all_messages()`` of each run on the instance and auto-injects
        it as ``message_history`` on the next call if the caller doesn't supply
        one — one instance then represents one conversation. Pass an explicit
        ``message_history=`` to override for a single call.
        """
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise UserError(
                'KitaruAgent requires the wrapped agent to define a concrete model at construction time; '
                'pass `model=` to the Agent constructor.'
            )

        self._name = name or wrapped.name
        if self._name is None:
            raise UserError(
                'KitaruAgent requires a stable `name`; pass `name=` to KitaruAgent or set the wrapped agent name.'
            )
        self._capture = capture or CapturePolicy()
        self._event_stream_handler = event_stream_handler
        self._turn_checkpoint_config: dict[str, Any] = (
            {**turn_checkpoint_config} if turn_checkpoint_config else {}
        )
        self._granular_checkpoints = granular_checkpoints
        # Kitaru MVP forbids nested checkpoints, so these kwargs only apply in granular mode.
        if granular_checkpoints:
            self._model_checkpoint_config = model_checkpoint_config
            self._tool_checkpoint_config = tool_checkpoint_config
            self._tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = (
                dict(tool_checkpoint_config_by_name) if tool_checkpoint_config_by_name else None
            )
            self._mcp_checkpoint_config = mcp_checkpoint_config
        else:
            self._model_checkpoint_config = None
            self._tool_checkpoint_config = None
            self._tool_checkpoint_config_by_name = None
            self._mcp_checkpoint_config = None
        self._model = KitaruModel(
            wrapped.model,
            capture=self._capture,
            agent_name=self._name,
            checkpoint_config=self._model_checkpoint_config,
        )
        self._toolsets = self._prepare_toolsets(list(wrapped.toolsets))
        self._wrapped_handlers: weakref.WeakSet[EventStreamHandler[AgentDepsT]] = weakref.WeakSet()
        self._persist_message_history = persist_message_history
        self._last_messages: list[_messages.ModelMessage] | None = None
        self._message_history_lock = threading.Lock()
        track(
            AnalyticsEvent.PYDANTIC_AI_WRAPPED,
            {
                'toolset_count': len(self._toolsets),
                'granular_checkpoints': granular_checkpoints,
                'persist_message_history': persist_message_history,
            },
        )

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        raise UserError('The agent name cannot be changed after creation. Create a new KitaruAgent instead.')

    @property
    def model(self) -> Model:
        return self._model

    @property
    def toolsets(self) -> Sequence[AbstractToolset[AgentDepsT]]:
        return self._toolsets

    @property
    def event_stream_handler(self) -> EventStreamHandler[AgentDepsT] | None:
        return self._event_stream_handler or super().event_stream_handler

    @property
    def capture(self) -> CapturePolicy:
        return self._capture

    @contextmanager
    def _kitaru_overrides(self) -> Iterator[None]:
        with super().override(model=self._model, toolsets=self._toolsets, tools=[]):
            yield

    def _prepare_toolsets(self, toolsets: Sequence[AbstractToolset[AgentDepsT]]) -> list[AbstractToolset[AgentDepsT]]:
        def _visit(value: AbstractToolset[AgentDepsT]) -> AbstractToolset[AgentDepsT]:
            return kitaruify_toolset(
                value,
                capture=self._capture,
                tool_checkpoint_config=self._tool_checkpoint_config,
                tool_checkpoint_config_by_name=self._tool_checkpoint_config_by_name,
                mcp_checkpoint_config=self._mcp_checkpoint_config,
            )

        return [toolset.visit_and_replace(_visit) for toolset in toolsets]

    def _prepare_event_stream_handler(
        self,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
    ) -> EventStreamHandler[AgentDepsT] | None:
        effective_handler = event_stream_handler or self.event_stream_handler
        if effective_handler is None or effective_handler in self._wrapped_handlers:
            return effective_handler

        async def _tracked_handler(ctx: Any, stream: AsyncIterable[Any]) -> None:
            started_at = time.perf_counter()
            error: BaseException | None = None
            try:
                await effective_handler(ctx, stream)
            except BaseException as exc:
                error = exc
                raise
            finally:
                tracker = get_current_tracker()
                if tracker is not None:
                    tracker.record_stream_event(
                        duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                        error=error,
                    )

        self._wrapped_handlers.add(_tracked_handler)
        return _tracked_handler

    def _validate_model_override(self, model: models.Model | models.KnownModelName | str | None) -> None:
        if model is None:
            return
        raise UserError(
            'KitaruAgent does not support per-run `model=` overrides; create a new KitaruAgent '
            'wrapping a different agent instead.'
        )

    @contextmanager
    def override(
        self,
        *,
        name: str | _utils.Unset = _utils.UNSET,
        deps: AgentDepsT | _utils.Unset = _utils.UNSET,
        model: models.Model | models.KnownModelName | str | _utils.Unset = _utils.UNSET,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | _utils.Unset = _utils.UNSET,
        tools: Sequence[Any] | _utils.Unset = _utils.UNSET,
        instructions: AgentInstructions[AgentDepsT] | _utils.Unset = _utils.UNSET,
        model_settings: AgentModelSettings[AgentDepsT] | _utils.Unset = _utils.UNSET,
        spec: dict[str, Any] | None = None,
    ) -> Iterator[None]:
        unsupported: list[str] = []
        if _utils.is_set(name):
            unsupported.append('`name=`')
        if _utils.is_set(model):
            unsupported.append('`model=`')
        if _utils.is_set(toolsets):
            unsupported.append('`toolsets=`')
        if _utils.is_set(tools):
            unsupported.append('`tools=`')
        if unsupported:
            overrides = ', '.join(unsupported)
            raise UserError(
                f'KitaruAgent does not support contextual {overrides} overrides; create a new KitaruAgent instead.'
            )

        with super().override(
            deps=deps,
            instructions=instructions,
            model_settings=model_settings,
            spec=spec,
        ):
            yield

    def _should_track(self) -> bool:
        return is_inside_checkpoint() and not _TRACKING_ACTIVE.get()

    @contextmanager
    def _tracking_scope(self) -> Iterator[None]:
        if not self._should_track():
            yield
            return

        token = _TRACKING_ACTIVE.set(True)
        try:
            with tracker_scope(self._name):
                yield
        finally:
            _TRACKING_ACTIVE.reset(token)

    async def _auto_checkpoint_async(self, body: Callable[[], Awaitable[Any]]) -> Any:
        return await run_async_in_checkpoint(
            config=self._turn_checkpoint_config,
            step_name=self._name or 'agent',
            body=body,
        )

    def _auto_checkpoint_sync(self, body: Callable[[], Any]) -> Any:
        reject_isolated_runtime(self._turn_checkpoint_config)

        def _turn() -> Any:
            return body()

        _turn.__name__ = self._name or 'agent'
        step_output = kitaru.checkpoint(**self._turn_checkpoint_config)(_turn)()
        return materialize_step_output(step_output)

    def _invoke_in_auto_flow(self, body: Callable[[], Any]) -> Any:
        run_id = uuid.uuid4().hex
        slot = _AutoFlowSlot(body)
        with _AUTO_FLOW_LOCK:
            _AUTO_FLOW_BODIES[run_id] = slot
        try:
            handle = _kitaru_pydantic_ai_auto_flow.run(run_id)
            handle.wait()
        finally:
            with _AUTO_FLOW_LOCK:
                _AUTO_FLOW_BODIES.pop(run_id, None)

        if slot.error is not None:
            raise slot.error
        if not slot.has_result:
            raise RuntimeError(
                'Kitaru auto-flow finished without populating a result. This '
                'is a bug in kitaru.adapters.pydantic_ai.'
            )
        return slot.result

    def _effective_message_history(
        self,
        explicit: Sequence[_messages.ModelMessage] | None,
    ) -> Sequence[_messages.ModelMessage] | None:
        if explicit is not None or not self._persist_message_history:
            return explicit
        with self._message_history_lock:
            return list(self._last_messages) if self._last_messages else None

    def _remember_messages(self, result: Any) -> None:
        if not self._persist_message_history:
            return
        with self._message_history_lock:
            self._last_messages = list(result.all_messages())

    def _use_granular(self, force_turn_checkpoint: bool) -> bool:
        # Granular mode cannot apply to streaming turns: per-call checkpointing
        # a streamed ``request_stream`` would require draining and replaying
        # the stream inside a sync ZenML step. Fall back to the turn checkpoint
        # so model/tool events still land under a tracked boundary.
        return self._granular_checkpoints and not force_turn_checkpoint

    async def _run_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        force_turn_checkpoint: bool = False,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return await body()
            return await self._auto_checkpoint_async(body)

        # Outside any flow: auto-open one. FlowHandle.wait() is sync-blocking,
        # so we dispatch the flow to a worker thread (no running loop there,
        # so asyncio.run is safe for the agent coroutine).
        async def _await_body() -> Any:
            return await body()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._invoke_in_auto_flow(lambda: asyncio.run(_await_body())),
        )

    def _run_sync(
        self,
        body: Callable[[], Any],
        *,
        force_turn_checkpoint: bool = False,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return body()
            return self._auto_checkpoint_sync(body)
        return self._invoke_in_auto_flow(body)

    def _require_explicit_checkpoint(self, method_name: str) -> None:
        if is_inside_checkpoint():
            return
        raise UserError(
            f'`agent.{method_name}()` requires an explicit `@kitaru.checkpoint`. '
            'Kitaru cannot auto-open one around a streaming context manager; '
            'wrap the surrounding block in `@kitaru.flow` + `@kitaru.checkpoint`, '
            'or use `agent.run()` with an `event_stream_handler` instead.'
        )

    async def run(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._validate_model_override(model)
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)

        async def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope():
                return await super(KitaruAgent, self).run(
                    user_prompt,
                    output_type=output_type,
                    message_history=effective_history,
                    deferred_tool_results=deferred_tool_results,
                    model=None,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=prepared_toolsets,
                    builtin_tools=builtin_tools,
                    event_stream_handler=wrapped_handler,
                    spec=spec,
                )

        error: BaseException | None = None
        try:
            result = await self._run_async(_body, force_turn_checkpoint=wrapped_handler is not None)
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed('run', error)

    def run_sync(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> Any:
        self._validate_model_override(model)
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        effective_history = self._effective_message_history(message_history)

        def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope():
                return super(KitaruAgent, self).run_sync(
                    user_prompt,
                    output_type=output_type,
                    message_history=effective_history,
                    deferred_tool_results=deferred_tool_results,
                    model=None,
                    instructions=instructions,
                    deps=deps,
                    model_settings=model_settings,
                    usage_limits=usage_limits,
                    usage=usage,
                    metadata=metadata,
                    infer_name=infer_name,
                    toolsets=prepared_toolsets,
                    builtin_tools=builtin_tools,
                    event_stream_handler=wrapped_handler,
                    spec=spec,
                )

        error: BaseException | None = None
        try:
            result = self._run_sync(_body, force_turn_checkpoint=wrapped_handler is not None)
            self._remember_messages(result)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed('run_sync', error)

    @asynccontextmanager
    async def run_stream(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[Any]:
        self._validate_model_override(model)
        self._require_explicit_checkpoint('run_stream')
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)

        with self._kitaru_overrides(), self._tracking_scope():
            async with super(KitaruAgent, self).run_stream(
                user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                model=None,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=prepared_toolsets,
                builtin_tools=builtin_tools,
                event_stream_handler=wrapped_handler,
                spec=spec,
            ) as streamed_result:
                yield streamed_result

    @asynccontextmanager
    async def iter(
        self,
        user_prompt: str | Sequence[_messages.UserContent] | None = None,
        *,
        output_type: OutputSpec[Any] | None = None,
        message_history: Sequence[_messages.ModelMessage] | None = None,
        deferred_tool_results: DeferredToolResults | None = None,
        model: models.Model | models.KnownModelName | str | None = None,
        instructions: AgentInstructions[AgentDepsT] = None,
        deps: AgentDepsT | None = None,
        model_settings: AgentModelSettings[AgentDepsT] | None = None,
        usage_limits: _usage.UsageLimits | None = None,
        usage: _usage.RunUsage | None = None,
        metadata: AgentMetadata[AgentDepsT] | None = None,
        infer_name: bool = True,
        toolsets: Sequence[AbstractToolset[AgentDepsT]] | None = None,
        builtin_tools: Sequence[AgentBuiltinTool[AgentDepsT]] | None = None,
        spec: dict[str, Any] | None = None,
    ) -> AsyncIterator[AgentRun[AgentDepsT, Any]]:
        # iter() yields a run handle inside an `async with` body; auto-checkpointing it
        # would require a checkpoint primitive that itself is a context manager, which
        # kitaru.checkpoint isn't. Wrap iter() in an explicit @kitaru.checkpoint instead.
        self._validate_model_override(model)
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        with self._kitaru_overrides(), self._tracking_scope():
            async with self.wrapped.iter(
                user_prompt=user_prompt,
                output_type=output_type,
                message_history=message_history,
                deferred_tool_results=deferred_tool_results,
                model=None,
                instructions=instructions,
                deps=deps,
                model_settings=model_settings,
                usage_limits=usage_limits,
                usage=usage,
                metadata=metadata,
                infer_name=infer_name,
                toolsets=prepared_toolsets,
                builtin_tools=builtin_tools,
                spec=spec,
            ) as run:
                yield run
