import asyncio
import os
import sys
import tempfile
import threading
import time
import uuid
import warnings
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any

import kitaru
from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruRuntimeError, KitaruUsageError
from kitaru.flow import _is_multiple_terminal_steps_output_error

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
from ._logging import logger
from ._model import KitaruModel
from ._policy import CapturePolicy
from ._toolset import kitaruify_toolset
from ._tracking import get_current_tracker, tracker_scope
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverrides,
    checkpoint_input_value,
    run_async_in_checkpoint,
    run_sync_in_checkpoint,
    turn_cache_key,
    validate_checkpoint_config,
    validate_tool_checkpoint_overrides,
)

_TRACKING_ACTIVE: ContextVar[bool] = ContextVar('kitaru_tracking_active', default=False)
_INTERNAL_ITER_ALLOWED: ContextVar[bool] = ContextVar('kitaru_internal_iter_allowed', default=False)

# Auto-flow bodies keyed by uuid. The @kitaru.flow entrypoint must be module-
# level for ZenML dynamic-pipeline source resolution, so it can't close over
# its body — the registry bridges the gap. In-process only; remote stacks
# require an explicit @kitaru.flow.
_AUTO_FLOW_BODIES: dict[str, '_AutoFlowSlot'] = {}
_AUTO_FLOW_LOCK = threading.Lock()

if f'src.{__name__}' not in sys.modules:
    sys.modules[f'src.{__name__}'] = sys.modules[__name__]


class _AutoFlowSlot:
    __slots__ = ('body', 'error', 'has_result', 'result')

    def __init__(self, body: Callable[[], Any]) -> None:
        self.body = body
        self.result: Any = None
        self.error: BaseException | None = None
        self.has_result = False


def _load_auto_flow_body(serialized_body_path: str) -> Callable[[], Any]:
    try:
        import cloudpickle
    except ImportError as error:  # pragma: no cover - depends on env packaging
        raise KitaruUsageError(
            'Auto-flow requires `cloudpickle` in the local runtime environment.'
        ) from error
    with open(serialized_body_path, 'rb') as stream:
        return cloudpickle.load(stream)


def _try_serialize_auto_flow_body(body: Callable[[], Any]) -> str | None:
    """Best-effort cloudpickle of ``body`` for remote-stack workers; returns ``None`` on failure."""
    try:
        import cloudpickle
    except ImportError:
        return None
    path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix='.kitaru-autoflow'
        ) as stream:
            path = stream.name
            cloudpickle.dump(body, stream)
        return path
    except Exception:
        logger.debug(
            'Auto-flow body could not be cloudpickled; remote stacks will need '
            'an explicit `@kitaru.flow` wrapper.',
            exc_info=True,
        )
        if path is not None:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        return None


@kitaru.flow
def _kitaru_pydantic_ai_auto_flow(run_id: str, serialized_body_path: str | None = None) -> Any:
    with _AUTO_FLOW_LOCK:
        slot = _AUTO_FLOW_BODIES.get(run_id)
    if slot is None and serialized_body_path is not None:
        slot = _AutoFlowSlot(_load_auto_flow_body(serialized_body_path))
    if slot is None:
        raise KitaruUsageError(
            f'Kitaru auto-flow body {run_id!r} not found in registry. Auto-flow '
            'is local-only; wrap your agent call in an explicit `@kitaru.flow` '
            'for remote stacks.'
        )
    try:
        slot.result = slot.body()
        slot.has_result = True
        return slot.result
    except Exception as exc:
        slot.error = exc
        raise


def _is_wrapped_handler(handler: Any) -> bool:
    if getattr(handler, '_kitaru_wrapped', False):
        return True
    inner = getattr(handler, 'func', None) or getattr(handler, '__func__', None)
    return bool(inner is not None and getattr(inner, '_kitaru_wrapped', False))


def _track_run_completed(method: str, error: BaseException | None) -> None:
    if error is None:
        status = 'completed'
    elif isinstance(error, asyncio.CancelledError):
        status = 'cancelled'
    else:
        status = 'failed'
    payload: dict[str, Any] = {'method': method, 'status': status}
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
        granular_checkpoints: bool = True,
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
        mcp_checkpoint_config: CheckpointConfig | None = None,
        persist_message_history: bool = False,
    ) -> None:
        """Wrap an agent so its runs become durable under Kitaru.

        Outside a flow, ``run()`` / ``run_sync()`` auto-open a ``@kitaru.flow``.

        **Granular mode (default):** no turn checkpoint; each top-level
        model/tool/MCP call per turn opens its own checkpoint, giving per-call
        replay/retry boundaries and a less crowded artifact view. Sub-calls
        nested inside an already-open granular checkpoint (for example tool
        calls inside the turn's model request) fall back to inline tracking —
        they do *not* open a second checkpoint. The ``model_/tool_/mcp_checkpoint_config``
        kwargs (and the per-tool ``tool_checkpoint_config_by_name`` map, where
        ``False`` opts a tool out entirely) are honored in this mode. Cross-run
        cache behavior for adapter-created granular checkpoints is still being
        tightened.

        **Turn mode** (``granular_checkpoints=False``): each run opens one
        ``@kitaru.checkpoint`` named after the agent; model/tool/MCP calls are
        recorded as child events under that checkpoint.

        When ``persist_message_history=True``, the adapter remembers the final
        ``result.all_messages()`` of each run on the instance and auto-injects
        it as ``message_history`` on the next call if the caller doesn't supply
        one — one instance then represents one conversation. Pass an explicit
        ``message_history=`` to override for a single call.

        Limits of ``persist_message_history``:

        - **In-memory only**: history lives on the Python instance. Adapter-owned
          cached run results can refresh it, but restarts, new processes, and
          replay paths that skip this adapter call start with no instance history.
        - **Serial use**: concurrent ``run`` / ``run_sync`` calls on the same
          instance race on the stored history. Gate concurrency externally or
          use one instance per concurrent conversation.
        - **Unbounded**: the list grows with each successful run; apply your
          own truncation or summarization for long-lived conversations.
        - **Success-only**: history is only updated after a successful run,
          so a partial failure leaves the last-successful history in place.
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
        self._turn_checkpoint_config: CheckpointConfig = (
            validate_checkpoint_config(turn_checkpoint_config, context='turn_checkpoint_config')
            or {}
        )
        self._granular_checkpoints = granular_checkpoints
        self._warned_streaming_fallback = False
        self._warned_checkpoint_history_limit = False
        has_granular_configs = any(
            value is not None
            for value in (
                model_checkpoint_config,
                tool_checkpoint_config,
                tool_checkpoint_config_by_name,
                mcp_checkpoint_config,
            )
        )
        if has_granular_configs and not granular_checkpoints:
            raise KitaruUsageError(
                'Per-call checkpoint configs require `granular_checkpoints=True`.'
            )
        if granular_checkpoints:
            self._model_checkpoint_config = (
                validate_checkpoint_config(
                    model_checkpoint_config or {},
                    context='model_checkpoint_config',
                )
                or {}
            )
            self._tool_checkpoint_config = (
                validate_checkpoint_config(
                    tool_checkpoint_config or {},
                    context='tool_checkpoint_config',
                )
                or {}
            )
            self._tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = (
                validate_tool_checkpoint_overrides(
                    tool_checkpoint_config_by_name,
                    context='tool_checkpoint_config_by_name',
                )
            )
            self._mcp_checkpoint_config = (
                validate_checkpoint_config(
                    mcp_checkpoint_config or {},
                    context='mcp_checkpoint_config',
                )
                or {}
            )
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
        if effective_handler is None:
            return None
        if _is_wrapped_handler(effective_handler):
            return effective_handler

        async def _tracked_handler(ctx: Any, stream: AsyncIterable[Any]) -> None:
            started_at = time.perf_counter()
            error: Exception | None = None
            try:
                await effective_handler(ctx, stream)
            except Exception as exc:
                error = exc
                raise
            finally:
                tracker = get_current_tracker()
                if tracker is not None:
                    tracker.record_stream_event(
                        duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                        error=error,
                    )

        _tracked_handler._kitaru_wrapped = True  # type: ignore[attr-defined]
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
        if _TRACKING_ACTIVE.get():
            return False
        if is_inside_checkpoint():
            return True
        return self._granular_checkpoints and is_inside_flow()

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

    @contextmanager
    def _allow_internal_iter(self) -> Iterator[None]:
        token = _INTERNAL_ITER_ALLOWED.set(True)
        try:
            yield
        finally:
            _INTERNAL_ITER_ALLOWED.reset(token)

    @staticmethod
    def _turn_checkpoint_inputs(
        *,
        user_prompt: str | Sequence[_messages.UserContent] | None,
        message_history: Sequence[_messages.ModelMessage] | None,
    ) -> dict[str, Any]:
        inputs: dict[str, Any] = {}
        if user_prompt is not None:
            inputs['user_prompt'] = checkpoint_input_value(user_prompt)
        if message_history is not None:
            inputs['message_history'] = checkpoint_input_value(list(message_history))
        return inputs

    async def _auto_checkpoint_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
    ) -> Any:
        return await run_async_in_checkpoint(
            config=self._turn_checkpoint_config,
            step_name=self._name or 'agent',
            body=body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _auto_checkpoint_sync(
        self,
        body: Callable[[], Any],
        *,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
    ) -> Any:
        return run_sync_in_checkpoint(
            config=self._turn_checkpoint_config,
            step_name=self._name or 'agent',
            body=body,
            cache_key=cache_key,
            checkpoint_inputs=checkpoint_inputs,
        )

    def _invoke_in_auto_flow(self, body: Callable[[], Any]) -> Any:
        run_id = uuid.uuid4().hex
        slot = _AutoFlowSlot(body)
        serialized_body_path: str | None = None
        flow_result: Any = None
        with _AUTO_FLOW_LOCK:
            _AUTO_FLOW_BODIES[run_id] = slot
        try:
            serialized_body_path = _try_serialize_auto_flow_body(body)
            handle = _kitaru_pydantic_ai_auto_flow.run(run_id, serialized_body_path)
            try:
                flow_result = handle.wait()
            except KitaruRuntimeError as error:
                # Granular auto-flows can finish with multiple terminal adapter
                # checkpoints. The auto-flow body ran in this process, so prefer
                # the in-memory result that the module-level flow stored for us.
                if slot.has_result and _is_multiple_terminal_steps_output_error(error):
                    flow_result = slot.result
                else:
                    raise
        finally:
            with _AUTO_FLOW_LOCK:
                _AUTO_FLOW_BODIES.pop(run_id, None)
            if serialized_body_path is not None:
                try:
                    os.remove(serialized_body_path)
                except FileNotFoundError:
                    pass

        if slot.error is not None:
            raise slot.error
        if slot.has_result:
            return slot.result
        return flow_result

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
        all_messages = getattr(result, 'all_messages', None)
        if not callable(all_messages):
            raise KitaruRuntimeError(
                'KitaruAgent could not refresh persisted message history because '
                'the run result does not expose all_messages().'
            )
        with self._message_history_lock:
            self._last_messages = list(all_messages())

    def _warn_if_persist_history_inside_checkpoint(self) -> None:
        if (
            not self._persist_message_history
            or self._warned_checkpoint_history_limit
            or not is_inside_checkpoint()
        ):
            return
        self._warned_checkpoint_history_limit = True
        message = (
            '`persist_message_history=True` is only in-memory. This agent call '
            'is running inside an existing `@kitaru.checkpoint`; if that '
            'checkpoint is served from cache during replay/resume, the adapter '
            'will not execute and cannot restore `_last_messages`. For '
            'resume-safe conversations, call the agent at flow scope in '
            'granular mode, or pass `message_history=` explicitly from durable '
            'storage such as `kitaru.memory`.'
        )
        warnings.warn(message, UserWarning, stacklevel=3)

    def _use_granular(self, force_turn_checkpoint: bool) -> bool:
        # Granular mode cannot apply to streaming turns: per-call checkpointing
        # a streamed ``request_stream`` would require draining and replaying
        # the stream inside a sync ZenML step. Fall back to the turn checkpoint
        # so model/tool events still land under a tracked boundary.
        return self._granular_checkpoints and not force_turn_checkpoint

    def _log_streaming_fallback(self) -> None:
        if self._warned_streaming_fallback:
            return
        dropped_configs = [
            name
            for name, config in (
                ('model_checkpoint_config', self._model_checkpoint_config),
                ('tool_checkpoint_config', self._tool_checkpoint_config),
                ('tool_checkpoint_config_by_name', self._tool_checkpoint_config_by_name),
                ('mcp_checkpoint_config', self._mcp_checkpoint_config),
            )
            if config
        ]
        if not dropped_configs:
            return
        self._warned_streaming_fallback = True
        logger.warning(
            'Falling back to turn checkpointing for a streamed PydanticAI run; '
            'granular checkpoint configs are ignored for this call.',
            extra={'dropped_configs': dropped_configs, 'agent_name': self._name},
        )
        if is_inside_flow() and not is_inside_checkpoint():
            kitaru.log(
                adapter='pydantic_ai',
                streaming_fallback=True,
                dropped_checkpoint_configs=dropped_configs,
            )

    @staticmethod
    def _ensure_run_sync_safe() -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise KitaruUsageError(
            '`KitaruAgent.run_sync()` cannot be called from a running event loop. '
            'Use `await agent.run(...)` instead.'
        )

    async def _run_async(
        self,
        body: Callable[[], Awaitable[Any]],
        *,
        force_turn_checkpoint: bool = False,
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return await body()
            return await self._auto_checkpoint_async(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )

        # Outside any flow: auto-open one. FlowHandle.wait() is sync-blocking,
        # so we dispatch the flow to a worker thread (no running loop there,
        # so asyncio.run is safe for the agent coroutine).
        async def _await_body() -> Any:
            return await self._run_async(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )

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
        cache_key: str | None = None,
        checkpoint_inputs: Mapping[str, Any] | None = None,
    ) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint() or self._use_granular(force_turn_checkpoint):
                return body()
            return self._auto_checkpoint_sync(
                body,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )
        return self._invoke_in_auto_flow(
            lambda: self._run_sync(
                body,
                force_turn_checkpoint=force_turn_checkpoint,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )
        )

    def _require_explicit_checkpoint(self, method_name: str) -> None:
        if _INTERNAL_ITER_ALLOWED.get():
            return
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
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        if wrapped_handler is not None and self._granular_checkpoints:
            self._log_streaming_fallback()
        effective_history = self._effective_message_history(message_history)

        async def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope(), self._allow_internal_iter():
                result = await super(KitaruAgent, self).run(
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
            return result

        cache_key = turn_cache_key(
            agent_name=self._name,
            user_prompt=user_prompt,
            message_history=effective_history if effective_history is not None else message_history,
            deferred_tool_results=deferred_tool_results,
            instructions=instructions,
            model_settings=model_settings,
        )
        checkpoint_inputs = self._turn_checkpoint_inputs(
            user_prompt=user_prompt,
            message_history=effective_history,
        )

        error: BaseException | None = None
        try:
            result = await self._run_async(
                _body,
                force_turn_checkpoint=wrapped_handler is not None,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )
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
        self._ensure_run_sync_safe()
        self._validate_model_override(model)
        self._warn_if_persist_history_inside_checkpoint()
        prepared_toolsets = self._prepare_toolsets(toolsets) if toolsets is not None else None
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)
        if wrapped_handler is not None and self._granular_checkpoints:
            self._log_streaming_fallback()
        effective_history = self._effective_message_history(message_history)

        def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope(), self._allow_internal_iter():
                result = super(KitaruAgent, self).run_sync(
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
            return result

        cache_key = turn_cache_key(
            agent_name=self._name,
            user_prompt=user_prompt,
            message_history=effective_history if effective_history is not None else message_history,
            deferred_tool_results=deferred_tool_results,
            instructions=instructions,
            model_settings=model_settings,
        )
        checkpoint_inputs = self._turn_checkpoint_inputs(
            user_prompt=user_prompt,
            message_history=effective_history,
        )

        error: BaseException | None = None
        try:
            result = self._run_sync(
                _body,
                force_turn_checkpoint=wrapped_handler is not None,
                cache_key=cache_key,
                checkpoint_inputs=checkpoint_inputs,
            )
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
        self._require_explicit_checkpoint('iter')
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
