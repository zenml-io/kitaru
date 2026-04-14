import asyncio
import threading
import time
import uuid
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Sequence,
)
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any

from pydantic_ai import _utils, models
from pydantic_ai import messages as _messages
from pydantic_ai import usage as _usage
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

import kitaru
from kitaru.analytics import AnalyticsEvent, track

from ._kitaru_internal import is_inside_checkpoint, is_inside_flow
from ._model import KitaruModel
from ._policy import CapturePolicy
from ._toolset import kitaruify_toolset
from ._tracking import get_current_tracker, tracker_scope
from ._utils import CheckpointConfig


def _track_run_completed(method: str, error: BaseException | None) -> None:
    payload: dict[str, Any] = {
        "method": method,
        "status": "failed" if error is not None else "completed",
    }
    if error is not None:
        payload["error_type"] = type(error).__name__
    track(AnalyticsEvent.PYDANTIC_AI_RUN_COMPLETED, payload)


_TRACKING_ACTIVE: ContextVar[bool] = ContextVar("kitaru_tracking_active", default=False)

# Auto-flow bodies live in a module-level registry keyed by uuid so the flow
# entrypoint (which kitaru may re-import during replay) can look them up by a
# JSON-serializable argument. The registry is in-process, so auto-flow only
# works on local stacks; remote stacks need an explicit `@kitaru.flow`.
_AUTO_FLOW_BODIES: dict[str, Callable[[], Any]] = {}
_AUTO_FLOW_LOCK = threading.Lock()


def _register_auto_flow_body(body: Callable[[], Any]) -> str:
    run_id = uuid.uuid4().hex
    with _AUTO_FLOW_LOCK:
        _AUTO_FLOW_BODIES[run_id] = body
    return run_id


def _pop_auto_flow_body(run_id: str) -> Callable[[], Any] | None:
    with _AUTO_FLOW_LOCK:
        return _AUTO_FLOW_BODIES.pop(run_id, None)


@kitaru.flow
def _kitaru_pydantic_ai_auto_flow(run_id: str) -> Any:
    body = _pop_auto_flow_body(run_id)
    if body is None:
        raise RuntimeError(
            "Kitaru auto-flow body was not found. Auto-flow is local-only; "
            "wrap your agent call in an explicit `@kitaru.flow` for remote stacks."
        )
    return body()


class KitaruAgent(WrapperAgent[AgentDepsT, OutputDataT]):
    def __init__(
        self,
        wrapped: AbstractAgent[AgentDepsT, OutputDataT],
        *,
        name: str | None = None,
        capture: CapturePolicy | None = None,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None = None,
        turn_checkpoint_config: CheckpointConfig | None = None,
    ) -> None:
        """Wrap an agent to make it durable under Kitaru.

        `run()` / `run_sync()` auto-open a `@kitaru.flow` when called outside one,
        and auto-open a `@kitaru.checkpoint` for each turn when called inside a
        flow but outside a checkpoint. Model requests, tool calls, MCP
        communication, and event-stream-handler invocations are recorded as
        Kitaru artifacts and summarized in a `RunSummary`.

        After wrapping, the original agent can still be used as normal outside
        of Kitaru.

        Args:
            wrapped: The agent to wrap. Its model must be a concrete `Model` at
                construction time; late model binding is not supported.
            name: Required stable agent name, used for artifact keys and auto-created
                flow/checkpoint names. If omitted, the wrapped agent must already
                define one.
            capture: Policy controlling what is persisted per run.
            event_stream_handler: Optional default handler used when no per-call
                handler is passed.
            turn_checkpoint_config: Kwargs applied to the synthetic
                ``@kitaru.checkpoint`` the adapter opens around each agent run
                when called at flow-scope without an enclosing checkpoint. Use
                ``{"runtime": "isolated"}`` to run each turn in its own container
                on remote stacks.
        """
        super().__init__(wrapped)

        if not isinstance(wrapped.model, Model):
            raise UserError(
                "KitaruAgent requires the wrapped agent to define a concrete "
                "model at construction time; pass `model=` to the Agent "
                "constructor."
            )

        self._name = name or wrapped.name
        if self._name is None:
            raise UserError(
                "KitaruAgent requires a stable `name`; pass `name=` to "
                "KitaruAgent or set the wrapped agent name."
            )
        self._capture = capture or CapturePolicy()
        self._event_stream_handler = event_stream_handler
        self._turn_checkpoint_config: dict[str, Any] = (
            {**turn_checkpoint_config} if turn_checkpoint_config else {}
        )
        self._model = KitaruModel(
            wrapped.model, capture=self._capture, agent_name=self._name
        )
        self._toolsets = self._prepare_toolsets(list(wrapped.toolsets))

        track(
            AnalyticsEvent.PYDANTIC_AI_WRAPPED,
            {"toolset_count": len(self._toolsets)},
        )

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, value: str | None) -> None:
        raise UserError(
            "The agent name cannot be changed after creation. "
            "Create a new KitaruAgent instead."
        )

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

    def _prepare_toolsets(
        self, toolsets: Sequence[AbstractToolset[AgentDepsT]]
    ) -> list[AbstractToolset[AgentDepsT]]:
        return [
            toolset.visit_and_replace(
                lambda value: kitaruify_toolset(value, capture=self._capture)
            )
            for toolset in toolsets
        ]

    def _prepare_event_stream_handler(
        self,
        event_stream_handler: EventStreamHandler[AgentDepsT] | None,
    ) -> EventStreamHandler[AgentDepsT] | None:
        effective_handler = event_stream_handler or self.event_stream_handler
        if effective_handler is None or getattr(
            effective_handler, "_kitaru_wrapped", False
        ):
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

        _tracked_handler._kitaru_wrapped = True  # type: ignore[attr-defined]
        return _tracked_handler

    def _validate_model_override(
        self, model: models.Model | models.KnownModelName | str | None
    ) -> None:
        if model is None:
            return
        raise UserError(
            "KitaruAgent does not support per-run `model=` overrides; "
            "create a new KitaruAgent wrapping a different agent instead."
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
            unsupported.append("`name=`")
        if _utils.is_set(model):
            unsupported.append("`model=`")
        if _utils.is_set(toolsets):
            unsupported.append("`toolsets=`")
        if _utils.is_set(tools):
            unsupported.append("`tools=`")
        if unsupported:
            overrides = ", ".join(unsupported)
            raise UserError(
                f"KitaruAgent does not support contextual {overrides} "
                "overrides; create a new KitaruAgent instead."
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
        async def _turn() -> Any:
            return await body()

        _turn.__name__ = self._name or "agent"
        return await kitaru.checkpoint(**self._turn_checkpoint_config)(_turn)()

    def _auto_checkpoint_sync(self, body: Callable[[], Any]) -> Any:
        def _turn() -> Any:
            return body()

        _turn.__name__ = self._name or "agent"
        return kitaru.checkpoint(**self._turn_checkpoint_config)(_turn)()

    def _invoke_in_auto_flow(self, body: Callable[[], Any]) -> Any:
        # Re-raise from the flow body so Kitaru records status=failed, but
        # preserve the original typed exception so callers can still match on
        # ApprovalRequired / CallDeferred / etc.
        result_holder: list[Any] = []
        error_holder: list[BaseException] = []

        def _do_run() -> None:
            try:
                result_holder.append(body())
            except BaseException as exc:
                error_holder.append(exc)
                raise

        run_id = _register_auto_flow_body(_do_run)
        try:
            handle = _kitaru_pydantic_ai_auto_flow.run(run_id)
            try:
                handle.wait()
            except BaseException:
                if error_holder:
                    raise error_holder[0] from None
                raise
        finally:
            _pop_auto_flow_body(run_id)

        return result_holder[0]

    async def _run_async(self, body: Callable[[], Awaitable[Any]]) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint():
                return await body()
            return await self._auto_checkpoint_async(body)

        # Outside any flow: spin one up. `FlowHandle.wait()` is sync and blocking,
        # so submission + wait happen in an executor so the caller's event loop
        # stays responsive. The executor thread has no running loop, so it can
        # `asyncio.run(...)` the agent coroutine when the flow body fires.
        async def _await_body() -> Any:
            return await body()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._invoke_in_auto_flow(lambda: asyncio.run(_await_body())),
        )

    def _run_sync(self, body: Callable[[], Any]) -> Any:
        if is_inside_flow():
            if is_inside_checkpoint():
                return body()
            return self._auto_checkpoint_sync(body)
        return self._invoke_in_auto_flow(body)

    def _require_explicit_checkpoint(self, method_name: str) -> None:
        if is_inside_checkpoint():
            return
        raise UserError(
            f"`agent.{method_name}()` requires an explicit `@kitaru.checkpoint`. "
            "Kitaru cannot auto-open one around a streaming context manager; "
            "wrap the surrounding block in `@kitaru.flow` + `@kitaru.checkpoint`, "
            "or use `agent.run()` with an `event_stream_handler` instead."
        )

    async def run(  # ty: ignore[invalid-method-override]
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
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)

        async def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope():
                return await super(KitaruAgent, self).run(
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
                )

        error: BaseException | None = None
        try:
            return await self._run_async(_body)
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run", error)

    def run_sync(  # ty: ignore[invalid-method-override]
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
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)

        def _body() -> Any:
            with self._kitaru_overrides(), self._tracking_scope():
                return super(KitaruAgent, self).run_sync(
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
                )

        error: BaseException | None = None
        try:
            return self._run_sync(_body)
        except BaseException as exc:
            error = exc
            raise
        finally:
            _track_run_completed("run_sync", error)

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
        self._require_explicit_checkpoint("run_stream")
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
        wrapped_handler = self._prepare_event_stream_handler(event_stream_handler)

        with self._kitaru_overrides(), self._tracking_scope():
            async with super().run_stream(
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
        # iter() yields a run handle inside an `async with` body;
        # auto-checkpointing it would require a checkpoint primitive that is
        # itself a context manager, which kitaru.checkpoint isn't. Wrap iter()
        # in an explicit @kitaru.checkpoint instead.
        self._validate_model_override(model)
        prepared_toolsets = (
            self._prepare_toolsets(toolsets) if toolsets is not None else None
        )
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
